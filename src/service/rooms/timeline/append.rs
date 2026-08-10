use std::{
	collections::{BTreeMap, HashSet},
	sync::Arc,
};

use conduwuit::trace;
use conduwuit_core::{
	Result, err, error, implement, info,
	matrix::{
		event::Event,
		pdu::{PduCount, PduEvent, PduId, RawPduId},
	},
	utils::{self, ReadyExt},
	warn,
};
use futures::StreamExt;
use ruma::{
	CanonicalJsonObject, OwnedEventId, RoomVersionId, UserId,
	events::{
		GlobalAccountDataEventType, StateEventType, TimelineEventType,
		push_rules::PushRulesEvent,
		room::{
			encrypted::Relation, power_levels::RoomPowerLevelsEventContent,
			redaction::RoomRedactionEventContent, tombstone::RoomTombstoneEventContent,
		},
	},
	push::{Action, Ruleset, Tweak},
};

use super::{ExtractBody, ExtractRelatesTo, ExtractRelatesToEventId, RoomMutexGuard};
use crate::{
	appservice::NamespaceRegex,
	rooms::state_compressor::{CompressedState, HashSetCompressStateEvent},
};

/// State/soft-fail options for [`append_pdu`], grouped to keep the argument
/// count within clippy's threshold.
pub struct AppendOptions {
	pub resolved_state: Option<HashSetCompressStateEvent>,
	pub soft_fail: bool,
}

/// Inputs shared by push-rule evaluation in live append and receipt-based
/// recomputation.
pub(super) struct PduPushEval<'a> {
	pub pdu: &'a PduEvent,
	pub serialized: &'a ruma::serde::Raw<ruma::events::AnySyncTimelineEvent>,
	pub room_id: &'a ruma::RoomId,
	pub rules_for_user: &'a Ruleset,
	pub power_levels: &'a RoomPowerLevelsEventContent,
	pub soft_fail: bool,
}

/// Append the incoming event setting the state snapshot to the state from
/// the server that sent the event.
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
#[allow(clippy::too_many_arguments)]
pub async fn append_incoming_pdu<'a, Leaves>(
	&'a self,
	pdu: &'a PduEvent,
	pdu_json: CanonicalJsonObject,
	new_room_leaves: Leaves,
	state_ids_compressed: Arc<CompressedState>,
	resolved_state: Option<HashSetCompressStateEvent>,
	soft_fail: bool,
	state_lock: &'a RoomMutexGuard,
	room_id: &'a ruma::RoomId,
) -> Result<Option<RawPduId>>
where
	Leaves: Iterator<Item = OwnedEventId> + Send + 'a,
{
	// We append to state before appending the pdu, so we don't have a moment in
	// time with the pdu without it's state. This is okay because append_pdu can't
	// fail.
	self.services
		.state
		.set_event_state(&pdu.event_id, room_id, state_ids_compressed)
		.await?;

	// Soft-failed events pass auth against the state at the event but fail
	// against the current room state. Per spec §11.33.2.6 they SHOULD NOT
	// appear in /sync or /messages. Store the state association (above) for
	// DAG integrity, but do NOT append to the timeline sequence.
	if soft_fail {
		self.clear_outlier_flag(pdu.event_id());
		self.services
			.pdu_metadata
			.unmark_event_rejected(pdu.event_id());

		conduwuit::debug_warn!(
			event_id = %pdu.event_id,
			"Event soft-failed; stored state but omitted from timeline"
		);
		return Ok(None);
	}

	let pdu_id = self
		.append_pdu(
			pdu,
			pdu_json,
			new_room_leaves,
			AppendOptions { resolved_state, soft_fail },
			state_lock,
			room_id,
		)
		.await?;

	// Clean up the outlier table entry now that this event is in the timeline.
	// Without this, events upgraded via the federation path remain in both the
	// timeline and outlier tables indefinitely (the "stuck" state bug).
	self.clear_outlier_flag(pdu.event_id());

	// Clear any stale rejection flags now that the event is accepted into
	// the timeline. Without this, events that were rejected during initial
	// backfill (e.g., due to temporarily missing auth events) remain
	// permanently poisoned — cascading auth failures through state
	// resolution. Soft-fail flags are intentional and must persist.
	self.services
		.pdu_metadata
		.unmark_event_rejected(pdu.event_id());

	// Process admin commands for federation events
	if *pdu.kind() == TimelineEventType::RoomMessage {
		let content: ExtractBody = pdu.get_content()?;
		if let Some(body) = content.body {
			if let Some(source) = self
				.services
				.admin
				.is_admin_command(pdu, &body, false)
				.await
			{
				self.services.admin.command_with_sender(
					body,
					Some(pdu.event_id().into()),
					source,
					pdu.sender.clone().into(),
				)?;
			}
		}
	}

	Ok(Some(pdu_id))
}

/// Creates a new persisted data unit and adds it to a room.
///
/// By this point the incoming event should be fully authenticated, no auth
/// happens in `append_pdu`.
///
/// Returns pdu id
#[implement(super::Service)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn append_pdu<'a, Leaves>(
	&'a self,
	pdu: &'a PduEvent,
	mut pdu_json: CanonicalJsonObject,
	leaves: Leaves,
	options: AppendOptions,
	state_lock: &'a RoomMutexGuard,
	room_id: &'a ruma::RoomId,
) -> Result<RawPduId>
where
	Leaves: Iterator<Item = OwnedEventId> + Send + 'a,
{
	let AppendOptions { resolved_state, soft_fail } = options;
	// Coalesce timeline writes; flush before pub'ing receipt changes / waking sync.
	let cork = self.db.db.cork_and_flush();

	let shortroomid = self
		.services
		.short
		.get_shortroomid(room_id)
		.await
		.map_err(|_| err!(Database("Room does not exist")))?;

	// Make unsigned fields correct. This is not properly documented in the spec,
	// but state events need to have previous content in the unsigned field, so
	// clients can easily interpret things like membership changes
	if let Some(state_key) = pdu.state_key() {
		if let Ok(shortstatehash) = self
			.services
			.state_accessor
			.pdu_shortstatehash(pdu.event_id())
			.await
		{
			match self
				.services
				.state_accessor
				.state_get(shortstatehash, &pdu.kind().to_string().into(), state_key)
				.await
			{
				| Ok(prev_state) => {
					let prev_content_value = prev_state.get_content_as_value();
					let curr_content_value = pdu.get_content_as_value();

					// Log no-op membership transitions (identical content)
					if pdu.kind() == &TimelineEventType::RoomMember
						&& prev_content_value == curr_content_value
					{
						info!(
							event_id = %pdu.event_id(),
							sender = %pdu.sender(),
							state_key = %state_key,
							prev_event_id = %prev_state.event_id(),
							room_id = %room_id,
							"no-op membership event: content identical to prev_content \
							 (possible stale state lookup during DAG fork)",
						);
					}

					if let Err(e) = crate::rooms::timeline::update_unsigned_prev_content(
						&mut pdu_json,
						&prev_state,
					) {
						error!(%room_id, event_id = %pdu.event_id(), "Failed to update unsigned.prev_content: {e}");
					}
				},
				| Err(e) => {
					// It's normal for prev_state to be missing, especially for new members
					// joining a room. No need to log an error.
					conduwuit::debug!(
						event_id = %pdu.event_id(),
						%shortstatehash,
						%state_key,
						"state_get failed for prev_content (expected for new members): {e}",
					);
				},
			}
		}
	}

	// We must keep track of all events that have been referenced.
	// EXCEPT for soft-failed events, which are invisible to DAG tips.
	if !soft_fail {
		self.services
			.pdu_metadata
			.mark_as_referenced(room_id, pdu.prev_events().map(AsRef::as_ref));
	}

	trace!("setting forward extremities");
	self.services
		.state
		.set_forward_extremities(room_id, leaves, Some(pdu.event_id()), state_lock)
		.await;

	let insert_lock = self.mutex_insert.lock(room_id).await;

	let existing_pdu = if self.non_outlier_pdu_exists(pdu.event_id()).await {
		warn!(
			target: "backfill_debug",
			event_id = %pdu.event_id(),
			%room_id,
			"append_pdu: event already exists in timeline under the insert lock -- \
			 skipping redundant DB insert but continuing with state/push processing"
		);
		if let (Ok(pdu_id), Ok(pdu_count)) =
			(self.get_pdu_id(pdu.event_id()).await, self.get_pdu_count(pdu.event_id()).await)
		{
			Some((pdu_id, pdu_count))
		} else {
			None
		}
	} else {
		None
	};

	self.services
		.user
		.reset_notification_counts(pdu.sender(), room_id);

	let (pdu_id, pdu_count, private_read_count) = if let Some((existing_id, existing_count)) =
		existing_pdu
	{
		(existing_id, existing_count, match existing_count {
			| PduCount::Normal(count) => Some(count),
			| PduCount::Backfilled(_) => None,
		})
	} else {
		let count = self.services.globals.next_count()?;
		let pdu_count = PduCount::Normal(count);
		let pdu_id: RawPduId = PduId { shortroomid, shorteventid: pdu_count }.into();

		// TEMPORARY diagnostic only
		info!(target: "backfill_debug", event_id = %pdu.event_id(), ?pdu_count, "append_pdu: about to insert");

		// Write first, then publish the count
		self.db.append_pdu(&pdu_id, pdu, &pdu_json, pdu_count).await;
		info!(target: "backfill_debug", event_id = %pdu.event_id(), ?pdu_count, "append_pdu: insert complete");

		self.last_timeline_count_cache
			.insert(room_id.to_owned(), pdu_count);

		(pdu_id, pdu_count, Some(count))
	};
	drop(cork);

	let resolved_state_applied = resolved_state.is_some();
	if let Some(HashSetCompressStateEvent { shortstatehash, added, removed }) = resolved_state {
		// Still holding `insert_lock`: force_state's outlier-demotion step must not
		// try to re-acquire it (self-deadlock), so pass it through as proof.
		Box::pin(self.services.state.force_state_insert_locked(
			room_id,
			shortstatehash,
			added,
			removed,
			state_lock,
			&insert_lock,
		))
		.await?;
	}

	// Flattened Auth Chain Cache:
	// Pre-calculate the auth chain closure for this PDU by doing a single
	// get_auth_chain lookup on its auth_events. Because the auth events
	// were already appended, their closures are cached, making this an
	// O(1) DB hit per auth event rather than a 30-second DAG crawl later.
	let short_event_id = self
		.services
		.short
		.get_or_create_shorteventid(pdu.event_id())
		.await;
	if let Ok(full_auth_chain) = self
		.services
		.auth_chain
		.get_auth_chain(room_id, pdu.auth_events().map(AsRef::as_ref))
		.await
	{
		// The auth chain closure for this PDU must include both the
		// transitive ancestors returned by get_auth_chain AND the PDU's
		// own direct auth_events (which get_auth_chain uses as *starting*
		// points but does not include in its output).
		let mut bm = roaring::RoaringTreemap::new();
		for id in &full_auth_chain {
			bm.insert(*id);
		}
		for auth_event_id in pdu.auth_events() {
			let short = self
				.services
				.short
				.get_or_create_shorteventid(auth_event_id)
				.await;
			bm.insert(short);
		}

		self.services
			.auth_chain
			.cache_auth_chain_bitmap(vec![short_event_id], &bm);
	}

	let receipt_content = BTreeMap::from_iter([(
		pdu.event_id().to_owned(),
		BTreeMap::from_iter([(
			ruma::events::receipt::ReceiptType::ReadPrivate,
			BTreeMap::from_iter([(pdu.sender().to_owned(), ruma::events::receipt::Receipt {
				ts: Some(ruma::MilliSecondsSinceUnixEpoch::now()),
				thread: ruma::events::receipt::ReceiptThread::Unthreaded,
			})]),
		)]),
	)]);
	let receipt_event = ruma::events::receipt::ReceiptEvent {
		content: ruma::events::receipt::ReceiptEventContent(receipt_content),
		room_id: room_id.to_owned(),
	};

	// Wake sync only after the event is visible in the room timeline.
	if let Some(count) = private_read_count {
		self.services.read_receipt.private_read_set(
			room_id,
			pdu.sender(),
			count,
			&receipt_event,
		)?;
	}

	drop(insert_lock);

	// See if the event matches any known pushers via power level
	let power_levels: RoomPowerLevelsEventContent = self
		.services
		.state_accessor
		.room_state_get_content(room_id, &StateEventType::RoomPowerLevels, "")
		.await
		.unwrap_or_default();

	let mut push_target: HashSet<_> = self
			.services
			.state_cache
			.active_local_users_in_room(room_id)
			.map(ToOwned::to_owned)
			// Don't notify the sender of their own events, and dont send from ignored users
			.ready_filter(|user| *user != pdu.sender())
			.filter_map(|recipient_user| async move { (!self.services.users.user_is_ignored(pdu.sender(), &recipient_user).await).then_some(recipient_user) })
			.collect()
			.await;

	let mut notifies = Vec::with_capacity(push_target.len().saturating_add(1));
	let mut highlights = Vec::with_capacity(push_target.len().saturating_add(1));
	let thread_root = self.services.threads.get_thread_id(pdu).await;

	if *pdu.kind() == TimelineEventType::RoomMember {
		if let Some(state_key) = pdu.state_key() {
			let target_user_id = UserId::parse(state_key)?;

			if self.services.users.is_active_local(target_user_id).await {
				push_target.insert(target_user_id.to_owned());
			}
		}
	}

	let serialized = pdu.to_format();
	for user in &push_target {
		let rules_for_user = self
			.services
			.account_data
			.get_global(user, GlobalAccountDataEventType::PushRules)
			.await
			.map_or_else(
				|_| Ruleset::server_default(user),
				|ev: PushRulesEvent| ev.content.global,
			);

		let eval = PduPushEval {
			pdu,
			serialized: &serialized,
			room_id,
			rules_for_user: &rules_for_user,
			power_levels: &power_levels,
			soft_fail,
		};
		let (notify, highlight) = self.evaluate_pdu_for_user(user, &eval).await;

		if !(notify || highlight) {
			continue;
		}

		if notify {
			notifies.push(user.clone());
		}

		if highlight {
			highlights.push(user.clone());
		}

		self.services
			.pusher
			.get_pushkeys(user)
			.ready_for_each(|push_key| {
				if let Err(e) =
					self.services
						.sending
						.send_pdu_push(&pdu_id, user, push_key.to_owned())
				{
					warn!("Failed to queue push notification: {e}");
				}
			})
			.await;
	}

	self.db
		.increment_notification_counts(room_id, notifies, highlights, thread_root.as_deref());

	if *pdu.kind() == TimelineEventType::RoomTombstone {
		if let Ok(tombstone) = pdu.get_content::<RoomTombstoneEventContent>() {
			let replacement_room = tombstone.replacement_room.as_ref();
			super::copy_room_push_rules_for_upgrade(self, room_id, replacement_room).await?;
		}
	}

	match *pdu.kind() {
		| TimelineEventType::RoomRedaction => {
			use RoomVersionId::*;

			let room_version_id = self.services.state.get_room_version(room_id).await?;
			match room_version_id {
				| V1 | V2 | V3 | V4 | V5 | V6 | V7 | V8 | V9 | V10 => {
					if let Some(redact_id) = pdu.redacts() {
						if self
							.services
							.state_accessor
							.user_can_redact(redact_id, pdu.sender(), room_id, false)
							.await?
						{
							self.redact_pdu(redact_id, pdu, shortroomid).await?;
						}
					}
				},
				| _ => {
					let content: RoomRedactionEventContent = pdu.get_content()?;
					if let Some(redact_id) = &content.redacts {
						if self
							.services
							.state_accessor
							.user_can_redact(redact_id, pdu.sender(), room_id, false)
							.await?
						{
							self.redact_pdu(redact_id, pdu, shortroomid).await?;
						}
					}
				},
			}
		},
		| TimelineEventType::SpaceChild =>
			if let Some(_state_key) = pdu.state_key() {
				self.services
					.spaces
					.roomid_spacehierarchy_cache
					.lock()
					.await
					.remove(room_id);
			},
		| TimelineEventType::RoomMember if !resolved_state_applied => {
			if let Some(state_key) = pdu.state_key() {
				// if the state_key fails
				let target_user_id =
					UserId::parse(state_key).expect("This state_key was previously validated");

				// Update our membership info, we do this here incase a user is invited or
				// knocked and immediately leaves we need the DB to record the invite or
				// knock event for auth
				self.services
					.state_cache
					.update_membership(room_id, target_user_id, pdu, true)
					.await?;

				if let Ok(content) =
					pdu.get_content::<ruma::events::room::member::RoomMemberEventContent>()
				{
					if content.membership == ruma::events::room::member::MembershipState::Join
						&& self.services.globals.user_is_local(target_user_id)
					{
						self.services
							.users
							.mark_device_key_update(target_user_id)
							.await;
					}
				}

				// Invalidate hierarchy cache: membership changes can affect
				// restricted room accessibility (the `allow` list checks
				// whether the requesting user/server is joined to this room).
				self.services
					.spaces
					.roomid_spacehierarchy_cache
					.lock()
					.await
					.remove(room_id);
			}
		},
		| TimelineEventType::RoomMessage => {
			self.index_pdu_search(shortroomid, &pdu_id, pdu);
		},
		| _ => {},
	}

	// CONCERN: If we receive events with a relation out-of-order, we never write
	// their relation / thread. We need some kind of way to trigger when we receive
	// this event, and potentially a way to rebuild the table entirely.

	if let Ok(content) = pdu.get_content::<ExtractRelatesToEventId>() {
		if let Ok(related_pducount) = self.get_pdu_count(&content.relates_to.event_id).await {
			self.services
				.pdu_metadata
				.add_relation(pdu_count, related_pducount);
		}
	}

	if let Ok(content) = pdu.get_content::<ExtractRelatesTo>() {
		match content.relates_to {
			| Relation::Reply { in_reply_to } => {
				// We need to do it again here, because replies don't have
				// event_id as a top level field
				if let Ok(related_pducount) = self.get_pdu_count(&in_reply_to.event_id).await {
					self.services
						.pdu_metadata
						.add_relation(pdu_count, related_pducount);
				}
			},
			| Relation::Thread(thread) => {
				if let Err(e) = self
					.services
					.threads
					.add_to_thread(&thread.event_id, pdu)
					.await
				{
					// Thread root may not be in the timeline yet (e.g. during
					// rescue-room reorder or when the root is itself an outlier).
					// Store the PDU anyway; thread metadata will be missing until
					// the root is also promoted to the timeline.
					info!(
						?e,
						event_id = %pdu.event_id,
						"failed to add event to thread (root not yet in timeline)"
					);
				}
			},
			| _ => {}, // TODO: Aggregate other types
		}
	}

	if let Ok(content) = pdu.get_content::<super::ExtractMsc2836Relationship>() {
		if let Some(relationship) = content.relationship {
			self.services.pdu_metadata.msc2836_add_child(
				&relationship.event_id,
				pdu.event_id(),
				&relationship.rel_type,
			);
		}
	}

	for appservice in self.services.appservice.read().await.values() {
		if self
			.services
			.state_cache
			.appservice_in_room(room_id, appservice)
			.await
		{
			self.services
				.sending
				.send_pdu_appservice(appservice.registration.id.clone(), pdu_id)?;
			continue;
		}

		// If the RoomMember event has a non-empty state_key, it is targeted at someone.
		// If it is our appservice user, we send this PDU to it.
		if *pdu.kind() == TimelineEventType::RoomMember {
			if let Some(state_key_uid) = &pdu
				.state_key
				.as_ref()
				.and_then(|state_key| UserId::parse(state_key.as_str()).ok())
			{
				let appservice_uid = appservice.registration.sender_localpart.as_str();
				if state_key_uid == &appservice_uid {
					self.services
						.sending
						.send_pdu_appservice(appservice.registration.id.clone(), pdu_id)?;
					continue;
				}
			}
		}

		let matching_users = |users: &NamespaceRegex| {
			appservice.users.is_match(pdu.sender().as_str())
				|| *pdu.kind() == TimelineEventType::RoomMember
					&& pdu
						.state_key
						.as_ref()
						.is_some_and(|state_key| users.is_match(state_key))
		};
		let matching_aliases = |aliases: NamespaceRegex| {
			self.services
				.alias
				.local_aliases_for_room(room_id)
				.ready_any(move |room_alias| aliases.is_match(room_alias.as_str()))
		};

		if matching_aliases(appservice.aliases.clone()).await
			|| appservice.rooms.is_match(room_id.as_str())
			|| matching_users(&appservice.users)
		{
			self.services
				.sending
				.send_pdu_appservice(appservice.registration.id.clone(), pdu_id)?;
		}
	}

	Ok(pdu_id)
}

/// Evaluate whether `user` would be notified and/or highlighted by an
/// already-serialized `pdu`, per their current push rules and the room's
/// current power levels.
///
/// This owns the skip gates that must match live append and historical
/// recompute:
/// - self notifications
/// - ignored senders
/// - soft-failed events
/// - historical/backfilled events older than 10 minutes
///
/// Keeping those checks here avoids drifting behavior between
/// `append_pdu` and receipt recomputation.
#[implement(super::Service)]
pub(super) async fn evaluate_pdu_for_user(
	&self,
	user: &UserId,
	eval: &PduPushEval<'_>,
) -> (bool, bool) {
	let pdu = eval.pdu;
	if eval.soft_fail {
		trace!("Event {} is soft-failed, skipping push notifications", pdu.event_id());
		return (false, false);
	}

	if pdu.sender() == user {
		return (false, false);
	}

	if self
		.services
		.users
		.user_is_ignored(pdu.sender(), user)
		.await
	{
		return (false, false);
	}

	// Skip push notifications for historical events (backfilled, rescued,
	// or heavily delayed federation events) to avoid notification storms.
	let now = utils::millis_since_unix_epoch();
	let is_historical = now.saturating_sub(pdu.origin_server_ts().0.into()) > 10 * 60 * 1000;
	if is_historical {
		trace!("Event {} is historical, skipping push notifications", pdu.event_id());
		return (false, false);
	}

	let mut notify = false;
	let mut highlight = false;

	for action in self
		.services
		.pusher
		.get_actions(user, eval.rules_for_user, eval.power_levels, eval.serialized, eval.room_id)
		.await
	{
		match action {
			| Action::Notify => notify = true,
			| Action::SetTweak(Tweak::Highlight(true)) => {
				highlight = true;
			},
			| _ => {},
		}

		// Break early if both conditions are true
		if notify && highlight {
			break;
		}
	}

	(notify, highlight)
}
