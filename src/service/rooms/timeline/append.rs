use std::collections::{BTreeMap, HashSet};

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
use slipstream::{
	CanonicalJsonObject, CanonicalJsonValue, EventId, UserId,
	events::{
		GlobalAccountDataEventType, StateEventType, TimelineEventType,
		push_rules::PushRulesEvent,
		room::{
			encrypted::Relation, power_levels::RoomPowerLevelsEventContent,
			tombstone::RoomTombstoneEventContent,
		},
	},
	push::{Action, Ruleset, Tweak},
};

use super::{ExtractBody, ExtractRelatesTo, ExtractRelatesToEventId, RoomMutexGuard};
use crate::appservice::NamespaceRegex;

pub struct AppendPduContext<'a> {
	pub state_lock: &'a RoomMutexGuard,
	pub room_id: &'a slipstream::RoomId,
	pub state_root_handle: Option<rezzy::hamt::RootHandle>,
	pub prev_state_root_handle: Option<rezzy::hamt::RootHandle>,
	pub advance_current_state: bool,
	/// Membership of the event's state key in this room, sampled *before* a
	/// `/send_join` state root was installed. `None` means "no such prior
	/// sample"; the live cache is consulted instead.
	pub was_joined_before_state_install: Option<(&'a UserId, bool)>,
}

/// Inputs shared by push-rule evaluation in live append and receipt-based
/// recomputation.
pub(super) struct PduPushEval<'a> {
	pub pdu: &'a PduEvent,
	pub serialized: &'a slipstream::serde::Raw<slipstream::events::AnySyncTimelineEvent>,
	pub room_id: &'a slipstream::RoomId,
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
	soft_fail: bool,
	resolved_state_applied: bool,
	ctx: AppendPduContext<'a>,
) -> Result<Option<RawPduId>>
where
	Leaves: Iterator<Item = &'a EventId> + Send + 'a,
{
	let AppendPduContext {
		state_lock,
		room_id,
		state_root_handle,
		prev_state_root_handle,
		advance_current_state,
		was_joined_before_state_install,
	} = ctx;

	// Soft-failed events pass auth against the state at the event but fail
	// against the current room state. Per spec §11.33.2.6 they SHOULD NOT
	// appear in /sync or /messages. Record only the historical state-root
	// association (needed by state resolution and auth lookups that reference
	// the event) without touching current room state, and do NOT append to the
	// timeline sequence or clear the outlier marker. The event still isn't in
	// the timeline at this point, so it must remain an outlier until a
	// successful append happens.
	if soft_fail {
		if let Some(root_handle) = state_root_handle
			.as_ref()
			.or(prev_state_root_handle.as_ref())
		{
			self.services
				.state
				.set_event_roothandle(pdu.event_id(), root_handle)
				.await?;
		}

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
		.append_pdu(pdu, pdu_json, new_room_leaves, resolved_state_applied, AppendPduContext {
			state_lock,
			room_id,
			state_root_handle,
			prev_state_root_handle,
			advance_current_state,
			was_joined_before_state_install,
		})
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
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(level = "debug", skip_all)]
pub async fn append_pdu<'a, Leaves>(
	&'a self,
	pdu: &'a PduEvent,
	mut pdu_json: CanonicalJsonObject,
	leaves: Leaves,
	resolved_state_applied: bool,
	ctx: AppendPduContext<'a>,
) -> Result<RawPduId>
where
	Leaves: Iterator<Item = &'a EventId> + Send + 'a,
{
	let AppendPduContext {
		state_lock,
		room_id,
		state_root_handle,
		prev_state_root_handle,
		advance_current_state,
		was_joined_before_state_install,
	} = ctx;

	// Coalesce timeline writes; flush before pub'ing receipt changes / waking sync.
	let cork = self.db.db.cork_and_flush();
	// Soft-failed events return before `append_pdu` (see `append_incoming_pdu`),
	// so this path is always a non-soft-failed append.
	let soft_fail = false;

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
		let event_type: StateEventType = pdu.kind().to_string().into();
		if let CanonicalJsonValue::Object(unsigned) = pdu_json
			.entry("unsigned".to_owned())
			.or_insert_with(|| CanonicalJsonValue::Object(BTreeMap::default()))
		{
			if let Some(prev_root_handle) = prev_state_root_handle.as_ref() {
				if let Ok(prev_state) = self
					.services
					.state_accessor
					.state_get_in_room_hamt(room_id, prev_root_handle, &event_type, state_key)
					.await
				{
					unsigned.insert(
						"prev_content".to_owned(),
						CanonicalJsonValue::Object(
							utils::to_canonical_object(prev_state.get_content_as_value())
								.map_err(|e| {
									err!(Database(error!(
										"Failed to convert prev_state to canonical JSON: {e}",
									)))
								})?,
						),
					);
					unsigned.insert(
						String::from("prev_sender"),
						CanonicalJsonValue::String(prev_state.sender().to_string()),
					);
					unsigned.insert(
						String::from("replaces_state"),
						CanonicalJsonValue::String(prev_state.event_id().to_string()),
					);
				}
			}
		} else {
			error!("Invalid unsigned type in pdu.");
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
		.set_forward_extremities(
			room_id,
			leaves.map(ToOwned::to_owned),
			Some(pdu.event_id()),
			state_lock,
		)
		.await;

	let insert_lock = self.mutex_insert.lock(room_id).await;
	info!(
		target: "watermark_debug",
		%room_id, event_id = %pdu.event_id(),
		"append_pdu: acquired insert_lock"
	);

	let existing_pdu = if self.non_outlier_pdu_exists(pdu.event_id()).await {
		warn!(
			target: "timeline_debug",
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
		info!(target: "timeline_debug", event_id = %pdu.event_id(), ?pdu_count, "append_pdu: about to insert");

		// Write first, then publish the count
		self.db.append_pdu(&pdu_id, pdu, &pdu_json, pdu_count).await;
		info!(target: "timeline_debug", event_id = %pdu.event_id(), ?pdu_count, "append_pdu: insert complete");

		info!(
			target: "watermark_debug",
			%room_id, event_id = %pdu.event_id(), ?pdu_count,
			"append_pdu: publishing last_timeline_count_cache"
		);
		self.last_timeline_count_cache
			.insert(room_id.to_owned(), pdu_count);

		(pdu_id, pdu_count, Some(count))
	};

	// Commit the event's state association inside the same cork as the timeline
	// write, so a flush never exposes the PDU without its state root.
	Box::pin(self.services.state.set_event_state_with_root(
		room_id,
		pdu,
		state_lock,
		state_root_handle.as_ref(),
		prev_state_root_handle.as_ref(),
	))
	.await?;
	// Recovered outlier timelines carry no state event of their own, so the batch
	// above did not advance the room pointer for them. A state event already had
	// it committed inside that batch, so re-writing it here would be redundant.
	if advance_current_state
		&& !crate::rooms::state::is_state_event(pdu)
		&& let Some(root_handle) = state_root_handle.as_ref()
	{
		self.services
			.state
			.set_room_state_hamt(room_id, root_handle, state_lock);
	}
	drop(cork);
	let receipt_content = BTreeMap::from_iter([(
		pdu.event_id().to_owned(),
		BTreeMap::from_iter([(
			slipstream::events::receipt::ReceiptType::ReadPrivate,
			BTreeMap::from_iter([(
				pdu.sender().to_owned(),
				slipstream::events::receipt::Receipt {
					ts: Some(slipstream::MilliSecondsSinceUnixEpoch::now().get()),
					thread: slipstream::events::receipt::ReceiptThread::Unthreaded,
				},
			)]),
		)]),
	)]);
	let receipt_event = slipstream::events::receipt::ReceiptEvent {
		content: slipstream::events::receipt::ReceiptEventContent(receipt_content),
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
	let power_levels: RoomPowerLevelsEventContent = match state_root_handle {
		| Some(root_handle) => self
			.services
			.state_accessor
			.state_get_in_room_hamt(room_id, &root_handle, &StateEventType::RoomPowerLevels, "")
			.await
			.and_then(|pdu| pdu.get_content())
			.unwrap_or_default(),
		| None => self
			.services
			.state_accessor
			.room_state_get_content(room_id, &StateEventType::RoomPowerLevels, "")
			.await
			.unwrap_or_default(),
	};

	let mut push_target: HashSet<_> = self
		.services
		.state_cache
		.active_local_users_in_room(room_id)
		// Don't notify the sender of their own events, and dont send from ignored users
		.ready_filter(|user| user != pdu.sender())
		.filter_map(|recipient_user| async move {
			(!self
				.services
				.users
				.user_is_ignored(pdu.sender(), &recipient_user)
				.await)
				.then_some(recipient_user)
		})
		.collect()
		.await;

	let mut notifies = Vec::with_capacity(push_target.len().saturating_add(1));
	let mut highlights = Vec::with_capacity(push_target.len().saturating_add(1));
	let thread_root = self.services.threads.get_thread_id(pdu).await;

	if *pdu.kind() == TimelineEventType::RoomMember {
		if let Some(state_key) = pdu.state_key() {
			let target_user_id = UserId::parse(state_key)
				.map_err(|e| err!(Request(InvalidParam("Invalid state key: {e}"))))?;

			if self.services.users.is_active_local(&target_user_id).await {
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
				|_| Ruleset::server_default(user.as_str()),
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
		.increment_notification_counts(room_id, notifies, highlights, thread_root.as_ref());

	if *pdu.kind() == TimelineEventType::RoomTombstone {
		if let Ok(tombstone) = pdu.get_content::<RoomTombstoneEventContent>() {
			let replacement_room = tombstone.replacement_room.as_ref();
			super::copy_room_push_rules_for_upgrade(self, room_id, replacement_room).await?;
		}
	}

	match *pdu.kind() {
		| TimelineEventType::RoomRedaction => {
			let room_version_id = self.services.state.get_room_version(room_id).await?;
			if let Some(redact_id) = pdu.redacts_id(&room_version_id) {
				if self
					.services
					.state_accessor
					.user_can_redact(&redact_id, pdu.sender(), room_id, false)
					.await?
				{
					self.redact_pdu(&redact_id, pdu, shortroomid).await?;
				}
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
				let target_user_id = UserId::parse(state_key)
					.map_err(|e| err!(Request(InvalidParam("Invalid state key: {e}"))))?;

				// Capture whether the target was already joined *before* this event. A
				// membership event whose membership stays `join` (e.g. a display name or
				// avatar profile update) must not be treated as a device-list change; that
				// would spuriously notify other users to rotate their room keys.
				//
				// A `/send_join` state root installs the room state *including* the joining
				// user's own membership, so by this point the live cache already reports the
				// target as joined and would suppress the notification on a genuine first
				// join. Callers that installed such a state pass the pre-install sample; all
				// others fall back to the cache.
				let was_joined = match was_joined_before_state_install {
					| Some((sampled_user_id, was_joined)) if *sampled_user_id == target_user_id =>
						was_joined,
					| _ =>
						self.services
							.state_cache
							.is_joined(&target_user_id, room_id)
							.await,
				};

				// Update our membership info, we do this here incase a user is invited or
				// knocked and immediately leaves we need the DB to record the invite or
				// knock event for auth
				self.services
					.state_cache
					.update_membership(room_id, &target_user_id, pdu, true)
					.await?;

				if let Ok(content) =
					pdu.get_content::<slipstream::events::room::member::RoomMemberEventContent>()
				{
					if content.membership
						== slipstream::events::room::member::MembershipState::Join
						&& !was_joined
						&& self.services.globals.user_is_local(&target_user_id)
					{
						self.services
							.users
							.mark_device_key_update(&target_user_id)
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
