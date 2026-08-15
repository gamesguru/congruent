use std::{
	collections::{BTreeMap, HashMap, hash_map},
	time::Instant,
};

use conduwuit::{
	Err, Event, Result, debug::INFO_SPAN_LEVEL, debug_error, debug_info, defer, err, implement,
	info, trace, utils::stream::IterStream, warn,
};
use futures::{
	FutureExt, TryFutureExt, TryStreamExt,
	future::{OptionFuture, try_join4},
};
use ruma::{
	CanonicalJsonValue, EventId, OwnedEventId, OwnedUserId, RoomId, ServerName, UserId,
	events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
};
use tracing::debug;

use super::handle_outlier_pdu::AuthRecoveryStage;
use crate::rooms::timeline::{RawPduId, pdu_fits};

async fn should_rescind_invite(
	services: &crate::rooms::event_handler::Services,
	content: &BTreeMap<String, CanonicalJsonValue>,
	sender: &UserId,
	room_id: &RoomId,
) -> Result<bool> {
	let event_room_id = content.get("room_id").and_then(|v| v.as_str());
	let event_sender = content.get("sender").and_then(|v| v.as_str());
	let event_type = content.get("type").and_then(|v| v.as_str());
	let state_key = content.get("state_key").and_then(|v| v.as_str());

	if event_room_id.is_some_and(|r| r != room_id.as_str())
		|| event_sender != Some(sender.as_str())
		|| event_type != Some("m.room.member")
		|| state_key.is_none()
	{
		return Ok(false);
	}

	let target_user_id = UserId::parse(state_key.unwrap())?;

	let membership = content
		.get("content")
		.and_then(|c| c.as_object())
		.and_then(|c| c.get("membership"))
		.and_then(|m| m.as_str());

	// TODO: what about "kick" events, too?
	if membership != Some("leave") && membership != Some("ban") {
		return Ok(false); // Only leave and ban can rescind an invite
	}

	if target_user_id.server_name() != services.globals.server_name() {
		return Ok(false);
	}

	// Does the target user have a pending invite?
	let Ok(pending_invite_state) = services
		.state_cache
		.invite_state(target_user_id, room_id)
		.await
	else {
		return Ok(false); // No pending invite, so nothing to rescind
	};
	for event in pending_invite_state {
		if event
			.get_field::<String>("type")?
			.is_some_and(|t| t == "m.room.member")
			&& event
				.get_field::<OwnedUserId>("state_key")?
				.is_some_and(|s| s == *target_user_id)
			&& event
				.get_field::<OwnedUserId>("sender")?
				.is_some_and(|s| s == *sender)
			&& event
				.get_field::<RoomMemberEventContent>("content")?
				.is_some_and(|c| c.membership == MembershipState::Invite)
		{
			return Ok(true);
		}
	}

	Ok(false)
}

/// When receiving an event one needs to:
/// 0. Check the server is in the room
/// 1. Skip the PDU if we already know about it
/// 1.1. Remove unsigned field
/// 2. Check signatures, otherwise drop
/// 3. Check content hash, redact if doesn't match
/// 4. Fetch any missing auth events doing all checks listed here starting at 1.
///    These are not timeline events
/// 5. Reject "due to auth events" if can't get all the auth events or some of
///    the auth events are also rejected "due to auth events"
/// 6. Reject "due to auth events" if the event doesn't pass auth based on the
///    auth events
/// 7. Persist this event as an outlier
/// 8. If not timeline event: stop
/// 9. Fetch any missing prev events doing all checks listed here starting at 1.
///    These are timeline events
/// 10. Fetch missing state and auth chain events by calling `/state_ids` at
///     backwards extremities doing all the checks in this list starting at
///     1. These are not timeline events
/// 11. Check the auth of the event passes based on the state of the event
/// 12. Ensure that the state is derived from the previous current state (i.e.
///     we calculated by doing state res where one of the inputs was a
///     previously trusted set of state, don't just trust a set of state we got
///     from a remote)
/// 13. Use state resolution to find new room state
/// 14. Check if the event passes auth based on the "current state" of the room,
///     if not soft fail it
#[implement(super::Service)]
#[tracing::instrument(
	name = "pdu",
	level = INFO_SPAN_LEVEL,
	skip_all,
	fields(%room_id, %event_id),
)]
pub async fn handle_incoming_pdu<'a>(
	&self,
	origin: &'a ServerName,
	room_id: &'a RoomId,
	event_id: &'a EventId,
	value: BTreeMap<String, CanonicalJsonValue>,
	is_timeline_event: bool,
	room_version_override: Option<&'a ruma::RoomVersionId>,
) -> Result<Option<RawPduId>> {
	// Prepare outlier value in case we need to soft-fail on timeout
	let mut outlier_value = value.clone();
	outlier_value
		.insert("event_id".to_owned(), CanonicalJsonValue::String(event_id.as_str().to_owned()));

	let fut = self.handle_incoming_pdu_inner(
		origin,
		room_id,
		event_id,
		value,
		is_timeline_event,
		room_version_override,
	);

	let pdu_timeout = self.services.server.config.pdu_receive_timeout;
	match Box::pin(tokio::time::timeout(std::time::Duration::from_secs(pdu_timeout), fut)).await {
		| Ok(res) => res,
		| Err(_) => {
			warn!(
				%event_id,
				%room_id,
				%origin,
				pdu_timeout,
				"PDU processing timed out, storing as outlier"
			);

			// Store the event data as an outlier so subsequent events
			// referencing it as a prev_event have something to build on.
			// Do NOT mark it soft-failed — it didn't fail auth, it just
			// ran out of time. It can be retried or upgraded later.
			self.services
				.outlier
				.add_pdu_outlier(event_id, &outlier_value, Some(room_id))
				.await;

			Err!(Request(Unknown("PDU processing timed out, please retry later.")))
		},
	}
}

#[implement(super::Service)]
pub(super) async fn handle_incoming_pdu_inner<'a>(
	&self,
	origin: &'a ServerName,
	room_id: &'a RoomId,
	event_id: &'a EventId,
	value: BTreeMap<String, CanonicalJsonValue>,
	is_timeline_event: bool,
	room_version_override: Option<&'a ruma::RoomVersionId>,
) -> Result<Option<RawPduId>> {
	// Skip if it's already an accepted timeline event.
	if let Ok(pdu_id) = self.services.timeline.get_pdu_id(event_id).await {
		if self.services.pdu_metadata.is_event_accepted(event_id).await {
			return Ok(Some(pdu_id));
		}
	}

	// NATIVE RETRY INTERCEPTION: If it's a known outlier that was rejected, check
	// local auth.
	if is_timeline_event
		&& self
			.services
			.outlier
			.get_pdu_outlier(event_id)
			.await
			.is_ok()
	{
		let pdu = self
			.services
			.outlier
			.get_pdu_outlier(event_id)
			.await
			.unwrap();
		let is_accepted = self.services.pdu_metadata.is_event_accepted(event_id).await;
		let is_rejected = self.services.pdu_metadata.is_event_rejected(event_id).await;
		info!(
			"Native retry interception: event {event_id} is_accepted={is_accepted} \
			 is_rejected={is_rejected}"
		);
		if !is_accepted {
			// Fast local check: are all auth events AND prev_events NOW in the timeline?
			let mut all_deps_satisfied = true;
			for aid in pdu.auth_events() {
				if !self.services.pdu_metadata.is_event_accepted(aid).await {
					info!("Native retry: auth event {aid} not accepted");
					all_deps_satisfied = false;
					break;
				}
			}
			if all_deps_satisfied {
				for prev_id in pdu.prev_events() {
					// Prev must exist in the timeline (not just as outlier)
					// for the unreject upgrade to have the state it needs.
					if self.services.timeline.get_pdu_id(prev_id).await.is_err() {
						info!("Native retry: prev event {prev_id} not in timeline");
						all_deps_satisfied = false;
						break;
					}
				}
			}

			if all_deps_satisfied {
				// All auth deps are satisfied: clear the rejection flag so
				// upgrade_outlier_pdu won't bail early with "Event has been rejected".
				info!("Un-rejecting event {event_id}: all auth events now accepted");
				self.services.pdu_metadata.unmark_event_rejected(event_id);

				// The auth chain is finally valid! Bypass handle_outlier_pdu (we already
				// verified sigs/hashes when we first saved it) and push to timeline
				// upgrade.
				let create_event = self
					.services
					.state_accessor
					.room_state_get(room_id, &StateEventType::RoomCreate, "")
					.await?;
				let val = self
					.services
					.outlier
					.get_outlier_pdu_json(event_id)
					.await
					.unwrap_or_else(|_| value.clone());
				return Box::pin(self.process_timeline_upgrade(
					pdu,
					val,
					&create_event,
					origin,
					room_id,
				))
				.await;
			}
			// Still missing/rejected dependencies. Return Ok(None) to ACK the transaction
			// instantly WITHOUT triggering network fetches or state resolution lockups.
			info!("Native retry: deps not satisfied for {event_id}, returning Ok(None)");
			return Ok(None);
		}
	} else if is_timeline_event {
		info!("Native retry interception SKIPPED: outlier not found for {event_id}");
	}
	if !pdu_fits(&mut value.clone()) {
		warn!(
			"dropping incoming PDU {event_id} in room {room_id} from {origin} because it \
			 exceeds 65535 bytes or is otherwise too large."
		);
		return Err!(Request(TooLarge("PDU is too large")));
	}
	trace!("processing incoming PDU from {origin} for room {room_id} with event id {event_id}");

	// Check we even know about the room
	let meta_exists = self.services.metadata.exists(room_id).map(Ok);

	// Check if the room is disabled
	let is_disabled = self.services.metadata.is_disabled(room_id).map(Ok);

	// Check room ACL on origin field/server
	let origin_acl_check = self.acl_check(origin, room_id);

	// Check room ACL on sender's server name
	let sender: &UserId = value
		.get("sender")
		.try_into()
		.map_err(|e| err!(Request(InvalidParam("PDU does not have a valid sender key: {e}"))))?;

	let sender_acl_check: OptionFuture<_> = sender
		.server_name()
		.ne(origin)
		.then(|| self.acl_check(sender.server_name(), room_id))
		.into();

	let (meta_exists, is_disabled, (), ()) = try_join4(
		meta_exists,
		is_disabled,
		origin_acl_check,
		sender_acl_check.map(|o| o.unwrap_or(Ok(()))),
	)
	.await
	.inspect_err(|e| debug_error!(%origin, "failed to handle incoming PDU {event_id}: {e}"))?;

	if is_disabled {
		return Err!(Request(Forbidden("Federation of this room is disabled by this server.")));
	}

	if !self
		.services
		.state_cache
		.server_in_room(self.services.globals.server_name(), room_id)
		.await
	{
		let is_room_member_event =
			value.get("type").and_then(|t| t.as_str()) == Some("m.room.member");

		// Is this a federated invite rescind?
		// copied from https://github.com/element-hq/synapse/blob/7e4588a/synapse/handlers/federation_event.py#L255-L300
		if is_room_member_event {
			if should_rescind_invite(&self.services, &value, sender, room_id).await? {
				let state_key = value
					.get("state_key")
					.and_then(|v| v.as_str())
					.unwrap_or_default();
				let target_user = UserId::parse(state_key).unwrap_or(sender);
				debug_info!(
					"Invite to {room_id} appears to have been rescinded by {sender}, marking \
					 target {target_user} as left"
				);
				self.services
					.state_cache
					.mark_as_left(target_user, room_id, None)
					.await;
				// Store the leave/ban as an outlier so the remote server's
				// retry finds it and doesn't loop with 404s.
				self.services
					.outlier
					.add_pdu_outlier(event_id, &value, Some(room_id))
					.await;
				return Ok(None);
			}
		}

		if meta_exists && is_room_member_event {
			info!(
				%origin,
				%room_id,
				"Accepting inbound membership PDU for known room before participation cache catches up"
			);
		} else if is_room_member_event {
			// We're not in this room but got a member event we couldn't
			// rescind. Store it as an outlier so the remote server doesn't
			// retry endlessly with 404s.
			info!(
				%origin,
				%room_id,
				"Storing unprocessable member PDU as outlier (not participating)"
			);
			self.services
				.outlier
				.add_pdu_outlier(event_id, &value, Some(room_id))
				.await;
			return Ok(None);
		} else {
			info!(
				%origin,
				%room_id,
				"Dropping inbound PDU for room we aren't participating in"
			);
			return Err!(Request(NotFound("This server is not participating in that room.")));
		}
	}

	if !meta_exists {
		return Err!(Request(NotFound("Room is unknown to this server")));
	}

	// Fetch create event
	let create_event = &(self
		.services
		.state_accessor
		.room_state_get(room_id, &StateEventType::RoomCreate, "")
		.await?);

	let (incoming_pdu, val) = match Box::pin(self.handle_outlier_pdu(
		origin,
		Some(create_event),
		event_id,
		room_id,
		value.clone(),
		false,
		false,
		room_version_override,
		AuthRecoveryStage::BeforeStateIds,
	))
	.await
	{
		| Ok(res) => res,
		| Err(conduwuit::Error::MissingAuthEvents(missing)) => {
			// A backfill-driven `/event`/`/context` fetch (see backfill.rs's
			// `get_remote_pdu`) can hand us a `missing` list running into the
			// hundreds for a deep or adversarial auth chain (the same MSC4297
			// scenario documented in fetch_and_handle_outliers.rs). Without a
			// bound, one inbound event could drive hundreds of sequential
			// `/event` requests here and monopolize the 600s PDU receive
			// timeout. Mirror handle_outlier_pdu's MAX_INLINE_FETCH: resolve
			// only a small prefix synchronously; anything beyond that is left
			// unresolved and falls through to the outlier fallback below the
			// same as if the whole retry had failed, to be picked up
			// opportunistically later (e.g. once a dependent event references
			// it) instead of blocking this request.
			const MAX_INLINE_FETCH: usize = 5;

			// Before attempting expensive /state/ federation requests, check
			// whether the missing auth events are already known to be
			// *permanently* rejected. If they are, this event inherits the
			// rejection and no network fetch is needed (spec step 5: reject
			// if auth events are rejected). A merely-pending/retryable
			// verdict on `mid` (e.g. left by `handle_outlier_pdu`'s own
			// missing-auth-event recovery) must not cascade here -- fall
			// through to the /state_ids retry below instead.
			for mid in &missing {
				if self
					.services
					.pdu_metadata
					.is_event_permanently_rejected(mid)
					.await
				{
					info!(
						"Event {event_id} rejected: missing auth event {mid} is already marked \
						 rejected; skipping /state/ fetch"
					);
					self.services
						.outlier
						.add_pdu_outlier(event_id, &value, Some(room_id))
						.await;
					self.services
						.pdu_metadata
						.mark_event_rejected(
							event_id,
							&crate::rooms::pdu_metadata::RejectionCode::DependsOnRejectedAuthEvent
								.with_detail(mid),
						)
						.await;
					return Ok(None);
				}
			}

			// The auth event chain for this PDU may be deeper than the
			// iterative fetcher's per-call limit. Try calling /state_ids on
			// the origin to retrieve and store the complete auth chain in one
			// shot, then retry handle_outlier_pdu. This also satisfies the
			// Matrix spec requirement that servers call /state_ids when auth
			// events are unresolvable via the normal backfill path.
			let parsed_pdu =
				conduwuit::PduEvent::from_id_val(event_id, value.clone(), Some(room_id)).ok();
			let direct_prev = parsed_pdu.as_ref().and_then(|pdu| {
				let mut prev_events = pdu.prev_events();
				let first_prev = prev_events.next()?.to_owned();
				prev_events.next().is_none().then_some(first_prev)
			});
			let mut state_ids_anchor = direct_prev.clone().unwrap_or_else(|| event_id.to_owned());

			if is_timeline_event
				&& let Some(pdu) = parsed_pdu.as_ref()
				&& direct_prev.is_some()
			{
				match Box::pin(self.fetch_prev(
					origin,
					room_id,
					event_id,
					pdu.prev_events(),
					Some(pdu.sender().server_name()),
				))
				.await
				{
					// `fetch_prev` found a fetched-but-still-unresolved candidate
					// one hop further back than `event_id`'s own direct prev
					// (e.g. /get_missing_events only returned a single gap-filler
					// whose own prev_event we still don't have) -- anchor the
					// upcoming /state_ids retry there instead, since that's the
					// point the sending server can actually provide a snapshot
					// for.
					| Ok((_, _, Some(deeper_anchor), _)) => {
						state_ids_anchor = deeper_anchor;
					},
					| Ok(_) => {},
					| Err(e) => {
						warn!(
							event_id = %event_id,
							"failed to fetch prev_events before /state_ids retry: {e}"
						);
					},
				}
			}

			let retry_result = Box::pin(async {
				Box::pin(self.fetch_state(
					origin,
					create_event,
					room_id,
					&state_ids_anchor,
					false,
				))
				.await?;

				let room_version_id = self.services.state.get_room_version(room_id).await?;
				let mut inline_fetches = 0_usize;
				for missing_id in &missing {
					if self.services.timeline.pdu_exists(missing_id).await {
						continue;
					}

					if inline_fetches >= MAX_INLINE_FETCH {
						let remaining = missing.len().saturating_sub(inline_fetches);
						debug_info!(
							event_id = %event_id,
							remaining,
							total = missing.len(),
							"Reached inline missing-auth-event fetch limit; deferring the rest"
						);
						break;
					}
					inline_fetches = inline_fetches.saturating_add(1);

					let request = ruma::api::federation::event::get_event::v1::Request {
						event_id: missing_id.to_owned(),
						include_unredacted_content: None,
					};

					let Ok(response) = self
						.services
						.sending
						.send_federation_request(origin, request)
						.await
					else {
						continue;
					};

					let Ok((parsed_id, value)) =
						conduwuit::matrix::event::gen_event_id_canonical_json(
							&response.pdu,
							&room_version_id,
						)
					else {
						continue;
					};

					if parsed_id != *missing_id {
						warn!(
							expected = %missing_id,
							actual = %parsed_id,
							"fetched missing auth event ID mismatch"
						);
						continue;
					}

					if let Err(e) = Box::pin(self.handle_outlier_pdu(
						origin,
						Some(create_event),
						missing_id,
						room_id,
						value,
						false,
						false,
						Some(&room_version_id),
						AuthRecoveryStage::AfterStateIds,
					))
					.await
					{
						debug_info!(
							"failed to handle directly fetched auth event {missing_id}: {e}"
						);
					}
				}

				Box::pin(self.handle_outlier_pdu(
					origin,
					Some(create_event),
					event_id,
					room_id,
					value.clone(),
					false,
					false,
					room_version_override,
					AuthRecoveryStage::AfterStateIds,
				))
				.await
			})
			.await;

			match retry_result {
				| Ok(res) => res,
				| Err(_) => {
					// /state_ids didn't help — fall back to background healer.
					info!(
						target: "state_res_debug",
						event_id = %event_id,
						count = missing.len(),
						"Storing incoming PDU as outlier; missing auth events will be \
						 fetched in background"
					);
					self.services
						.outlier
						.add_pdu_outlier(event_id, &value, Some(room_id))
						.await;

					return Ok(None);
				},
			}
		},
		| Err(conduwuit::Error::Request(_, ref msg, ..))
			if msg.contains("Cannot determine state: all prev_events are rejected") =>
		{
			info!(
				"Event {event_id} rejected because it depends on rejected prev event(s). \
				 Returning Ok(None) to acknowledge the transaction."
			);
			self.services
				.outlier
				.add_pdu_outlier(event_id, &value, Some(room_id))
				.await;
			return Ok(None);
		},
		| Err(conduwuit::Error::Request(_, ref msg, ..))
			if msg.contains("Event depends on rejected auth event")
				|| msg.contains("is already known and rejected") =>
		{
			info!(
				"Event {event_id} rejected because it depends on rejected auth event. Returning \
				 Ok(None) to acknowledge the transaction."
			);
			self.services
				.outlier
				.add_pdu_outlier(event_id, &value, Some(room_id))
				.await;
			// Only the "depends on rejected auth event" case is actually a fresh
			// cascading rejection to record here. The "is already known and
			// rejected" message (from handle_outlier_pdu's early-return branch)
			// means the event was already marked rejected earlier -- for some
			// non-retryable reason, since a retryable one would have been
			// cleared by `take_retry_if_rejection_retryable` before that message
			// could be produced -- and re-marking it here with a
			// `DependsOnRejectedAuthEvent` tag would overwrite and lose that
			// original, more specific rejection reason.
			if msg.contains("Event depends on rejected auth event") {
				self.services
					.pdu_metadata
					.mark_event_rejected(
						event_id,
						&crate::rooms::pdu_metadata::RejectionCode::DependsOnRejectedAuthEvent
							.with_detail(msg),
					)
					.await;
			}
			return Ok(None);
		},
		| Err(e) => return Err(e),
	};

	// if not timeline event: stop
	if !is_timeline_event {
		return Ok(None);
	}

	// Run the timeline upgrade synchronously inline.
	// We no longer need an MPSC worker because state resolution lockups (the V2.1
	// drain trap) are fixed, so this runs blazingly fast without starving EDUs or
	// OCC storms!
	Box::pin(self.process_timeline_upgrade(incoming_pdu, val, create_event, origin, room_id))
		.await
}

#[implement(super::Service)]
#[tracing::instrument(
	name = "pdu_upgrade",
	level = INFO_SPAN_LEVEL,
	skip_all,
	fields(%room_id, %event_id = %incoming_pdu.event_id()),
)]
pub async fn process_timeline_upgrade(
	&self,
	incoming_pdu: conduwuit::PduEvent,
	val: BTreeMap<String, CanonicalJsonValue>,
	create_event: &conduwuit::PduEvent,
	origin: &ServerName,
	room_id: &RoomId,
) -> Result<Option<RawPduId>> {
	let event_id = incoming_pdu.event_id().to_owned();

	// Skip old events
	let first_ts_in_room = self
		.services
		.timeline
		.first_pdu_in_room(room_id)
		.await?
		.origin_server_ts();
	let room_version_id = self.services.state.get_room_version(room_id).await?;

	// Fetch any missing prev events before taking the write cork so remote I/O
	// does not suppress unrelated WAL flushes across the whole server.
	// These are timeline events.
	let (
		sorted_prev_events,
		fetched_prev_events,
		prev_fetch_deeper_anchor,
		prev_fetch_had_invalid_data,
	) = Box::pin(self.fetch_prev(
		origin,
		room_id,
		event_id.as_ref(),
		incoming_pdu.prev_events(),
		Some(incoming_pdu.sender().server_name()),
	))
	.await?;

	debug!(events = ?sorted_prev_events, "Handling previous events");

	// Corked (but not flushed) for the whole loop so a gap with many
	// predecessors doesn't pay for one synchronous engine flush per outlier
	// persisted inside `handle_outlier_pdu` -- it releases this cork itself
	// around each federation round-trip it makes, so unrelated flushes
	// elsewhere on the server still aren't held back while we wait on the
	// network.
	let mut eventid_info = self
		.services
		.timeline
		.with_cork(|| async {
			let mut eventid_info: HashMap<
				OwnedEventId,
				(conduwuit::PduEvent, BTreeMap<String, CanonicalJsonValue>),
			> = HashMap::new();

			for prev_id in &sorted_prev_events {
				let Some(val) = fetched_prev_events.get(prev_id).cloned() else {
					continue;
				};

				if let Ok((pdu, val)) = Box::pin(self.handle_outlier_pdu(
					origin,
					Some(create_event),
					prev_id,
					room_id,
					val,
					false,
					false,
					Some(&room_version_id),
					AuthRecoveryStage::AfterStateIds,
				))
				.await
				{
					eventid_info.insert(prev_id.clone(), (pdu, val));
				}
			}

			eventid_info
		})
		.await;

	// Keep the actual write phase inside one flush boundary so prev-event
	// repairs and the incoming event become visible together.
	self.services
		.timeline
		.with_cork_and_flush(|| async move {
			sorted_prev_events
				.iter()
				.try_stream()
				.map_ok(AsRef::as_ref)
				.try_for_each(|prev_id| {
					self.handle_prev_pdu(
						origin,
						event_id.as_ref(),
						room_id,
						eventid_info.remove(prev_id),
						create_event,
						first_ts_in_room,
						prev_id,
					)
					.inspect_err(move |e| {
						warn!("Prev {prev_id} failed: {e}");
						match self
							.services
							.globals
							.bad_event_ratelimiter
							.write()
							.entry(prev_id.into())
						{
							| hash_map::Entry::Vacant(e) => {
								e.insert((Instant::now(), 1));
							},
							| hash_map::Entry::Occupied(mut e) => {
								let tries = e.get().1.saturating_add(1);
								*e.get_mut() = (Instant::now(), tries);
							},
						}
					})
					.map(|_| self.services.server.check_running())
				})
				.boxed()
				.await?;

			// Done with prev events, now handling the incoming event
			let start_time = Instant::now();
			self.federation_handletime
				.write()
				.insert(room_id.into(), (event_id.clone(), start_time));

			defer! {{
				if self.services.server.running() {
					self.federation_handletime
						.write()
						.remove(room_id);
				}
			}};

			Box::pin(self.upgrade_outlier_to_timeline_pdu(
				incoming_pdu,
				val,
				create_event,
				origin,
				room_id,
				false,
				true,
				prev_fetch_had_invalid_data,
				prev_fetch_deeper_anchor,
				true,
			))
			.await
		})
		.await
}
