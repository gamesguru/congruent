//! Re-authorisation of the events received while a room's state was partial.
//!
//! While the state is partial, events are accepted on the strength of their own
//! `auth_events` since the members they depend on may be unknown. Once the full
//! state arrives, each of those events is checked again against the state at
//! that point in the DAG (like Synapse's `update_state_for_partial_state_event`):
//! a wrongly accepted event becomes rejected, a wrongly rejected one is
//! accepted, and the state snapshots and current state follow.

use std::collections::{HashMap, HashSet};

use conduwuit::{
	PduEvent, debug, info,
	matrix::{Event, StateKey, state_res},
	warn,
};
use futures::{StreamExt, TryStreamExt};
use slipstream::{
	EventId, OwnedEventId, OwnedUserId, RoomId, UserId,
	events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
};

use crate::rooms::{pdu_metadata::RejectionCode, short::ShortStateKey, state_partial::Service};

type StateMap = HashMap<ShortStateKey, OwnedEventId>;

/// Sanity ceiling on the events re-authorised in one resync. Past it the resync
/// fails and is retried; it is never completed without re-authorising.
const WINDOW_LIMIT: usize = 500_000;

impl Service {
	/// Re-authorise the events between the resync point (`anchor`, whose full
	/// state is `anchor_state`) and the room's forward extremities.
	///
	/// Returns the room's new current state, the remote users who were
	/// joined and then left or banned within the window, and the forward
	/// extremities it was built from (sorted), or `None` if the window could not
	/// be reconstructed (the caller then falls back to merging).
	pub(super) async fn reauth_window(
		&self,
		room_id: &RoomId,
		anchor: &EventId,
		anchor_state: StateMap,
	) -> Option<(StateMap, HashSet<OwnedUserId>, Vec<OwnedEventId>)> {
		let create_pdu = self
			.services
			.state_accessor
			.room_state_get(room_id, &StateEventType::RoomCreate, "")
			.await
			.ok()?;
		let room_version_id =
			crate::rooms::event_handler::get_room_version_id(&create_pdu).ok()?;
		let room_version = crate::rooms::event_handler::to_room_version(&room_version_id);

		let anchor_depth = self
			.services
			.timeline
			.get_pdu(anchor)
			.await
			.map(|pdu| pdu.depth())
			.ok();

		// Walk back from the forward extremities to the anchor.
		let extremities: Vec<OwnedEventId> = self
			.services
			.state
			.get_forward_extremities(room_id)
			.collect()
			.await;
		let mut stack = extremities.clone();
		let mut window: HashMap<OwnedEventId, PduEvent> = HashMap::new();
		while let Some(event_id) = stack.pop() {
			if event_id == anchor || window.contains_key(&event_id) {
				continue;
			}
			let Ok(pdu) = self.services.timeline.get_pdu(&event_id).await else {
				debug!(target: "reauth_debug", %event_id, "window event not found");
				continue;
			};
			if anchor_depth.is_some_and(|depth| pdu.depth() <= depth) {
				continue;
			}
			if window.len() >= WINDOW_LIMIT {
				warn!(target: "reauth_debug", %room_id, "re-auth window too large; giving up");
				return None;
			}
			stack.extend(pdu.prev_events().map(ToOwned::to_owned));
			window.insert(event_id, pdu);
		}

		let mut order: Vec<&PduEvent> = window.values().collect();
		order.sort_by(|a, b| {
			a.depth()
				.cmp(&b.depth())
				.then_with(|| a.event_id().cmp(b.event_id()))
		});

		info!(
			target: "reauth_debug",
			%room_id, %anchor, events = order.len(), extremities = extremities.len(),
			"re-authorising events received while the state was partial"
		);

		let mut left: HashSet<OwnedUserId> = HashSet::new();
		let mut after: HashMap<OwnedEventId, StateMap> = HashMap::new();
		after.insert(anchor.to_owned(), anchor_state);

		// A state map is only needed until its last in-window child has used it, so
		// the retained state maps follow the width of the DAG. (The PDUs of the
		// whole window are still held in memory.)
		let mut children: HashMap<OwnedEventId, usize> = HashMap::new();
		for pdu in &order {
			for prev in pdu.prev_events() {
				let count = children.entry(prev.to_owned()).or_default();
				*count = count.saturating_add(1);
			}
		}

		for pdu in order {
			let event_id = pdu.event_id().to_owned();

			// State before this event: that after its predecessors.
			let mut predecessor_states: Vec<StateMap> = Vec::new();
			let mut unavailable = false;
			for prev in pdu.prev_events() {
				if let Some(prev_state) = after.get(prev) {
					predecessor_states.push(prev_state.clone());
				} else if window.contains_key(prev) {
					// Inside the window, so it must have been processed already.
					warn!(target: "reauth_debug", %event_id, %prev, "predecessor state missing; cannot re-authorise window");
					return None;
				} else {
					// Outside the window: only acceptable if it is known to be at or
					// below the anchor's depth, and then its own stored state is used.
					let older = match (self.services.timeline.get_pdu(prev).await, anchor_depth) {
						| (Ok(pred), Some(depth)) => pred.depth() <= depth,
						| _ => false,
					};
					let stored = if older {
						self.stored_state(room_id, prev).await
					} else {
						None
					};
					let Some(stored) = stored else {
						unavailable = true;
						break;
					};
					predecessor_states.push(stored);
				}
			}
			let before = if unavailable {
				// A predecessor was never held locally (it was skipped over when this
				// event arrived), so the state before this event is the one recorded
				// when it was ingested, taken from the remote server's state at that
				// predecessor.
				let Some(recorded) = self.stored_pre_state(room_id, &event_id).await else {
					warn!(target: "reauth_debug", %event_id, "predecessor state unavailable and none recorded; cannot re-authorise window");
					return None;
				};
				info!(target: "reauth_debug", %event_id, "predecessor unavailable; using the state recorded at ingestion");
				recorded
			} else if predecessor_states.is_empty() {
				warn!(target: "reauth_debug", %event_id, "event has no prev_events; cannot re-authorise window");
				return None;
			} else {
				self.resolve_short_state_maps(room_id, &room_version_id, &predecessor_states)
					.await?
			};

			let was_rejected = self
				.services
				.pdu_metadata
				.is_event_rejected(&event_id)
				.unwrap_or(false);
			let reason = self
				.services
				.pdu_metadata
				.get_rejection_reason(&event_id)
				.ok()
				.flatten();
			// Only auth failures are revisited; other rejections are about the
			// bytes of the event, not the state it was judged against.
			let revisit =
				!was_rejected || reason.as_deref() == Some(RejectionCode::AuthCheckFailed.tag());
			let local = self.services.globals.user_is_local(pdu.sender());

			let mut accepted = !was_rejected;
			if revisit && !local {
				let before_ref = &before;
				let state_fetch = |kind: StateEventType, state_key: StateKey| async move {
					let shortstatekey = self
						.services
						.short
						.get_shortstatekey(&kind, &state_key)
						.await
						.ok()?;
					let id = before_ref.get(&shortstatekey)?;
					self.services.timeline.get_pdu(id).await.ok()
				};
				match state_res::event_auth::auth_check(
					&room_version,
					pdu,
					None,
					|ty, sk| state_fetch(ty.clone(), sk.into()),
					create_pdu.as_pdu(),
				)
				.await
				{
					| Ok(ok) => accepted = ok,
					| Err(e) => {
						warn!(target: "reauth_debug", %event_id, "auth check errored: {e:?}");
					},
				}
			}

			if accepted == was_rejected {
				let result = if accepted {
					self.services.pdu_metadata.unmark_event_rejected(&event_id)
				} else {
					self.services
						.pdu_metadata
						.mark_event_rejected(&event_id, RejectionCode::AuthCheckFailed.tag())
				};
				if let Err(e) = result {
					warn!(target: "reauth_debug", %event_id, "failed to update verdict: {e}");
				}
				// State resolution caches events with their verdict.
				self.services.short.leanevent_cache.invalidate(&event_id);
				info!(
					target: "reauth_debug",
					%event_id, was_rejected, now_rejected = !accepted,
					kind = %pdu.kind(), state_size = before.len(),
					"verdict changed after resync"
				);
			}

			let mut state = before;
			if accepted {
				// A remote member who really left or was removed in the window missed
				// device updates, even if they are back in by the end.
				if pdu.kind() == rezzy::basespec::event_types::M_ROOM_MEMBER {
					if let Some(user_id) = pdu
						.state_key()
						.and_then(|k| UserId::parse(k).ok())
						.filter(|u| !self.services.globals.user_is_local(u))
					{
						let ends_membership =
							pdu.get_content::<RoomMemberEventContent>().is_ok_and(|c| {
								matches!(
									c.membership,
									MembershipState::Leave | MembershipState::Ban
								)
							});
						if ends_membership && self.was_joined(&state, &user_id).await {
							left.insert(user_id);
						}
					}
				}
				if let Some(state_key) = pdu.state_key() {
					let shortstatekey = self
						.services
						.short
						.get_or_create_shortstatekey(&pdu.kind().to_string().into(), state_key)
						.await;
					state.insert(shortstatekey, event_id.clone());
				}
			}

			match self
				.services
				.event_handler
				.state_map_to_root_handle(room_id, &state)
				.await
			{
				| Ok((root, nodes)) => {
					if let Err(e) = self
						.services
						.state
						.set_event_roothandle_with_nodes(room_id, &event_id, &root, &nodes)
						.await
					{
						warn!(target: "reauth_debug", %event_id, "failed to store snapshot: {e}");
					}
				},
				| Err(e) => {
					warn!(target: "reauth_debug", %event_id, "failed to build snapshot: {e}");
				},
			}
			after.insert(event_id, state);

			for prev in pdu.prev_events() {
				if let Some(remaining) = children.get_mut(prev) {
					*remaining = remaining.saturating_sub(1);
					if *remaining == 0 && !extremities.iter().any(|e| e == prev) {
						after.remove(prev);
					}
				}
			}
		}

		// Current state is the room-version resolution of all forward-tip states.
		let mut tips: Vec<(&OwnedEventId, &StateMap, u64)> = Vec::new();
		for extremity in &extremities {
			if let (Some(state), Some(pdu)) = (after.get(extremity), window.get(extremity)) {
				tips.push((extremity, state, pdu.depth()));
			} else if extremity == anchor {
				if let Some(state) = after.get(extremity) {
					tips.push((extremity, state, anchor_depth.unwrap_or(0)));
				}
			}
		}
		if tips.is_empty() {
			return None;
		}
		let tip_states: Vec<StateMap> =
			tips.iter().map(|(_, state, _)| (*state).clone()).collect();
		let current = self
			.resolve_short_state_maps(room_id, &room_version_id, &tip_states)
			.await?;
		let mut seen = extremities;
		seen.sort();
		Some((current, left, seen))
	}

	/// Resolve short-keyed state maps with the room-version state-resolution
	/// algorithm. This is needed both when a DAG event has conflicting
	/// predecessor states and when the room has multiple forward extremities.
	async fn resolve_short_state_maps(
		&self,
		room_id: &RoomId,
		room_version_id: &slipstream::RoomVersionId,
		states: &[StateMap],
	) -> Option<StateMap> {
		if states.len() == 1 {
			return states.first().cloned();
		}

		let mut full_states = Vec::with_capacity(states.len());
		for state in states {
			let mut full_state = state_res::StateMap::new();
			for (short_key, event_id) in state {
				let (event_type, state_key) = self
					.services
					.short
					.get_statekey_from_short(*short_key)
					.await
					.ok()?;
				full_state.insert((event_type, state_key), event_id.clone());
			}
			full_states.push(full_state);
		}

		let resolved = self
			.services
			.event_handler
			.state_resolution(room_id, room_version_id, full_states.iter(), None)
			.ok()?;
		let mut short_state = HashMap::with_capacity(resolved.len());
		for ((event_type, state_key), event_id) in resolved {
			let short_key = self
				.services
				.short
				.get_or_create_shortstatekey(&event_type, state_key.as_str())
				.await;
			short_state.insert(short_key, event_id);
		}
		Some(short_state)
	}

	/// The state recorded as the state before `event_id` when it was ingested.
	async fn stored_pre_state(&self, room_id: &RoomId, event_id: &EventId) -> Option<StateMap> {
		let root = self
			.services
			.state_accessor
			.pdu_roothandle_before_event(event_id)
			.await
			.ok()?;
		self.services
			.state_accessor
			.state_full_ids_hamt(room_id, &root)
			.try_collect::<StateMap>()
			.await
			.ok()
	}

	/// The full state stored for `event_id`, if it has a snapshot.
	async fn stored_state(&self, room_id: &RoomId, event_id: &EventId) -> Option<StateMap> {
		let shorteventid = self.services.short.get_shorteventid(event_id).await.ok()?;
		let root = self
			.services
			.state
			.get_roothandle(shorteventid)
			.await
			.ok()?;
		self.services
			.state_accessor
			.state_full_ids_hamt(room_id, &root)
			.try_collect::<StateMap>()
			.await
			.ok()
	}

	async fn was_joined(&self, state: &StateMap, user_id: &UserId) -> bool {
		let Ok(key) = self
			.services
			.short
			.get_shortstatekey(&StateEventType::RoomMember, user_id.as_str())
			.await
		else {
			return false;
		};
		let Some(id) = state.get(&key) else {
			return false;
		};
		self.services.timeline.get_pdu(id).await.is_ok_and(|pdu| {
			pdu.get_content::<RoomMemberEventContent>()
				.is_ok_and(|c| c.membership == MembershipState::Join)
		})
	}

	/// Fix device-list tracking for members whose join/leave differs between the
	/// state held during the partial window and the corrected state.
	pub(super) async fn reconcile_tracking(
		&self,
		room_id: &RoomId,
		old: &StateMap,
		new: &StateMap,
	) {
		let member_type = StateEventType::RoomMember;
		for (key, new_id) in new {
			if old.get(key) == Some(new_id) {
				continue;
			}
			let Ok((ty, state_key)) = self.services.short.get_statekey_from_short(*key).await
			else {
				continue;
			};
			if ty != member_type {
				continue;
			}
			let Ok(user_id) = UserId::parse(state_key.as_str()) else {
				continue;
			};
			if self.services.globals.user_is_local(&user_id) {
				continue;
			}
			let Ok(pdu) = self.services.timeline.get_pdu(new_id).await else {
				continue;
			};
			let joined = pdu
				.get_content::<RoomMemberEventContent>()
				.is_ok_and(|c| c.membership == MembershipState::Join);
			info!(target: "reauth_debug", %user_id, joined, "membership differs after resync");
			if joined {
				self.services.users.track_remote_user(&user_id, room_id);
			} else {
				self.services.users.untrack_remote_user(&user_id, room_id);
				// Whoever still shares another room with us must refetch: their cached
				// keys may have missed updates while we wrongly tracked them.
				self.services.users.mark_device_key_update(&user_id).await;
			}
		}
	}
}
