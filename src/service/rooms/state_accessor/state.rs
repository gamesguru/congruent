use conduwuit::{
	Pdu, Result, err, implement,
	matrix::{Event, StateKey, state_res::StateMap},
	utils::stream::{IterStream, ReadyExt, TryIgnore},
};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use slipstream::{
	EventId, OwnedEventId, RoomId, UserId,
	events::{
		StateEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
};

use crate::rooms::{
	short::ShortStateKey,
	state_hamt::{StateHamtKey, StateHamtValue, hamt_key, parse_hamt_entry, room_structural_key},
};

/// The user was a joined member at this state (potentially in the past)
#[implement(super::Service)]
#[inline]
pub async fn user_was_joined_hamt(
	&self,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	user_id: &UserId,
) -> bool {
	self.user_membership_hamt(room_id, root_handle, user_id)
		.await == MembershipState::Join
}

/// The user was an invited or joined room member at this state (potentially
/// in the past)
#[implement(super::Service)]
#[inline]
pub async fn user_was_invited_hamt(
	&self,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	user_id: &UserId,
) -> bool {
	let s = self
		.user_membership_hamt(room_id, root_handle, user_id)
		.await;
	s == MembershipState::Join || s == MembershipState::Invite
}

/// Get membership for given user in state at a HAMT root.
#[implement(super::Service)]
pub async fn user_membership_hamt(
	&self,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	user_id: &UserId,
) -> MembershipState {
	self.state_get_content_hamt(
		room_id,
		root_handle,
		&StateEventType::RoomMember,
		user_id.as_str(),
	)
	.await
	.map_or(MembershipState::Leave, |c: RoomMemberEventContent| c.membership)
}

/// Get a user's membership at the point in time when a specific event was
/// created.
///
/// Resolves the event's state snapshot via `pdu_roothandle_before_event`, then
/// queries the membership from that HAMT root. Falls back to the current
/// membership from `state_cache` when no snapshot is available (e.g.
/// backfilled events without state).
#[implement(super::Service)]
pub async fn user_membership_at_event(
	&self,
	event_id: &EventId,
	room_id: &RoomId,
	user_id: &UserId,
) -> MembershipState {
	if let Ok(root_handle) = self.pdu_roothandle_before_event(event_id).await {
		self.user_membership_hamt(room_id, &root_handle, user_id)
			.await
	} else {
		// No state snapshot... fall back to current membership
		self.services
			.state_cache
			.user_membership(user_id, room_id)
			.await
			.unwrap_or(MembershipState::Leave)
	}
}

/// Returns a single PDU from `room_id` with key (`event_type`,`state_key`)
/// at a HAMT root.
#[implement(super::Service)]
pub async fn state_get_content_hamt<T>(
	&self,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<T>
where
	T: slipstream::codec::Deserialize,
{
	self.state_get_in_room_hamt(room_id, root_handle, event_type, state_key)
		.await
		.and_then(|event| event.get_content())
}

#[implement(super::Service)]
/// Tests whether a HAMT root contains any state event of `event_type`.
pub async fn state_contains_type_hamt(
	&self,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	event_type: &StateEventType,
) -> bool {
	std::future::ready(()).await;
	let event_type = event_type.to_string();
	let structural_key = room_structural_key(room_id);
	let store = &self.services.state_hamt.store;
	let Ok(root_node) =
		store.get_node(room_id.as_bytes(), &structural_key, &root_handle.structural_hash)
	else {
		return false;
	};
	let mut resolver = store.get_room_resolver(room_id.as_bytes(), &structural_key);
	root_node
		.any_entry(&mut resolver, &mut |(ty, _), _| Ok(*ty == event_type))
		.unwrap_or(false)
}

#[implement(super::Service)]
/// Resolves the event ID for a state key at a HAMT root.
pub async fn state_get_event_id_hamt(
	&self,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<OwnedEventId> {
	std::future::ready(()).await;
	let structural_key = room_structural_key(room_id);
	let store = &self.services.state_hamt.store;
	let root_node =
		store.get_node(room_id.as_bytes(), &structural_key, &root_handle.structural_hash)?;
	let mut resolver = store.get_room_resolver(room_id.as_bytes(), &structural_key);
	let event_id = root_node
		.search(&structural_key, &hamt_key(event_type, state_key), &mut resolver)
		.map_err(|e| err!(error!("HAMT lookup failed: {e:?}")))?
		.ok_or_else(|| err!(Request(NotFound("Not found in room state"))))?;
	OwnedEventId::try_from(event_id)
		.map_err(|e| err!(Database(error!("Invalid event ID in state HAMT leaf: {e}"))))
}

#[implement(super::Service)]
/// Resolves a state PDU from a HAMT root.
pub async fn state_get_in_room_hamt(
	&self,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<Pdu> {
	let event_id = self
		.state_get_event_id_hamt(room_id, root_handle, event_type, state_key)
		.await?;
	self.services
		.timeline
		.get_pdu_in_room(Some(room_id), &event_id)
}

/// Returns a single EventId at the given HAMT root with key
/// (`event_type`, `state_key`).
#[implement(super::Service)]
pub async fn state_get_id_hamt(
	&self,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<OwnedEventId> {
	self.state_get_event_id_hamt(room_id, root_handle, event_type, state_key)
		.await
}

/// Returns a PDU from `room_id` with key `(event_type, state_key)` via HAMT.
#[implement(super::Service)]
pub async fn room_state_get_hamt_legacy(
	&self,
	room_id: &RoomId,
	event_type: &StateEventType,
	state_key: &str,
) -> Result<std::sync::Arc<conduwuit::PduEvent>> {
	let root_handle = self.services.state.get_room_state_hamt(room_id).await?;
	let event_id = self
		.state_get_event_id_hamt(room_id, &root_handle, event_type, state_key)
		.await?;
	self.services
		.timeline
		.get_pdu(&event_id)
		.map(std::sync::Arc::new)
}

/// Returns a Stream of all `(shortstatekey, event_id)` for a given RootHandle.
///
/// Traversal errors are emitted as `Err` items rather than being swallowed,
/// so a caller that must fail closed can observe an unavailable/partial state
/// instead of silently receiving incomplete data. The short state keys are
/// this server's local index; the HAMT itself holds the strings.
#[implement(super::Service)]
pub fn state_full_ids_hamt<'a>(
	&'a self,
	room_id: &'a RoomId,
	root_handle: &'a rezzy::hamt::RootHandle,
) -> futures::stream::BoxStream<'a, Result<(ShortStateKey, OwnedEventId)>> {
	self.state_full_entries_hamt(room_id, root_handle.clone())
		.map_ok(move |((event_type, state_key), event_id)| {
			let shortstatekey = self
				.services
				.short
				.get_or_create_shortstatekey_blocking(&event_type, &state_key);
			(shortstatekey, event_id)
		})
		.boxed()
}

/// Streams every `((event_type, state_key), event_id)` in a HAMT root, sorted
/// by `(event_type, state_key)`.
///
/// Traversal errors are emitted as `Err` items.
#[implement(super::Service)]
pub fn state_full_entries_hamt<'a>(
	&'a self,
	room_id: &'a RoomId,
	root_handle: rezzy::hamt::RootHandle,
) -> futures::stream::BoxStream<'a, Result<((StateEventType, StateKey), OwnedEventId)>> {
	let load = async move {
		let mut entries = self.load_state_hamt_leaves(room_id, &root_handle)?;
		// Sorting keeps iteration deterministic (e.g. a space's
		// `children_state`); the HAMT's own order follows routing hashes.
		entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
		entries
			.iter()
			.map(|(key, value)| parse_hamt_entry(key, value))
			.collect::<Result<Vec<_>>>()
	};
	load.map_ok(Vec::into_iter)
		.map_ok(IterStream::try_stream)
		.try_flatten_stream()
		.boxed()
}

/// Returns a Stream of all the full state PDUs for a given RootHandle.
#[implement(super::Service)]
pub fn state_full_pdus_hamt<'a>(
	&'a self,
	room_id: &'a RoomId,
	root_handle: rezzy::hamt::RootHandle,
) -> impl Stream<Item = impl Event> + Send + 'a {
	self.state_full_entries_hamt(room_id, root_handle)
		.ignore_err()
		.map(move |(_, event_id)| async move { self.services.timeline.get_pdu(&event_id).ok() })
		.buffer_unordered(self.services.server.concurrency_scaled(32))
		.filter_map(std::future::ready)
}

/// Strict variant of [`Self::state_full_pdus_hamt`]: traversal and PDU lookup
/// failures are emitted as `Err` items instead of being dropped, so callers
/// that cache or serve a snapshot can fail closed on partial state.
#[implement(super::Service)]
pub fn state_full_pdus_hamt_strict<'a>(
	&'a self,
	room_id: &'a RoomId,
	root_handle: rezzy::hamt::RootHandle,
) -> impl Stream<Item = Result<Pdu>> + Send + 'a {
	self.state_full_entries_hamt(room_id, root_handle)
		.map(move |result| async move {
			let (_, event_id) = result?;
			self.services.timeline.get_pdu(&event_id)
		})
		.buffer_unordered(self.services.server.concurrency_scaled(32))
}

/// Returns a Stream of all the full state (type, key, event) for a given
/// RootHandle.
#[implement(super::Service)]
pub fn state_full_hamt<'a>(
	&'a self,
	room_id: &'a RoomId,
	root_handle: rezzy::hamt::RootHandle,
) -> impl Stream<Item = ((StateEventType, StateKey), impl Event)> + Send + 'a {
	self.state_full_pdus_hamt(room_id, root_handle)
		.ready_filter_map(|pdu| Some(((pdu.kind().clone().into(), pdu.state_key()?.into()), pdu)))
}

/// Iterates the state_keys for an event_type at a HAMT root; current state
/// event_id included.
#[implement(super::Service)]
pub fn state_keys_with_ids_hamt<'a>(
	&'a self,
	room_id: &'a RoomId,
	root_handle: rezzy::hamt::RootHandle,
	event_type: &'a StateEventType,
) -> impl Stream<Item = (StateKey, OwnedEventId)> + Send + 'a {
	self.state_full_entries_hamt(room_id, root_handle)
		.ignore_err()
		.ready_filter_map(move |((event_type_, state_key), event_id)| {
			(event_type_ == *event_type).then_some((state_key, event_id))
		})
}

#[implement(super::Service)]
#[inline]
/// Returns entries present in the first root and absent from the second.
pub fn state_removed_hamt(
	&self,
	room_id: &RoomId,
	root_handles: (&rezzy::hamt::RootHandle, &rezzy::hamt::RootHandle),
) -> Result<Vec<((StateEventType, StateKey), OwnedEventId)>> {
	self.state_added_hamt(room_id, (root_handles.1, root_handles.0))
}

#[implement(super::Service)]
/// Returns entries present in the second root and absent from the first.
pub fn state_added_hamt(
	&self,
	room_id: &RoomId,
	root_handles: (&rezzy::hamt::RootHandle, &rezzy::hamt::RootHandle),
) -> Result<Vec<((StateEventType, StateKey), OwnedEventId)>> {
	let structural_key = room_structural_key(room_id);
	let store = &self.services.state_hamt.store;
	let root_a =
		store.get_node(room_id.as_bytes(), &structural_key, &root_handles.0.structural_hash)?;
	let root_b =
		store.get_node(room_id.as_bytes(), &structural_key, &root_handles.1.structural_hash)?;
	let mut resolver = store.get_room_resolver(room_id.as_bytes(), &structural_key);
	let (added, _removed) = rezzy::hamt::delta::diff_hamt_nodes::<
		StateHamtKey,
		StateHamtValue,
		_,
		conduwuit::Error,
	>(&root_a, &root_b, &mut resolver)
	.map_err(|e| match e {
		| rezzy::hamt::delta::HamtTraversalError::Resolve(inner) => inner,
		| rezzy::hamt::delta::HamtTraversalError::MaxDepthExceeded { depth } => {
			err!(error!("HAMT diff exceeded max depth at {depth}"))
		},
	})?;

	added
		.iter()
		.map(|(key, value)| parse_hamt_entry(key, value))
		.collect()
}

#[implement(super::Service)]
/// Tests whether a HAMT root has no entries.
pub fn state_is_empty_hamt(
	&self,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
) -> Result<bool> {
	let structural_key = room_structural_key(room_id);
	let root_node = self.services.state_hamt.store.get_node(
		room_id.as_bytes(),
		&structural_key,
		&root_handle.structural_hash,
	)?;

	// O(1) structural check on the root's datamap/nodemap bitmaps, rather than
	// materializing the full tree to test `.is_empty()`.
	Ok(root_node.is_empty())
}

#[implement(super::Service)]
/// Collects the raw leaves stored beneath a HAMT root.
pub fn load_state_hamt_leaves(
	&self,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
) -> Result<Vec<(StateHamtKey, StateHamtValue)>> {
	let structural_key = room_structural_key(room_id);
	let store = &self.services.state_hamt.store;
	let root_node =
		store.get_node(room_id.as_bytes(), &structural_key, &root_handle.structural_hash)?;
	let mut leaves = Vec::new();
	let mut resolver = store.get_room_resolver(room_id.as_bytes(), &structural_key);
	root_node
		.visit_entries(&mut resolver, &mut |key, value| {
			leaves.push((key.clone(), value.clone()));
			Ok(())
		})
		.map_err(|e| err!(error!("HAMT visit failed: {e:?}")))?;
	Ok(leaves)
}

#[implement(super::Service)]
/// Materializes all state entries stored beneath a HAMT root.
pub fn load_full_state_hamt(
	&self,
	room_id: &RoomId,
	root_handle: &rezzy::hamt::RootHandle,
) -> Result<StateMap<OwnedEventId>> {
	self.load_state_hamt_leaves(room_id, root_handle)?
		.iter()
		.map(|(key, value)| parse_hamt_entry(key, value))
		.collect()
}

/// Returns the HAMT RootHandle for the state *after* this pdu, i.e. including
/// the pdu's own state change.
#[implement(super::Service)]
pub async fn pdu_roothandle_after_event(
	&self,
	event_id: &EventId,
) -> Result<rezzy::hamt::RootHandle> {
	let shorteventid = self.services.short.get_shorteventid(event_id).await?;
	self.services.state.get_roothandle(shorteventid).await
}

/// Returns the HAMT RootHandle for the state immediately *before* this pdu's
/// own state change was applied (matching the legacy `pdu_shortstatehash`
/// semantics).
///
/// `pdu_roothandle_after_event` resolves the state *after* a PDU, which for
/// state events (e.g. a membership leave) already includes the event's own
/// change, so a user/server that was joined before it would no longer pass a
/// `user_was_joined` check. Falls back to the post-event root when the event
/// has multiple predecessors; the first event in a room has the empty state
/// before it.
#[implement(super::Service)]
pub async fn pdu_roothandle_before_event(
	&self,
	event_id: &EventId,
) -> Result<rezzy::hamt::RootHandle> {
	// State before an event is determined by its DAG predecessors, not by the
	// event that happens to precede it in the repaired linear timeline. The
	// latter can be a concurrent fork and therefore carry unrelated state.
	let pdu = self.services.timeline.get_pdu(event_id)?;

	// A non-state event doesn't change state, so its stored root *is* the state
	// before it. For a merge event this is the resolved state of all its
	// parents; picking one parent's root would return that fork's state alone.
	if pdu.state_key.is_none() {
		return self.pdu_roothandle_after_event(event_id).await;
	}

	// An event with no predecessors (the room's first event) is preceded by
	// the empty state; returning its own post-event root would leak the event's
	// state change into the "before" view.
	if pdu.prev_events().next().is_none() {
		if let Some(room_id) = pdu.room_id() {
			let structural_key = room_structural_key(room_id);
			let (empty_root, empty_node) = rezzy::hamt::build_hamt_root_handle(
				&structural_key,
				&rezzy::incremental::LtHash::default(),
				Vec::new(),
			)
			.map_err(|e| err!(error!("Failed to build empty HAMT root: {e:?}")))?;
			self.services
				.state_hamt
				.store
				.persist_node_recursive(room_id.as_bytes(), empty_node)?;

			return Ok(empty_root);
		}
	}

	if pdu.prev_events().count() == 1 {
		if let Some(prev_event_id) = pdu.prev_events().next() {
			let shorteventid = self.services.short.get_shorteventid(prev_event_id).await?;
			if let Ok(root_handle) = self.services.state.get_roothandle(shorteventid).await {
				return Ok(root_handle);
			}
		}
	}

	self.pdu_roothandle_after_event(event_id).await
}
