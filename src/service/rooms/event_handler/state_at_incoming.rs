use std::{collections::HashMap, iter::Iterator};

use conduwuit::{
	Result, debug, err, implement,
	matrix::{Event, StateMap},
	trace,
	utils::stream::{BroadbandExt, IterStream, ReadyExt, TryBroadbandExt, TryWidebandExt},
};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use slipstream::{EventId, OwnedEventId, RoomId, RoomVersionId};

// TODO: if we know the prev_events of the incoming event we can avoid the
#[implement(super::Service)]
// request and build the state from a known point and resolve if > 1 prev_event
pub(crate) async fn state_at_incoming_degree_one<Pdu>(
	&self,
	incoming_pdu: &Pdu,
	room_id: &RoomId,
) -> Result<Option<HashMap<u64, OwnedEventId>>>
where
	Pdu: Event + Send + Sync,
{
	let prev_event = incoming_pdu
		.prev_events()
		.next()
		.expect("at least one prev_event");

	// Not found locally is a legitimate, common case (the prev_event was never
	// delivered to us, we joined after it, etc.), not a database malfunction.
	// Return None so the caller falls back to fetch_state().
	let Ok(mut prev_pdu) = self
		.services
		.timeline
		.get_pdu_in_room(Some(room_id), prev_event)
		.await
	else {
		debug!("prev_event {prev_event} not found locally; falling back to fetch_state");
		return Ok(None);
	};

	if prev_pdu.room_id() != Some(room_id) {
		return Err(err!(Database("prev_event is not in the same room")));
	}

	// A rejected predecessor changes no state, so the state after it is the
	// state before it. It keeps no snapshot of its own, so walk back through
	// single-parent rejected events to the nearest one that does.
	let mut prev_event = prev_event.to_owned();
	let prev_roothandle = loop {
		if let Ok(handle) = self
			.services
			.state_accessor
			.pdu_roothandle_after_event(&prev_event)
			.await
		{
			break handle;
		}

		if !self
			.services
			.pdu_metadata
			.is_event_rejected(&prev_event)
			.unwrap_or(false)
		{
			return Ok(None);
		}

		let parent = {
			let mut parents = prev_pdu.prev_events();
			let (Some(parent), None) = (parents.next(), parents.next()) else {
				return Ok(None);
			};
			parent.to_owned()
		};
		let Ok(parent_pdu) = self
			.services
			.timeline
			.get_pdu_in_room(Some(room_id), &parent)
			.await
		else {
			return Ok(None);
		};
		prev_event = parent;
		prev_pdu = parent_pdu;
	};
	let prev_event: &EventId = &prev_event;

	let mut state: HashMap<_, _> = self
		.services
		.state_accessor
		.state_full_ids_hamt(room_id, &prev_roothandle)
		.try_collect()
		.await?;

	debug!("Using cached state");

	if let Some(state_key) = &prev_pdu.state_key {
		let shortstatekey = self
			.services
			.short
			.get_or_create_shortstatekey(&prev_pdu.kind().to_string().into(), state_key)
			.await;

		state.insert(shortstatekey, prev_event.to_owned());
		// Now it's the state after the pdu
	}

	debug_assert!(!state.is_empty(), "should be returning None for empty HashMap result");

	Ok(Some(state))
}

#[implement(super::Service)]
pub(crate) async fn state_at_incoming_resolved<Pdu>(
	&self,
	incoming_pdu: &Pdu,
	room_id: &RoomId,
	room_version_id: &RoomVersionId,
) -> Result<Option<HashMap<u64, OwnedEventId>>>
where
	Pdu: Event + Send + Sync,
{
	trace!("Calculating extremity root handles...");
	let Ok(extremity_roothandles) = incoming_pdu
		.prev_events()
		.try_stream()
		.broad_and_then(|prev_eventid| {
			self.services
				.timeline
				.get_pdu_in_room(Some(room_id), prev_eventid)
				.and_then(move |prev_event| async move {
					if prev_event.room_id() != Some(room_id) {
						return Err(err!(Database("prev_event is not in the same room")));
					}
					Ok((prev_eventid, prev_event))
				})
		})
		.broad_and_then(|(prev_eventid, prev_event)| {
			self.services
				.state_accessor
				.pdu_roothandle_after_event(prev_eventid)
				.map_ok(move |root_handle| (root_handle, prev_event))
		})
		.try_collect::<HashMap<_, _>>()
		.await
	else {
		return Ok(None);
	};

	let mut unique_forks = Vec::new();
	let mut all_succeeded = true;
	for (root_handle, prev_event) in &extremity_roothandles {
		match self.get_extremity_lthash(root_handle, prev_event).await {
			| Ok(lthash) =>
				if !unique_forks.iter().any(|(hash, _)| *hash == lthash) {
					unique_forks.push((lthash, (root_handle.clone(), prev_event)));
				},
			| Err(_) => {
				all_succeeded = false;
				break;
			},
		}
	}

	if all_succeeded && unique_forks.len() == 1 && extremity_roothandles.len() > 1 {
		trace!(
			"LtHash digests match across all {} forks! Bypassing state resolution.",
			extremity_roothandles.len()
		);
		let (root_handle, prev_event) = unique_forks[0].1.clone();
		let Ok(fork_state) = self
			.state_at_incoming_fork(room_id, root_handle, prev_event)
			.await
		else {
			return Ok(None);
		};
		return fork_state
			.into_iter()
			.stream()
			.broad_then(|((event_type, state_key), event_id)| async move {
				self.services
					.short
					.get_or_create_shortstatekey(&event_type, &state_key)
					.map(move |shortstatekey| (shortstatekey, event_id))
					.await
			})
			.collect()
			.map(Some)
			.map(Ok)
			.await;
	}

	trace!("Calculating fork states...");
	let fork_states: Vec<StateMap<_>> = extremity_roothandles
		.into_iter()
		.try_stream()
		.wide_and_then(|(root_handle, prev_event)| {
			self.state_at_incoming_fork(room_id, root_handle, prev_event)
		})
		.try_collect()
		.await?;

	let Ok(new_state) = self.state_resolution(room_id, room_version_id, fork_states.iter(), None)
	else {
		return Ok(None);
	};

	new_state
		.into_iter()
		.stream()
		.broad_then(|((event_type, state_key), event_id)| async move {
			self.services
				.short
				.get_or_create_shortstatekey(&event_type, &state_key)
				.map(move |shortstatekey| (shortstatekey, event_id))
				.await
		})
		.collect()
		.map(Some)
		.map(Ok)
		.await
}

#[implement(super::Service)]
async fn state_at_incoming_fork<Pdu>(
	&self,
	room_id: &RoomId,
	root_handle: rezzy::hamt::RootHandle,
	prev_event: Pdu,
) -> Result<StateMap<OwnedEventId>>
where
	Pdu: Event,
{
	let mut leaf_state: HashMap<_, _> = self
		.services
		.state_accessor
		.state_full_ids_hamt(room_id, &root_handle)
		.try_collect()
		.await?;

	if let Some(state_key) = prev_event.state_key() {
		let shortstatekey = self
			.services
			.short
			.get_or_create_shortstatekey(&prev_event.kind().to_string().into(), state_key)
			.await;

		let event_id = prev_event.event_id();
		leaf_state.insert(shortstatekey, event_id.to_owned());
		// Now it's the state after the pdu
	}

	leaf_state
		.iter()
		.stream()
		.broad_then(|(k, id)| {
			self.services
				.short
				.get_statekey_from_short(*k)
				.map_ok(|(ty, sk)| ((ty, sk), id.clone()))
		})
		.ready_filter_map(Result::ok)
		.collect()
		.map(Ok)
		.await
}

#[implement(super::Service)]
async fn get_extremity_lthash<Pdu>(
	&self,
	_root_handle: &rezzy::hamt::RootHandle,
	_prev_event: &Pdu,
) -> Result<rezzy::incremental::LtHash>
where
	Pdu: Event + Send + Sync,
{
	std::future::ready(()).await;
	// TODO(MSC00DC/HAMT): re-implement LtHash retrieval from HAMT store.
	Err(err!(Request(NotImplemented(
		"LtHash retrieval from HAMT store is not yet implemented"
	))))
}

/// Resolves the room state across an explicit set of DAG extremities and
/// returns a freshly-built HAMT `RootHandle`.
///
/// Unlike [`Self::state_at_incoming_resolved`] (which resolves the prev_events
/// of a single incoming PDU), this takes an arbitrary extremity set. It is used
/// by local event creation when the room has diverged: state must be resolved
/// across every fork, not just the room's current-state pointer.
#[implement(super::Service)]
pub(crate) async fn resolve_extremities<'a, I>(
	&self,
	prev_events: I,
	room_id: &RoomId,
	room_version_id: &RoomVersionId,
) -> Result<Option<rezzy::hamt::RootHandle>>
where
	I: Iterator<Item = &'a EventId> + Send,
{
	let Ok(extremity_roothandles) = prev_events
		.try_stream()
		.broad_and_then(|prev_eventid| {
			self.services
				.timeline
				.get_pdu_in_room(Some(room_id), prev_eventid)
				.and_then(move |prev_event| async move {
					if prev_event.room_id() != Some(room_id) {
						return Err(err!(Database("prev_event is not in the same room")));
					}
					Ok((prev_eventid, prev_event))
				})
		})
		.broad_and_then(|(prev_eventid, prev_event)| {
			self.services
				.state_accessor
				.pdu_roothandle_after_event(prev_eventid)
				.map_ok(move |root_handle| (root_handle, prev_event))
		})
		.try_collect::<HashMap<_, _>>()
		.await
	else {
		return Ok(None);
	};

	if extremity_roothandles.is_empty() {
		return Ok(None);
	}

	let fork_states: Vec<StateMap<_>> = extremity_roothandles
		.into_iter()
		.try_stream()
		.wide_and_then(|(root_handle, prev_event)| {
			self.state_at_incoming_fork(room_id, root_handle, prev_event)
		})
		.try_collect()
		.await?;

	let Ok(new_state) = self.state_resolution(room_id, room_version_id, fork_states.iter(), None)
	else {
		return Ok(None);
	};

	// Build a HAMT root handle from the resolved state.
	let mut lattice = rezzy::incremental::LtHash::default();
	let mut entries = Vec::with_capacity(new_state.len());
	for ((ty, sk), id) in &new_state {
		lattice.insert(ty.to_string().as_str(), sk.as_str(), id.as_str());

		let shortstatekey = self
			.services
			.short
			.get_or_create_shortstatekey(ty, sk)
			.await;
		let shorteventid = self.services.short.get_or_create_shorteventid(id).await;
		entries.push((shortstatekey, shorteventid));
	}

	let structural_key = crate::rooms::state_hamt::room_structural_key(
		&self.services.globals.server_secret,
		room_id,
	);
	let (root_handle, root_node) =
		rezzy::hamt::build_hamt_root_handle(&structural_key, &lattice, entries)
			.map_err(|e| err!("Failed to build HAMT root: {e:?}"))?;

	self.services
		.state_hamt
		.store
		.persist_node_recursive(room_id.as_bytes(), root_node)?;

	Ok(Some(root_handle))
}

/// The state before an event with several predecessors, one of which we have
/// no state for but whose state after it was fetched from the sender.
///
/// The fetched state stands in for that predecessor's own; the others come from
/// what we hold. Returns `None` if any other predecessor lacks state too.
#[implement(super::Service)]
pub(crate) async fn state_at_incoming_with_fetched_prev<Pdu>(
	&self,
	incoming_pdu: &Pdu,
	room_id: &RoomId,
	room_version_id: &RoomVersionId,
	fetched_prev: &EventId,
	fetched_state: &HashMap<u64, OwnedEventId>,
) -> Result<Option<HashMap<u64, OwnedEventId>>>
where
	Pdu: Event + Send + Sync,
{
	let mut forks: Vec<StateMap<OwnedEventId>> = Vec::new();
	for prev in incoming_pdu.prev_events() {
		if prev == fetched_prev {
			let fork: StateMap<OwnedEventId> = fetched_state
				.iter()
				.stream()
				.broad_then(|(k, id)| {
					self.services
						.short
						.get_statekey_from_short(*k)
						.map_ok(|(ty, sk)| ((ty, sk), id.clone()))
				})
				.ready_filter_map(Result::ok)
				.collect()
				.await;
			forks.push(fork);
			continue;
		}

		let Ok(prev_event) = self
			.services
			.timeline
			.get_pdu_in_room(Some(room_id), prev)
			.await
		else {
			return Ok(None);
		};
		let Ok(root_handle) = self
			.services
			.state_accessor
			.pdu_roothandle_after_event(prev)
			.await
		else {
			return Ok(None);
		};
		forks.push(
			self.state_at_incoming_fork(room_id, root_handle, prev_event)
				.await?,
		);
	}

	let Some(first) = forks.first() else {
		return Ok(None);
	};
	let resolved = if forks.iter().all(|fork| fork == first) {
		first.clone()
	} else {
		let Ok(state) = self.state_resolution(room_id, room_version_id, forks.iter(), None)
		else {
			return Ok(None);
		};
		state
	};

	resolved
		.into_iter()
		.stream()
		.broad_then(|((event_type, state_key), event_id)| async move {
			self.services
				.short
				.get_or_create_shortstatekey(&event_type, &state_key)
				.map(move |shortstatekey| (shortstatekey, event_id))
				.await
		})
		.collect()
		.map(Some)
		.map(Ok)
		.await
}
