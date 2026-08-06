use std::{
	borrow::Borrow,
	collections::{HashMap, HashSet},
	iter::Iterator,
};

use conduwuit::{
	Err, Result, debug, err, implement,
	matrix::{Event, StateMap},
	trace,
	utils::stream::{BroadbandExt, IterStream, ReadyExt, TryBroadbandExt, TryWidebandExt},
};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt, future::try_join};
use ruma::{OwnedEventId, RoomId, RoomVersionId};

use crate::rooms::short::ShortStateHash;

// TODO: if we know the prev_events of the incoming event we can avoid the
#[implement(super::Service)]
// request and build the state from a known point and resolve if > 1 prev_event
#[tracing::instrument(name = "state", level = "debug", skip_all)]
pub(super) async fn state_at_incoming_degree_one<Pdu>(
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

	// Not found locally is a legitimate, common case -- e.g. the prev_event was
	// never delivered to us (a prior transaction was rejected, we joined the
	// room after it, etc.), not a database malfunction. Fall through to the
	// caller's fetch_state() federation fallback instead of hard-failing the
	// whole incoming event, matching how state_at_incoming_resolved's
	// multi-prev-event sibling path already degrades on the equivalent lookup.
	let Ok(prev_pdu) = self
		.services
		.timeline
		.get_pdu_in_room(Some(room_id), prev_event)
		.await
	else {
		debug!("prev_event {prev_event} not found locally; falling back to fetch_state");
		return Ok(None);
	};

	// Unlike "not found locally", a prev_event that resolves to a PDU in a
	// *different* room isn't a delivery gap -- there's no missing state a
	// federation round trip could supply that would make this legitimate. It's
	// a malformed or hostile event. Falling back to fetch_state here would
	// turn a single cheap-to-craft event into a guaranteed outbound
	// /state_ids request per inbound event, at whatever rate a malicious
	// origin cares to push transactions. Hard reject instead.
	if prev_pdu.room_id() != Some(room_id) {
		return Err!(Database("prev_event {prev_event} claims a different room than {room_id}"));
	}

	let Ok(prev_event_sstatehash) = self
		.services
		.state_accessor
		.pdu_shortstatehash(prev_event)
		.await
	else {
		return Ok(None);
	};

	let mut state: HashMap<_, _> = self
		.services
		.state_accessor
		.state_full_ids(prev_event_sstatehash)
		.collect()
		.await;

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
#[tracing::instrument(name = "state", level = "debug", skip_all)]
pub(super) async fn state_at_incoming_resolved<Pdu>(
	&self,
	incoming_pdu: &Pdu,
	room_id: &RoomId,
	room_version_id: &RoomVersionId,
) -> Result<Option<HashMap<u64, OwnedEventId>>>
where
	Pdu: Event + Send + Sync,
{
	trace!("Calculating extremity statehashes...");
	let Ok(extremity_sstatehashes) = incoming_pdu
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
				.pdu_shortstatehash(prev_eventid)
				.map_ok(move |sstatehash| (sstatehash, prev_event))
		})
		.try_collect::<HashMap<_, _>>()
		.await
	else {
		return Ok(None);
	};

	trace!("Calculating fork states...");
	let (fork_states, auth_chain_sets): (Vec<StateMap<_>>, Vec<HashSet<_>>) =
		extremity_sstatehashes
			.into_iter()
			.try_stream()
			.wide_and_then(|(sstatehash, prev_event)| {
				self.state_at_incoming_fork(room_id, sstatehash, prev_event)
			})
			.try_collect()
			.map_ok(Vec::into_iter)
			.map_ok(Iterator::unzip)
			.await?;

	let Ok(new_state) = self
		.state_resolution(room_id, room_version_id, fork_states.iter(), &auth_chain_sets)
		.boxed()
		.await
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
	sstatehash: ShortStateHash,
	prev_event: Pdu,
) -> Result<(StateMap<OwnedEventId>, HashSet<OwnedEventId>)>
where
	Pdu: Event,
{
	let mut leaf_state: HashMap<_, _> = self
		.services
		.state_accessor
		.state_full_ids(sstatehash)
		.collect()
		.await;

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

	let auth_chain = self
		.services
		.auth_chain
		.event_ids_iter(room_id, leaf_state.values().map(Borrow::borrow))
		.try_collect();

	let fork_state = leaf_state
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
		.map(Ok);

	try_join(fork_state, auth_chain).await
}
