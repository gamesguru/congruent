use std::{
	collections::{HashMap, HashSet},
	iter::Iterator,
};

use conduwuit::{
	Err, Result, debug, err, implement,
	matrix::{Event, PduEvent, StateKey, StateMap},
	trace,
	utils::stream::{IterStream, TryBroadbandExt},
};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt, future::ready};
use ruma::{
	EventId, OwnedEventId, RoomId, RoomVersionId,
	events::{StateEventType, TimelineEventType},
};

use super::resolve_state::PduCache;

/// Looks up the PDU for a single `(StateEventType, StateKey)` within a
/// compressed fork state, going through the short-ID tables. Shared by
/// callers that need to resolve auth events out of a `CompressedState`
/// (e.g. state resolution over diverged forward extremities) so the
/// shortstatekey/shorteventid unpacking isn't duplicated at each call site.
#[implement(super::Service)]
pub async fn find_pdu_in_compressed_state(
	&self,
	state_ty: &StateEventType,
	state_key: &StateKey,
	compressed_state: &crate::rooms::state_compressor::CompressedState,
) -> Option<PduEvent> {
	let shortstatekey = self
		.services
		.short
		.get_shortstatekey(state_ty, state_key)
		.await
		.ok()?;

	let event_bytes = compressed_state
		.iter()
		.find(|bytes| bytes.starts_with(&shortstatekey.to_be_bytes()))?;

	let mut id_bytes = [0_u8; 8];
	id_bytes.copy_from_slice(&event_bytes[8..16]);
	let shorteventid = u64::from_be_bytes(id_bytes);

	let event_id = self
		.services
		.short
		.get_eventid_from_short::<OwnedEventId>(shorteventid)
		.await
		.ok()?;

	self.services.timeline.get_pdu(&event_id).await.ok()
}

// TODO: if we know the prev_events of the incoming event we can avoid the
// request and build the state from a known point and resolve if > 1 prev_event
#[implement(super::Service)]
#[tracing::instrument(name = "state", level = "debug", skip_all)]
pub(super) async fn state_at_incoming_degree_one<Pdu>(
	&self,
	incoming_pdu: &Pdu,
	room_id: &RoomId,
) -> Result<Option<std::sync::Arc<crate::rooms::state_compressor::CompressedState>>>
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

	let mut state = self
		.services
		.state_compressor
		.load_shortstatehash_info(prev_event_sstatehash)
		.await?
		.pop()
		.unwrap()
		.full_state
		.unwrap()
		.as_ref()
		.clone();

	debug!("Using cached state");

	if let Some(state_key) = &prev_pdu.state_key {
		let shortstatekey = self
			.services
			.short
			.get_or_create_shortstatekey(&prev_pdu.kind().to_string().into(), state_key)
			.await;
		let shorteventid = self
			.services
			.short
			.get_or_create_shorteventid(prev_event)
			.await;

		let old_compressed = state
			.iter()
			.find(|bytes| bytes.starts_with(&shortstatekey.to_be_bytes()))
			.copied();
		if let Some(old) = old_compressed {
			state.remove(&old);
		}
		state.insert(crate::rooms::state_compressor::compress_state_event(
			shortstatekey,
			shorteventid,
		));
		// Now it's the state after the pdu
	}

	debug_assert!(!state.is_empty(), "should be returning None for empty CompressedState result");

	Ok(Some(std::sync::Arc::new(state)))
}

#[implement(super::Service)]
#[tracing::instrument(name = "state", level = "debug", skip_all)]
pub async fn state_at_incoming_resolved<Pdu>(
	&self,
	incoming_pdu: &Pdu,
	room_id: &RoomId,
	room_version_id: &RoomVersionId,
	prefetch_cache: Option<PduCache>,
) -> Result<Option<std::sync::Arc<crate::rooms::state_compressor::CompressedState>>>
where
	Pdu: Event + Send + Sync,
{
	self.resolve_extremities(incoming_pdu.prev_events(), room_id, room_version_id, prefetch_cache)
		.await
}

#[implement(super::Service)]
#[tracing::instrument(name = "state", level = "debug", skip_all)]
pub async fn resolve_extremities<'a, I>(
	&self,
	prev_events: I,
	room_id: &RoomId,
	room_version_id: &RoomVersionId,
	prefetch_cache: Option<PduCache>,
) -> Result<Option<std::sync::Arc<crate::rooms::state_compressor::CompressedState>>>
where
	I: Iterator<Item = &'a EventId> + Send,
{
	let fn_start = std::time::Instant::now();
	trace!("Calculating extremity statehashes...");
	let Ok(extremity_sstatehashes) = prev_events
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
		.try_collect::<Vec<(u64, PduEvent)>>()
		.await
	else {
		return Ok(None);
	};

	let mut fork_compressed_states = Vec::with_capacity(extremity_sstatehashes.len());
	for &(sstatehash, ref prev_event) in &extremity_sstatehashes {
		let mut state = self
			.services
			.state_compressor
			.load_shortstatehash_info(sstatehash)
			.await?
			.pop()
			.unwrap()
			.full_state
			.unwrap()
			.as_ref()
			.clone();

		if let Some(state_key) = prev_event.state_key() {
			let shortstatekey = self
				.services
				.short
				.get_or_create_shortstatekey(&prev_event.kind().to_string().into(), state_key)
				.await;
			let shorteventid = self
				.services
				.short
				.get_or_create_shorteventid(prev_event.event_id())
				.await;

			let old_compressed = state
				.iter()
				.find(|bytes| bytes.starts_with(&shortstatekey.to_be_bytes()))
				.copied();
			if let Some(old) = old_compressed {
				state.remove(&old);
			}
			state.insert(crate::rooms::state_compressor::compress_state_event(
				shortstatekey,
				shorteventid,
			));
		}
		fork_compressed_states.push(state);
	}

	fork_compressed_states.sort();
	fork_compressed_states.dedup();
	let num_forks = fork_compressed_states.len();
	trace!("Calculating fork states ({num_forks} forks)...");

	// Build ssk → set of (shorteventid) values across ALL forks.
	// A key is only truly conflicting if multiple forks assign it DIFFERENT values.
	// Keys present in only one fork are additions — auto-merged, no resolution
	// needed.
	let mut ssk_values: HashMap<u64, HashSet<u64>> = HashMap::new();
	for fork in &fork_compressed_states {
		for bytes in fork {
			let mut ssk_bytes = [0_u8; 8];
			ssk_bytes.copy_from_slice(&bytes[0..8]);
			let ssk = u64::from_be_bytes(ssk_bytes);

			let mut id_bytes = [0_u8; 8];
			id_bytes.copy_from_slice(&bytes[8..16]);
			let sei = u64::from_be_bytes(id_bytes);

			ssk_values.entry(ssk).or_default().insert(sei);
		}
	}

	let conflicting_ssks: HashSet<u64> = ssk_values
		.iter()
		.filter(|(_, values)| values.len() > 1)
		.map(|(ssk, _)| *ssk)
		.collect();

	let non_conflicting_additions = ssk_values.len().saturating_sub(conflicting_ssks.len());

	println!(
		"state_at_incoming_resolved: {num_forks} forks, {} truly conflicting keys, {} \
		 auto-merged additions, {} total ssk (took {:?} to compute)",
		conflicting_ssks.len(),
		non_conflicting_additions,
		ssk_values.len(),
		fn_start.elapsed(),
	);

	if conflicting_ssks.is_empty() {
		// No conflicting keys — build merged state from all forks' entries
		println!("state_at_incoming_resolved: TRIVIAL MERGE (0 conflicts) — skipping resolution");
		let mut state_map = std::collections::BTreeSet::new();
		// Collect the winning value for each ssk (all forks agree or it's a unique
		// addition)
		for fork in &fork_compressed_states {
			for bytes in fork {
				state_map.insert(*bytes);
			}
		}
		return Ok(Some(std::sync::Arc::new(state_map)));
	}

	// Determine which state keys are auth-critical (affects resolution outcome)
	let mut auth_ssks = HashSet::new();
	for ty in &[
		StateEventType::RoomCreate,
		StateEventType::RoomPowerLevels,
		StateEventType::RoomJoinRules,
		StateEventType::RoomServerAcl,
	] {
		if let Ok(ssk) = self.services.short.get_shortstatekey(ty, "").await {
			auth_ssks.insert(ssk);
		}
	}

	// All conflicts go through full state resolution to ensure correctness.
	// A previous "FAST PATH" optimization existed here that bypassed resolution
	// for non-auth conflicts by picking winners via (origin_server_ts, event_id),
	// but that heuristic doesn't match the V2 mainline sort algorithm and
	// produced incorrect results when timestamps were equal.

	let mut conflicting_event_ids = HashSet::new();
	for fork in &fork_compressed_states {
		for ssk in &conflicting_ssks {
			let event_bytes = fork
				.iter()
				.find(|bytes| bytes.starts_with(&ssk.to_be_bytes()));
			if let Some(bytes) = event_bytes {
				let mut id_bytes = [0_u8; 8];
				id_bytes.copy_from_slice(&bytes[8..16]);
				let shorteventid = u64::from_be_bytes(id_bytes);
				if let Ok(eid) = self
					.services
					.short
					.get_eventid_from_short(shorteventid)
					.await
				{
					conflicting_event_ids.insert(eid);
				}
			}
		}
	}

	let conflicting_pdus: Vec<_> = self
		.services
		.timeline
		.multi_get_pdus(Some(room_id), futures::stream::iter(conflicting_event_ids.into_iter()))
		.filter_map(|r| ready(r.ok()))
		.collect()
		.await;

	// Extend auth_ssks with sender membership keys
	for pdu in conflicting_pdus {
		if let Ok(ssk) = self
			.services
			.short
			.get_shortstatekey(&StateEventType::RoomMember, pdu.sender().as_ref())
			.await
		{
			auth_ssks.insert(ssk);
		}
		if pdu.kind() == &TimelineEventType::RoomMember {
			if let Some(sk) = pdu.state_key() {
				if let Ok(ssk) = self
					.services
					.short
					.get_shortstatekey(&StateEventType::RoomMember, sk)
					.await
				{
					auth_ssks.insert(ssk);
				}
			}
		}
		if pdu.kind() == &TimelineEventType::RoomThirdPartyInvite {
			if let Some(sk) = pdu.state_key() {
				if let Ok(ssk) = self
					.services
					.short
					.get_shortstatekey(&StateEventType::RoomThirdPartyInvite, sk)
					.await
				{
					auth_ssks.insert(ssk);
				}
			}
		}
	}

	let relevant_ssks: HashSet<_> = conflicting_ssks.union(&auth_ssks).copied().collect();

	let mut fork_states: Vec<StateMap<_>> = Vec::new();
	for fork in &fork_compressed_states {
		let mut state_map = StateMap::new();
		for ssk in &relevant_ssks {
			let event_bytes = fork
				.iter()
				.find(|bytes| bytes.starts_with(&ssk.to_be_bytes()));
			if let Some(bytes) = event_bytes {
				let mut id_bytes = [0_u8; 8];
				id_bytes.copy_from_slice(&bytes[8..16]);
				let shorteventid = u64::from_be_bytes(id_bytes);
				if let Ok(eid) = self
					.services
					.short
					.get_eventid_from_short(shorteventid)
					.await
				{
					if let Ok((ty, sk)) = self.services.short.get_statekey_from_short(*ssk).await
					{
						state_map.insert((ty, sk), eid);
					}
				}
			}
		}
		fork_states.push(state_map);
	}

	let resolve_start = std::time::Instant::now();
	let Ok(resolved_partial) = self
		.state_resolution(room_id, room_version_id, fork_states.iter(), prefetch_cache)
		.boxed()
		.await
	else {
		println!(
			"state_at_incoming_resolved: resolution FAILED after {:?} (total {:?})",
			resolve_start.elapsed(),
			fn_start.elapsed(),
		);
		return Ok(None);
	};
	println!(
		"state_at_incoming_resolved: resolution took {:?}, total {:?}",
		resolve_start.elapsed(),
		fn_start.elapsed(),
	);

	// Build final state: unconflicted entries from all forks + resolved conflicts
	let mut final_state = std::collections::BTreeSet::new();
	for fork in &fork_compressed_states {
		for bytes in fork {
			let mut ssk_bytes = [0_u8; 8];
			ssk_bytes.copy_from_slice(&bytes[0..8]);
			let ssk = u64::from_be_bytes(ssk_bytes);

			if conflicting_ssks.contains(&ssk) {
				continue; // We'll take this from resolved_partial
			}

			final_state.insert(*bytes);
		}
	}

	for ((ty, sk), eid) in resolved_partial {
		let ssk = self
			.services
			.short
			.get_or_create_shortstatekey(&ty, sk.as_ref())
			.await;
		let shorteventid = self.services.short.get_or_create_shorteventid(&eid).await;
		final_state
			.insert(crate::rooms::state_compressor::compress_state_event(ssk, shorteventid));
	}

	Ok(Some(std::sync::Arc::new(final_state)))
}
