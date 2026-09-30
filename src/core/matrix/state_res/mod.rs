#![cfg_attr(test, allow(warnings))]

pub(crate) mod error;
pub mod event_auth;
mod power_levels;
mod room_version;

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod benches;

use std::{
	borrow::Borrow,
	cmp::Ordering,
	collections::{HashMap, HashSet},
	hash::{BuildHasher, Hash},
	sync::Arc,
};

use dashmap::DashMap;
use futures::{
	Future, FutureExt, Stream, StreamExt, TryStreamExt, future, stream::FuturesUnordered,
};
use ruma::{
	EventId, Int, MilliSecondsSinceUnixEpoch, OwnedEventId, RoomVersionId,
	events::{
		StateEventType, TimelineEventType,
		room::member::{MembershipState, RoomMemberEventContent},
	},
	int,
};
use serde_json::from_str as from_json_str;
use smallvec::SmallVec;
use tokio::sync::OnceCell;

use self::power_levels::PowerLevelsContentFields;
pub use self::{
	error::Error,
	event_auth::{auth_check, auth_types_for_event},
	room_version::RoomVersion,
};
use crate::{
	debug, debug_error, info,
	matrix::{Event, StateKey},
	state_res::room_version::StateResolutionVersion,
	trace,
	utils::stream::{BroadbandExt, IterStream, ReadyExt, TryWidebandExt, WidebandExt},
	warn,
};

/// A mapping of event type and state_key to some value `T`, usually an
/// `EventId`.
pub type StateMap<T> = HashMap<TypeStateKey, T>;
pub type StateMapItem<T> = (TypeStateKey, T);
pub type TypeStateKey = (StateEventType, StateKey);

type Result<T, E = Error> = crate::Result<T, E>;

/// Hard cap on the conflicted set size during state resolution.
/// If the conflicted set exceeds this, we bail out immediately.
const STATE_RES_MAX_CONFLICTED: usize = 200_000;

/// Resolve sets of state events as they come in.
///
/// Internally `StateResolution` builds a graph and an auth chain to allow for
/// state conflict resolution.
///
/// ## Arguments
///
/// * `state_sets` - The incoming state to resolve. Each `StateMap` represents a
///   possible fork in the state of a room.
///
/// * `auth_chain_sets` - The full recursive set of `auth_events` for each event
///   in the `state_sets`.
///
/// * `event_fetch` - Any event not found in the `event_map` will defer to this
///   closure to find the event.
///
/// ## Invariants
///
/// The caller of `resolve` must ensure that all the events are from the same
/// room. Although this function takes a `RoomId` it does not check that each
/// event is part of the same room.
//#[tracing::instrument(level = "debug", skip(state_sets, auth_chain_sets,
//#[tracing::instrument(level event_fetch))]
#[allow(clippy::cognitive_complexity)]
pub async fn resolve<
	'a,
	Pdu,
	Sets,
	SetIter,
	Hasher,
	Fetch,
	FetchFut,
	BatchFetch,
	BatchFut,
	AuthFetch,
	AuthFut,
	Cb,
>(
	room_version: &RoomVersionId,
	state_sets: Sets,
	event_fetch: &Fetch,
	event_batch_fetch: Option<&BatchFetch>,
	auth_chain_fetch: &AuthFetch,
	event_missing_cb: Option<&Cb>,
) -> Result<StateMap<OwnedEventId>>
where
	Fetch: Fn(OwnedEventId) -> FetchFut + Sync,
	FetchFut: Future<Output = Option<Pdu>> + Send,
	BatchFetch: Fn(Vec<OwnedEventId>) -> BatchFut + Sync,
	BatchFut: Future<Output = Vec<Pdu>> + Send,
	AuthFetch: Fn(Vec<OwnedEventId>) -> AuthFut + Sync,
	AuthFut: Future<Output = HashSet<OwnedEventId, Hasher>> + Send,
	Cb: Fn(Vec<OwnedEventId>) + Sync,
	Sets: IntoIterator<IntoIter = SetIter> + Clone + Send,
	SetIter: Iterator<Item = &'a StateMap<OwnedEventId>> + Clone + Send,
	Hasher: BuildHasher + Send + Sync,
	Pdu: Event + Clone + Send + Sync,
	for<'b> &'b Pdu: Event + Send,
{
	use RoomVersionId::*;
	let stateres_version = match room_version {
		| V1 => StateResolutionVersion::V1,
		| V2 | V3 | V4 | V5 | V6 | V7 | V8 | V9 | V10 | V11 => StateResolutionVersion::V2,
		| _ => StateResolutionVersion::V2_1,
	};
	let resolve_start = std::time::Instant::now();
	debug!(version = ?stateres_version, "State resolution starting");
	let fetch_cache: Arc<DashMap<OwnedEventId, Arc<OnceCell<Option<Pdu>>>>> =
		Arc::new(DashMap::new());
	let parsed_pl_cache: Arc<DashMap<OwnedEventId, Arc<PowerLevelsContentFields>>> =
		Arc::new(DashMap::new());
	let sender_pl_cache: Arc<DashMap<(ruma::OwnedUserId, Option<OwnedEventId>), Int>> =
		Arc::new(DashMap::new());

	let cached_fetch = |id: OwnedEventId| {
		let cache = Arc::clone(&fetch_cache);
		async move {
			let cell = {
				if let Some(cell_ref) = cache.get(&id) {
					Arc::clone(cell_ref.value())
				} else {
					Arc::clone(
						cache
							.entry(id.clone())
							.or_insert_with(|| Arc::new(OnceCell::new()))
							.value(),
					)
				}
			};
			cell.get_or_init(|| async { event_fetch(id).await })
				.await
				.clone()
		}
	};

	let is_cached = |id: &EventId| {
		fetch_cache
			.get(id)
			.is_some_and(|cell| cell.value().get().is_some())
	};

	let separate_start = std::time::Instant::now();
	// Split non-conflicting and conflicting state
	let (unconflicted, conflicting) = separate(state_sets.clone().into_iter());
	println!("resolve: separate() took {:?}", separate_start.elapsed());

	debug!(count = unconflicted.len(), "non-conflicting events");
	trace!(map = ?unconflicted, "non-conflicting events");

	if conflicting.is_empty() {
		println!(
			"resolve: no conflicts, returning unconflicted ({} entries) in {:?}",
			unconflicted.len(),
			resolve_start.elapsed()
		);
		debug!("no conflicting state found");
		return Ok(unconflicted);
	}

	println!(
		"resolve: {} unconflicted, {} conflicting state keys",
		unconflicted.len(),
		conflicting.len()
	);
	trace!(map = ?conflicting, "conflicting events");
	let (conflicted_state_subgraph, initial_state) =
		if stateres_version == StateResolutionVersion::V2_1 {
			// MSC4297: V2.1 starts from the empty set as initial state.
			// The conflicting set remains as-is — only actually conflicting
			// events need subgraph traversal. Do NOT drain unconflicted into
			// conflicting; that causes the entire room history to be crawled
			// (the "drain trap" — 90+ second stalls on large rooms).
			let (csg, missing) = calculate_conflicted_subgraph(&conflicting, &cached_fetch)
				.await
				.ok_or_else(|| {
					Error::InvalidPdu("Failed to calculate conflicted subgraph".to_owned())
				})?;
			debug!(count = csg.len(), "conflicted subgraph");
			trace!(set = ?csg, "conflicted subgraph");
			if !missing.is_empty() {
				if let Some(cb) = event_missing_cb {
					cb(missing);
				}
			}
			(csg, HashMap::new())
		} else {
			(HashSet::new(), unconflicted.clone())
		};

	// `all_conflicted` contains unique items
	// synapse says `full_set = {eid for eid in full_conflicted_set if eid in
	// event_map}`
	// Hydra: Also consider the conflicted state subgraph
	let mut auth_chain_sets = Vec::new();
	let auth_diff_stream = if stateres_version == StateResolutionVersion::V2_1 {
		futures::stream::empty().boxed()
	} else {
		for state_set in state_sets {
			let mut conflicting_for_this_fork = Vec::new();
			for (key, id) in state_set {
				if conflicting.contains_key(key) {
					conflicting_for_this_fork.push(id.clone());
				}
			}
			let auth_chain = auth_chain_fetch(conflicting_for_this_fork).await;
			auth_chain_sets.push(auth_chain);
		}
		get_auth_chain_diff(&auth_chain_sets).boxed()
	};

	let all_conflicted_ids: HashSet<_> = auth_diff_stream
		.chain(conflicting.into_values().flatten().stream())
		.chain(conflicted_state_subgraph.into_iter().stream())
		.collect()
		.await;

	if let Some(batch_fetch) = event_batch_fetch {
		let ids: Vec<_> = all_conflicted_ids.iter().cloned().collect();
		let _ = batch_fetch(ids).await;

		// FAST PATH: Pre-fetch 1-hop auth events to prevent massive cache stampedes
		// during concurrent power-level lookups later in
		// reverse_topological_power_sort.
		let mut extra_auth_ids = HashSet::new();
		for id in &all_conflicted_ids {
			if let Some(ev) = cached_fetch(id.clone()).await {
				for aid in ev.auth_events() {
					if !is_cached(aid) {
						extra_auth_ids.insert(aid.to_owned());
					}
				}
			}
		}
		if !extra_auth_ids.is_empty() {
			let ids: Vec<_> = extra_auth_ids.into_iter().collect();
			let _ = batch_fetch(ids).await;
		}
	}

	let conflicted_set_start = std::time::Instant::now();
	let all_conflicted: HashSet<_> = all_conflicted_ids
		.into_iter()
		.stream()
		// Filter out non-existent events and non-state events in a single fetch.
		.broad_filter_map(async |id| {
			let ev = cached_fetch(id.clone()).await?;
			ev.state_key().is_some().then_some(id)
		})
		.collect()
		.await;
	println!(
		"resolve: conflicted set ({} events) computed in {:?}",
		all_conflicted.len(),
		conflicted_set_start.elapsed()
	);
	trace!(set = ?all_conflicted, "full conflicted set");

	if all_conflicted.len() > 5_000 {
		info!(
			count = all_conflicted.len(),
			"Conflicted set exceeds 5k events — state resolution may be slow"
		);
	}

	// Hard cap: if the conflicted set is truly enormous, full iterative auth check
	// will take minutes (226s observed in production for 16k events). Rather than
	// blocking the federation executor, bail out early and return just the
	// unconflicted state. The incoming PDU will be soft-failed and can be retried
	// once the room DAG is healed (e.g. via `yolo rescue-room`).
	if all_conflicted.len() > STATE_RES_MAX_CONFLICTED {
		warn!(
			count = all_conflicted.len(),
			limit = STATE_RES_MAX_CONFLICTED,
			"Conflicted set exceeds hard cap -- skipping full state resolution to prevent \
			 federation stall. Returning unconflicted state only. Run `yolo rescue-room` to \
			 repair this room's DAG."
		);
		// TODO: revert this for something better
		return Ok(unconflicted);
	}

	// We used to check that all events are events from the correct room
	// this is now a check the caller of `resolve` must make.

	let room_version = RoomVersion::new(room_version)?;

	// -- Isolate ALL power/foundation events for sub-resolution. --
	//    V2.1 starts from empty state, so the PL auth check requires:
	//    1. m.room.create - to establish the room
	//    2. m.room.join_rules - needed for join auth checks
	//    3. m.room.member - sender's join must be in state for PL auth
	//    4. m.room.power_levels - the actual PL events
	//    This matches Synapse/rezzy behavior where ALL member events
	//    go through the Kahn-sorted power event path.
	//
	//    NOTE: This is ONLY needed for V2.1 which starts from empty state.
	//    V2 rooms already have unconflicted state as initial_state, so the
	//    normal 2-phase resolution (control events + remaining) suffices.
	//    Running this for V2 was a regression that tripled auth-check work.
	let mut global_pl_context = None;
	if stateres_version == StateResolutionVersion::V2_1 {
		let conflicted_pl_events: Vec<_> = all_conflicted
			.iter()
			.stream()
			.wide_filter_map(async |id| {
				let ev = cached_fetch(id.clone()).await?;
				let dominated = matches!(
					ev.event_type(),
					TimelineEventType::RoomPowerLevels
						| TimelineEventType::RoomCreate
						| TimelineEventType::RoomJoinRules
						| TimelineEventType::RoomMember
				);
				dominated.then_some(id.clone())
			})
			.collect()
			.await;

		debug!(
			"PL sub-resolution: found {} power/foundation events in conflicted set \
			 (all_conflicted={})",
			conflicted_pl_events.len(),
			all_conflicted.len()
		);

		// -- Sub-resolve the PL+create events using the 100/0 bootstrap --
		//    (global_pl_context = None)
		let sorted_pl_events = reverse_topological_power_sort(
			conflicted_pl_events,
			&all_conflicted,
			&cached_fetch,
			None, // Bootstrap mode
			&parsed_pl_cache,
			&sender_pl_cache,
		)
		.await?;

		let partially_resolved_pl_state = iterative_auth_check(
			&room_version,
			sorted_pl_events.iter().stream().map(AsRef::as_ref),
			vec![initial_state.clone()],
			&cached_fetch,
			event_batch_fetch,
			Some(&is_cached),
		)
		.await?;

		debug!(entries = partially_resolved_pl_state.len(), "partially resolved PL state");

		// -- Extract the authoritative global power level context --
		let power_levels_ty_sk = (StateEventType::RoomPowerLevels, StateKey::new());
		if let Some(pl_event_id) = partially_resolved_pl_state.get(&power_levels_ty_sk) {
			debug!(%pl_event_id, "selected global PL event");
			if let Some(pl_event) = cached_fetch(pl_event_id.clone()).await {
				if let Ok(mut c) =
					from_json_str::<PowerLevelsContentFields>(pl_event.content().get())
				{
					// For v12+ rooms, creators are stripped from the PL users map
					// but need Int::MAX during state resolution sorting. Auth events
					// also omit the create event (MSC4291), so the only reliable
					// source is the unconflicted state.
					if room_version.explicitly_privilege_room_creators {
						inject_privileged_creators(&mut c, &unconflicted, &cached_fetch).await;
					}
					global_pl_context = Some(c);
				} else {
					warn!(%pl_event_id, "failed to parse global PL event content");
				}
			} else {
				warn!(%pl_event_id, "failed to fetch global PL event");
			}
		} else {
			// PL is unconflicted (same event in all forks), so the sub-resolve
			// step found no PL events to process.  Fall back to the unconflicted
			// PL event so the power sort has real power data instead of defaults.
			let unconflicted_pl = unconflicted.get(&power_levels_ty_sk);
			if let Some(pl_event_id) = unconflicted_pl {
				debug!(%pl_event_id, "PL is unconflicted; using as global PL context");
				if let Some(pl_event) = cached_fetch(pl_event_id.clone()).await {
					if let Ok(mut c) =
						from_json_str::<PowerLevelsContentFields>(pl_event.content().get())
					{
						if room_version.explicitly_privilege_room_creators {
							inject_privileged_creators(&mut c, &unconflicted, &cached_fetch)
								.await;
						}
						global_pl_context = Some(c);
					}
				}
			} else {
				warn!(
					n_unconflicted = unconflicted.len(),
					"no global PL event in partially resolved PL or unconflicted state"
				);
			}
		}
	}

	// Get only the control events with a state_key: "" or ban/kick event (sender !=
	// state_key)
	let control_events: Vec<_> = all_conflicted
		.iter()
		.stream()
		.wide_filter_map(async |id| {
			is_power_event_id(id, &cached_fetch)
				.await
				.then_some(id.clone())
		})
		.collect()
		.await;

	// -- Sort the control events based on power_level/clock/event_id and --
	// outgoing/incoming edges, using the global context
	let kahn_start = std::time::Instant::now();
	let sorted_control_levels = reverse_topological_power_sort(
		control_events,
		&all_conflicted,
		&cached_fetch,
		global_pl_context.as_ref(),
		&parsed_pl_cache,
		&sender_pl_cache,
	)
	.await?;
	println!(
		"resolve: Kahn sort ({} control events) took {:?}",
		sorted_control_levels.len(),
		kahn_start.elapsed()
	);
	if sorted_control_levels.len() <= 10 {
		info!(
			"using {} sorted power events: {:?}",
			sorted_control_levels.len(),
			sorted_control_levels
		);
	} else {
		info!(
			"using {} sorted power events: {:?} ... {:?}",
			sorted_control_levels.len(),
			&sorted_control_levels[..5],
			&sorted_control_levels[sorted_control_levels.len().saturating_sub(5)..],
		);
	}
	trace!(list = ?sorted_control_levels, "sorted power events");

	// Sequentially auth check each control event.
	let auth_check_start = std::time::Instant::now();
	let resolved_control = iterative_auth_check(
		&room_version,
		sorted_control_levels.iter().stream().map(AsRef::as_ref),
		vec![initial_state],
		&cached_fetch,
		event_batch_fetch,
		Some(&is_cached),
	)
	.await?;
	println!(
		"resolve: iterative auth check (control, {} events → {} resolved) took {:?}",
		sorted_control_levels.len(),
		resolved_control.len(),
		auth_check_start.elapsed()
	);
	trace!(map = ?resolved_control, "resolved power events");

	// At this point the control_events have been resolved we now have to
	// sort the remaining events using the mainline of the resolved power level.
	let deduped_power_ev: HashSet<_> = sorted_control_levels.into_iter().collect();

	debug!(count = deduped_power_ev.len(), "deduped power events");
	trace!(set = ?deduped_power_ev, "deduped power events");

	// This removes the control events that passed auth and more importantly those
	// that failed auth
	let events_to_resolve: Vec<_> = all_conflicted
		.iter()
		.filter(|&id| !deduped_power_ev.contains(id))
		.cloned()
		.collect();

	debug!(count = events_to_resolve.len(), "events left to resolve");
	trace!(list = ?events_to_resolve, "events left to resolve");

	// This "epochs" power level event
	let power_levels_ty_sk = (StateEventType::RoomPowerLevels, StateKey::new());
	let power_event = resolved_control.get(&power_levels_ty_sk);

	trace!(event_id = ?power_event, "power event");

	let mainline_start = std::time::Instant::now();
	let sorted_left_events =
		mainline_sort(&events_to_resolve, power_event.cloned(), &cached_fetch).await?;
	println!(
		"resolve: mainline sort ({} events) took {:?}",
		sorted_left_events.len(),
		mainline_start.elapsed()
	);

	trace!(list = ?sorted_left_events, "events left, sorted, running iterative auth check");

	let final_auth_start = std::time::Instant::now();
	let mut resolved_state = iterative_auth_check(
		&room_version,
		sorted_left_events.iter().stream().map(AsRef::as_ref),
		vec![resolved_control],
		&cached_fetch,
		event_batch_fetch,
		Some(&is_cached),
	)
	.await?;
	println!(
		"resolve: final auth check ({} events → {} resolved) took {:?}",
		sorted_left_events.len(),
		resolved_state.len(),
		final_auth_start.elapsed()
	);

	// Ensure unconflicting state is in the final state
	resolved_state.extend(unconflicted);

	println!(
		"resolve: TOTAL took {:?} ({} final state entries)",
		resolve_start.elapsed(),
		resolved_state.len()
	);

	Ok(resolved_state)
}

/// Split the events that have no conflicts from those that are conflicting.
///
/// The return tuple looks like `(unconflicted, conflicted)`.
///
/// State is determined to be conflicting if for the given key (StateEventType,
/// StateKey) there is not exactly one event ID. This includes missing events,
/// if one state_set includes an event that none of the other have this is a
/// conflicting event.
fn separate<'a, Id>(
	state_sets_iter: impl Iterator<Item = &'a StateMap<Id>>,
) -> (StateMap<Id>, StateMap<Vec<Id>>)
where
	Id: Clone + Eq + Hash + 'a,
{
	let mut state_set_count: usize = 0;
	let mut occurrences = HashMap::<_, HashMap<_, _>>::new();

	let state_sets_iter =
		state_sets_iter.inspect(|_| state_set_count = state_set_count.saturating_add(1));

	for (k, v) in state_sets_iter.flatten() {
		occurrences
			.entry(k)
			.or_default()
			.entry(v)
			.and_modify(|x: &mut usize| *x = x.saturating_add(1))
			.or_insert(1);
	}

	let mut unconflicted_state = StateMap::new();
	let mut conflicted_state = StateMap::new();

	for (k, v) in occurrences {
		for (id, occurrence_count) in v {
			if occurrence_count == state_set_count {
				unconflicted_state.insert((k.0.clone(), k.1.clone()), id.clone());
			} else {
				conflicted_state
					.entry((k.0.clone(), k.1.clone()))
					.and_modify(|x: &mut Vec<_>| x.push(id.clone()))
					.or_insert_with(|| vec![id.clone()]);
			}
		}
	}

	(unconflicted_state, conflicted_state)
}

/// Calculate the conflicted subgraph
pub(crate) async fn calculate_conflicted_subgraph<F, Fut, E>(
	conflicted: &StateMap<Vec<OwnedEventId>>,
	fetch_event: &F,
) -> Option<(HashSet<OwnedEventId>, Vec<OwnedEventId>)>
where
	F: Fn(OwnedEventId) -> Fut + Sync,
	Fut: Future<Output = Option<E>> + Send,
	E: Event + Send + Sync,
{
	let conflicted_events: HashSet<_> = conflicted.values().flatten().cloned().collect();

	// FAST CONCURRENT DEPTH DISCOVERY
	let depths: Vec<_> = conflicted_events
		.iter()
		.stream()
		.broad_filter_map(async |id| {
			let evt = fetch_event(id.clone()).await?;
			Some(evt.depth())
		})
		.collect()
		.await;

	let min_depth = depths.into_iter().min().unwrap_or(ruma::UInt::MAX);

	let mut backwards_reachable: HashSet<OwnedEventId> = HashSet::new();
	let mut missing = Vec::new();
	let mut children_map: HashMap<OwnedEventId, Vec<OwnedEventId>> = HashMap::new();

	// Concurrent work-stealing BFS: instead of waiting for each layer to
	// complete before starting the next (which serializes on layer boundaries
	// and caused 3+ minute stalls on 47k-event rooms), we use
	// FuturesUnordered as a work queue. Each completed fetch immediately
	// enqueues its prev_events for fetching, maximizing DB I/O concurrency
	// across the full DAG traversal.
	let make_fetch = |id: OwnedEventId| {
		let fut = fetch_event(id.clone());
		async move { (id, fut.await) }
	};

	let mut work: FuturesUnordered<_> = conflicted_events
		.iter()
		.filter(|id| backwards_reachable.insert((*id).clone()))
		.map(|id| make_fetch(id.clone()))
		.collect();

	while let Some((event_id, evt_opt)) = work.next().await {
		if let Some(evt) = evt_opt {
			if evt.depth() < min_depth {
				continue; // Cut off traversal if we go deeper than the conflicted set
			}

			for prev in evt.prev_events() {
				let prev_owned = prev.to_owned();
				// Store reverse edges for the forwards BFS
				children_map
					.entry(prev_owned.clone())
					.or_default()
					.push(event_id.clone());

				// Immediately enqueue unseen prev_events for fetching
				if backwards_reachable.insert(prev_owned.clone()) {
					work.push(make_fetch(prev_owned));
				}
			}
		} else {
			missing.push(event_id);
		}
	}

	// Forwards BFS (finds descendants from the seeds)
	let mut forwards_reachable = HashSet::new();
	let mut f_queue: std::collections::VecDeque<OwnedEventId> =
		conflicted_events.iter().cloned().collect();

	while let Some(event_id) = f_queue.pop_front() {
		if !forwards_reachable.insert(event_id.clone()) {
			continue;
		}
		if let Some(children) = children_map.get(&event_id) {
			f_queue.extend(children.iter().cloned());
		}
	}

	// Subgraph is the linear intersection of paths
	let subgraph: HashSet<OwnedEventId> = backwards_reachable
		.into_iter()
		.filter(|id| forwards_reachable.contains(id))
		.collect();

	if !missing.is_empty() {
		info!(
			n_missing = missing.len(),
			n_subgraph = subgraph.len(),
			"conflicted subgraph has missing prev_events (DAG holes)"
		);
	}
	Some((subgraph, missing))
}

/// Returns a Vec of deduped EventIds that appear in some chains but not others.
#[allow(clippy::arithmetic_side_effects)]
fn get_auth_chain_diff<'a, Id, Hasher>(
	auth_chain_sets: &'a [HashSet<Id, Hasher>],
) -> impl Stream<Item = Id> + Send + use<'a, Id, Hasher>
where
	Id: Clone + Eq + Hash + Send + Sync + 'a,
	Hasher: BuildHasher + Send + Sync,
{
	use roaring::RoaringBitmap;

	if auth_chain_sets.len() == 2 {
		let diff: Vec<Id> = auth_chain_sets[0]
			.symmetric_difference(&auth_chain_sets[1])
			.cloned()
			.collect();
		return diff.into_iter().stream();
	}

	let mut id_to_index = HashMap::new();
	let mut index_to_id = Vec::new();

	for id in auth_chain_sets.iter().flatten() {
		if !id_to_index.contains_key(id) {
			let idx = u32::try_from(index_to_id.len()).expect("too many event IDs");
			id_to_index.insert(id, idx);
			index_to_id.push(id);
		}
	}

	let mut union = RoaringBitmap::new();
	let mut intersection = RoaringBitmap::new();
	let mut first = true;

	for set in auth_chain_sets {
		let mut bitmap = RoaringBitmap::new();
		for id in set {
			if let Some(&idx) = id_to_index.get(id) {
				bitmap.insert(idx);
			}
		}
		if first {
			union.clone_from(&bitmap);
			intersection = bitmap;
			first = false;
		} else {
			union |= &bitmap;
			intersection &= bitmap;
		}
	}

	let diff = union - intersection;
	let result_ids: Vec<Id> = diff
		.into_iter()
		.map(move |idx| {
			let index = usize::try_from(idx).expect("idx fits in usize");
			(*index_to_id[index]).clone()
		})
		.collect();

	result_ids.into_iter().stream()
}

/// Events are sorted from "earliest" to "latest".
///
/// They are compared using the negative power level (reverse topological
/// ordering), the origin server timestamp and in case of a tie the `EventId`s
/// are compared lexicographically.
///
/// The power level is negative because a higher power level is equated to an
/// earlier (further back in time) origin server timestamp.
#[tracing::instrument(level = "debug", skip_all)]
async fn reverse_topological_power_sort<E, F, Fut>(
	events_to_sort: Vec<OwnedEventId>,
	auth_diff: &HashSet<OwnedEventId>,
	fetch_event: &F,
	global_pl_context: Option<&PowerLevelsContentFields>,
	parsed_pl_cache: &DashMap<OwnedEventId, Arc<PowerLevelsContentFields>>,
	sender_pl_cache: &DashMap<(ruma::OwnedUserId, Option<OwnedEventId>), Int>,
) -> Result<Vec<OwnedEventId>>
where
	F: Fn(OwnedEventId) -> Fut + Sync,
	Fut: Future<Output = Option<E>> + Send,
	E: Event + Send + Sync + Clone,
{
	debug!("reverse topological sort of power events");

	let start_reverse = std::time::Instant::now();

	let mut all_needed: HashSet<OwnedEventId> = auth_diff.clone();
	for id in &events_to_sort {
		all_needed.insert(id.clone());
	}

	let mut local_cache_map: rustc_hash::FxHashMap<OwnedEventId, Option<E>> =
		rustc_hash::FxHashMap::default();
	let fetched: Vec<(OwnedEventId, Option<E>)> = all_needed
		.into_iter()
		.stream()
		.broad_filter_map(|id| async move {
			let ev = fetch_event(id.clone()).await;
			Some((id, ev))
		})
		.collect()
		.await;

	for (id, ev) in fetched {
		local_cache_map.insert(id, ev);
	}

	let fast_fetch = |id: OwnedEventId| {
		let cached = local_cache_map.get(&id).cloned();
		async move {
			if let Some(ev_opt) = cached {
				ev_opt
			} else {
				fetch_event(id).await
			}
		}
	};

	let mut graph: rustc_hash::FxHashMap<OwnedEventId, rustc_hash::FxHashSet<OwnedEventId>> =
		rustc_hash::FxHashMap::default();
	for event_id in events_to_sort {
		let mut state = vec![event_id];
		while let Some(eid) = state.pop() {
			if graph.contains_key(&eid) {
				continue;
			}
			graph.entry(eid.clone()).or_default();
			let auth_events = local_cache_map
				.get(&eid)
				.and_then(|o| o.as_ref())
				.map(Event::auth_events)
				.into_iter()
				.flatten();

			for aid in auth_events {
				if auth_diff.contains(aid) {
					if !graph.contains_key(aid) {
						state.push(aid.to_owned());
					}

					graph
						.get_mut(&eid)
						.expect("We just inserted this at the start of the while loop")
						.insert(aid.to_owned());
				}
			}
		}
	}

	println!(
		"reverse_topological_power_sort phase 1 (graph build) took {:?}",
		start_reverse.elapsed()
	);
	let start_reverse_2 = std::time::Instant::now();

	let event_to_key: HashMap<_, _> = graph
		.keys()
		.cloned()
		.stream()
		.broad_filter_map(async |event_id| {
			let pl = get_power_level_for_sender(
				&event_id,
				&fast_fetch,
				global_pl_context,
				parsed_pl_cache,
				sender_pl_cache,
			)
			.await;

			let ev = local_cache_map.get(&event_id).and_then(Clone::clone)?;

			Some((event_id, (pl, ev.origin_server_ts())))
		})
		.inspect(|(event_id, (pl, _))| {
			debug!(
				event_id = event_id.as_str(),
				power_level = i64::from(*pl),
				"found the power level of an event's sender",
			);
		})
		.collect()
		.boxed()
		.await;

	let fetcher = async |event_id: OwnedEventId| {
		event_to_key
			.get(&event_id)
			.copied()
			.ok_or_else(|| Error::NotFound(String::new()))
	};

	let res = lexicographical_topological_sort(&graph, &fetcher).await;
	println!(
		"reverse_topological_power_sort phase 2 (lexicographical sort) took {:?}",
		start_reverse_2.elapsed()
	);
	res
}

/// Sorts the event graph based on number of outgoing/incoming edges.
///
/// `key_fn` is used as to obtain the power level and age of an event for
/// breaking ties (together with the event ID).
#[tracing::instrument(level = "debug", skip_all)]
pub async fn lexicographical_topological_sort<Id, F, Fut, Hasher, S>(
	graph: &HashMap<Id, HashSet<Id, Hasher>, S>,
	key_fn: &F,
) -> Result<Vec<Id>>
where
	F: Fn(Id) -> Fut + Sync,
	Fut: Future<Output = Result<(Int, MilliSecondsSinceUnixEpoch)>> + Send,
	Id: Borrow<EventId> + Clone + Eq + Hash + Ord + Send + Sync,
	Hasher: BuildHasher + Default + Clone + Send + Sync,
	S: BuildHasher + Clone + Send + Sync,
{
	debug!("starting lexicographical topological sort");

	let mut id_to_index: rustc_hash::FxHashMap<&Id, usize> = rustc_hash::FxHashMap::default();
	let mut index_to_id: Vec<&Id> = Vec::new();

	for (node, edges) in graph {
		if !id_to_index.contains_key(node) {
			id_to_index.insert(node, index_to_id.len());
			index_to_id.push(node);
		}
		for edge in edges {
			if !id_to_index.contains_key(edge) {
				id_to_index.insert(edge, index_to_id.len());
				index_to_id.push(edge);
			}
		}
	}

	let num_nodes = index_to_id.len();

	// Pre-fetch all keys to avoid await overhead in the sort loop
	let mut keys = Vec::with_capacity(num_nodes);
	for &id in &index_to_id {
		keys.push(key_fn(id.clone()).await?);
	}

	let nodes = index_to_id.into_iter().enumerate().map(|(idx, id)| {
		let parents = graph
			.get::<Id>(id)
			.map(|edges| edges.iter().cloned())
			.into_iter()
			.flatten();
		let (power_level, origin_server_ts) = keys[idx];
		// TieBreaker:
		// 1. highest power level (Reverse)
		// 2. lowest origin_server_ts
		// 3. lowest event_id
		let key = (std::cmp::Reverse(power_level), origin_server_ts, id.clone());
		(id.clone(), parents, key)
	});

	Ok(crate::utils::kahns_sort::kahn_sort(nodes))
}

/// Find the power level for the sender of `event_id` or return a default value
/// of zero.
///
/// Do NOT use this any where but topological sort, we find the power level for
/// the eventId at the eventId's generation (we walk backwards to `EventId`s
/// most recent previous power level event).
async fn get_power_level_for_sender<E, F, Fut>(
	event_id: &EventId,
	fetch_event: &F,
	global_pl_context: Option<&PowerLevelsContentFields>,
	parsed_pl_cache: &DashMap<OwnedEventId, Arc<PowerLevelsContentFields>>,
	sender_pl_cache: &DashMap<(ruma::OwnedUserId, Option<OwnedEventId>), Int>,
) -> Int
where
	F: Fn(OwnedEventId) -> Fut + Sync,
	Fut: Future<Output = Option<E>> + Send,
	E: Event + Send + Clone,
{
	debug!("fetch event ({event_id}) senders power level");

	let event = fetch_event(event_id.to_owned()).await;
	let sender_str = event.as_ref().map(|e| Event::sender(e).to_owned());

	if let Some(global_context) = global_pl_context {
		if let Some(s) = &sender_str {
			if let Some(&user_level) = global_context.get_user_power(s) {
				return user_level;
			}
			return global_context.users_default;
		}
		return int!(0);
	}

	if let Some(ev) = event {
		if is_type_and_key(&ev, &TimelineEventType::RoomCreate, "") {
			return Int::MAX;
		}

		let mut creator_event: Option<E> = None;
		let mut pl_event: Option<E> = None;

		// 1-HOP STRICT SCAN: avoids O(N * E) exponential BFS tarpit
		for aid in ev.auth_events() {
			// FAST PATH: check if we already parsed this power levels event
			if parsed_pl_cache.contains_key(aid) {
				if let Some(aev) = fetch_event(aid.to_owned()).await {
					pl_event = Some(aev);
				}
				continue;
			}

			if let Some(aev) = fetch_event(aid.to_owned()).await {
				if is_type_and_key(&aev, &TimelineEventType::RoomCreate, "")
					&& creator_event.is_none()
				{
					creator_event = Some(aev);
				} else if is_type_and_key(&aev, &TimelineEventType::RoomPowerLevels, "")
					&& pl_event.is_none()
				{
					pl_event = Some(aev);
				}
			}
			if creator_event.is_some() && pl_event.is_some() {
				break;
			}
		}

		if let Some(pl_ev) = pl_event {
			let pl_id = pl_ev.event_id().to_owned();
			let parsed_pl = parsed_pl_cache
				.entry(pl_id.clone())
				.or_insert_with(|| {
					Arc::new(
						from_json_str::<PowerLevelsContentFields>(pl_ev.content().get())
							.unwrap_or_else(|_| PowerLevelsContentFields {
								users_default: int!(0),
								users: Vec::new(),
							}),
					)
				})
				.value()
				.clone();

			if let Some(s) = &sender_str {
				let cache_key = (s.clone(), Some(pl_id));
				if let Some(cached_pl) = sender_pl_cache.get(&cache_key) {
					return *cached_pl;
				}

				// Check if sender is a v12 privileged room creator
				let is_privileged_creator = creator_event.as_ref().is_some_and(|creator_ev| {
					from_json_str::<ruma::events::room::create::RoomCreateEventContent>(
						creator_ev.content().get(),
					)
					.is_ok_and(|cc| {
						RoomVersion::new(&cc.room_version)
							.is_ok_and(|rv| rv.explicitly_privilege_room_creators)
							&& (creator_ev.sender().as_str() == s.as_str()
								|| cc.additional_creators.as_ref().is_some_and(|cs| {
									cs.iter().any(|c| c.as_str() == s.as_str())
								}))
					})
				});

				if is_privileged_creator {
					sender_pl_cache.insert(cache_key, Int::MAX);
					return Int::MAX;
				}

				if let Some(&user_level) = parsed_pl.get_user_power(s) {
					sender_pl_cache.insert(cache_key, user_level);
					return user_level;
				}
				sender_pl_cache.insert(cache_key, parsed_pl.users_default);
				return parsed_pl.users_default;
			}
		} else if let Some(creator_ev) = creator_event {
			let mut is_creator = creator_ev.sender() == ev.sender();
			if let Ok(create_content) = from_json_str::<
				ruma::events::room::create::RoomCreateEventContent,
			>(creator_ev.content().get())
			{
				#[allow(deprecated)]
				if let Some(creator_user) = create_content.creator {
					is_creator = creator_user == ev.sender();
				}
			}
			if is_creator {
				return Int::MAX;
			}
		}
	}
	int!(0)
}

/// Check the that each event is authenticated based on the events before it.
///
/// ## Returns
///
/// The `unconflicted_state` combined with the newly auth'ed events. So any
/// event that fails the `event_auth::auth_check` will be excluded from the
/// returned state map.
///
/// For each `events_to_check` event we gather the events needed to auth it from
/// the the `fetch_event` closure and verify each event using the
/// `event_auth::auth_check` function.
#[tracing::instrument(level = "trace", skip_all)]
async fn iterative_auth_check<'a, E, F, Fut, S, BatchFetch, BatchFut, IsCached>(
	room_version: &RoomVersion,
	events_to_check: S,
	mut unconflicted_state_sets: Vec<StateMap<OwnedEventId>>,
	fetch_event: &F,
	event_batch_fetch: Option<&BatchFetch>,
	is_cached: Option<&IsCached>,
) -> Result<StateMap<OwnedEventId>>
where
	E: Event + Clone + Send + Sync,
	F: Fn(OwnedEventId) -> Fut + Sync,
	Fut: Future<Output = Option<E>> + Send,
	S: Stream<Item = &'a EventId> + Send + 'a,
	BatchFetch: Fn(Vec<OwnedEventId>) -> BatchFut + Sync,
	BatchFut: Future<Output = Vec<E>> + Send,
	IsCached: Fn(&EventId) -> bool + Sync,
	for<'b> &'b E: Event + Send,
{
	debug!("starting iterative auth check");

	let events_to_check: Vec<_> = events_to_check
		.map(Result::Ok)
		// NOTE: wide_and_then (ordered) is required here, NOT broad_and_then.
		// The input stream is topologically sorted; broad_and_then uses
		// buffer_unordered which destroys that ordering, causing the wrong
		// power-level event to win during state resolution.
		.wide_and_then(async |event_id| {
			fetch_event(event_id.to_owned())
				.await
				.ok_or_else(|| Error::NotFound(format!("Failed to find {event_id}")))
		})
		// SYNAPSE CHECK 1: Pre-filter rejected events synchronously via trait
		.wide_filter_map(|res| async {
			match res {
				| Ok(event) if event.rejected() => {
					info!(
						target: "state_res_debug",
						event_id = event.event_id().as_str(),
						"skipping previously rejected event"
					);
					None
				},
				| Ok(event) => Some(Ok(event)),
				| Err(e) => Some(Err(e)),
			}
		})
		.try_collect()
		.boxed()
		.await?;

	trace!(list = ?events_to_check, "events to check");
	if events_to_check.len() > 5_000 {
		info!(
			count = events_to_check.len(),
			"iterative_auth_check processing >5k events — possible fork storm"
		);
	}
	if events_to_check.is_empty() {
		debug!("no events to check, returning unconflicted state");
		let mut resolved_state = unconflicted_state_sets.pop().unwrap_or_default();
		for state in unconflicted_state_sets {
			resolved_state.extend(state);
		}
		return Ok(resolved_state);
	}

	let auth_event_ids: HashSet<OwnedEventId> = events_to_check
		.iter()
		.flat_map(|event: &E| event.auth_events().map(ToOwned::to_owned))
		.collect();

	trace!(set = ?auth_event_ids, "auth event IDs to fetch");

	if let Some(batch_fetch) = event_batch_fetch {
		// Only batch fetch events that are NOT already in our DashMap cache!
		let ids: Vec<_> = auth_event_ids
			.iter()
			.filter(|id| {
				if let Some(is_cached) = is_cached {
					!is_cached(id)
				} else {
					true
				}
			})
			.cloned()
			.collect();

		if !ids.is_empty() {
			let _ = batch_fetch(ids).await;
		}
	}

	let auth_events: HashMap<OwnedEventId, E> = auth_event_ids
		.into_iter()
		.stream()
		.broad_filter_map(|id| async move {
			let auth_event = fetch_event(id.clone()).await?;
			if auth_event.rejected() {
				trace!(
					target: "state_res",
					event_id = auth_event.event_id().as_str(),
					"skipping rejected auth event"
				);
				None
			} else {
				Some((id, auth_event))
			}
		})
		.collect()
		.await;

	trace!(map = ?auth_events.keys().collect::<Vec<_>>(), "fetched auth events");

	let auth_events = &auth_events;
	let mut resolved_state = unconflicted_state_sets.pop().unwrap_or_default();

	for state in unconflicted_state_sets {
		resolved_state.extend(state);
	}
	let mut local_create_event: Option<E> = None;

	// For room versions that use hashed room IDs (v12+), the create event is
	// derived from the room_id on every event. Hoist this fetch out of the loop
	// so we only do it once for the entire batch instead of once per event.
	if room_version.room_ids_as_hashes {
		if let Some(first) = events_to_check.first() {
			if let Some(room_id) = first.room_id_or_hash() {
				let create_event_id_raw = room_id.as_str().replacen('!', "$", 1);
				if let Ok(create_event_id) = EventId::parse(&create_event_id_raw) {
					local_create_event = fetch_event(create_event_id.into()).await;
				}
			}
		}
	}

	for event in events_to_check {
		trace!(event_id = event.event_id().as_str(), "checking event");
		let Some(state_key) = event.state_key() else {
			warn!("event {} failed the authentication check (no state key)", event.event_id());
			continue;
		};

		let auth_types = match auth_types_for_event(
			event.event_type(),
			event.sender(),
			Some(state_key),
			event.content(),
			room_version,
		) {
			| Ok(types) => types,
			| Err(e) => {
				warn!(
					"event {} failed the authentication check (invalid auth_types: {e})",
					event.event_id()
				);
				continue;
			},
		};

		trace!(list = ?auth_types, event_id = event.event_id().as_str(), "auth types for event");

		// SmallVec<[_; 4]> — stack-allocated for the common case (≤4 auth
		// events: create, join_rules, power_levels, sender_membership).
		// Eliminates per-iteration HashMap allocation.
		let mut auth_state: SmallVec<[(TypeStateKey, E); 4]> = SmallVec::new();
		if room_version.room_ids_as_hashes {
			if *event.event_type() == TimelineEventType::RoomCreate {
				auth_state.push((event.event_type().with_state_key(""), event.clone()));
			} else {
				// Use the hoisted cached_create_event (fetched once before the loop).
				if let Some(create_event) = &local_create_event {
					auth_state.push((
						create_event.event_type().with_state_key(""),
						create_event.clone(),
					));
				} else {
					warn!(
						"event {} failed the authentication check (missing create event)",
						event.event_id()
					);
					continue;
				}
			}
		}
		for aid in event.auth_events() {
			if let Some(ev) = auth_events.get(aid) {
				// Skip rejected events (Synapse parity: checks rejected flag)
				if ev.rejected() {
					trace!(event_id = aid.as_str(), "skipping rejected auth event");
					continue;
				}
				trace!(event_id = aid.as_str(), "found auth event");
				let key = ev
					.event_type()
					.with_state_key(ev.state_key().ok_or_else(|| {
						Error::InvalidPdu("State event had no state key".to_owned())
					})?);
				auth_state.push((key, ev.clone()));
			} else {
				warn!(event_id = aid.as_str(), "missing auth event");
			}
		}

		// Merge resolved_state into auth context for all room versions.
		// Without this, auth_check evaluates events in a vacuum and rejects
		// valid membership transitions (e.g. "Sender cannot leave").
		// V2.0: resolved_state provides authoritative state context.
		// V2.1: resolved_state provides membership context from the
		// partially-resolved PL state, which is needed to find the
		// sender's join event during leave/kick auth checks.
		{
			let supplemental: Vec<_> = auth_types
				.iter()
				.stream()
				.ready_filter_map(|key| Some((key, resolved_state.get(key)?)))
				.filter_map(|(key, ev_id)| async move {
					// Exclude rejected events from resolved_state (Synapse parity)
					// Fetch the event to check its rejected flag
					if let Some(event) = auth_events.get(ev_id) {
						if event.rejected() {
							return None;
						}
						Some((key.to_owned(), event.clone()))
					} else {
						let fetched = fetch_event(ev_id.clone()).await?;
						if fetched.rejected() {
							return None;
						}
						Some((key.to_owned(), fetched))
					}
				})
				.collect()
				.await;

			for (key, event) in supplemental {
				auth_state.push((key, event));
			}
		}

		// Sort + dedup: binary search requires ascending order, and duplicates
		// from overlapping auth_events/resolved_state must be collapsed.
		// Supplemental entries are pushed AFTER auth_events, so:
		// - For V2.0 rooms (state_res < V2_1): reverse+dedup keeps supplemental
		//   (matching old HashMap overwrite semantics).
		// - For V2.1 rooms (state_res >= V2_1): dedup without reversing keeps the
		//   event's own auth_events (maintaining MSC4297 spec compliance).
		auth_state.sort_by(|a, b| a.0.cmp(&b.0));
		if room_version.state_res < StateResolutionVersion::V2_1 {
			auth_state.reverse();
			auth_state.dedup_by(|a, b| a.0.eq(&b.0));
			auth_state.sort_by(|a, b| a.0.cmp(&b.0));
		} else {
			auth_state.dedup_by(|a, b| a.0.eq(&b.0));
		}

		trace!(
			keys = ?auth_state.iter().map(|(k, _)| k).collect::<Vec<_>>(),
			event_id = event.event_id().as_str(),
			"auth state for event"
		);

		if *event.event_type() == TimelineEventType::RoomPowerLevels {
			debug!(
				event_id = event.event_id().as_str(),
				sender = %event.sender(),
				"iterative_auth_check: about to auth PL event"
			);
		}
		debug!(event_id = event.event_id().as_str(), "Running auth checks");

		// Binary search for third party invite in the sorted auth_state
		let current_third_party = auth_state.iter().find_map(|(_, pdu)| {
			(*pdu.event_type() == TimelineEventType::RoomThirdPartyInvite).then_some(pdu)
		});

		// Binary search closure: O(log n) lookup instead of HashMap hashing.
		// For ≤4 elements this is 1-2 comparisons.
		let fetch_state = |ty: &StateEventType, key: &str| {
			let needle = ty.with_state_key(key);
			future::ready(
				auth_state
					.binary_search_by(|(k, _)| k.cmp(&needle))
					.ok()
					.map(|i| auth_state[i].1.clone()),
			)
		};

		// If the event IS the create event, use it directly; otherwise use
		// the memoized create event (or discover it once from auth_state).
		// This avoids redundant auth_state lookups on every iteration.
		let create_event = if *event.event_type() == TimelineEventType::RoomCreate {
			local_create_event = Some(event.clone()); // <-- Keep hoisted cache warm
			event.clone()
		} else if let Some(ref ce) = local_create_event {
			ce.clone()
		} else {
			match fetch_state(&StateEventType::RoomCreate, "").await {
				| Some(ce) => {
					local_create_event = Some(ce.clone());
					ce
				},
				| None => {
					warn!(
						"event {} failed the authentication check (missing create event)",
						event.event_id()
					);
					continue;
				},
			}
		};

		let auth_result =
			auth_check(room_version, &event, current_third_party, fetch_state, &create_event)
				.await;

		match auth_result {
			| Ok(true) => {
				// add event to resolved state map
				if *event.event_type() == TimelineEventType::RoomPowerLevels {
					debug!(
						event_id = event.event_id().as_str(),
						"iterative_auth_check: PL event PASSED auth, adding to resolved state"
					);
				}
				trace!(
					event_id = event.event_id().as_str(),
					"event passed the authentication check, adding to resolved state"
				);
				resolved_state.insert(
					event.event_type().with_state_key(state_key),
					event.event_id().to_owned(),
				);
				trace!(map = ?resolved_state, "new resolved state");
			},
			| Ok(false) => {
				// synapse passes here on AuthError. We do not add this event to resolved_state.
				if *event.event_type() == TimelineEventType::RoomPowerLevels {
					warn!(
						event_id = event.event_id().as_str(),
						"iterative_auth_check: PL event FAILED auth check!"
					);
				}
				warn!("event {} failed the authentication check", event.event_id());
			},
			| Err(e) => {
				debug_error!("event {} failed the authentication check: {e}", event.event_id());
				return Err(e);
			},
		}
	}
	trace!(map = ?resolved_state, "final resolved state from iterative auth check");
	debug!("iterative auth check finished");
	Ok(resolved_state)
}

/// Returns the sorted `to_sort` list of `EventId`s based on a mainline sort
/// using the depth of `resolved_power_level`, the server timestamp, and the
/// eventId.
async fn mainline_sort<E, F, Fut>(
	to_sort: &[OwnedEventId],
	resolved_power_level: Option<OwnedEventId>,
	fetch_event: &F,
) -> Result<Vec<OwnedEventId>>
where
	F: Fn(OwnedEventId) -> Fut + Sync,
	Fut: Future<Output = Option<E>> + Send,
	E: Event + Clone + Send + Sync,
{
	debug!("mainline sort of events");

	// There are no EventId's to sort, bail.
	if to_sort.is_empty() {
		return Ok(vec![]);
	}

	let start_mainline = std::time::Instant::now();

	// Fast-path local cache to eliminate async Tokio overhead and DashMap locking.
	// For massive DAGs (68,000+ events), we repeatedly walk the exact same PL
	// chains. Hitting `fetch_event(id).await` sequentially forces 300,000+ task
	// context switches. By locally caching the PDU, the async state machine
	// completes immediately and synchronously.
	let mut local_cache: rustc_hash::FxHashMap<OwnedEventId, Option<E>> =
		rustc_hash::FxHashMap::default();

	macro_rules! get {
		($id:expr) => {
			if let Some(ev_opt) = local_cache.get($id) {
				ev_opt.clone()
			} else {
				let id_owned = $id.to_owned();
				let ev = fetch_event(id_owned.clone()).await;
				local_cache.insert(id_owned, ev.clone());
				ev
			}
		};
	}

	let mut mainline_depth: rustc_hash::FxHashMap<OwnedEventId, usize> =
		rustc_hash::FxHashMap::default();
	let mut pl = resolved_power_level;
	let mut position: usize = 0;
	let mut walk_iter: usize = 0;
	while let Some(p) = pl {
		walk_iter = walk_iter.saturating_add(1);
		if walk_iter.is_multiple_of(1000) {
			println!("mainline_sort walk iter {walk_iter}");
		}
		let Some(event) = get!(&p) else {
			break;
		};

		mainline_depth.insert(event.event_id().to_owned(), position);
		position = position.saturating_add(1);

		pl = None;
		for aid in event.auth_events() {
			let Some(aev) = get!(aid) else {
				continue;
			};
			if is_type_and_key(&aev, &TimelineEventType::RoomPowerLevels, "") {
				pl = Some(aid.to_owned());
				break;
			}
		}
	}

	println!(
		"mainline_sort phase 1 (pre-fetch PL mainline) took {:?}",
		start_mainline.elapsed()
	);
	let start_phase_2 = std::time::Instant::now();

	// Step 2: For each event to sort, find its associated power level event,
	// then look up its mainline position in O(1).
	let mut is_pl_cache: rustc_hash::FxHashMap<OwnedEventId, bool> =
		rustc_hash::FxHashMap::default();

	let mut sort_keys = Vec::with_capacity(to_sort.len());

	for ev_id in to_sort {
		let Some(event) = get!(ev_id) else {
			continue;
		};
		let ts = event.origin_server_ts();

		let depth: Option<usize> =
			if is_type_and_key(&event, &TimelineEventType::RoomPowerLevels, "") {
				// The event IS a power level event — look itself up directly
				mainline_depth.get(event.event_id()).copied()
			} else {
				// Find the 1-hop PL event
				let mut current_pl = None;
				for aid in event.auth_events() {
					let is_pl = match is_pl_cache.get(aid) {
						| Some(&b) => b,
						| None => {
							let is_pl = get!(aid).is_some_and(|aev| {
								is_type_and_key(&aev, &TimelineEventType::RoomPowerLevels, "")
							});
							is_pl_cache.insert(aid.to_owned(), is_pl);
							is_pl
						},
					};

					if is_pl {
						current_pl = get!(aid);
						break;
					}
				}

				// Iteratively walk PL chain w/ path memo
				let mut path = Vec::new();
				let mut visited: rustc_hash::FxHashSet<OwnedEventId> =
					rustc_hash::FxHashSet::default();
				let mut found_depth = None;
				let mut walk_iter2: usize = 0;
				while let Some(c_pl) = current_pl {
					walk_iter2 = walk_iter2.saturating_add(1);
					if walk_iter2.is_multiple_of(1000) {
						println!("mainline phase2 walk iter {walk_iter2}");
					}
					let current_id = c_pl.event_id().to_owned();
					if !visited.insert(current_id.clone()) {
						// Cycle detected in auth chain — break to prevent infinite loop.
						break;
					}
					path.push(current_id.clone());
					if let Some(&depth) = mainline_depth.get(&current_id) {
						found_depth = Some(depth);
						break;
					}
					current_pl = None;
					for aid in c_pl.auth_events() {
						let Some(aev) = get!(aid) else {
							continue;
						};
						if is_type_and_key(&aev, &TimelineEventType::RoomPowerLevels, "") {
							current_pl = Some(aev);
							break;
						}
					}
				}

				if let Some(depth) = found_depth {
					for id in path {
						mainline_depth.insert(id, depth);
					}
				}
				found_depth
			};

		sort_keys.push((depth, ts, ev_id.clone()));
	}

	println!("mainline_sort phase 2 (event traversal) took {:?}", start_phase_2.elapsed());
	let start_phase_3 = std::time::Instant::now();

	sort_keys.sort_unstable_by(|(da, ta, a), (db, tb, b)| match (da, db) {
		| (None, None) => ta.cmp(tb).then(a.as_str().cmp(b.as_str())),
		| (None, Some(_)) => Ordering::Less,
		| (Some(_), None) => Ordering::Greater,
		| (Some(pa), Some(pb)) => pb.cmp(pa).then(ta.cmp(tb)).then(a.as_str().cmp(b.as_str())),
	});

	let mut sort_event_ids: Vec<OwnedEventId> =
		sort_keys.into_iter().map(|(_, _, id)| id).collect();

	sort_event_ids.dedup();
	println!("mainline_sort phase 3 (sorting) took {:?}", start_phase_3.elapsed());
	println!("mainline_sort total took {:?}", start_mainline.elapsed());

	Ok(sort_event_ids)
}

async fn is_power_event_id<E, F, Fut>(event_id: &EventId, fetch: &F) -> bool
where
	F: Fn(OwnedEventId) -> Fut + Sync,
	Fut: Future<Output = Option<E>> + Send,
	E: Event + Send,
{
	match fetch(event_id.to_owned()).await.as_ref() {
		| Some(state) => is_power_event(state),
		| _ => false,
	}
}

/// For v12+ rooms, inject privileged creators into the global PL context with
/// Int::MAX. In these rooms creators are stripped from the PL users map and
/// auth events omit the create event (MSC4291), so the only reliable source
/// is the unconflicted state.
async fn inject_privileged_creators<E, F, Fut>(
	c: &mut PowerLevelsContentFields,
	unconflicted: &StateMap<OwnedEventId>,
	cached_fetch: &F,
) where
	F: Fn(OwnedEventId) -> Fut + Sync,
	Fut: Future<Output = Option<E>> + Send,
	E: Event + Send + Sync + Clone,
{
	let create_ty_sk = (StateEventType::RoomCreate, StateKey::new());
	let Some(create_id) = unconflicted.get(&create_ty_sk) else {
		return;
	};
	let Some(create_ev) = cached_fetch(create_id.clone()).await else {
		return;
	};
	let Ok(cc) = from_json_str::<ruma::events::room::create::RoomCreateEventContent>(
		create_ev.content().get(),
	) else {
		return;
	};

	let creator = create_ev.sender().to_owned();
	if c.get_user_power(&creator).is_none() {
		c.users.push((creator, Int::MAX));
	}
	if let Some(additional) = cc.additional_creators {
		for ac in additional {
			if c.get_user_power(&ac).is_none() {
				c.users.push((ac, Int::MAX));
			}
		}
	}
	c.users.sort_by(|a, b| a.0.cmp(&b.0));
}

fn is_type_and_key(ev: &impl Event, ev_type: &TimelineEventType, state_key: &str) -> bool {
	ev.event_type() == ev_type && ev.state_key() == Some(state_key)
}

fn is_power_event(event: &impl Event) -> bool {
	match event.event_type() {
		| TimelineEventType::RoomPowerLevels
		| TimelineEventType::RoomJoinRules
		| TimelineEventType::RoomCreate => event.state_key() == Some(""),
		| TimelineEventType::RoomMember => {
			if let Ok(content) = from_json_str::<RoomMemberEventContent>(event.content().get()) {
				if [MembershipState::Leave, MembershipState::Ban].contains(&content.membership) {
					return Some(event.sender().as_str()) != event.state_key();
				}
			}

			false
		},
		| _ => false,
	}
}

/// Convenience trait for adding event type plus state key to state maps.
pub trait EventTypeExt {
	fn with_state_key(self, state_key: impl Into<StateKey>) -> (StateEventType, StateKey);
}

impl EventTypeExt for StateEventType {
	fn with_state_key(self, state_key: impl Into<StateKey>) -> (StateEventType, StateKey) {
		(self, state_key.into())
	}
}

impl EventTypeExt for TimelineEventType {
	fn with_state_key(self, state_key: impl Into<StateKey>) -> (StateEventType, StateKey) {
		(self.into(), state_key.into())
	}
}

impl<T> EventTypeExt for &T
where
	T: EventTypeExt + Clone,
{
	fn with_state_key(self, state_key: impl Into<StateKey>) -> (StateEventType, StateKey) {
		self.to_owned().with_state_key(state_key)
	}
}
