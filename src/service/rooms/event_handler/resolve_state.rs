use std::{borrow::Borrow, collections::HashMap, sync::Arc};

use conduwuit::{
	Error, Result, err, implement, info,
	state_res::StateMap,
	trace,
	utils::stream::{IterStream, ReadyExt, WidebandExt},
	warn,
};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use ruma::{OwnedEventId, RoomId, RoomVersionId};

use crate::rooms::state_compressor::CompressedState;

/// Pre-loaded event cache to avoid per-event RocksDB lookups during
/// state resolution. Populated once at the start of bulk operations
/// like rebuild_state.
pub(crate) type PduCache =
	Arc<tokio::sync::RwLock<HashMap<OwnedEventId, Arc<conduwuit_core::PduEvent>>>>;

#[implement(super::Service)]
#[tracing::instrument(name = "resolve", level = "debug", skip_all)]
pub async fn resolve_state(
	&self,
	room_id: &RoomId,
	room_version_id: &RoomVersionId,
	incoming_state: HashMap<u64, OwnedEventId>,
) -> Result<Arc<CompressedState>> {
	trace!("Loading current room state ids");
	let current_sstatehash = self
		.services
		.state
		.get_room_shortstatehash(room_id)
		.map_err(|e| err!(Database(error!("No state for {room_id:?}: {e:?}"))))
		.await?;

	let current_state_ids: HashMap<_, _> = self
		.services
		.state_accessor
		.state_full_ids(current_sstatehash)
		.collect()
		.await;

	trace!("Loading fork states");
	let fork_states = [current_state_ids, incoming_state];

	// Build OwnedEventId -> ShortStateKey reverse map from the fork states BEFORE
	// they are consumed into streams below. After state resolution completes, we
	// use this for O(1) fast-path shortstatehash lookups instead of issuing
	// ~50k concurrent get_or_create_shortstatekey DB calls.
	//
	// State resolution selects its output event_ids exclusively from the input
	// fork states, so every resolved entry will normally hit this fast path.
	// The get_or_create_shortstatekey fallback handles truly new state events
	// (rare -- e.g., a new join that wasn't in either input fork).
	let eid_to_ssk: HashMap<OwnedEventId, u64> = fork_states
		.iter()
		.flat_map(|fs| fs.iter().map(|(&ssk, eid)| (eid.clone(), ssk)))
		.collect();

	let fork_states = fork_states
		.iter()
		.stream()
		.wide_then(|fork_state| {
			let shortstatekeys = fork_state.keys().copied().stream();
			let event_ids = fork_state.values().cloned().stream();
			self.services
				.short
				.multi_get_statekey_from_short(shortstatekeys)
				.zip(event_ids)
				.ready_filter_map(|(ty_sk, id)| Some((ty_sk.ok()?, id)))
				.collect()
		})
		.map(Ok::<_, Error>)
		.try_collect::<Vec<StateMap<OwnedEventId>>>();

	let fork_states = fork_states.await?;

	// Do NOT fetch from federation here. State resolution must be local-only
	// to avoid blocking. Missing auth chain events cause state_res to skip those
	// subgraph branches — producing a best-effort result with local data. The
	// ingestion pipeline (handle_outlier_pdu, fetch_prev) is responsible for
	// pre-fetching auth events before we reach this point.

	// Diagnostic: log PL events in each fork state
	for (i, fork) in fork_states.iter().enumerate() {
		for ((ty, sk), eid) in fork {
			if ty.to_string() == "m.room.power_levels" {
				info!("resolve_state fork[{i}] PL ({ty},{sk}) => {eid}");
			}
		}
	}

	trace!("Resolving state");
	let n_fork_states: usize = fork_states.iter().map(HashMap::len).sum();
	info!(%room_id, n_fork_states, "state_res: fork states loaded, starting resolution");
	let t = std::time::Instant::now();
	let state = self
		.state_resolution(room_id, room_version_id, fork_states.iter(), None)
		.boxed()
		.await?;
	info!(%room_id, n_resolved = state.len(), elapsed = ?t.elapsed(), "state_res: resolution complete");

	// Diagnostic: log resolved PL and JoinRules
	for ((ty, sk), eid) in &state {
		if ty.to_string() == "m.room.power_levels" || ty.to_string() == "m.room.join_rules" {
			info!("resolve_state RESULT ({ty},{sk}) => {eid}");
		}
	}
	trace!("State resolution done.");
	let eid_to_ssk = &eid_to_ssk;
	let state_events: Vec<_> = state
		.iter()
		.stream()
		.wide_then(|((event_type, state_key), event_id)| async move {
			// FAST PATH: ~99.9% of resolved events were in a fork state; their
			// ShortStateKey is already known in memory — no DB call needed.
			if let Some(&ssk) = eid_to_ssk.get(event_id) {
				return (ssk, event_id.clone());
			}
			// SLOW PATH: truly new state event (e.g., a new join member event).
			let ssk = self
				.services
				.short
				.get_or_create_shortstatekey(event_type, state_key)
				.await;
			(ssk, event_id.clone())
		})
		.collect()
		.await;

	trace!("Compressing state...");
	let new_room_state: CompressedState = self
		.services
		.state_compressor
		.compress_state_events(state_events.iter().map(|(ssk, eid)| (ssk, eid.borrow())))
		.collect()
		.await;

	Ok(Arc::new(new_room_state))
}

#[implement(super::Service)]
#[tracing::instrument(name = "rezzy", level = "debug", skip_all, fields(%room_id))]
pub async fn state_resolution<'a, StateSets>(
	&'a self,
	room_id: &RoomId,
	room_version: &'a RoomVersionId,
	state_sets: StateSets,
	prefetch_cache: Option<PduCache>,
) -> Result<StateMap<OwnedEventId>>
where
	StateSets: Iterator<Item = &'a StateMap<OwnedEventId>> + Clone + Send,
{
	let state_sets_vec: Vec<&StateMap<OwnedEventId>> = state_sets.collect();
	let num_maps = state_sets_vec.len();

	if num_maps == 0 {
		return Ok(StateMap::new());
	}
	if num_maps == 1 {
		return Ok(state_sets_vec[0].clone());
	}

	let lean_state_sets: Vec<rezzy::SharedState<String>> = state_sets_vec
		.iter()
		.map(|map| {
			let mut ss = rezzy::SharedState::new();
			for ((ty, sk), id) in *map {
				ss.insert((ty.to_string().into(), sk.to_string()), id.to_string());
			}
			ss
		})
		.collect();

	// Map room version early
	let version = match room_version.as_str() {
		| "1" => rezzy::StateResVersion::V1,
		| "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "10" | "11" =>
			rezzy::StateResVersion::V2,
		| "12" => rezzy::StateResVersion::V2_1,
		| _ => rezzy::StateResVersion::V2_1_1,
	};

	struct LocalArenaProvider<'a, F> {
		global_cache: &'a moka::sync::Cache<OwnedEventId, Arc<rezzy::LeanEvent<String>>>,
		arena: typed_arena::Arena<Arc<rezzy::LeanEvent<String>>>,
		version: rezzy::StateResVersion,
		fetch_pdu: F,
	}

	impl<F> rezzy::basespec::rezzy_types::EventProvider<String, serde_json::Value>
		for LocalArenaProvider<'_, F>
	where
		F: Fn(&OwnedEventId) -> Option<conduwuit_core::PduEvent>,
	{
		fn get_event(&self, id: &String) -> Option<&rezzy::LeanEvent<String>> {
			let event_id = OwnedEventId::try_from(id.as_str()).ok()?;

			if let Some(cached_arc) = self.global_cache.get(&event_id) {
				let local_arc = self.arena.alloc(cached_arc);
				return Some(&**local_arc);
			}

			let pdu = (self.fetch_pdu)(&event_id)?;
			let power_level = sender_power_level_from_auth(self.version, &pdu, |auth_event_id| {
				(self.fetch_pdu)(auth_event_id)
			});
			let lean = Arc::new(pdu_to_lean(&pdu, power_level));

			self.global_cache.insert(event_id, lean.clone());

			let local_arc = self.arena.alloc(lean);
			Some(&**local_arc)
		}
	}

	let timeline = &self.services.timeline;
	let prefetch_cache_ref = prefetch_cache.as_ref();
	let meta = &self.services.pdu_metadata;
	let handle = tokio::runtime::Handle::current();

	let fetch_pdu = move |eid: &OwnedEventId| -> Option<conduwuit_core::PduEvent> {
		let do_fetch = |handle: &tokio::runtime::Handle, eid: &OwnedEventId| {
			handle.block_on(async {
				if let Some(cache) = prefetch_cache_ref {
					if let Some(pdu) = cache.read().await.get(eid) {
						return Some((**pdu).clone());
					}
				}

				if let Ok(mut pdu) = timeline.get_pdu_in_room(Some(room_id), eid).await {
					if meta.is_event_rejected(&pdu.event_id).await
						&& timeline.non_outlier_pdu_exists(&pdu.event_id).await
					{
						warn!(
							event_id = %pdu.event_id,
							"state_res: clearing stale rejection flag on timeline event"
						);
						meta.unmark_event_rejected(&pdu.event_id);
						pdu.rejected = false;
					}
					Some(pdu)
				} else {
					None
				}
			})
		};

		// block_in_place yields the current worker slot so other tasks can
		// progress while we block.  On CurrentThread runtimes (unit tests)
		// there is no spare worker, so we spawn a dedicated thread instead.
		if matches!(handle.runtime_flavor(), tokio::runtime::RuntimeFlavor::MultiThread) {
			tokio::task::block_in_place(|| do_fetch(&handle, eid))
		} else {
			let eid = eid.clone();
			let handle = handle.clone();
			std::thread::scope(|s| s.spawn(|| do_fetch(&handle, &eid)).join().unwrap())
		}
	};

	let provider = LocalArenaProvider {
		global_cache: &self.services.short.leanevent_cache,
		arena: typed_arena::Arena::new(),
		version,
		fetch_pdu,
	};

	let resolved_lean = if matches!(
		version,
		rezzy::StateResVersion::V2_1
			| rezzy::StateResVersion::V2_1_1
			| rezzy::StateResVersion::V2_2
	) {
		// V2.1+ subgraph computation needs full auth chain visibility.
		// Build event context by BFS-walking auth chains from all fork state events.
		let mut ctx: HashMap<String, rezzy::LeanEvent<String>> = HashMap::new();
		let mut q: std::collections::VecDeque<String> = lean_state_sets
			.iter()
			.flat_map(|ss| ss.values().cloned())
			.collect();
		while let Some(eid) = q.pop_front() {
			if ctx.contains_key(&eid) {
				continue;
			}
			let ev = <_ as rezzy::basespec::rezzy_types::EventProvider<
				String,
				serde_json::Value,
			>>::get_event(&provider, &eid);
			if let Some(ev) = ev {
				for aid in &ev.auth_events {
					if !ctx.contains_key(aid) {
						q.push_back(aid.clone());
					}
				}
				ctx.insert(eid, ev.clone());
			}
		}
		rezzy::resolve::multi::resolve_state_maps(&lean_state_sets, &ctx, version)
	} else {
		// V2 and below: lazy BFS is sufficient.
		rezzy::resolve::multi::resolve_state_maps_lazy_with_diff(
			&lean_state_sets,
			&provider,
			None::<Vec<String>>,
			version,
		)
	};

	// Convert back to Ruma StateMap
	let mut resolved = StateMap::new();
	for ((ty_str, sk_str), eid_str) in resolved_lean {
		let ty: ruma::events::StateEventType = ty_str.to_string().into();
		let sk: conduwuit_core::matrix::StateKey = sk_str.into();
		if let Ok(eid) = OwnedEventId::try_from(eid_str.as_str()) {
			resolved.insert((ty, sk), eid);
		}
	}

	Ok(resolved)
}

fn sender_power_level_from_auth<F>(
	version: rezzy::StateResVersion,
	pdu: &conduwuit_core::PduEvent,
	mut fetch_auth: F,
) -> i64
where
	F: FnMut(&OwnedEventId) -> Option<conduwuit_core::PduEvent>,
{
	if pdu.kind == ruma::events::TimelineEventType::RoomCreate {
		return i64::MAX;
	}

	// Room version 12+ ("explicitly privilege room creators", see
	// `RoomVersion::explicitly_privilege_room_creators` and the auth-rules
	// checks in `state_res::event_auth`): the room creator, and any user
	// listed in the create event's `additional_creators`, always sort with
	// `i64::MAX` power for state-resolution purposes -- this overrides
	// whatever (if anything) the power-levels event says about them.
	let explicitly_privilege_room_creators = matches!(
		version,
		rezzy::StateResVersion::V2_1
			| rezzy::StateResVersion::V2_1_1
			| rezzy::StateResVersion::V2_2
	);

	// Scan the full auth chain up-front so both the create event and a
	// power-levels event can be found regardless of which order they appear
	// in `auth_events` -- a PL-bearing event must not return before the
	// creator-privilege override below has had a chance to apply.
	let mut create_pdu = None;
	let mut pl_level = None;

	for auth_event_id in &pdu.auth_events {
		if create_pdu.is_some() && pl_level.is_some() {
			break;
		}

		let Some(auth_pdu) = fetch_auth(auth_event_id) else {
			continue;
		};

		if auth_pdu.kind == ruma::events::TimelineEventType::RoomCreate
			&& auth_pdu.state_key.as_deref() == Some("")
		{
			create_pdu = Some(auth_pdu);
			continue;
		}

		if pl_level.is_some()
			|| auth_pdu.kind != ruma::events::TimelineEventType::RoomPowerLevels
			|| auth_pdu.state_key.as_deref() != Some("")
		{
			continue;
		}

		let content_val: serde_json::Value =
			serde_json::from_str(auth_pdu.content.get()).unwrap_or(serde_json::Value::Null);
		let parse_intlike = |value: &serde_json::Value| {
			value
				.as_i64()
				.or_else(|| value.as_str().and_then(|s| s.parse().ok()))
		};

		let level = content_val
			.get("users")
			.and_then(serde_json::Value::as_object)
			.and_then(|users| users.get(pdu.sender.as_str()))
			.and_then(parse_intlike)
			.or_else(|| content_val.get("users_default").and_then(parse_intlike))
			.unwrap_or(0);

		pl_level = Some(level);
	}

	let Some(create_pdu) = create_pdu.as_ref() else {
		return pl_level.unwrap_or(0);
	};

	if explicitly_privilege_room_creators {
		// Room version 12+: creator status is determined solely by "sent the
		// create event" or "listed in `additional_creators`" -- the deprecated
		// `creator` content field is a pre-v11 concept and irrelevant here (see
		// `state_res::event_auth`'s identical v12 check on `sender_power_level`).
		let is_v12_creator = create_pdu.sender == pdu.sender
			|| serde_json::from_str::<ruma::events::room::create::RoomCreateEventContent>(
				create_pdu.content.get(),
			)
			.is_ok_and(|create_content| {
				create_content
					.additional_creators
					.as_ref()
					.is_some_and(|creators| creators.iter().any(|creator| creator == &pdu.sender))
			});

		if is_v12_creator {
			return i64::MAX;
		}

		return pl_level.unwrap_or(0);
	}

	if let Some(level) = pl_level {
		return level;
	}

	// Pre-v12 fallback: with no power-levels event found in the auth chain,
	// only the room's original creator gets an implicit power level of 100.
	#[allow(deprecated)]
	let is_pre_v12_creator = create_pdu.sender == pdu.sender
		|| serde_json::from_str::<ruma::events::room::create::RoomCreateEventContent>(
			create_pdu.content.get(),
		)
		.is_ok_and(|create_content| {
			create_content
				.creator
				.as_ref()
				.is_some_and(|creator| creator == &pdu.sender)
		});

	if is_pre_v12_creator { 100 } else { 0 }
}

fn pdu_to_lean(pdu: &conduwuit_core::PduEvent, power_level: i64) -> rezzy::LeanEvent<String> {
	let content_val: serde_json::Value =
		serde_json::from_str(pdu.content.get()).unwrap_or(serde_json::Value::Null);
	rezzy::LeanEvent {
		event_id: pdu.event_id.to_string(),
		event_type: pdu.kind.to_string(),
		state_key: pdu.state_key.as_ref().map(|k| format!("{k}")),
		power_level,
		origin_server_ts: pdu.origin_server_ts.into(),
		sender: pdu.sender.to_string(),
		content: content_val,
		prev_events: pdu.prev_events.iter().map(|id| format!("{id}")).collect(),
		auth_events: pdu.auth_events.iter().map(|id| format!("{id}")).collect(),
		depth: u64::from(pdu.depth),
		..Default::default()
	}
}
