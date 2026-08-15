use std::{
	collections::{HashMap, HashSet},
	fs::File,
	io::BufReader,
	sync::Arc,
};

use conduwuit_core::matrix::{Pdu, state_res::StateMap};
use ruma::{OwnedEventId, RoomVersionId};

enum Mode {
	/// Single JSONL file with DAG events, auto-detect heads
	Dag {
		path: String,
		room_version: RoomVersionId,
		baseline_path: Option<String>,
	},
	/// Two state response JSON files representing competing state sets
	StateSets {
		state_set_a: String,
		state_set_b: String,
		room_version: RoomVersionId,
		baseline_path: Option<String>,
	},
}

fn parse_room_version(s: &str) -> RoomVersionId {
	match s {
		| "1" => RoomVersionId::V1,
		| "2" => RoomVersionId::V2,
		| "3" => RoomVersionId::V3,
		| "4" => RoomVersionId::V4,
		| "5" => RoomVersionId::V5,
		| "6" => RoomVersionId::V6,
		| "7" => RoomVersionId::V7,
		| "8" => RoomVersionId::V8,
		| "9" => RoomVersionId::V9,
		| "10" => RoomVersionId::V10,
		| "11" => RoomVersionId::V11,
		| "12" => RoomVersionId::V12,
		| v => panic!("Unknown room version: {v}"),
	}
}

fn parse_args() -> Mode {
	let args: Vec<String> = std::env::args().collect();

	if args.get(1).map(|s| s.as_str()) == Some("--state-sets") {
		let state_set_a = args.get(2).cloned().expect(
			"Usage: compare --state-sets <state_a.json> <state_b.json> <version> [baseline.json]",
		);
		let state_set_b = args.get(3).cloned().expect(
			"Usage: compare --state-sets <state_a.json> <state_b.json> <version> [baseline.json]",
		);
		let version_str = args.get(4).map(|s| s.as_str()).unwrap_or("12");
		let baseline_path = args.get(5).cloned();
		Mode::StateSets {
			state_set_a,
			state_set_b,
			room_version: parse_room_version(version_str),
			baseline_path,
		}
	} else {
		let path = args.get(1).cloned().unwrap_or_else(|| {
			"/run/media/shane/shane4tb-ent/dags/\
			 merged-sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE-unredacted-lounge-v12-d1-84135.\
			 jsonl"
				.to_string()
		});
		let version_str = args.get(2).map(|s| s.as_str()).unwrap_or("12");
		let baseline_path = args.get(3).cloned();
		Mode::Dag {
			path,
			room_version: parse_room_version(version_str),
			baseline_path,
		}
	}
}

fn load_events_jsonl(path: &str) -> HashMap<OwnedEventId, Arc<Pdu>> {
	use std::io::BufRead;
	let mut events_map = HashMap::new();
	let file = File::open(path).unwrap_or_else(|e| panic!("Failed to open {path}: {e}"));
	let reader = BufReader::new(file);
	for line in reader.lines().map_while(Result::ok) {
		if line.trim().is_empty() {
			continue;
		}
		if let Ok(pdu) = serde_json::from_str::<Pdu>(&line) {
			events_map.insert(pdu.event_id.clone(), Arc::new(pdu));
		}
	}
	events_map
}

fn load_events_json(path: &str) -> Vec<Pdu> {
	let file = File::open(path).unwrap_or_else(|e| panic!("Failed to open {path}: {e}"));
	let reader = BufReader::new(file);
	let raw_events: Vec<serde_json::Value> =
		serde_json::from_reader(reader).unwrap_or_else(|e| panic!("Failed to parse {path}: {e}"));

	raw_events
		.into_iter()
		.filter_map(|mut ev| {
			// State response events may lack prev_events, depth, hashes
			if ev.get("prev_events").is_none() {
				ev.as_object_mut()
					.unwrap()
					.insert("prev_events".into(), serde_json::json!([]));
			}
			if ev.get("depth").is_none() {
				ev.as_object_mut()
					.unwrap()
					.insert("depth".into(), serde_json::json!(0));
			}
			if ev.get("hashes").is_none() {
				ev.as_object_mut()
					.unwrap()
					.insert("hashes".into(), serde_json::json!({"sha256": ""}));
			}
			if ev.get("auth_events").is_none() {
				ev.as_object_mut()
					.unwrap()
					.insert("auth_events".into(), serde_json::json!([]));
			}
			match serde_json::from_value::<Pdu>(ev) {
				| Ok(pdu) => Some(pdu),
				| Err(e) => {
					eprintln!("Warning: skipping event: {e}");
					None
				},
			}
		})
		.collect()
}

fn compare_against_baseline(
	conduwuit_resolved: &conduwuit_core::matrix::StateMap<OwnedEventId>,
	events_map: &HashMap<OwnedEventId, Arc<Pdu>>,
	baseline_path: &str,
) {
	let output_file = File::open(baseline_path)
		.unwrap_or_else(|e| panic!("Failed to open baseline {baseline_path}: {e}"));
	let output_reader = BufReader::new(output_file);
	let baseline_ids: Vec<OwnedEventId> =
		serde_json::from_reader(output_reader).expect("Failed to parse baseline");

	let baseline_set: HashSet<OwnedEventId> = baseline_ids.into_iter().collect();
	let conduwuit_set: HashSet<OwnedEventId> = conduwuit_resolved.values().cloned().collect();

	let missing: Vec<_> = baseline_set.difference(&conduwuit_set).collect();
	let extra: Vec<_> = conduwuit_set.difference(&baseline_set).collect();

	if missing.is_empty() && extra.is_empty() {
		println!("SUCCESS: Conduwuit perfectly matches baseline!");
	} else {
		println!(
			"DIVERGENCE: {} missing, {} extra (baseline={}, conduwuit={})",
			missing.len(),
			extra.len(),
			baseline_set.len(),
			conduwuit_set.len()
		);
		for id in missing.iter().take(10) {
			if let Some(ev) = events_map.get(*id) {
				println!(
					"  MISSING: {} type={}, state_key={}, sender={}",
					id,
					ev.kind,
					ev.state_key.as_deref().unwrap_or(""),
					ev.sender
				);
			} else {
				println!("  MISSING: {} (event not in local map)", id);
			}
		}
		for id in extra.iter().take(10) {
			if let Some(ev) = events_map.get(*id) {
				println!(
					"  EXTRA:   {} type={}, state_key={}, sender={}",
					id,
					ev.kind,
					ev.state_key.as_deref().unwrap_or(""),
					ev.sender
				);
			}
		}
	}
}

fn print_membership_counts(
	resolved: &conduwuit_core::matrix::StateMap<OwnedEventId>,
	events_map: &HashMap<OwnedEventId, Arc<Pdu>>,
) {
	let (mut joined, mut left, mut banned, mut invite, mut knock) = (0, 0, 0, 0, 0);
	for id in resolved.values() {
		if let Some(ev) = events_map.get(id)
			&& ev.kind == ruma::events::TimelineEventType::RoomMember
			&& let Ok(member) = serde_json::from_str::<
				ruma::events::room::member::RoomMemberEventContent,
			>(ev.content.get())
		{
			match member.membership {
				| ruma::events::room::member::MembershipState::Join => joined += 1,
				| ruma::events::room::member::MembershipState::Leave => left += 1,
				| ruma::events::room::member::MembershipState::Ban => banned += 1,
				| ruma::events::room::member::MembershipState::Invite => invite += 1,
				| ruma::events::room::member::MembershipState::Knock => knock += 1,
				| _ => {},
			}
		}
	}
	println!(
		"CONDUWUIT COUNTS: {} joined, {} left, {} banned, {} invite, {} knock",
		joined, left, banned, invite, knock
	);
}

fn export_results(
	resolved: &conduwuit_core::matrix::StateMap<OwnedEventId>,
	events_map: &HashMap<OwnedEventId, Arc<Pdu>>,
	duration: std::time::Duration,
	room_version: &RoomVersionId,
) {
	let conduwuit_set: HashSet<OwnedEventId> = resolved.values().cloned().collect();
	let mut conduwuit_pdus: Vec<Arc<Pdu>> = conduwuit_set
		.iter()
		.filter_map(|id| events_map.get(id).cloned())
		.collect();
	conduwuit_pdus.sort_by(|a, b| {
		let a_key = (a.kind.to_string(), a.state_key.as_deref().unwrap_or(""));
		let b_key = (b.kind.to_string(), b.state_key.as_deref().unwrap_or(""));
		a_key.cmp(&b_key)
	});
	let sorted_ids: Vec<String> = conduwuit_pdus
		.into_iter()
		.map(|p| p.event_id.to_string())
		.collect();

	let output_json = serde_json::json!({
		"resolved_state_size": sorted_ids.len(),
		"state_event_ids": sorted_ids,
		"duration_ms": duration.as_millis(),
		"room_version": format!("{:?}", room_version),
	});

	let mut out_file = File::create("/tmp/conduwuit-output.json").unwrap();
	serde_json::to_writer_pretty(&mut out_file, &output_json).unwrap();
	println!("Exported to /tmp/conduwuit-output.json");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	tracing_subscriber::fmt::init();

	let mode = parse_args();

	match mode {
		| Mode::Dag { path, room_version, baseline_path } =>
			run_dag_mode(&path, &room_version, baseline_path.as_deref()).await,
		| Mode::StateSets {
			state_set_a,
			state_set_b,
			room_version,
			baseline_path,
		} =>
			run_state_sets_mode(
				&state_set_a,
				&state_set_b,
				&room_version,
				baseline_path.as_deref(),
			)
			.await,
	}
}

async fn run_state_sets_mode(
	state_a_path: &str,
	state_b_path: &str,
	room_version: &RoomVersionId,
	baseline_path: Option<&str>,
) -> anyhow::Result<()> {
	let start_time = std::time::Instant::now();

	println!("Loading state set A: {}...", state_a_path);
	let state_a_events = load_events_json(state_a_path);
	println!("  {} events", state_a_events.len());

	println!("Loading state set B: {}...", state_b_path);
	let state_b_events = load_events_json(state_b_path);
	println!("  {} events", state_b_events.len());

	// Build combined event map
	let mut events_map: HashMap<OwnedEventId, Arc<Pdu>> = HashMap::new();
	for ev in state_a_events.iter().chain(state_b_events.iter()) {
		events_map
			.entry(ev.event_id.clone())
			.or_insert_with(|| Arc::new(ev.clone()));
	}
	println!("Combined event pool: {} unique events", events_map.len());

	// Build state maps (type, state_key) -> event_id
	let state_map_a: conduwuit_core::matrix::StateMap<OwnedEventId> = state_a_events
		.iter()
		.filter_map(|ev| {
			let sk = ev.state_key.as_deref()?;
			Some((ev.kind.clone().into(), sk.into()))
		})
		.zip(state_a_events.iter().map(|ev| ev.event_id.clone()))
		.collect();

	let state_map_b: conduwuit_core::matrix::StateMap<OwnedEventId> = state_b_events
		.iter()
		.filter_map(|ev| {
			let sk = ev.state_key.as_deref()?;
			Some((ev.kind.clone().into(), sk.into()))
		})
		.zip(state_b_events.iter().map(|ev| ev.event_id.clone()))
		.collect();

	// Count conflicts
	let mut conflicts = 0;
	for key in state_map_a.keys() {
		if let Some(id_b) = state_map_b.get(key)
			&& state_map_a[key] != *id_b
		{
			conflicts += 1;
		}
	}
	let only_a = state_map_a
		.keys()
		.filter(|k| !state_map_b.contains_key(k))
		.count();
	let only_b = state_map_b
		.keys()
		.filter(|k| !state_map_a.contains_key(k))
		.count();
	println!("State A: {} keys, State B: {} keys", state_map_a.len(), state_map_b.len());
	println!("Conflicts: {}, Only in A: {}, Only in B: {}", conflicts, only_a, only_b);

	// Resolve using rezzy
	let state_sets = vec![state_map_a, state_map_b];
	let conduwuit_resolved = resolve_via_rezzy(&state_sets, &events_map, room_version);

	let duration = start_time.elapsed();
	println!(
		"Rezzy resolved {} events in {:.2}s",
		conduwuit_resolved.len(),
		duration.as_secs_f64()
	);

	print_membership_counts(&conduwuit_resolved, &events_map);

	if let Some(bp) = baseline_path {
		compare_against_baseline(&conduwuit_resolved, &events_map, bp);
	} else {
		println!("No baseline provided — skipping comparison.");
	}

	export_results(&conduwuit_resolved, &events_map, duration, room_version);

	Ok(())
}

async fn run_dag_mode(
	path: &str,
	room_version: &RoomVersionId,
	baseline_path: Option<&str>,
) -> anyhow::Result<()> {
	let start_time = std::time::Instant::now();

	println!("Loading {}...", path);
	let events_map = load_events_jsonl(path);
	println!("Loaded {} total events.", events_map.len());

	// Compute heads (forward extremities)
	let mut referenced = HashSet::new();
	for pdu in events_map.values() {
		for pe in &pdu.prev_events {
			referenced.insert(pe.clone());
		}
	}
	let all_ids: HashSet<OwnedEventId> = events_map.keys().cloned().collect();
	let mut heads: Vec<OwnedEventId> = all_ids.difference(&referenced).cloned().collect();
	heads.sort();

	println!("Found {} heads", heads.len());

	// Build state sets per head via backward walk
	let mut state_sets = Vec::new();
	for head_id in &heads {
		let mut reachable = Vec::new();
		let mut visited = HashSet::new();
		let mut stack = vec![head_id.clone()];
		while let Some(ev_id) = stack.pop() {
			if visited.insert(ev_id.clone())
				&& let Some(ev) = events_map.get(&ev_id)
			{
				reachable.push(ev.clone());
				for pe in &ev.prev_events {
					stack.push(pe.clone());
				}
			}
		}

		reachable.sort_by(|a, b| a.depth.cmp(&b.depth).then(a.event_id.cmp(&b.event_id)));

		let mut state_map = HashMap::new();
		for ev in reachable {
			if let Some(state_key) = ev.state_key.as_deref() {
				let key = (ev.kind.clone().into(), state_key.into());
				state_map.insert(key, ev.event_id.clone());
			}
		}
		state_sets.push(state_map);
	}

	// Resolve using rezzy
	let conduwuit_resolved = resolve_via_rezzy(&state_sets, &events_map, room_version);

	let duration = start_time.elapsed();
	println!(
		"Rezzy resolved {} events in {:.2}s",
		conduwuit_resolved.len(),
		duration.as_secs_f64()
	);

	print_membership_counts(&conduwuit_resolved, &events_map);

	if let Some(bp) = baseline_path {
		compare_against_baseline(&conduwuit_resolved, &events_map, bp);
	} else {
		println!("No baseline provided — skipping comparison.");
	}

	export_results(&conduwuit_resolved, &events_map, duration, room_version);

	Ok(())
}

/// Resolve state using rezzy::resolve_iterative_sort instead of ruma's
/// state_res::resolve.
///
/// Builds the unconflicted/conflicted/auth_context split from state sets and
/// the full events map, then calls rezzy synchronously.
fn resolve_via_rezzy(
	state_sets: &[conduwuit_core::matrix::StateMap<OwnedEventId>],
	events_map: &HashMap<OwnedEventId, Arc<Pdu>>,
	room_version: &RoomVersionId,
) -> StateMap<OwnedEventId> {
	use rezzy::state::at::SharedState;

	// Map room version to rezzy StateResVersion
	let version = match room_version.as_str() {
		| "1" => rezzy::StateResVersion::V1,
		| "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "10" | "11" =>
			rezzy::StateResVersion::V2,
		| "12" => rezzy::StateResVersion::V2_1,
		| "12.1" => rezzy::StateResVersion::V2_1_1,
		| v => panic!("Unknown room version: {v}"),
	};

	let num_maps = state_sets.len();
	if num_maps == 0 {
		return HashMap::new();
	}
	if num_maps == 1 {
		return state_sets[0].clone();
	}

	// Separate unconflicted vs conflicted keys
	let mut counts: HashMap<(String, String, String), usize> = HashMap::new();
	let mut key_to_ids: HashMap<(String, String), HashSet<String>> = HashMap::new();

	for map in state_sets {
		for ((ty, sk), id) in map {
			let ty_s = ty.to_string();
			let sk_s = sk.to_string();
			let id_s = id.to_string();
			*counts
				.entry((ty_s.clone(), sk_s.clone(), id_s.clone()))
				.or_insert(0) += 1;
			key_to_ids.entry((ty_s, sk_s)).or_default().insert(id_s);
		}
	}

	let mut unconflicted: SharedState = SharedState::new();
	let mut conflicted_eids: HashSet<String> = HashSet::new();

	for (key, ids) in &key_to_ids {
		if ids.len() == 1 {
			let id = ids.iter().next().unwrap();
			let count = counts
				.get(&(key.0.clone(), key.1.clone(), id.clone()))
				.copied()
				.unwrap_or(0);
			if count == num_maps {
				unconflicted.insert((key.0.clone(), key.1.clone()), id.clone());
				continue;
			}
		}
		for id in ids {
			conflicted_eids.insert(id.clone());
		}
	}

	// Collect all state event IDs for auth chain computation
	let all_state_eids: HashSet<String> = state_sets
		.iter()
		.flat_map(|m| m.values())
		.map(ToString::to_string)
		.collect();

	// Walk auth chains from all state events
	let mut auth_chain_union: HashSet<String> = HashSet::new();
	let mut stack: Vec<String> = all_state_eids.into_iter().collect();
	let mut visited = HashSet::new();
	while let Some(eid) = stack.pop() {
		if !visited.insert(eid.clone()) {
			continue;
		}
		if let Ok(eid_owned) = OwnedEventId::try_from(eid.as_str())
			&& let Some(ev) = events_map.get(&eid_owned)
		{
			auth_chain_union.insert(eid.clone());
			for auth_id in &ev.auth_events {
				stack.push(auth_id.to_string());
			}
		}
	}

	// Auth chain diff: add diff events to conflicted set
	let is_v2_1_plus = matches!(
		version,
		rezzy::StateResVersion::V2_1
			| rezzy::StateResVersion::V2_1_1
			| rezzy::StateResVersion::V2_2
	);

	if !is_v2_1_plus {
		// V2: auth chain diff adds events to conflicted set
		let mut per_set_chains: Vec<HashSet<String>> = Vec::new();
		for map in state_sets {
			let mut chain = HashSet::new();
			let mut stack: Vec<String> = map.values().map(ToString::to_string).collect();
			let mut visited = HashSet::new();
			while let Some(eid) = stack.pop() {
				if !visited.insert(eid.clone()) {
					continue;
				}
				if let Ok(eid_owned) = OwnedEventId::try_from(eid.as_str())
					&& let Some(ev) = events_map.get(&eid_owned)
				{
					chain.insert(eid.clone());
					for auth_id in &ev.auth_events {
						stack.push(auth_id.to_string());
					}
				}
			}
			per_set_chains.push(chain);
		}
		let intersect: HashSet<String> = per_set_chains
			.iter()
			.skip(1)
			.fold(per_set_chains[0].clone(), |acc, c| acc.intersection(c).cloned().collect());
		for eid in &auth_chain_union {
			if !intersect.contains(eid) {
				conflicted_eids.insert(eid.clone());
			}
		}
	}

	// Convert PDUs to LeanEvents
	let to_lean = |pdu: &Pdu| -> rezzy::LeanEvent {
		let content_val: serde_json::Value =
			serde_json::from_str(pdu.content.get()).unwrap_or(serde_json::Value::Null);
		rezzy::LeanEvent {
			event_id: pdu.event_id.to_string(),
			event_type: pdu.kind.to_string(),
			state_key: pdu.state_key.as_ref().map(ToString::to_string),
			power_level: content_val
				.get("power_level")
				.and_then(|pl| pl.as_i64())
				.unwrap_or(0),
			origin_server_ts: pdu.origin_server_ts.into(),
			sender: pdu.sender.to_string(),
			content: content_val,
			prev_events: pdu.prev_events.iter().map(ToString::to_string).collect(),
			auth_events: pdu.auth_events.iter().map(ToString::to_string).collect(),
			depth: u64::from(pdu.depth),
			rejected: false,
			soft_fail: false,
		}
	};

	// Build full auth context
	let mut auth_context: HashMap<String, rezzy::LeanEvent> = auth_chain_union
		.iter()
		.filter_map(|eid| {
			OwnedEventId::try_from(eid.as_str())
				.ok()
				.and_then(|owned| events_map.get(&owned))
				.map(|pdu| (eid.clone(), to_lean(pdu)))
		})
		.collect();

	// Extract conflicted events from auth_context
	let conflicted_events: HashMap<String, rezzy::LeanEvent> = if is_v2_1_plus {
		let direct: Vec<String> = conflicted_eids.iter().cloned().collect();
		let subgraph = rezzy::compute_v2_1_conflicted_subgraph(&auth_context, &direct);
		for id in subgraph.keys() {
			auth_context.remove(id);
		}
		subgraph
	} else {
		let mut ce = HashMap::new();
		for eid in &conflicted_eids {
			if let Some(lean) = auth_context.remove(eid) {
				ce.insert(eid.clone(), lean);
			}
		}
		ce
	};

	println!(
		"rezzy: {} unconflicted, {} conflicted, {} auth_context",
		unconflicted.len(),
		conflicted_events.len(),
		auth_context.len()
	);

	let mut pl_cache = HashMap::new();
	let resolved_lean = rezzy::resolve_iterative_sort(
		unconflicted,
		conflicted_events,
		&auth_context,
		version,
		&mut pl_cache,
	);

	// Convert back to StateMap
	let mut resolved = HashMap::new();
	for ((ty_str, sk_str), eid_str) in resolved_lean {
		let ty: ruma::events::StateEventType = ty_str.into();
		let sk: conduwuit_core::matrix::state_key::StateKey = sk_str.into();
		if let Ok(eid) = OwnedEventId::try_from(eid_str.as_str()) {
			resolved.insert((ty, sk), eid);
		}
	}

	resolved
}
