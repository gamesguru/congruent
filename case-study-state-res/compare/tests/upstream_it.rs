//! Integration tests running upstream `ruma-state-res` fixtures against
//! Conduwuit's engine via rezzy.
//!
//! Each test loads the same fixture files used by upstream ruma-state-res
//! tests. "Batched" tests load multiple PDU files, each representing one side
//! of a DAG fork (plus a common bootstrap). State is built per-fork and fed to
//! `rezzy::resolve_iterative_sort` as separate state sets.
//!
//! "State map" tests (MSC4297) load explicit state maps + PDU definitions.

use std::{
	collections::{BTreeSet, HashMap, HashSet, VecDeque},
	fs,
	path::Path,
};

use conduwuit_core::{
	PduEvent,
	matrix::{Event, state_res::StateMap},
};
use ruma::{CanonicalJsonValue, EventId, OwnedEventId, RoomVersionId};

const FIXTURES_DIR: &str = "../../ruma-upstream/crates/ruma-state-res/tests/it/resolve/fixtures";
const SNAPSHOTS_DIR: &str =
	"../../ruma-upstream/crates/ruma-state-res/tests/it/resolve/snapshots";

// ==========================================
// Fixture Loading
// ==========================================

fn fixtures_path() -> Option<&'static Path> {
	let p = Path::new(FIXTURES_DIR);
	p.exists().then_some(p)
}

fn snapshots_path() -> Option<&'static Path> {
	let p = Path::new(SNAPSHOTS_DIR);
	p.exists().then_some(p)
}

fn load_pdus_from_file(filename: &str) -> Vec<PduEvent> {
	let path = fixtures_path()
		.expect("fixtures_path checked before load")
		.join(filename);
	let content = fs::read_to_string(&path)
		.unwrap_or_else(|_| panic!("Failed to read fixture: {:?}", path));

	let json_array: Vec<CanonicalJsonValue> =
		serde_json::from_str(&content).expect("Fixture is not valid JSON");

	json_array
		.into_iter()
		.map(|v| {
			let mut obj = match v {
				| CanonicalJsonValue::Object(o) => o,
				| _ => panic!("Event is not a JSON object"),
			};

			// Inject required fields missing from upstream fixtures
			if !obj.contains_key("room_id") {
				obj.insert(
					"room_id".to_string(),
					CanonicalJsonValue::String("!test_room:conduwuit.local".to_string()),
				);
			}
			if !obj.contains_key("depth") {
				obj.insert("depth".to_string(), CanonicalJsonValue::Integer(ruma::Int::from(0)));
			}
			if !obj.contains_key("hashes") {
				use std::collections::BTreeMap;
				let mut hashes = BTreeMap::new();
				hashes.insert("sha256".to_string(), CanonicalJsonValue::String(String::new()));
				obj.insert("hashes".to_string(), CanonicalJsonValue::Object(hashes));
			}
			if !obj.contains_key("signatures") {
				use std::collections::BTreeMap;
				obj.insert("signatures".to_string(), CanonicalJsonValue::Object(BTreeMap::new()));
			}

			let event_id_str = obj.get("event_id").unwrap().as_str().unwrap().to_owned();
			let event_id = EventId::parse(&event_id_str).unwrap();

			PduEvent::from_id_val(event_id, obj, None).expect("Failed to parse PduEvent")
		})
		.collect()
}

fn load_event_id_list(filename: &str) -> Vec<OwnedEventId> {
	let path = fixtures_path()
		.expect("fixtures_path checked before load")
		.join(filename);
	let content = fs::read_to_string(&path)
		.unwrap_or_else(|_| panic!("Failed to read state map: {:?}", path));

	serde_json::from_str(&content).expect("State map file is not valid JSON")
}

// ==========================================
// Auth Chain / State Building
// ==========================================

struct EventStore {
	events: HashMap<OwnedEventId, PduEvent>,
}

impl EventStore {
	fn new(all_events: &[PduEvent]) -> Self {
		let mut map = HashMap::new();
		for ev in all_events {
			map.insert(ev.event_id().to_owned(), ev.clone());
		}
		Self { events: map }
	}

	fn auth_chain(&self, event_ids: impl Iterator<Item = OwnedEventId>) -> HashSet<OwnedEventId> {
		let mut chain = HashSet::new();
		let mut queue: VecDeque<OwnedEventId> = event_ids.collect();

		while let Some(id) = queue.pop_front() {
			if !chain.insert(id.clone()) {
				continue;
			}
			if let Some(ev) = self.events.get(&id) {
				queue.extend(ev.auth_events().map(ToOwned::to_owned));
			}
		}
		chain
	}

	/// Build the accumulated state at each event by walking the DAG forward
	/// from roots. Returns state maps at each DAG leaf (forward extremity).
	fn build_state_at_leaves(&self, events: &[PduEvent]) -> Vec<StateMap<OwnedEventId>> {
		// Build forward graph: for each event, which events have it as prev_event?
		let mut forward_graph: HashMap<OwnedEventId, Vec<OwnedEventId>> = HashMap::new();
		let mut has_children: HashSet<OwnedEventId> = HashSet::new();
		let mut roots = Vec::new();

		for ev in events {
			if ev.prev_events().next().is_none() {
				roots.push(ev.event_id().to_owned());
			}
			for prev in ev.prev_events() {
				has_children.insert(prev.to_owned());
				forward_graph
					.entry(prev.to_owned())
					.or_default()
					.push(ev.event_id().to_owned());
			}
		}

		// Walk the DAG forward, accumulating state at each event
		let mut state_at: HashMap<OwnedEventId, StateMap<OwnedEventId>> = HashMap::new();
		let mut queue: VecDeque<OwnedEventId> = roots.into_iter().collect();
		let mut processed = HashSet::new();

		while let Some(eid) = queue.pop_front() {
			if processed.contains(&eid) {
				continue;
			}

			let ev = match self.events.get(&eid) {
				| Some(e) => e,
				| None => continue,
			};

			// Check all prev_events have been processed
			let all_prevs_ready = ev.prev_events().all(|p| state_at.contains_key(p));
			if !all_prevs_ready {
				// Re-queue for later
				queue.push_back(eid);
				continue;
			}

			// Merge state from all prev_events
			let mut merged_state: StateMap<OwnedEventId> = StateMap::new();
			for prev in ev.prev_events() {
				if let Some(prev_state) = state_at.get(prev) {
					for (k, v) in prev_state {
						merged_state.insert(k.clone(), v.clone());
					}
				}
			}

			// Apply this event's state
			if let Some(sk) = ev.state_key() {
				merged_state
					.insert((ev.kind().to_string().into(), sk.into()), ev.event_id().to_owned());
			}

			state_at.insert(eid.clone(), merged_state);
			processed.insert(eid.clone());

			// Queue children
			if let Some(children) = forward_graph.get(&eid) {
				for child in children {
					queue.push_back(child.clone());
				}
			}
		}

		// Find leaves (events not referenced as prev_events by any other event)
		let leaves: Vec<_> = events
			.iter()
			.filter(|ev| !has_children.contains(ev.event_id()))
			.collect();

		leaves
			.into_iter()
			.filter_map(|ev| state_at.remove(ev.event_id()))
			.collect()
	}
}

// ==========================================
// Snapshot Comparison
// ==========================================

fn extract_snapshot(snapshot_name: &str) -> String {
	let path = snapshots_path()
		.expect("snapshots_path checked before load")
		.join(format!("{snapshot_name}@resolved_state.snap"));

	let content = fs::read_to_string(&path)
		.unwrap_or_else(|_| panic!("Failed to read snapshot: {:?}", path));

	// Insta snapshots have a YAML header separated by "---\n". We want the JSON.
	let parts: Vec<&str> = content.split("---\n").collect();
	parts.last().unwrap().trim().to_string()
}

/// Format resolved state as a sorted JSON array matching upstream snapshot
/// format: each entry has event_type, state_key, event_id, and content.
fn format_state_for_snapshot(state: &StateMap<OwnedEventId>, store: &EventStore) -> String {
	#[derive(serde::Serialize)]
	struct Entry<'a> {
		#[serde(rename = "type")]
		event_type: &'a str,
		state_key: &'a str,
		event_id: &'a str,
		content: serde_json::Value,
	}

	impl PartialEq for Entry<'_> {
		fn eq(&self, other: &Self) -> bool {
			self.event_type == other.event_type && self.state_key == other.state_key
		}
	}

	impl Eq for Entry<'_> {}

	impl Ord for Entry<'_> {
		fn cmp(&self, other: &Self) -> std::cmp::Ordering {
			self.event_type
				.cmp(other.event_type)
				.then_with(|| self.state_key.cmp(other.state_key))
		}
	}

	impl PartialOrd for Entry<'_> {
		fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
			Some(self.cmp(other))
		}
	}

	let entries: BTreeSet<Entry<'_>> = state
		.iter()
		.filter_map(|((ev_type, sk), eid)| {
			let ev = store.events.get(eid)?;
			let ev_type_str = ev_type.to_string();
			let ev_type_ref: &str = Box::leak(ev_type_str.into_boxed_str());
			let content: serde_json::Value =
				serde_json::from_str(ev.content().get()).unwrap_or_default();
			Some(Entry {
				event_type: ev_type_ref,
				state_key: sk.as_str(),
				event_id: eid.as_str(),
				content,
			})
		})
		.collect();

	serde_json::to_string_pretty(&entries).unwrap()
}

// ==========================================
// Resolution Modes
// ==========================================

/// Convert a PduEvent to a rezzy LeanEvent.
fn to_lean(pdu: &PduEvent) -> rezzy::LeanEvent {
	let content_val: serde_json::Value =
		serde_json::from_str(pdu.content.get()).unwrap_or(serde_json::Value::Null);
	let power_level = content_val
		.get("power_level")
		.and_then(|pl| {
			pl.as_i64()
				.or_else(|| pl.as_str().and_then(|s| s.parse().ok()))
		})
		.unwrap_or(0);
	rezzy::LeanEvent {
		event_id: pdu.event_id.to_string(),
		event_type: pdu.kind.to_string(),
		state_key: pdu.state_key.as_ref().map(ToString::to_string),
		power_level,
		origin_server_ts: pdu.origin_server_ts.into(),
		sender: pdu.sender.to_string(),
		content: content_val,
		prev_events: pdu.prev_events.iter().map(ToString::to_string).collect(),
		auth_events: pdu.auth_events.iter().map(ToString::to_string).collect(),
		depth: u64::from(pdu.depth),
		rejected: false,
		soft_fail: false,
	}
}

/// Map RoomVersionId to rezzy's StateResVersion.
fn to_rezzy_version(v: &RoomVersionId) -> rezzy::StateResVersion {
	match v.as_str() {
		| "1" => rezzy::StateResVersion::V1,
		| "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "10" | "11" =>
			rezzy::StateResVersion::V2,
		| "12" => rezzy::StateResVersion::V2_1,
		| "12.1" => rezzy::StateResVersion::V2_1_1,
		| _ => panic!("unknown room version: {}", v.as_str()),
	}
}

/// Resolve state sets via rezzy: pre-separate unconflicted/conflicted,
/// compute auth diff, convert to LeanEvent, call resolve_iterative_sort.
fn resolve_via_rezzy(
	state_sets: &[StateMap<OwnedEventId>],
	store: &EventStore,
	room_version: &RoomVersionId,
) -> StateMap<OwnedEventId> {
	use rezzy::state::at::SharedState;

	let num_maps = state_sets.len();

	// Pre-separate unconflicted/conflicted
	let mut counts: HashMap<(String, String, String), usize> = HashMap::new();
	let mut key_to_ids: HashMap<(String, String), HashSet<String>> = HashMap::new();

	for map in state_sets {
		for ((ty, sk), id) in map {
			let ty_s = ty.to_string();
			let sk_s = sk.to_string();
			let id_s = id.to_string();
			let entry = counts
				.entry((ty_s.clone(), sk_s.clone(), id_s.clone()))
				.or_insert(0);
			*entry = entry.saturating_add(1);
			key_to_ids.entry((ty_s, sk_s)).or_default().insert(id_s);
		}
	}

	let mut unconflicted: SharedState = SharedState::new();
	let mut conflicted_keys: HashSet<(String, String)> = HashSet::new();

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
		conflicted_keys.insert(key.clone());
	}

	// Collect conflicted event IDs
	let mut conflicted_eids: HashSet<OwnedEventId> = HashSet::new();
	for map in state_sets {
		for ((ty, sk), id) in map {
			if conflicted_keys.contains(&(ty.to_string(), sk.to_string())) {
				conflicted_eids.insert(id.clone());
			}
		}
	}

	if conflicted_eids.is_empty() {
		return state_sets[0].clone();
	}

	// Compute auth chain difference
	let mut per_set_chains: Vec<HashSet<OwnedEventId>> = Vec::new();
	for map in state_sets {
		let chain = store.auth_chain(map.values().cloned());
		per_set_chains.push(chain);
	}

	let mut union_auth: HashSet<OwnedEventId> = HashSet::new();
	let mut intersect_auth: HashSet<OwnedEventId> = per_set_chains[0].clone();
	for chain in &per_set_chains {
		union_auth.extend(chain.iter().cloned());
		intersect_auth.retain(|eid| chain.contains(eid));
	}

	for eid in &union_auth {
		if !intersect_auth.contains(eid) {
			conflicted_eids.insert(eid.clone());
		}
	}

	// Build LeanEvent maps
	let mut conflicted_events: HashMap<String, rezzy::LeanEvent> = HashMap::new();
	let mut auth_context: HashMap<String, rezzy::LeanEvent> = HashMap::new();
	let mut pl_cache: HashMap<String, i64> = HashMap::new();

	// All state set values + union auth
	let mut all_ids: HashSet<OwnedEventId> = union_auth;
	for map in state_sets {
		all_ids.extend(map.values().cloned());
	}
	all_ids.extend(conflicted_eids.iter().cloned());

	for eid in &all_ids {
		if let Some(pdu) = store.events.get(eid) {
			let lean = to_lean(pdu);
			if conflicted_eids.contains(eid) {
				conflicted_events.insert(eid.to_string(), lean);
			} else {
				auth_context.insert(eid.to_string(), lean);
			}
		}
	}

	let version = to_rezzy_version(room_version);
	let resolved_lean = rezzy::resolve_iterative_sort(
		unconflicted,
		conflicted_events,
		&auth_context,
		version,
		&mut pl_cache,
	);

	// Convert back to Ruma StateMap
	let mut resolved = StateMap::new();
	for ((ty_str, sk_str), eid_str) in resolved_lean {
		let ty: ruma::events::StateEventType = ty_str.into();
		let sk: conduwuit_core::matrix::StateKey = sk_str.into();
		if let Ok(eid) = OwnedEventId::try_from(eid_str.as_str()) {
			resolved.insert((ty, sk), eid);
		}
	}
	resolved
}

/// Resolve by building per-leaf state maps from the DAG structure.
async fn resolve_batched(
	fixture_files: &[&str],
	room_version: &RoomVersionId,
) -> StateMap<OwnedEventId> {
	// Load all PDUs from all fixture files
	let mut all_events: Vec<PduEvent> = Vec::new();
	for file in fixture_files {
		all_events.extend(load_pdus_from_file(file));
	}

	let store = EventStore::new(&all_events);
	let state_sets = store.build_state_at_leaves(&all_events);

	if state_sets.is_empty() {
		panic!("No DAG leaves found — fixture DAG has no extremities");
	}

	// If there's only one leaf, no conflict to resolve — just return it
	if state_sets.len() == 1 {
		return state_sets.into_iter().next().unwrap();
	}

	resolve_via_rezzy(&state_sets, &store, room_version)
}

/// Resolve MSC4297 state map tests: explicit state maps + PDU definitions.
async fn resolve_state_maps(
	state_map_files: &[&str],
	pdu_files: &[&str],
	room_version: &RoomVersionId,
) -> StateMap<OwnedEventId> {
	// Load all PDUs
	let mut all_events: Vec<PduEvent> = Vec::new();
	for file in pdu_files {
		all_events.extend(load_pdus_from_file(file));
	}

	let store = EventStore::new(&all_events);

	// Load explicit state maps: each file is a list of event_ids
	let state_sets: Vec<StateMap<OwnedEventId>> = state_map_files
		.iter()
		.map(|file| {
			let event_ids = load_event_id_list(file);
			event_ids
				.into_iter()
				.map(|eid| {
					let ev = store
						.events
						.get(&eid)
						.unwrap_or_else(|| panic!("State map references unknown event: {eid}"));
					let sk = ev.state_key().expect("State events must have state_key");
					((ev.kind().to_string().into(), sk.into()), eid)
				})
				.collect()
		})
		.collect();

	resolve_via_rezzy(&state_sets, &store, room_version)
}

// ==========================================
// Test Registration
// ==========================================

macro_rules! batched_test {
	($name:ident, [$($file:expr),+ $(,)?], $version:expr, $snapshot:expr) => {
		#[cfg_attr(
			not(compare_has_ruma_upstream),
			ignore = "requires upstream ruma-state-res fixtures/snapshots"
		)]
		#[tokio::test]
		async fn $name() {
			if fixtures_path().is_none() || snapshots_path().is_none() {
				eprintln!("skipping {}: upstream ruma-state-res data is unavailable", stringify!($name));
				return;
			}

			let state = resolve_batched(&[$($file),+], &$version).await;
			let store = {
				let mut all = Vec::new();
				$(all.extend(load_pdus_from_file($file));)+
				EventStore::new(&all)
			};

			let result = format_state_for_snapshot(&state, &store);
			let expected = extract_snapshot($snapshot);

			assert_eq!(
				result, expected,
				"State resolution mismatch for {}",
				$snapshot
			);
		}
	};
}

macro_rules! state_map_test {
	($name:ident, states: [$($sfile:expr),+], pdus: [$($pfile:expr),+], $version:expr, $snapshot:expr) => {
		#[cfg_attr(
			not(compare_has_ruma_upstream),
			ignore = "requires upstream ruma-state-res fixtures/snapshots"
		)]
		#[tokio::test]
		async fn $name() {
			if fixtures_path().is_none() || snapshots_path().is_none() {
				eprintln!("skipping {}: upstream ruma-state-res data is unavailable", stringify!($name));
				return;
			}

			let state = resolve_state_maps(&[$($sfile),+], &[$($pfile),+], &$version).await;
			let store = {
				let mut all = Vec::new();
				$(all.extend(load_pdus_from_file($pfile));)+
				EventStore::new(&all)
			};

			let result = format_state_for_snapshot(&state, &store);
			let expected = extract_snapshot($snapshot);

			assert_eq!(
				result, expected,
				"State resolution mismatch for {}",
				$snapshot
			);
		}
	};
}

// --- Batch tests (bootstrap + fork files) ---

batched_test!(
	minimal_private_chat,
	["bootstrap-private-chat.json"],
	RoomVersionId::V11,
	"minimal_private_chat"
);

batched_test!(
	minimal_public_chat,
	["bootstrap-public-chat.json"],
	RoomVersionId::V11,
	"minimal_public_chat"
);

batched_test!(
	origin_server_ts_tiebreak,
	["bootstrap-private-chat.json", "origin-server-ts-tiebreak.json"],
	RoomVersionId::V11,
	"origin_server_ts_tiebreak"
);

batched_test!(
	ban_vs_power_levels,
	[
		"bootstrap-public-chat.json",
		"ban-vs-power-levels-alice.json",
		"ban-vs-power-levels-bob.json",
	],
	RoomVersionId::V11,
	"ban_vs_power_levels"
);

batched_test!(
	topic_vs_power_levels,
	[
		"bootstrap-public-chat.json",
		"topic-vs-power-levels-alice.json",
		"topic-vs-power-levels-bob.json",
	],
	RoomVersionId::V11,
	"topic_vs_power_levels"
);

batched_test!(
	power_levels_admin_vs_mod,
	[
		"bootstrap-public-chat.json",
		"power-levels-admin-vs-mod-alice.json",
		"power-levels-admin-vs-mod-bob.json",
	],
	RoomVersionId::V11,
	"power_levels_admin_vs_mod"
);

batched_test!(
	topic_vs_ban,
	[
		"bootstrap-public-chat.json",
		"topic-vs-ban-common.json",
		"topic-vs-ban-alice.json",
		"topic-vs-ban-bob.json",
	],
	RoomVersionId::V11,
	"topic_vs_ban"
);

batched_test!(
	join_rules_vs_join,
	[
		"bootstrap-public-chat.json",
		"join-rules-vs-join-common.json",
		"join-rules-vs-join-alice.json",
		"join-rules-vs-join-ella.json",
	],
	RoomVersionId::V11,
	"join_rules_vs_join"
);

batched_test!(
	concurrent_joins,
	[
		"bootstrap-public-chat.json",
		"concurrent-joins-charlie.json",
		"concurrent-joins-ella.json",
	],
	RoomVersionId::V11,
	"concurrent_joins"
);

// --- MSC4297 state map tests ---

state_map_test!(
	msc4297_problem_a_state_res_v2_0,
	states: [
		"MSC4297-problem-A/state-bob.json",
		"MSC4297-problem-A/state-charlie.json"
	],
	pdus: ["MSC4297-problem-A/pdus-v11.json"],
	RoomVersionId::V11,
	"msc4297_problem_a_state_res_v2_0"
);

state_map_test!(
	msc4297_problem_a_state_res_v2_1,
	states: [
		"MSC4297-problem-A/state-bob.json",
		"MSC4297-problem-A/state-charlie.json"
	],
	pdus: ["MSC4297-problem-A/pdus-v12.json"],
	RoomVersionId::V12,
	"msc4297_problem_a_state_res_v2_1"
);

state_map_test!(
	msc4297_problem_b_state_res_v2_0,
	states: [
		"MSC4297-problem-B/state-eve.json",
		"MSC4297-problem-B/state-zara.json"
	],
	pdus: ["MSC4297-problem-B/pdus-v11.json"],
	RoomVersionId::V11,
	"msc4297_problem_b_state_res_v2_0"
);

state_map_test!(
	msc4297_problem_b_state_res_v2_1,
	states: [
		"MSC4297-problem-B/state-eve.json",
		"MSC4297-problem-B/state-zara.json"
	],
	pdus: ["MSC4297-problem-B/pdus-v12.json"],
	RoomVersionId::V12,
	"msc4297_problem_b_state_res_v2_1"
);
