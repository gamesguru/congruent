#![cfg(test)]

use std::{path::PathBuf, sync::Arc};

use conduwuit_core::{
	Server,
	config::Config,
	log::{Log, LogLevelReloadHandles, capture},
	matrix::{Event, PduEvent},
};
use figment::providers::Format;
use slipstream::{CanonicalJsonObject, EventId, RoomId, events::StateEventType};

use crate::Services;

static TEST_DB_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

// RocksDB's default Env is process-global: tearing one database down joins
// background threads shared by every database in the process. Full `Services`
// instances are also heavy, so serialize the tests that build one.
static DB_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

struct TempDbGuard {
	path: PathBuf,
	// Declared after `path` so the lock is released only once the database
	// directory has been removed.
	_lock: tokio::sync::MutexGuard<'static, ()>,
}

impl Drop for TempDbGuard {
	fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.path); }
}

async fn setup_test_services() -> (TempDbGuard, Arc<Server>, Arc<Services>) {
	// The test server drives HTTP via reqwest, which requires a TLS crypto
	// provider. `rustls` is a dev-dependency built with the `ring` feature (see
	// Cargo.toml), so this is unconditional and independent of which provider
	// the library's optional `ring`/`aws_lc_rs` features select for consumers.
	let _ = rustls::crypto::ring::default_provider().install_default();
	let lock = DB_TEST_MUTEX.lock().await;
	let count = TEST_DB_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	let db_path = std::env::temp_dir().join(format!("conduwuit_state_test_db_{count}"));
	let _ = std::fs::remove_dir_all(&db_path);

	let guard = TempDbGuard { path: db_path.clone(), _lock: lock };

	let figment = figment::Figment::new().merge(figment::providers::Toml::string(&format!(
		r#"
        server_name = "test.conduwuit.local"
        database_path = "{}"
        "#,
		db_path.to_string_lossy().replace('\\', "/")
	)));

	let config = Config::new(&figment).expect("failed to parse config");
	let runtime_handle = tokio::runtime::Handle::current();
	let server = Arc::new(Server::new(config, Some(&runtime_handle), Log {
		reload: LogLevelReloadHandles::default(),
		capture: Arc::new(capture::State::default()),
	}));

	let services = Services::build(server.clone())
		.await
		.expect("failed to build services");
	(guard, server, services)
}

fn create_dummy_pdu(
	room_id: &RoomId,
	event_id: &EventId,
	event_type: &str,
	state_key: &str,
) -> PduEvent {
	let mut json = CanonicalJsonObject::new();
	json.insert(
		"room_id".into(),
		slipstream::CanonicalJsonValue::String(room_id.as_str().to_owned()),
	);
	json.insert(
		"sender".into(),
		slipstream::CanonicalJsonValue::String("@alice:test.conduwuit.local".to_owned()),
	);
	json.insert("type".into(), slipstream::CanonicalJsonValue::String(event_type.to_owned()));
	json.insert("state_key".into(), slipstream::CanonicalJsonValue::String(state_key.to_owned()));
	json.insert(
		"content".into(),
		slipstream::CanonicalJsonValue::Object(std::collections::BTreeMap::default()),
	);
	json.insert(
		"origin_server_ts".into(),
		slipstream::CanonicalJsonValue::Number(123_456_789_u64.into()),
	);
	json.insert("depth".into(), slipstream::CanonicalJsonValue::Number(1_u64.into()));
	json.insert("prev_events".into(), slipstream::CanonicalJsonValue::Array(Vec::new()));
	json.insert("auth_events".into(), slipstream::CanonicalJsonValue::Array(Vec::new()));

	let mut hashes = CanonicalJsonObject::new();
	hashes.insert("sha256".into(), slipstream::CanonicalJsonValue::String("dummy".to_owned()));
	json.insert("hashes".into(), slipstream::CanonicalJsonValue::Object(hashes));

	PduEvent::from_id_val(event_id, json, Some(room_id)).expect("failed to create pdu")
}

/// Persist a synthetic PDU into the PDU/timeline store so that the
/// state-transition cache delta can resolve an added event's PDU. This mirrors
/// the production `append_pdu` write-then-associate ordering.
async fn persist_dummy_pdu(services: &Services, room_id: &RoomId, pdu: &PduEvent) {
	let value = pdu.to_canonical_object();
	services
		.rooms
		.timeline
		.force_insert_pdu(room_id, pdu.event_id(), pdu, &value, false)
		.await
		.expect("failed to persist dummy pdu");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_state_round_trip() {
	let (_guard, _server, services) = setup_test_services().await;

	let room_id = slipstream::OwnedRoomId::from("!test:test.conduwuit.local");
	let event_id = slipstream::OwnedEventId::from("$event1:test.conduwuit.local");
	let pdu = create_dummy_pdu(&room_id, &event_id, "m.room.create", "");

	// Acquire a state lock
	let mutex = services.rooms.state.mutex.lock(&room_id).await;

	// Use set_event_state instead of append_to_state
	// This generates a root handle and atomically maps the shortevent ID.
	let root_handle = services
		.rooms
		.state
		.set_event_state(&room_id, &pdu, &mutex)
		.await
		.expect("set_event_state failed");

	// Verify room state reflects the update
	let retrieved_root = services
		.rooms
		.state
		.get_room_state_hamt(&room_id)
		.await
		.expect("failed to get room state");
	assert_eq!(retrieved_root.structural_hash, root_handle.structural_hash);
	assert_eq!(retrieved_root.state_group_id, root_handle.state_group_id);

	// Verify the short-event mapping was actually created
	let shorteventid = services
		.rooms
		.short
		.get_shorteventid(&pdu.event_id)
		.await
		.expect("shorteventid should exist after set_event_state");
	let serialized = services
		.rooms
		.state
		.db
		.shorteventid_roothandle
		.get(&shorteventid.to_be_bytes())
		.await
		.expect("mapped roothandle should exist");

	let expected = super::root_handle_to_bytes(&root_handle);
	assert_eq!(&*serialized, &*expected);
}

/// A persisted `RootHandle` must round trip every field it carries.
///
/// The previous encoding wrote only the two hashes and refilled the codec and
/// routing versions with the running build's constants on read, so a value read
/// back could not be distinguished from one written under a different codec.
#[test]
fn test_root_handle_serialization_round_trip() {
	use rezzy::hamt::{HAMT_CODEC_VERSION, HAMT_ROUTING_VERSION, RootHandle};

	let handle = RootHandle {
		codec_version: HAMT_CODEC_VERSION,
		routing_version: HAMT_ROUTING_VERSION,
		routing_params: [0, 1, 2, 3],
		structural_hash: [7; 32],
		state_group_id: [9; 32],
	};

	let bytes = super::root_handle_to_bytes(&handle);
	assert_eq!(bytes.len(), super::ROOT_HANDLE_LEN);
	assert_eq!(bytes.len(), size_of::<RootHandle>());

	let parsed = super::root_handle_from_bytes(&bytes).expect("handle should parse");
	assert_eq!(parsed.codec_version, handle.codec_version);
	assert_eq!(parsed.routing_version, handle.routing_version);
	assert_eq!(parsed.routing_params, handle.routing_params);
	assert_eq!(parsed.structural_hash, handle.structural_hash);
	assert_eq!(parsed.state_group_id, handle.state_group_id);

	// Non-default routing params are the part the old two-hash encoding dropped
	// outright; assert the bytes really carry them rather than a zero fill.
	assert!(
		bytes.windows(4).any(|w| w == [0, 1, 2, 3]),
		"routing params must survive serialization"
	);
}

#[test]
fn test_root_handle_rejects_truncated_value() {
	let handle = super::root_handle_from_bytes(&[0; 32]).err().unwrap();
	assert!(
		handle.to_string().contains("invalid length"),
		"expected a length error, got {handle}"
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_force_state() {
	let (_guard, _server, services) = setup_test_services().await;

	let room_id = slipstream::OwnedRoomId::from("!test:test.conduwuit.local");
	let event = create_dummy_pdu(
		&room_id,
		&slipstream::OwnedEventId::from("$force-state:test.conduwuit.local"),
		"m.room.create",
		"",
	);

	let mutex = services.rooms.state.mutex.lock(&room_id).await;
	let expected_root = services
		.rooms
		.state
		.set_event_state(&room_id, &event, &mutex)
		.await
		.expect("set_event_state failed");
	services
		.rooms
		.state
		.force_state(&room_id, &expected_root, &mutex)
		.await
		.expect("force_state failed");

	let retrieved_root = services
		.rooms
		.state
		.get_room_state_hamt(&room_id)
		.await
		.expect("failed to get room state");
	assert_eq!(retrieved_root.structural_hash, expected_root.structural_hash);
	assert_eq!(retrieved_root.state_group_id, expected_root.state_group_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_state_equivalence() {
	let (_guard, _server, services) = setup_test_services().await;
	let room_id = slipstream::OwnedRoomId::from("!test:test.conduwuit.local");

	// Create multiple events to build the state
	let event1 = create_dummy_pdu(
		&room_id,
		&slipstream::OwnedEventId::from("$event1:test.conduwuit.local"),
		"m.room.create",
		"",
	);
	// Deliberately empty content: a redacted member event looks like this and
	// must be treated as "leave" rather than failing to deserialize.
	let event2 = create_dummy_pdu(
		&room_id,
		&slipstream::OwnedEventId::from("$event2:test.conduwuit.local"),
		"m.room.member",
		"@alice:test.conduwuit.local",
	);

	let mutex = services.rooms.state.mutex.lock(&room_id).await;

	// Persist the events first, mirroring `append_pdu`'s write-then-associate
	// ordering: `set_event_state`'s cache delta resolves the added state event's
	// PDU.
	persist_dummy_pdu(&services, &room_id, &event1).await;
	persist_dummy_pdu(&services, &room_id, &event2).await;

	// Exercise the public state-update path (set_event_state), which persists
	// the HAMT node, sets the room root, and atomically maps the shortevent ID.
	let root1 = services
		.rooms
		.state
		.set_event_state(&room_id, &event1, &mutex)
		.await
		.expect("set_event_state 1 failed");

	let root2 = services
		.rooms
		.state
		.set_event_state(&room_id, &event2, &mutex)
		.await
		.expect("set_event_state 2 failed");

	// In a real equivalence test, we would compare this against a from-scratch
	// build. For now, we assert that the incremental state contains the expected
	// state group ID and that building it iteratively produces a valid structural
	// hash.
	assert_ne!(root1.structural_hash, root2.structural_hash);

	let final_root = services
		.rooms
		.state
		.get_room_state_hamt(&room_id)
		.await
		.expect("failed to get state");
	assert_eq!(final_root.structural_hash, root2.structural_hash);

	let create_shortstatekey = services
		.rooms
		.short
		.get_shortstatekey(&StateEventType::RoomCreate, "")
		.await
		.expect("create shortstatekey should exist");
	let create_shorteventid = services
		.rooms
		.short
		.get_shorteventid(&event1.event_id)
		.await
		.expect("create shorteventid should exist");
	let member_shortstatekey = services
		.rooms
		.short
		.get_shortstatekey(&StateEventType::RoomMember, "@alice:test.conduwuit.local")
		.await
		.expect("member shortstatekey should exist");
	let member_shorteventid = services
		.rooms
		.short
		.get_shorteventid(&event2.event_id)
		.await
		.expect("member shorteventid should exist");

	let expected = std::collections::HashMap::from([
		(create_shortstatekey, create_shorteventid),
		(member_shortstatekey, member_shorteventid),
	]);
	let actual = services
		.rooms
		.state_accessor
		.load_full_state_hamt(&final_root)
		.await
		.expect("failed to load HAMT state");
	assert_eq!(actual, expected);
}

/// Builds a room state of `members` membership entries plus the create event.
///
/// Returns the resulting root. Each entry goes in through the ordinary
/// single-key state path so the tree reaches a realistic fan-out and depth.
async fn seed_membership_state(
	services: &Services,
	room_id: &RoomId,
	mutex: &super::RoomMutexGuard,
	members: usize,
) -> rezzy::hamt::RootHandle {
	let create = create_dummy_pdu(
		room_id,
		&slipstream::OwnedEventId::from("$seed-create:test.conduwuit.local"),
		"m.room.create",
		"",
	);
	persist_dummy_pdu(services, room_id, &create).await;
	let mut root = services
		.rooms
		.state
		.set_event_state(room_id, &create, mutex)
		.await
		.expect("set_event_state create failed");

	for index in 0..members {
		let event_id_raw = format!("$seed-member-{index}:test.conduwuit.local");
		let event_id = EventId::parse(&event_id_raw).expect("seed event id should parse");
		let event = create_dummy_pdu(
			room_id,
			&event_id,
			"m.room.member",
			&format!("@user{index}:test.conduwuit.local"),
		);
		persist_dummy_pdu(services, room_id, &event).await;
		root = services
			.rooms
			.state
			.set_event_state(room_id, &event, mutex)
			.await
			.expect("set_event_state member failed");
	}

	root
}

/// A bulk state update must copy only the changed spines, not re-emit the tree.
///
/// `resolve_state` used to call `build_hamt_root_handle` +
/// `persist_node_recursive`, which wrote every node in the room's HAMT on every
/// incoming state event — `O(S)` bytes where the path-copy spine is `O(log₃₂ S)`.
/// This pins the write volume so that regression cannot come back silently: for
/// a 200-entry room the tree is ~33 nodes, so a full re-materialization is an
/// order of magnitude above the bound asserted here.
#[tokio::test(flavor = "multi_thread")]
async fn test_bulk_state_update_writes_only_changed_spines() {
	let (_guard, _server, services) = setup_test_services().await;
	let room_id = slipstream::OwnedRoomId::from("!test:test.conduwuit.local");
	let mutex = services.rooms.state.mutex.lock(&room_id).await;

	let root = seed_membership_state(&services, &room_id, &mutex, 200).await;

	// Replace one membership entry: one changed leaf, so one changed spine.
	let updated = create_dummy_pdu(
		&room_id,
		&slipstream::OwnedEventId::from("$update-7:test.conduwuit.local"),
		"m.room.member",
		"@user7:test.conduwuit.local",
	);
	persist_dummy_pdu(&services, &room_id, &updated).await;

	let target_key = services
		.rooms
		.short
		.get_or_create_shortstatekey(&StateEventType::RoomMember, "@user7:test.conduwuit.local")
		.await;
	let target_value = services
		.rooms
		.short
		.get_or_create_shorteventid(&updated.event_id)
		.await;

	// Reuse the persisted lattice so the assertion is about node volume only.
	let mut lattice = services
		.rooms
		.state
		.get_root_lattice(&root)
		.await
		.expect("seeded root must have a retained lattice");
	lattice.replace(
		&StateEventType::RoomMember.to_string(),
		"@user7:test.conduwuit.local",
		"$seed-member-7:test.conduwuit.local",
		updated.event_id.as_str(),
	);

	let structural_key =
		crate::rooms::state_hamt::room_structural_key(&services.globals.server_secret, &room_id);

	let (written_before, elided_before) =
		services.rooms.state_hamt.store.write_stats().snapshot();
	let new_root = services
		.rooms
		.state
		.persist_state_hamt_mutations(
			&structural_key,
			&root,
			vec![(target_key, Some(target_value))],
			&lattice,
		)
		.expect("bulk mutation failed");
	let (written_after, elided_after) = services.rooms.state_hamt.store.write_stats().snapshot();

	let written = written_after - written_before;
	assert_ne!(
		new_root.structural_hash, root.structural_hash,
		"changing a leaf must change the root"
	);

	// `ceil(log32(201)) == 2`, so a single-key update rewrites at most the root
	// and its one child. The generous bound still separates path copying (~3
	// nodes) from a full re-materialization (~33).
	assert!(
		written <= 8,
		"single-key bulk update wrote {written} nodes; expected a path-copy spine (<=8), which \
		 suggests the tree was re-materialized"
	);

	// The updated leaf must be readable through the new root, and every other
	// entry must have survived the update.
	let actual = services
		.rooms
		.state_accessor
		.load_full_state_hamt(&new_root)
		.await
		.expect("failed to load HAMT state");
	assert_eq!(actual.len(), 201, "state size must be preserved by the update");
	assert_eq!(actual.get(&target_key), Some(&target_value));

	// Re-applying the identical mutation produces an identical root and must not
	// write anything: the content-addressed elision has nothing left to do.
	let (rewritten_before, _) = services.rooms.state_hamt.store.write_stats().snapshot();
	let repeat_root = services
		.rooms
		.state
		.persist_state_hamt_mutations(
			&structural_key,
			&new_root,
			vec![(target_key, Some(target_value))],
			&lattice,
		)
		.expect("repeat bulk mutation failed");
	let (rewritten_after, _) = services.rooms.state_hamt.store.write_stats().snapshot();

	assert_eq!(
		repeat_root.structural_hash, new_root.structural_hash,
		"re-applying the same mutation must be idempotent"
	);
	assert_eq!(
		rewritten_after - rewritten_before,
		0,
		"idempotent re-application must not write nodes"
	);
	// The decisive assertion. Elision alone would mask a full re-walk: the
	// unchanged nodes of a re-materialized tree are byte-identical to what the
	// store already holds, so they get elided and the write counter above still
	// looks small. Asserting *zero elisions* pins the structural property
	// instead -- the update was computed from the previous root's changed spines
	// and never touched the rest of the tree.
	//
	// This matters beyond write volume: an elided write still paid to encode and
	// BLAKE3-hash the node on the way in, and elision only holds while the
	// bounded node cache retains the hash. A room large enough to evict its own
	// unchanged subtrees would fall back to writing every one of them.
	assert_eq!(
		elided_after - elided_before,
		0,
		"path-copy update must not re-emit unchanged nodes"
	);
}

/// Node reclamation must remove exactly the unreachable nodes.
///
/// The live-root set is derived from the recorded root handles, so a test that
/// only exercised the store's own bookkeeping would not catch a regression in
/// `live_root_handles` — which is where a partial root set would silently turn
/// into live-state deletion.
#[tokio::test(flavor = "multi_thread")]
async fn test_sweep_reclaims_only_unreachable_nodes() {
	use std::time::Duration;

	let (_guard, _server, services) = setup_test_services().await;
	let room_id = slipstream::OwnedRoomId::from("!sweep:test.conduwuit.local");
	let mutex = services.rooms.state.mutex.lock(&room_id).await;
	let root = seed_membership_state(&services, &room_id, &mutex, 50).await;

	// Everything reachable from the recorded root must be pinned.
	let live_roots = services
		.rooms
		.state
		.live_root_handles()
		.await
		.expect("enumerate roots");
	assert!(!live_roots.is_empty(), "the seeded room must record roots");
	let live: Vec<rezzy::hamt::RootHandle> = live_roots.clone();

	// An orphan: a real, fully-built tree that no recorded root handle points
	// at, as if its only root had been deleted.
	let mut orphan_lattice = rezzy::state::LtHash::default();
	for index in 0..8_u64 {
		orphan_lattice.insert(
			"m.room.member",
			&format!("@orphan{index}:test.conduwuit.local"),
			&format!("$orphan{index}:test.conduwuit.local"),
		);
	}
	let (_, orphan) =
		rezzy::hamt::build_hamt_root_handle(&[0xAB; 32], &orphan_lattice, [(1_u64 << 40, 7_u64)])
			.expect("build orphan tree");
	let orphan_hash = orphan.structural_hash;
	services.rooms.state_hamt.store.put_node(orphan);

	// Grace window: a node written moments ago may still be in flight, so it is
	// spared even though nothing reaches it.
	let spared = services
		.rooms
		.state
		.sweep_hamt_nodes(Duration::from_secs(60), true)
		.await
		.expect("grace-window dry run");
	assert_eq!(spared.orphaned, 0, "a just-written node must be spared by the grace window");
	assert!(
		services
			.rooms
			.state_hamt
			.store
			.get_node(&orphan_hash)
			.is_ok(),
		"a just-written node must survive a grace-window dry run"
	);

	// Age it past the window and a dry run reports it without deleting it.
	services
		.rooms
		.state_hamt
		.store
		.age_node_for_test(&orphan_hash, Duration::from_hours(1));
	let dry = services
		.rooms
		.state
		.sweep_hamt_nodes(Duration::from_secs(60), true)
		.await
		.expect("dry-run sweep");
	assert!(dry.dry_run);
	assert!(dry.orphaned >= 1, "dry run should report the orphan");
	assert!(
		services
			.rooms
			.state_hamt
			.store
			.get_node(&orphan_hash)
			.is_ok(),
		"a dry run must not delete anything"
	);

	// A live run reclaims the orphan.
	let report = services
		.rooms
		.state
		.sweep_hamt_nodes(Duration::from_secs(60), false)
		.await
		.expect("sweep");
	assert!(!report.dry_run);
	assert!(
		services
			.rooms
			.state_hamt
			.store
			.get_node(&orphan_hash)
			.is_err(),
		"the unreachable node should have been reclaimed"
	);

	// The seeded root still resolves, so nothing live was reclaimed with it.
	services
		.rooms
		.state
		.sweep_hamt_nodes(Duration::from_secs(0), false)
		.await
		.expect("second sweep");
	assert_eq!(root.structural_hash, live[0].structural_hash);
	let node = services
		.rooms
		.state_hamt
		.store
		.get_node(&root.structural_hash);
	assert!(node.is_ok(), "live root node must survive the sweep");
}
