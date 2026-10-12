#![cfg(test)]

use std::{path::PathBuf, sync::Arc};

use conduwuit_core::{
	Server,
	config::Config,
	log::{Log, LogLevelReloadHandles, capture},
	matrix::{Event, PduEvent},
};
use slipstream::{
	CanonicalJsonObject, EventId, RoomId, event_id,
	events::{AnyStrippedStateEvent, StateEventType},
	room_id,
	sswire::Raw,
	user_id,
};

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

	let config_raw = conduwuit::config::RawConfig::from_toml(&format!(
		r#"
        server_name = "test.conduwuit.local"
        database_path = "{}"
        "#,
		db_path.to_string_lossy().replace('\\', "/")
	))
	.expect("failed to parse test config");

	let config = Config::new(&config_raw).expect("failed to parse config");
	let runtime_handle = ();
	let server = Arc::new(Server::new(config, Some(&runtime_handle), Log {
		reload: LogLevelReloadHandles,
		capture: Arc::new(capture::State),
	}));

	let services = Services::build(server.clone()).expect("failed to build services");
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

/// Build a synthetic `m.room.member` PDU with an explicit membership and
/// timestamp, for exercising membership-cache transitions directly.
fn create_member_pdu(
	room_id: &RoomId,
	event_id: &EventId,
	sender: &str,
	state_key: &str,
	membership: &str,
	origin_server_ts: u64,
) -> PduEvent {
	let mut content = CanonicalJsonObject::new();
	content.insert(
		"membership".into(),
		slipstream::CanonicalJsonValue::String(membership.to_owned()),
	);

	let mut json = CanonicalJsonObject::new();
	json.insert(
		"room_id".into(),
		slipstream::CanonicalJsonValue::String(room_id.as_str().to_owned()),
	);
	json.insert("sender".into(), slipstream::CanonicalJsonValue::String(sender.to_owned()));
	json.insert(
		"type".into(),
		slipstream::CanonicalJsonValue::String("m.room.member".to_owned()),
	);
	json.insert("state_key".into(), slipstream::CanonicalJsonValue::String(state_key.to_owned()));
	json.insert("content".into(), slipstream::CanonicalJsonValue::Object(content));
	json.insert(
		"origin_server_ts".into(),
		slipstream::CanonicalJsonValue::Number(origin_server_ts.into()),
	);
	json.insert("depth".into(), slipstream::CanonicalJsonValue::Number(1_u64.into()));
	json.insert("prev_events".into(), slipstream::CanonicalJsonValue::Array(Vec::new()));
	json.insert("auth_events".into(), slipstream::CanonicalJsonValue::Array(Vec::new()));

	let mut hashes = CanonicalJsonObject::new();
	hashes.insert("sha256".into(), slipstream::CanonicalJsonValue::String("dummy".to_owned()));
	json.insert("hashes".into(), slipstream::CanonicalJsonValue::Object(hashes));

	PduEvent::from_id_val(event_id, json, Some(room_id)).expect("failed to create member pdu")
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

#[conduwuit_macros::async_test]
async fn test_state_round_trip() {
	let (_guard, _server, services) = setup_test_services().await;

	let room_id = slipstream::OwnedRoomId::parse("!test:test.conduwuit.local").unwrap();
	let event_id = slipstream::OwnedEventId::parse("$event1:test.conduwuit.local").unwrap();
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
	let handle = match super::root_handle_from_bytes(&[0; 32]) {
		| Ok(_) => panic!("truncated RootHandle should be rejected"),
		| Err(error) => error,
	};
	assert!(
		handle.to_string().contains("invalid length"),
		"expected a length error, got {handle}"
	);
}

#[conduwuit_macros::async_test]
async fn test_force_state() {
	let (_guard, _server, services) = setup_test_services().await;

	let room_id = slipstream::OwnedRoomId::parse("!test:test.conduwuit.local").unwrap();
	let event = create_dummy_pdu(
		&room_id,
		&slipstream::OwnedEventId::parse("$force-state:test.conduwuit.local").unwrap(),
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

#[conduwuit_macros::async_test]
async fn test_state_equivalence() {
	let (_guard, _server, services) = setup_test_services().await;
	let room_id = slipstream::OwnedRoomId::parse("!test:test.conduwuit.local").unwrap();

	// Create multiple events to build the state
	let event1 = create_dummy_pdu(
		&room_id,
		&slipstream::OwnedEventId::parse("$event1:test.conduwuit.local").unwrap(),
		"m.room.create",
		"",
	);
	// Deliberately empty content: a redacted member event looks like this and
	// must be treated as "leave" rather than failing to deserialize.
	let event2 = create_dummy_pdu(
		&room_id,
		&slipstream::OwnedEventId::parse("$event2:test.conduwuit.local").unwrap(),
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
		&slipstream::OwnedEventId::parse("$seed-create:test.conduwuit.local").unwrap(),
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
#[conduwuit_macros::async_test]
async fn test_bulk_state_update_writes_only_changed_spines() {
	let (_guard, _server, services) = setup_test_services().await;
	let room_id = slipstream::OwnedRoomId::parse("!test:test.conduwuit.local").unwrap();
	let mutex = services.rooms.state.mutex.lock(&room_id).await;

	let root = seed_membership_state(&services, &room_id, &mutex, 200).await;

	// Replace one membership entry: one changed leaf, so one changed spine.
	let updated = create_dummy_pdu(
		&room_id,
		&slipstream::OwnedEventId::parse("$update-7:test.conduwuit.local").unwrap(),
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
		StateEventType::RoomMember.as_ref(),
		"@user7:test.conduwuit.local",
		"$seed-member-7:test.conduwuit.local",
		updated.event_id.as_str(),
	);

	let structural_key =
		crate::rooms::state_hamt::room_structural_key(&services.globals.server_secret, &room_id);

	let (written_before, elided_before) =
		services.rooms.state_hamt.store.write_stats().snapshot();
	let (new_root, _deferred_nodes) = services
		.rooms
		.state
		.persist_state_hamt_mutations(
			&room_id,
			&structural_key,
			&root,
			vec![(target_key, Some(target_value))],
			&lattice,
		)
		.expect("bulk mutation failed");
	let (written_after, elided_after) = services.rooms.state_hamt.store.write_stats().snapshot();

	let written = written_after
		.checked_sub(written_before)
		.expect("write counter moved backwards");
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
		.expect("failed to load HAMT state");
	assert_eq!(actual.len(), 201, "state size must be preserved by the update");
	assert_eq!(actual.get(&target_key), Some(&target_value));

	// Re-applying the identical mutation produces an identical root and must not
	// write anything: the content-addressed elision has nothing left to do.
	let (rewritten_before, _) = services.rooms.state_hamt.store.write_stats().snapshot();
	let (repeat_root, _deferred_nodes) = services
		.rooms
		.state
		.persist_state_hamt_mutations(
			&room_id,
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
		rewritten_after
			.checked_sub(rewritten_before)
			.expect("rewrite counter moved backwards"),
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
		elided_after
			.checked_sub(elided_before)
			.expect("elision counter moved backwards"),
		0,
		"path-copy update must not re-emit unchanged nodes"
	);
}

/// Shadow writes preserve the exact RocksDB encoding, and mtxdb reads reject
/// bytes selected through a colliding 16-byte node ID when the full hash does
/// not match.
#[conduwuit_macros::async_test]
async fn test_mtxdb_shadow_node_bytes_and_full_hash_verification() {
	use rezzy::{hamt::PersistedInternalNode, incremental::LtHash};

	let (_guard, _server, services) = setup_test_services().await;
	let room_id = slipstream::OwnedRoomId::parse("!mtxdb-hamt:test.conduwuit.local").unwrap();
	let structural_key =
		crate::rooms::state_hamt::room_structural_key(&services.globals.server_secret, &room_id);
	let entries = (0_u64..128).map(|key| {
		(
			key,
			key.checked_add(1000)
				.expect("fixture value must fit in u64"),
		)
	});
	let (_, node) =
		rezzy::hamt::build_hamt_root_handle(&structural_key, &LtHash::default(), entries)
			.expect("build HAMT node");
	let hash = node.structural_hash;
	let bytes = PersistedInternalNode::from(node.as_ref()).encode_v1();
	let mut nodes = Vec::new();
	let mut pending = vec![node.clone()];
	while let Some(current) = pending.pop() {
		let children: Vec<_> = current
			.children
			.iter()
			.filter_map(|child| match child {
				| rezzy::hamt::NodeRef::Resolved(child_node) => Some(child_node.clone()),
				| rezzy::hamt::NodeRef::Lazy(_) => None,
			})
			.collect();
		pending.extend(children);
		nodes.push(current);
	}
	assert!(nodes.len() > 1, "fixture must contain a multi-level HAMT");

	services
		.rooms
		.state_hamt
		.store
		.put_node(room_id.as_bytes(), node.clone());
	services
		.rooms
		.state_hamt
		.store
		.put_encoded_node(room_id.as_bytes(), hash, &bytes)
		.expect("write encoded node");
	services
		.rooms
		.state_hamt
		.store
		.persist_node_recursive(room_id.as_bytes(), node.clone());
	let shadow = conduwuit_mtxdb::state::StateAdapter::new(services.mtxdb.clone());
	for node in &nodes {
		let node_hash = node.structural_hash;
		let rocksdb_node = services.db["state_hamt_nodes"]
			.get_blocking(&node_hash)
			.expect("read RocksDB HAMT node");
		let shadow_node = shadow
			.get_nodes(room_id.as_bytes(), &[node_hash])
			.expect("read mtxdb HAMT node");
		assert_eq!(shadow_node[0].as_deref(), Some(&*rocksdb_node));
		services
			.rooms
			.state_hamt
			.store
			.get_shadow_node_verified(room_id.as_bytes(), &structural_key, &node_hash)
			.expect("verify mtxdb HAMT node")
			.expect("recursive shadow write must be present");
	}

	// Re-emitting an already durable multi-level tree must not write nodes to
	// mtxdb again, through either recursive or batched persistence.
	services
		.rooms
		.state_hamt
		.store
		.persist_node_recursive(room_id.as_bytes(), node.clone());
	let shadow_writes_after_first = services
		.rooms
		.state_hamt
		.store
		.write_stats()
		.shadow_writes();
	let mut batch = database::Batch::new();
	services
		.rooms
		.state_hamt
		.store
		.put_node_batch(room_id.as_bytes(), node, &mut batch);
	assert_eq!(
		services
			.rooms
			.state_hamt
			.store
			.write_stats()
			.shadow_writes(),
		shadow_writes_after_first,
		"encoded, batched, and recursive writes of a durable node must not rewrite mtxdb"
	);

	let rocksdb_bytes = services.db["state_hamt_nodes"]
		.get_blocking(&hash)
		.expect("read RocksDB node");
	let mtxdb_bytes = shadow
		.get_nodes(room_id.as_bytes(), &[hash])
		.expect("read mtxdb node");
	assert_eq!(mtxdb_bytes[0].as_deref(), Some(&*rocksdb_bytes));

	let verified = services
		.rooms
		.state_hamt
		.store
		.get_shadow_node_verified(room_id.as_bytes(), &structural_key, &hash)
		.expect("verify shadow node")
		.expect("shadow node is present");
	assert_eq!(verified.structural_hash, hash);

	let mut collision = hash;
	collision[31] ^= 1;
	assert_eq!(&collision[..16], &hash[..16]);
	let collision_error = services
		.rooms
		.state_hamt
		.store
		.get_shadow_node_verified(room_id.as_bytes(), &structural_key, &collision)
		.expect_err("a colliding truncated node ID must fail full-hash verification");
	assert!(collision_error.to_string().contains("structural hash"));

	let (_, elided_before) = services.rooms.state_hamt.store.write_stats().snapshot();
	services
		.rooms
		.state_hamt
		.store
		.put_node(room_id.as_bytes(), verified);
	services.rooms.state_hamt.store.put_node(
		room_id.as_bytes(),
		services
			.rooms
			.state_hamt
			.store
			.get_node_blocking(&hash)
			.expect("read durable RocksDB node"),
	);
	let (_, elided_after) = services.rooms.state_hamt.store.write_stats().snapshot();
	assert_eq!(
		elided_after
			.checked_sub(elided_before)
			.expect("elision counter moved backwards"),
		2
	);
	assert_eq!(
		services
			.rooms
			.state_hamt
			.store
			.write_stats()
			.shadow_writes(),
		shadow_writes_after_first,
		"durable-node elision must also skip mtxdb writes"
	);
}

/// Node reclamation must remove exactly the unreachable nodes.
///
/// The live-root set is derived from the recorded root handles, so a test that
/// only exercised the store's own bookkeeping would not catch a regression in
/// `live_root_handles` — which is where a partial root set would silently turn
/// into live-state deletion.
#[conduwuit_macros::async_test]
async fn test_sweep_reclaims_only_unreachable_nodes() {
	use std::time::Duration;

	let (_guard, _server, services) = setup_test_services().await;
	let room_id = slipstream::OwnedRoomId::parse("!sweep:test.conduwuit.local").unwrap();
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
	let mut orphan_lattice = rezzy::incremental::LtHash::default();
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
	services
		.rooms
		.state_hamt
		.store
		.put_node(room_id.as_bytes(), orphan);

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
	let state = services
		.rooms
		.state_accessor
		.load_full_state_hamt(&root)
		.expect("live root state must survive the sweep");
	assert_eq!(state.len(), 51, "live root state must remain readable");
}

/// A leave racing with a newer invite must not delete that invite.
///
/// This is the cache-level regression for `TestUnbanViaInvite`: an unban
/// `m.room.member`(leave) is applied during an outlier upgrade and then appended
/// again, while an invite for the same user arrives in between. By the time the
/// second leave application runs, `mark_as_invited` has already cleared the
/// `leftstate` marker, so preservation cannot use the `existing_leave ==
/// leave_pdu` shortcut and must fall back to the invite state's
/// `origin_server_ts`. The stripped invite event must carry that timestamp, or
/// the invite is silently deleted; `mark_as_left`/`mark_as_invited` also share
/// `membership_mutex` so a stale leave decision cannot land its batch after the
/// invite. The operations below are started together to exercise the actual
/// concurrent read/decide/write path rather than merely testing serialized
/// calls.
#[conduwuit_macros::async_test]
async fn test_stale_leave_does_not_delete_newer_invite() {
	let (_guard, _server, services) = setup_test_services().await;
	let room_id = room_id!("!invite-race:test.conduwuit.local").to_owned();
	let alice = user_id!("@alice:test.conduwuit.local").to_owned();
	let inviter = user_id!("@bob:test.conduwuit.local").to_owned();

	let leave = create_member_pdu(
		&room_id,
		&event_id!("$leave:test.conduwuit.local"),
		inviter.as_str(),
		alice.as_str(),
		"leave",
		1_000,
	);
	let invite = create_member_pdu(
		&room_id,
		&event_id!("$invite:test.conduwuit.local"),
		inviter.as_str(),
		alice.as_str(),
		"invite",
		2_000,
	);
	let invite_state = vec![invite.to_format::<Raw<AnyStrippedStateEvent>>()];

	// A leave marks alice as left.
	services
		.rooms
		.state_cache
		.mark_as_left(&alice, &room_id, Some(leave.clone()))
		.await;

	// Race the causally newer invite against the same, causally older leave as
	// the timeline append path can after an outlier upgrade. The invite clears
	// the left marker, while the stale leave must preserve the invite whether it
	// acquires the room lock before or after the invite.
	let (invite_result, ()) = futures::join!(
		services.rooms.state_cache.mark_as_invited(
			&alice,
			&room_id,
			&inviter,
			Some(invite_state),
			None,
		),
		services
			.rooms
			.state_cache
			.mark_as_left(&alice, &room_id, Some(leave)),
	);
	invite_result.expect("mark_as_invited should succeed");

	let pending = services
		.rooms
		.state_cache
		.invite_state(&alice, &room_id)
		.await
		.expect("invite state should survive a stale re-applied leave");
	assert!(!pending.is_empty(), "the newer invite must not be deleted by a stale leave");
	assert!(
		services
			.rooms
			.state_cache
			.get_invite_count(&room_id, &alice)
			.await
			.is_ok(),
		"the invite count must survive alongside the invite"
	);
}

/// Reads that miss the cache are compared against their mtxdb shadow without
/// ever being served from it: a missing shadow, a verified one, and one that
/// fails full-hash verification are each counted, and the caller always gets
/// the RocksDB node.
#[conduwuit_macros::async_test]
async fn test_verifying_reads_count_shadow_outcomes_and_serve_rocksdb() {
	use rezzy::{hamt::PersistedInternalNode, incremental::LtHash};

	let (_guard, _server, services) = setup_test_services().await;
	let room_id = slipstream::OwnedRoomId::parse("!mtxdb-verify:test.conduwuit.local").unwrap();
	let structural_key =
		crate::rooms::state_hamt::room_structural_key(&services.globals.server_secret, &room_id);
	let store = &services.rooms.state_hamt.store;
	let shadow = conduwuit_mtxdb::state::StateAdapter::new(services.mtxdb.clone());

	let build = |seed: u64| {
		let (_, node) = rezzy::hamt::build_hamt_root_handle(
			&structural_key,
			&LtHash::default(),
			(0_u64..4).map(move |key| (key, key.checked_add(seed).expect("fixture fits u64"))),
		)
		.expect("build HAMT node");
		let bytes = PersistedInternalNode::from(node.as_ref()).encode_v1();
		(node.structural_hash, bytes)
	};
	// Written straight to RocksDB so the node cache never holds them.
	let (absent, absent_bytes) = build(1000);
	let (present, present_bytes) = build(2000);
	let (corrupt, corrupt_bytes) = build(3000);
	for (hash, bytes) in
		[(absent, &absent_bytes), (present, &present_bytes), (corrupt, &corrupt_bytes)]
	{
		services.db["state_hamt_nodes"].insert(&hash, bytes);
	}
	shadow
		.put_node(room_id.as_bytes(), &present, &present_bytes)
		.expect("shadow-write verified node");
	// Another node's bytes under `corrupt`'s id: right key, wrong content.
	shadow
		.put_node(room_id.as_bytes(), &corrupt, &present_bytes)
		.expect("shadow-write mismatched node");

	let before = store.shadow_read_stats().snapshot();
	for hash in [absent, present, corrupt] {
		let node = store
			.get_node_verifying(room_id.as_bytes(), &structural_key, &hash)
			.expect("RocksDB serves the read");
		assert_eq!(node.structural_hash, hash, "the RocksDB node is what is returned");
	}
	let after = store.shadow_read_stats().snapshot();
	let delta =
		|after: u64, before: u64| after.checked_sub(before).expect("counter moved backwards");
	assert_eq!(delta(after.0, before.0), 1, "one verified shadow");
	assert_eq!(delta(after.1, before.1), 1, "one missing shadow");
	assert_eq!(delta(after.2, before.2), 0, "content-addressed nodes cannot verify yet differ");
	assert_eq!(delta(after.3, before.3), 1, "one shadow failing full-hash verification");

	// A cached node is not re-verified.
	store
		.get_node_verifying(room_id.as_bytes(), &structural_key, &present)
		.expect("cached read");
	assert_eq!(store.shadow_read_stats().snapshot(), after, "cache hits skip verification");
}

/// Every structural hash in a resolved tree.
fn tree_hashes(root: &Arc<rezzy::hamt::HamtNode<u64, u64>>) -> Vec<[u8; 32]> {
	let mut hashes = Vec::new();
	let mut pending = vec![root.clone()];
	while let Some(current) = pending.pop() {
		hashes.push(current.structural_hash);
		for child in &current.children {
			if let rezzy::hamt::NodeRef::Resolved(child_node) = child {
				pending.push(child_node.clone());
			}
		}
	}
	hashes
}

fn shadow_prefix(hash: &[u8; 32]) -> [u8; 16] {
	let mut prefix = [0_u8; 16];
	prefix.copy_from_slice(&hash[..16]);
	prefix
}

/// The shadow dry run reports the nodes no root of a room reaches, stops
/// calling a node an orphan once a root reaches it, and skips a room it cannot
/// judge rather than reporting its nodes as garbage.
#[conduwuit_macros::async_test]
async fn test_shadow_sweep_dry_run_reports_unrooted_nodes_and_skips_rooms_it_cannot_judge() {
	use rezzy::incremental::LtHash;

	use crate::rooms::state_hamt::shadow_sweep::ShadowRoomInput;

	let (_guard, _server, services) = setup_test_services().await;
	let room_id = slipstream::OwnedRoomId::parse("!sweep:test.conduwuit.local").unwrap();
	let structural_key =
		crate::rooms::state_hamt::room_structural_key(&services.globals.server_secret, &room_id);
	let shadow = conduwuit_mtxdb::state::StateAdapter::new(services.mtxdb.clone());
	let store = &services.rooms.state_hamt.store;
	let input = ShadowRoomInput {
		room_id: room_id.as_bytes().to_vec(),
		structural_key,
	};

	let build = |base: u64| {
		let entries = (0_u64..128).map(move |key| (key, key.checked_add(base).unwrap()));
		rezzy::hamt::build_hamt_root_handle(&structural_key, &LtHash::default(), entries)
			.expect("build HAMT")
	};

	// Tree A is rooted: its nodes are stored and the room's roots name it.
	let (handle_a, node_a) = build(1000);
	let hashes_a = tree_hashes(&node_a);
	assert!(hashes_a.len() > 1, "fixture must be a multi-level HAMT");
	store.persist_node_recursive(room_id.as_bytes(), node_a);
	let root_a = super::root_handle_to_bytes(&handle_a);
	shadow.put_room_root(room_id.as_bytes(), &root_a).unwrap();
	shadow
		.put_event_root(room_id.as_bytes(), b"$a:test.conduwuit.local", &root_a)
		.unwrap();

	let report = store.shadow_sweep_dry_run(std::slice::from_ref(&input), 0);
	assert_eq!(report.skipped.total(), 0, "{:?}", report.skipped);
	assert_eq!(report.rooms.len(), 1);
	assert_eq!(report.rooms[0].nodes, hashes_a.len());
	assert!(report.rooms[0].orphans.is_empty(), "everything is reachable from A");

	// Tree B is stored but no root names it: exactly its own nodes are orphans.
	let (handle_b, node_b) = build(5000);
	let hashes_b = tree_hashes(&node_b);
	store.persist_node_recursive(room_id.as_bytes(), node_b);
	let mut expected: Vec<[u8; 16]> = hashes_b
		.iter()
		.filter(|hash| !hashes_a.contains(hash))
		.map(shadow_prefix)
		.collect();
	expected.sort_unstable();
	assert!(!expected.is_empty());

	let report = store.shadow_sweep_dry_run(std::slice::from_ref(&input), 0);
	assert_eq!(report.rooms.len(), 1);
	assert_eq!(report.rooms[0].orphans, expected);
	assert_eq!(report.shadow_orphans(), expected.len());

	// A root now names B: nothing is an orphan any more.
	shadow
		.put_event_root(
			room_id.as_bytes(),
			b"$b:test.conduwuit.local",
			&super::root_handle_to_bytes(&handle_b),
		)
		.unwrap();
	let report = store.shadow_sweep_dry_run(std::slice::from_ref(&input), 0);
	assert!(report.rooms[0].orphans.is_empty());

	// A reachable node the shadow store lacks makes the walk incomplete. The
	// room is skipped, not reported as having orphans.
	let missing = *hashes_a.last().expect("fixture has hashes");
	let tx = services.mtxdb.begin_transaction();
	shadow
		.remove_nodes(&tx, room_id.as_bytes(), &[conduwuit_mtxdb::keys::hamt_node_id(&missing)])
		.unwrap();
	tx.commit().unwrap();
	let report = store.shadow_sweep_dry_run(std::slice::from_ref(&input), 0);
	assert!(report.rooms.is_empty());
	assert_eq!(report.skipped.incomplete_walk, 1, "{:?}", report.skipped);
}

#[conduwuit_macros::async_test]
async fn test_shadow_sweep_dry_run_skips_rooms_without_usable_roots() {
	use crate::rooms::state_hamt::shadow_sweep::ShadowRoomInput;

	let (_guard, _server, services) = setup_test_services().await;
	let shadow = conduwuit_mtxdb::state::StateAdapter::new(services.mtxdb.clone());
	let store = &services.rooms.state_hamt.store;

	let empty = slipstream::OwnedRoomId::parse("!empty:test.conduwuit.local").unwrap();
	let garbage = slipstream::OwnedRoomId::parse("!garbage:test.conduwuit.local").unwrap();
	// A node stored with no roots at all must not read as "everything is garbage".
	shadow
		.put_node(empty.as_bytes(), &[1; 32], b"node")
		.unwrap();
	shadow
		.put_event_root(garbage.as_bytes(), b"$e:test.conduwuit.local", b"not a root handle")
		.unwrap();

	let rooms = [&empty, &garbage].map(|room| ShadowRoomInput {
		room_id: room.as_bytes().to_vec(),
		structural_key: crate::rooms::state_hamt::room_structural_key(
			&services.globals.server_secret,
			room,
		),
	});
	let report = store.shadow_sweep_dry_run(&rooms, 2);

	assert!(report.rooms.is_empty());
	assert_eq!(report.skipped.no_shadow_roots, 1);
	assert_eq!(report.skipped.undecodable_root, 1);
	assert_eq!(report.skipped.bad_room_id, 2);
	assert_eq!(report.shadow_orphans(), 0);
}
