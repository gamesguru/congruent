#![cfg(test)]

use std::{path::PathBuf, sync::Arc};

use conduwuit_core::{
	Server,
	config::Config,
	log::{Log, LogLevelReloadHandles, capture},
	matrix::{Event, PduEvent},
};
use figment::providers::Format;
use ruma::{
	CanonicalJsonObject, EventId, RoomId, events::StateEventType, owned_event_id, owned_room_id,
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
	json.insert("room_id".into(), ruma::CanonicalJsonValue::String(room_id.as_str().to_owned()));
	json.insert(
		"sender".into(),
		ruma::CanonicalJsonValue::String("@alice:test.conduwuit.local".to_owned()),
	);
	json.insert("type".into(), ruma::CanonicalJsonValue::String(event_type.to_owned()));
	json.insert("state_key".into(), ruma::CanonicalJsonValue::String(state_key.to_owned()));
	json.insert(
		"content".into(),
		ruma::CanonicalJsonValue::Object(std::collections::BTreeMap::default()),
	);
	json.insert("origin_server_ts".into(), ruma::CanonicalJsonValue::Integer(123_456_789.into()));
	json.insert("depth".into(), ruma::CanonicalJsonValue::Integer(1.into()));
	json.insert("prev_events".into(), ruma::CanonicalJsonValue::Array(Vec::new()));
	json.insert("auth_events".into(), ruma::CanonicalJsonValue::Array(Vec::new()));

	let mut hashes = CanonicalJsonObject::new();
	hashes.insert("sha256".into(), ruma::CanonicalJsonValue::String("dummy".to_owned()));
	json.insert("hashes".into(), ruma::CanonicalJsonValue::Object(hashes));

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

	let room_id = owned_room_id!("!test:test.conduwuit.local");
	let event_id = owned_event_id!("$event1:test.conduwuit.local");
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

#[tokio::test(flavor = "multi_thread")]
async fn test_force_state() {
	let (_guard, _server, services) = setup_test_services().await;

	let room_id = owned_room_id!("!test:test.conduwuit.local");
	let event = create_dummy_pdu(
		&room_id,
		&owned_event_id!("$force-state:test.conduwuit.local"),
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
	let room_id = owned_room_id!("!test:test.conduwuit.local");

	// Create multiple events to build the state
	let event1 = create_dummy_pdu(
		&room_id,
		&owned_event_id!("$event1:test.conduwuit.local"),
		"m.room.create",
		"",
	);
	// Deliberately empty content: a redacted member event looks like this and
	// must be treated as "leave" rather than failing to deserialize.
	let event2 = create_dummy_pdu(
		&room_id,
		&owned_event_id!("$event2:test.conduwuit.local"),
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
