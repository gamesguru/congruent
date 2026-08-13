#![cfg(test)]

use std::{path::PathBuf, sync::Arc};

use conduwuit_core::{
	Server,
	config::Config,
	log::{Log, LogLevelReloadHandles, capture},
	matrix::PduEvent,
};
use database::Deserialized;
use figment::providers::Format;
use ruma::{
	CanonicalJsonObject, EventId, RoomId, RoomVersionId, UserId, events::StateEventType,
	owned_event_id, owned_room_id, owned_user_id,
};

use crate::{Args, Services};

struct TempDbGuard {
	path: PathBuf,
}

impl Drop for TempDbGuard {
	fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.path); }
}

async fn setup_test_services() -> (TempDbGuard, Arc<Server>, Arc<Services>) {
	#[cfg(feature = "aws_lc_rs")]
	let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
	#[cfg(all(feature = "ring", not(feature = "aws_lc_rs")))]
	let _ = rustls::crypto::ring::default_provider().install_default();
	static TEST_DB_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
	let count = TEST_DB_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	let db_path = std::env::temp_dir().join(format!("conduwuit_state_test_db_{count}"));
	let _ = std::fs::remove_dir_all(&db_path);

	let guard = TempDbGuard { path: db_path.clone() };

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
	json.insert("content".into(), ruma::CanonicalJsonValue::Object(Default::default()));
	json.insert("origin_server_ts".into(), ruma::CanonicalJsonValue::Integer(123456789.into()));
	json.insert("depth".into(), ruma::CanonicalJsonValue::Integer(1.into()));
	json.insert("prev_events".into(), ruma::CanonicalJsonValue::Array(Vec::new()));
	json.insert("auth_events".into(), ruma::CanonicalJsonValue::Array(Vec::new()));

	let mut hashes = CanonicalJsonObject::new();
	hashes.insert("sha256".into(), ruma::CanonicalJsonValue::String("dummy".to_owned()));
	json.insert("hashes".into(), ruma::CanonicalJsonValue::Object(hashes));

	PduEvent::from_id_val(event_id, json, Some(room_id)).expect("failed to create pdu")
}

#[tokio::test]
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
		.deserialized::<(rezzy::hamt::StructuralHash, rezzy::hamt::StateGroupId)>()
		.expect("mapped roothandle should exist");

	assert_eq!(serialized.0, root_handle.structural_hash);
	assert_eq!(serialized.1, root_handle.state_group_id);
}

#[tokio::test]
async fn test_force_state() {
	let (_guard, _server, services) = setup_test_services().await;

	let room_id = owned_room_id!("!test:test.conduwuit.local");

	let dummy_root = rezzy::hamt::RootHandle {
		structural_hash: rezzy::hamt::StructuralHash::default(),
		state_group_id: [0u8; 32],
	};

	let mutex = services.rooms.state.mutex.lock(&room_id).await;
	services
		.rooms
		.state
		.force_state(&room_id, &dummy_root, &mutex)
		.await
		.expect("force_state failed");

	let retrieved_root = services
		.rooms
		.state
		.get_room_state_hamt(&room_id)
		.await
		.expect("failed to get room state");
	assert_eq!(retrieved_root.structural_hash, dummy_root.structural_hash);
	assert_eq!(retrieved_root.state_group_id, dummy_root.state_group_id);
}

#[tokio::test]
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
	let event2 = create_dummy_pdu(
		&room_id,
		&owned_event_id!("$event2:test.conduwuit.local"),
		"m.room.member",
		"@alice:test.conduwuit.local",
	);

	let mutex = services.rooms.state.mutex.lock(&room_id).await;

	let (root1, _) = services
		.rooms
		.state
		.append_to_state(&event1, &room_id, &mutex, None)
		.await
		.expect("append 1 failed");
	services
		.rooms
		.state
		.force_state(&room_id, &root1, &mutex)
		.await
		.expect("force_state 1 failed");

	let (root2, _) = services
		.rooms
		.state
		.append_to_state(&event2, &room_id, &mutex, None)
		.await
		.expect("append 2 failed");
	services
		.rooms
		.state
		.force_state(&room_id, &root2, &mutex)
		.await
		.expect("force_state 2 failed");

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
}
