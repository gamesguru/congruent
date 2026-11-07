#![cfg(test)]

//! The pending-event marks that keep a partial-state room from resolving until
//! every event accepted meanwhile has been re-authorised.

use std::{
	collections::{HashMap, HashSet},
	path::PathBuf,
	sync::Arc,
};

use conduwuit_core::{
	Server,
	config::Config,
	log::{Log, LogLevelReloadHandles, capture},
	matrix::{Event, PduEvent},
};
use futures::TryStreamExt;
use slipstream::{CanonicalJsonObject, CanonicalJsonValue, EventId, RoomId, event_id, room_id};

use crate::Services;

static TEST_DB_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

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
	let _ = rustls::crypto::ring::default_provider().install_default();
	let lock = crate::DB_TEST_MUTEX.lock().await;
	let count = TEST_DB_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	let db_path = std::env::temp_dir().join(format!("conduwuit_marks_test_db_{count}"));
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

fn pdu_at_depth(room_id: &RoomId, event_id: &EventId, depth: u64) -> PduEvent {
	let mut json = CanonicalJsonObject::new();
	let string = |s: &str| CanonicalJsonValue::String(s.to_owned());
	json.insert("room_id".into(), string(room_id.as_str()));
	json.insert("sender".into(), string("@alice:test.conduwuit.local"));
	json.insert("type".into(), string("m.room.message"));
	json.insert(
		"content".into(),
		CanonicalJsonValue::Object(std::collections::BTreeMap::default()),
	);
	json.insert("origin_server_ts".into(), CanonicalJsonValue::Number(1_u64.into()));
	json.insert("depth".into(), CanonicalJsonValue::Number(depth.into()));
	json.insert("prev_events".into(), CanonicalJsonValue::Array(Vec::new()));
	json.insert("auth_events".into(), CanonicalJsonValue::Array(Vec::new()));
	let mut hashes = CanonicalJsonObject::new();
	hashes.insert("sha256".into(), string("dummy"));
	json.insert("hashes".into(), CanonicalJsonValue::Object(hashes));
	PduEvent::from_id_val(event_id, json, Some(room_id)).expect("failed to create pdu")
}

/// Keys of `map` under the room's prefix.
async fn keys_under(map: &database::Map, room_id: &RoomId) -> Vec<Vec<u8>> {
	let mut prefix = room_id.as_bytes().to_vec();
	prefix.push(database::SEP);
	let mut keys: Vec<Vec<u8>> = map
		.raw_stream_prefix(&prefix)
		.map_ok(|(key, _): (&[u8], &[u8])| key.to_vec())
		.try_collect()
		.await
		.expect("scan failed");
	keys.sort();
	keys
}

fn mark_key(room_id: &RoomId, raw_suffix: &[u8]) -> Vec<u8> {
	let mut key = room_id.as_bytes().to_vec();
	key.push(database::SEP);
	key.extend_from_slice(raw_suffix);
	key
}

#[conduwuit_macros::async_test]
async fn marks_are_read_back_per_room() {
	let (_guard, _server, services) = setup_test_services().await;
	let partial = &services.rooms.state_partial;
	let room_a = &room_id!("!a:test.conduwuit.local");
	let room_b = &room_id!("!b:test.conduwuit.local");
	let e1 = &event_id!("$one:test.conduwuit.local");
	let e2 = &event_id!("$two:test.conduwuit.local");
	let e3 = &event_id!("$three:test.conduwuit.local");

	partial.mark_pending_event(room_a, e1);
	partial.mark_pending_event(room_a, e2);
	partial.mark_pending_event(room_b, e3);
	// Marking twice is the same mark.
	partial.mark_pending_event(room_a, e1);

	let in_a: HashSet<_> = partial
		.pending_event_marks(room_a)
		.await
		.unwrap()
		.into_iter()
		.map(|(_, id)| id)
		.collect();
	assert_eq!(in_a, HashSet::from([e1.to_owned(), e2.to_owned()]));

	let in_b = partial.pending_event_marks(room_b).await.unwrap();
	assert_eq!(in_b.len(), 1);
	assert_eq!(in_b[0].1, *e3);
}

#[conduwuit_macros::async_test]
async fn unreadable_mark_is_discarded_and_does_not_hide_valid_ones() {
	let (_guard, _server, services) = setup_test_services().await;
	let partial = &services.rooms.state_partial;
	let room = &room_id!("!a:test.conduwuit.local");
	let good = &event_id!("$good:test.conduwuit.local");

	partial.mark_pending_event(room, good);
	let bad_key = mark_key(room, b"not an event id");
	partial
		.db
		.state_partial_pending_events
		.insert(&bad_key, b"x");

	let marks = partial.pending_event_marks(room).await.unwrap();
	assert_eq!(marks.len(), 1, "only the readable mark is reported");
	assert_eq!(marks[0].1, *good);

	let left = keys_under(&partial.db.state_partial_pending_events, room).await;
	assert_eq!(left, vec![mark_key(room, good.as_bytes())], "the corrupt row is dropped");
}

#[conduwuit_macros::async_test]
async fn clearing_markers_drops_the_room_anchor_and_marks_for_that_room_only() {
	let (_guard, _server, services) = setup_test_services().await;
	let partial = &services.rooms.state_partial;
	let room_a = &room_id!("!a:test.conduwuit.local");
	let room_b = &room_id!("!b:test.conduwuit.local");
	let e1 = &event_id!("$one:test.conduwuit.local");
	let e2 = &event_id!("$two:test.conduwuit.local");

	let remote = slipstream::ServerName::parse("remote.example").unwrap();
	for room in [room_a, room_b] {
		partial.register_partial(room, &remote, Some(e1));
	}
	partial.mark_pending_event(room_a, e1);
	partial.mark_pending_event(room_a, e2);
	partial.mark_pending_event(room_b, e1);

	partial.clear_partial_markers(room_a).await.unwrap();

	assert!(
		partial
			.pending_event_marks(room_a)
			.await
			.unwrap()
			.is_empty()
	);
	let _ = partial
		.db
		.state_partial_rooms
		.qry(room_a)
		.await
		.unwrap_err();
	let _ = partial
		.db
		.state_partial_events
		.qry(room_a)
		.await
		.unwrap_err();

	assert_eq!(partial.pending_event_marks(room_b).await.unwrap().len(), 1);
	let _ = partial.db.state_partial_rooms.qry(room_b).await.unwrap();
	let _ = partial.db.state_partial_events.qry(room_b).await.unwrap();
}

#[conduwuit_macros::async_test]
async fn install_refuses_and_keeps_the_mark_for_an_event_outside_the_window() {
	let (_guard, _server, services) = setup_test_services().await;
	let partial = &services.rooms.state_partial;
	let room = &room_id!("!a:test.conduwuit.local");
	let late = &event_id!("$late:test.conduwuit.local");
	services
		.rooms
		.short
		.set_room_version(room, &slipstream::RoomVersionId::V11)
		.expect("room version must be persisted");

	// Accepted after the walk: deeper than the anchor and not in the window.
	let pdu = pdu_at_depth(room, late, 9);
	let value = pdu.to_canonical_object();
	services
		.rooms
		.timeline
		.force_insert_pdu(room, late, &pdu, &value, false)
		.await
		.unwrap();
	partial.mark_pending_event(room, late);
	let expected_extremities = partial.sorted_extremities(room).await;

	let installed = partial
		.install_current_state(
			room,
			HashMap::default(),
			Some(3),
			&HashSet::new(),
			&expected_extremities,
		)
		.await
		.unwrap();

	assert!(!installed, "an uncovered event must stop the install");
	let marks = partial.pending_event_marks(room).await.unwrap();
	assert_eq!(marks.len(), 1, "the mark must survive for the next pass");
	assert_eq!(marks[0].1, *late);
}

#[conduwuit_macros::async_test]
async fn install_refuses_when_the_extremities_moved_and_touches_no_marks() {
	let (_guard, _server, services) = setup_test_services().await;
	let partial = &services.rooms.state_partial;
	let room = &room_id!("!a:test.conduwuit.local");
	let event = &event_id!("$one:test.conduwuit.local");
	let stale = [event_id!("$stale:test.conduwuit.local")];

	partial.mark_pending_event(room, event);
	let installed = partial
		.install_current_state(
			room,
			HashMap::default(),
			None,
			&HashSet::from([event.to_owned()]),
			&stale,
		)
		.await
		.unwrap();

	assert!(!installed);
	assert_eq!(partial.pending_event_marks(room).await.unwrap().len(), 1);
}

#[conduwuit_macros::async_test]
async fn send_pending_reports_unreadable_rows_and_keeps_them() {
	let (_guard, _server, services) = setup_test_services().await;
	let partial = &services.rooms.state_partial;
	let room = &room_id!("!a:test.conduwuit.local");

	let sendable = mark_key(room, &[7_u8; 16]);
	let corrupt = mark_key(room, b"short");
	partial.db.state_partial_pending.insert(&sendable, b"");
	partial.db.state_partial_pending.insert(&corrupt, b"");

	// The room has no remote servers, so the readable row has nothing left to send.
	let unreadable = partial.send_pending(room).await.unwrap();

	assert_eq!(unreadable, 1);
	let left = keys_under(&partial.db.state_partial_pending, room).await;
	assert_eq!(left, vec![corrupt], "only the unreadable row stays");
}
