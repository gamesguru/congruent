#![cfg(test)]

#[test]
fn get_help_short() { get_help_inner("-h"); }

#[test]
fn get_help_long() { get_help_inner("--help"); }

#[test]
fn get_help_subcommand() { get_help_inner("help"); }

fn get_help_inner(input: &str) {
	use clap::Parser;

	use crate::admin::AdminCommand;

	let Err(error) = AdminCommand::try_parse_from(["argv[0] doesn't matter", input]) else {
		panic!("no error!");
	};

	let error = error.to_string();
	// Search for a handful of keywords that suggest the help printed properly
	assert!(error.contains("Usage:"));
	assert!(error.contains("Commands:"));
	assert!(error.contains("Options:"));
}

// -- Yolo subcommand parsing tests --

fn parse_yolo(args: &[&str]) -> Result<crate::admin::AdminCommand, clap::Error> {
	use clap::Parser;

	let mut full_args = vec!["admin"];
	full_args.extend_from_slice(args);
	crate::admin::AdminCommand::try_parse_from(full_args)
}

#[test]
fn yolo_list_outliers_basic() { parse_yolo(&["yolo", "list-outliers"]).unwrap(); }

#[test]
fn yolo_list_outliers_with_room() {
	parse_yolo(&["yolo", "list-outliers", "!foo:example.org"]).unwrap();
}

#[test]
fn yolo_list_outliers_rejected_flag() {
	parse_yolo(&["yolo", "list-outliers", "!foo:example.org", "--rejected"]).unwrap();
}

#[test]
fn yolo_list_outliers_clear_requires_rejected() {
	// --clear without --rejected should fail
	parse_yolo(&["yolo", "list-outliers", "!foo:example.org", "--clear"]).unwrap_err();
}

#[test]
fn yolo_list_outliers_rejected_and_clear() {
	parse_yolo(&["yolo", "list-outliers", "!foo:example.org", "--rejected", "--clear"]).unwrap();
}

#[test]
fn yolo_list_outliers_with_limit() {
	parse_yolo(&["yolo", "list-outliers", "--limit", "50"]).unwrap();
}

#[test]
fn yolo_list_outliers_with_sender() {
	parse_yolo(&["yolo", "list-outliers", "--sender", "@user:example.org"]).unwrap();
}

#[test]
fn yolo_get_room_dag_negative_end() {
	// Tests allow_hyphen_values = true
	parse_yolo(&["yolo", "get-room-dag", "!foo:example.org", "0", "-1"]).unwrap();
}

#[test]
fn yolo_view_extremities_requires_room_or_all() {
	// Neither room nor --all should fail
	parse_yolo(&["yolo", "view-extremities"]).unwrap_err();
}

#[test]
fn yolo_view_extremities_all() { parse_yolo(&["yolo", "view-extremities", "--all"]).unwrap(); }

#[test]
fn yolo_view_extremities_with_room() {
	parse_yolo(&["yolo", "view-extremities", "!foo:example.org"]).unwrap();
}

// -- V11+/V12+ room_id stripping tests --

/// Helper: simulates the V11+/V12+ room_id stripping logic used in
/// import/export. V11: strips room_id from all non-create events (MSC3820)
/// V12+: strips room_id from ALL events including create (MSC4291)
fn strip_room_id_if_needed(
	obj: &mut serde_json::Map<String, serde_json::Value>,
	room_version: &str,
) -> bool {
	let is_create = obj.get("type").and_then(|v| v.as_str()) == Some("m.room.create");

	let room_version_id =
		ruma::RoomVersionId::try_from(room_version).unwrap_or(ruma::RoomVersionId::V1);
	let room_features = conduwuit_core::RoomVersion::new(&room_version_id)
		.unwrap_or(conduwuit_core::RoomVersion::V1);

	if room_features.strips_room_id(is_create) {
		obj.remove("room_id").is_some()
	} else {
		false
	}
}

#[test]
fn v12_create_event_strips_room_id() {
	let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.create","room_id":"!abc:example.org","content":{"creator":"@alice:example.org"}}"#,
	)
	.unwrap();
	assert!(strip_room_id_if_needed(&mut obj, "12"));
	assert!(!obj.contains_key("room_id"));
}

#[test]
fn v12_non_create_event_keeps_room_id() {
	let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.member","room_id":"!abc:example.org","content":{}}"#,
	)
	.unwrap();
	assert!(!strip_room_id_if_needed(&mut obj, "12"));
	assert!(obj.contains_key("room_id"));
}

#[test]
fn v11_non_create_event_keeps_room_id() {
	let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.member","room_id":"!abc:example.org","content":{}}"#,
	)
	.unwrap();
	assert!(!strip_room_id_if_needed(&mut obj, "11"));
	assert!(obj.contains_key("room_id"));
}

#[test]
fn v11_create_event_keeps_room_id() {
	let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.create","room_id":"!abc:example.org","content":{"creator":"@alice:example.org"}}"#,
	)
	.unwrap();
	assert!(!strip_room_id_if_needed(&mut obj, "11"));
	assert!(obj.contains_key("room_id"));
}

#[test]
fn v10_create_event_keeps_room_id() {
	let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.create","room_id":"!abc:example.org","content":{"creator":"@alice:example.org"}}"#,
	)
	.unwrap();
	assert!(!strip_room_id_if_needed(&mut obj, "10"));
	assert!(obj.contains_key("room_id"));
}

#[test]
fn v12_create_event_without_room_id_is_noop() {
	let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.create","content":{"creator":"@alice:example.org"}}"#,
	)
	.unwrap();
	assert!(!strip_room_id_if_needed(&mut obj, "12"));
}

// -- V11/V12 event format compliance tests --

/// Helper: simulates the import field-stripping pipeline.
/// Strips diagnostic fields and applies room_id transformations.
fn strip_import_fields(obj: &mut serde_json::Map<String, serde_json::Value>, room_version: &str) {
	// Diagnostic fields injected during export
	obj.remove("__shortstatehash");
	obj.remove("prev_state_events");
	obj.remove("state_jump_pointers");

	strip_room_id_if_needed(obj, room_version);
}

/// Helper: checks whether an event's auth_events list references the create
/// event.
fn auth_events_reference_create(
	obj: &serde_json::Map<String, serde_json::Value>,
	create_event_id: &str,
) -> bool {
	obj.get("auth_events")
		.and_then(|v| v.as_array())
		.is_some_and(|arr| arr.iter().any(|v| v.as_str() == Some(create_event_id)))
}

#[test]
fn import_strips_diagnostic_fields() {
	let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.message","room_id":"!abc:example.org","__shortstatehash":12345,"prev_state_events":[],"state_jump_pointers":[],"content":{}}"#,
	)
	.unwrap();
	strip_import_fields(&mut obj, "10");
	assert!(!obj.contains_key("__shortstatehash"));
	assert!(!obj.contains_key("prev_state_events"));
	assert!(!obj.contains_key("state_jump_pointers"));
	// Non-diagnostic fields preserved
	assert!(obj.contains_key("type"));
	assert!(obj.contains_key("room_id"));
	assert!(obj.contains_key("content"));
}

#[test]
fn v11_event_keeps_room_id_in_wire_format() {
	// In v11, room_id IS part of the wire format.
	let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.member","room_id":"!abc:example.org","content":{}}"#,
	)
	.unwrap();
	strip_import_fields(&mut obj, "11");
	assert!(obj.contains_key("room_id"));
}

#[test]
fn v12_create_event_full_import_pipeline() {
	let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.create","room_id":"!abc:example.org","__shortstatehash":999,"content":{"room_version":"12"},"auth_events":[],"prev_events":[]}"#,
	)
	.unwrap();
	strip_import_fields(&mut obj, "12");
	assert!(!obj.contains_key("room_id"), "V12 create must not have room_id");
	assert!(!obj.contains_key("__shortstatehash"), "diagnostic field must be stripped");
	assert!(obj.contains_key("content"));
	assert!(obj.contains_key("auth_events"));
}

#[test]
fn v12_non_create_event_keeps_room_id_after_import() {
	let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.member","room_id":"!abc:example.org","content":{}}"#,
	)
	.unwrap();
	strip_import_fields(&mut obj, "12");
	assert!(obj.contains_key("room_id"));
}

#[test]
fn v12_auth_events_must_not_reference_create() {
	let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.member","auth_events":["$power_levels","$join_rules"],"content":{}}"#,
	)
	.unwrap();
	assert!(!auth_events_reference_create(&obj, "$create_event"));
}

#[test]
fn v12_auth_events_rejects_explicit_create_reference() {
	let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.member","auth_events":["$create_event","$power_levels"],"content":{}}"#,
	)
	.unwrap();
	assert!(auth_events_reference_create(&obj, "$create_event"));
}

#[test]
fn v10_auth_events_must_reference_create() {
	let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.member","auth_events":["$create_event","$power_levels","$join_rules"],"content":{}}"#,
	)
	.unwrap();
	assert!(auth_events_reference_create(&obj, "$create_event"));
}

#[test]
fn v12_create_event_has_empty_auth_events() {
	let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.create","auth_events":[],"content":{"room_version":"12"}}"#,
	)
	.unwrap();
	let auth = obj.get("auth_events").and_then(|v| v.as_array()).unwrap();
	assert!(auth.is_empty(), "create event must have empty auth_events");
}

#[test]
fn strip_preserves_older_versions() {
	for version in &["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"] {
		let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
			r#"{"type":"m.room.member","room_id":"!abc:example.org","content":{}}"#,
		)
		.unwrap();
		strip_room_id_if_needed(&mut obj, version);
		assert!(
			obj.contains_key("room_id"),
			"room_id must be preserved for room version {version}"
		);
	}
}

#[test]
fn strip_v12_create_removes() {
	let mut obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
		r#"{"type":"m.room.create","room_id":"!abc:example.org","content":{}}"#,
	)
	.unwrap();
	strip_room_id_if_needed(&mut obj, "12");
	assert!(!obj.contains_key("room_id"), "room_id must be removed for V12 create events");
}

struct TempDbGuard {
	path: std::path::PathBuf,
	services: Option<std::sync::Arc<service::Services>>,
	_serial: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for TempDbGuard {
	fn drop(&mut self) {
		if let Some(services) = self.services.take() {
			let _ = std::thread::Builder::new()
				.name("admin-test-shutdown".into())
				.spawn(move || {
					if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
						.enable_all()
						.build()
					{
						runtime.block_on(async move {
							services.stop().await;
							drop(services);
						});
					}
				})
				.and_then(|thread| {
					thread
						.join()
						.map_err(|_| std::io::Error::other("admin test shutdown thread panicked"))
				});
		}

		let _ = std::fs::remove_dir_all(&self.path);
	}
}

async fn setup_test_services(prefix: &str) -> (std::sync::Arc<service::Services>, TempDbGuard) {
	use figment::providers::Format;
	let _ = rustls::crypto::ring::default_provider().install_default();

	static TEST_SERIAL: std::sync::OnceLock<std::sync::Arc<tokio::sync::Mutex<()>>> =
		std::sync::OnceLock::new();
	static TEST_DB_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
	let serial = TEST_SERIAL
		.get_or_init(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
		.clone()
		.lock_owned()
		.await;
	let count = TEST_DB_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	let db_path = std::env::temp_dir().join(format!("conduwuit_test_db_{prefix}_{count}"));
	let _ = std::fs::remove_dir_all(&db_path);

	let figment = figment::Figment::new().merge(figment::providers::Toml::string(&format!(
		r#"
			server_name = "test.conduwuit.local"
			database_path = "{}"

			# Test-specific RocksDB tuning to prevent WAL bloat and FD exhaustion.
			# Without these, parallel tests create 46 column families each with
			# unbounded WAL, consuming 1+ GiB per test and exhausting file descriptors.
			db_write_buffer_capacity_mb = 64.0
			db_cache_capacity_mb = 128.0
			rocksdb_parallelism_threads = 1
			rocksdb_wal_compression = "zstd"
			"#,
		db_path.to_string_lossy().replace('\\', "/")
	)));

	let config = conduwuit::config::Config::new(&figment).expect("failed to parse config");
	let runtime_handle = tokio::runtime::Handle::current();
	let server = std::sync::Arc::new(conduwuit::Server::new(
		config,
		Some(&runtime_handle),
		conduwuit::log::Log {
			reload: conduwuit::log::LogLevelReloadHandles::default(),
			capture: std::sync::Arc::new(conduwuit::log::capture::State::default()),
		},
	));

	let services = service::Services::build(server)
		.await
		.expect("failed to build services");
	let services = services.start().await.expect("failed to start services");

	// Boot admin module context references
	crate::init(&services.admin).await;

	let guard = TempDbGuard {
		path: db_path,
		services: Some(std::sync::Arc::clone(&services)),
		_serial: serial,
	};

	(services, guard)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_yolo_audit_membership_drift() {
	use conduwuit::pdu::PduBuilder;
	use ruma::{
		RoomId, RoomVersionId,
		events::room::{
			create::RoomCreateEventContent,
			member::{MembershipState, RoomMemberEventContent},
		},
	};
	let (services, _guard) = setup_test_services("yolo").await;

	let room_id = RoomId::new(services.globals.server_name());
	let _short_id = services
		.rooms
		.short
		.get_or_create_shortroomid(&room_id)
		.await;
	let state_lock = services.rooms.state.mutex.lock(&room_id).await;

	// Create bot user
	let server_user = services.globals.server_user.as_ref();
	services
		.users
		.create(server_user, None, None)
		.await
		.unwrap();

	// 1. Create event
	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomCreateEventContent {
				federate: true,
				predecessor: None,
				room_version: RoomVersionId::V11,
				..RoomCreateEventContent::new_v11()
			}),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	// 2. Bot user joins
	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				String::from(server_user),
				&RoomMemberEventContent::new(MembershipState::Join),
			),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	// Power levels event
	use ruma::events::room::power_levels::RoomPowerLevelsEventContent;
	let mut power_levels = RoomPowerLevelsEventContent::new();
	power_levels
		.users
		.insert(server_user.to_owned(), ruma::int!(100));
	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &power_levels),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	// Join rules event
	use ruma::events::room::join_rules::{JoinRule, RoomJoinRulesEventContent};
	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomJoinRulesEventContent::new(JoinRule::Public)),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();
	drop(state_lock);

	// Assert cache is currently consistent
	let res = services
		.admin
		.command_in_place(
			format!("yolo audit-membership {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "audit-membership failed: {res:?}");
	let output = match res {
		| Ok(Some(out)) => out.body().to_owned(),
		| _ => panic!("Expected output"),
	};
	assert!(
		output.contains("No actionable divergences."),
		"expected no divergences: {output}"
	);
	assert!(
		output.contains("OK: Membership cache is consistent"),
		"expected consistent cache: {output}"
	);

	// 1. Simulate user mismatch drift (user joined in state, but marked as left in
	//    cache)
	let user_id = ruma::user_id!("@user:test.conduwuit.local");
	services.users.create(user_id, None, None).await.unwrap();

	let state_lock = services.rooms.state.mutex.lock(&room_id).await;
	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				String::from(user_id.as_str()),
				&RoomMemberEventContent::new(MembershipState::Join),
			),
			user_id,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();
	drop(state_lock);

	// Manually mark as left in cache (corrupt cache)
	services
		.rooms
		.state_cache
		.mark_as_left_silent(user_id, &room_id)
		.await;
	services
		.rooms
		.state_cache
		.update_joined_count(&room_id)
		.await;

	// Run audit-membership and check it reports inconsistency and heals it
	let res = services
		.admin
		.command_in_place(
			format!("yolo audit-membership {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	let output = match res {
		| Ok(Some(out)) => out.body().to_owned(),
		| _ => panic!("Expected output"),
	};
	assert!(
		output.contains("✗ CACHE INCONSISTENCY"),
		"expected cache inconsistency: {output}"
	);
	assert!(output.contains("Cache repaired."), "expected cache to be repaired: {output}");

	// Assert cache is now consistent again
	let res = services
		.admin
		.command_in_place(
			format!("yolo audit-membership {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	let output = match res {
		| Ok(Some(out)) => out.body().to_owned(),
		| _ => panic!("Expected output"),
	};
	assert!(
		output.contains("OK: Membership cache is consistent"),
		"expected consistent cache: {output}"
	);

	// 2. Simulate aggregate count mismatch drift (count drift)
	services.db["roomid_joinedcount"].raw_put(&room_id, 999_u64);

	let res = services
		.admin
		.command_in_place(
			format!("yolo audit-membership {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	let output = match res {
		| Ok(Some(out)) => out.body().to_owned(),
		| _ => panic!("Expected output"),
	};
	assert!(
		output.contains("✗ CACHE INCONSISTENCY"),
		"expected count inconsistency: {output}"
	);
	assert!(output.contains("cache=999"), "expected cached count in output: {output}");
	assert!(output.contains("Cache repaired."), "expected cache to be repaired: {output}");

	// Assert cache is consistent again
	let res = services
		.admin
		.command_in_place(
			format!("yolo audit-membership {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	let output = match res {
		| Ok(Some(out)) => out.body().to_owned(),
		| _ => panic!("Expected output"),
	};
	assert!(
		output.contains("OK: Membership cache is consistent"),
		"expected consistent cache: {output}"
	);
}

/// Regression test for the demote-timeline-to-outlier "torn state" bug used
/// by `yolo audit-membership --clean`'s divergence repair: an event ends up
/// with `is_outlier: false` (stale) but no timeline pointers/state hash --
/// neither a normal timeline event nor a real outlier.
///
/// Root cause: `add_pdu_outlier_batch`'s "already in timeline" guard does a
/// live, synchronous `eventid_metadata` read. Queuing both
/// `remove_timeline_pointers_batch` and `add_pdu_outlier_batch` into one
/// `Batch` and applying it once meant the guard's read never saw the
/// removal -- it read the pre-removal metadata, concluded the event was
/// "already in timeline", and silently skipped writing the outlier copy,
/// while the removal half of the batch went through regardless.
///
/// This exercises the two calls exactly as `yolo/state.rs`'s clean-repair
/// path now does: the removal applied and committed *before*
/// `add_pdu_outlier_batch` runs, so its guard sees accurate state.
#[tokio::test(flavor = "multi_thread")]
async fn test_demote_timeline_to_outlier_leaves_no_torn_state() {
	use conduwuit::pdu::PduBuilder;
	use ruma::{
		RoomId, RoomVersionId,
		events::room::{
			create::RoomCreateEventContent,
			member::{MembershipState, RoomMemberEventContent},
		},
	};
	let (services, _guard) = setup_test_services("demote_torn").await;

	let room_id = RoomId::new(services.globals.server_name());
	let _short_id = services
		.rooms
		.short
		.get_or_create_shortroomid(&room_id)
		.await;
	let state_lock = services.rooms.state.mutex.lock(&room_id).await;

	let server_user = services.globals.server_user.as_ref();
	services
		.users
		.create(server_user, None, None)
		.await
		.unwrap();

	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomCreateEventContent {
				federate: true,
				predecessor: None,
				room_version: RoomVersionId::V11,
				..RoomCreateEventContent::new_v11()
			}),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				String::from(server_user),
				&RoomMemberEventContent::new(MembershipState::Join),
			),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	use ruma::events::room::{
		join_rules::{JoinRule, RoomJoinRulesEventContent},
		power_levels::RoomPowerLevelsEventContent,
	};
	let mut power_levels = RoomPowerLevelsEventContent::new();
	power_levels
		.users
		.insert(server_user.to_owned(), ruma::int!(100));
	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &power_levels),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();
	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomJoinRulesEventContent::new(JoinRule::Public)),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	let user_id = ruma::user_id!("@torn:test.conduwuit.local");
	services.users.create(user_id, None, None).await.unwrap();
	let event_id = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				String::from(user_id.as_str()),
				&RoomMemberEventContent::new(MembershipState::Join),
			),
			user_id,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();
	drop(state_lock);

	// Precondition: a normal, fully-appended timeline event.
	let meta_before = services
		.rooms
		.timeline
		.get_event_metadata(&event_id)
		.await
		.unwrap();
	assert!(!meta_before.is_outlier, "precondition: event should start as a timeline event");

	let pdu_json = services
		.rooms
		.timeline
		.get_pdu_json(&event_id)
		.await
		.unwrap();

	// Reproduce yolo/state.rs's demote-to-outlier sequence: two separately
	// applied batches under the room's insert lock, exactly as the fix
	// requires.
	let insert_lock = services.rooms.timeline.mutex_insert.lock(&room_id).await;

	let mut remove_batch = conduwuit_database::Batch::new();
	services
		.rooms
		.timeline
		.remove_timeline_pointers_batch(&mut remove_batch, &event_id)
		.await;
	services.rooms.timeline.apply_batch(remove_batch);

	let mut outlier_batch = conduwuit_database::Batch::new();
	services.rooms.outlier.add_pdu_outlier_batch(
		&mut outlier_batch,
		&event_id,
		&pdu_json,
		Some(&room_id),
	);
	services.rooms.timeline.apply_batch(outlier_batch);

	drop(insert_lock);

	let meta_after = services
		.rooms
		.timeline
		.get_event_metadata(&event_id)
		.await
		.unwrap();
	assert!(
		meta_after.is_outlier,
		"event must end up marked as an outlier after demotion -- is_outlier=false with no \
		 timeline pointers means it's torn between both states: {meta_after:?}"
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_yolo_reorder_timeline() {
	use conduwuit::pdu::PduBuilder;
	use ruma::{
		RoomId, RoomVersionId,
		events::room::{
			create::RoomCreateEventContent,
			member::{MembershipState, RoomMemberEventContent},
			message::RoomMessageEventContent,
		},
	};
	let (services, _guard) = setup_test_services("reorder").await;

	let room_id = RoomId::new(services.globals.server_name());
	let _short_id = services
		.rooms
		.short
		.get_or_create_shortroomid(&room_id)
		.await;

	let state_lock = services.rooms.state.mutex.lock(&room_id).await;

	// Create bot user
	let server_user = services.globals.server_user.as_ref();
	services
		.users
		.create(server_user, None, None)
		.await
		.unwrap();

	// 1. Create event
	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomCreateEventContent {
				federate: true,
				predecessor: None,
				room_version: RoomVersionId::V11,
				..RoomCreateEventContent::new_v11()
			}),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	// 2. Bot user joins
	let join_event = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				String::from(server_user),
				&RoomMemberEventContent::new(MembershipState::Join),
			),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	// 3. Append Event A
	let event_a = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain("Event A")),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	// Reset extremities to join_event so that Event B is concurrent (fork)
	services
		.rooms
		.state
		.set_forward_extremities(
			&room_id,
			vec![join_event.clone()].into_iter(),
			None,
			&state_lock,
		)
		.await;

	// 4. Append Event B
	let event_b = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain("Event B")),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	drop(state_lock);

	// Mutate origin_server_ts: Event A = 2000, Event B = 1000
	let mut json_a = services
		.rooms
		.timeline
		.get_pdu_json(&event_a)
		.await
		.unwrap();
	json_a.insert("origin_server_ts".to_owned(), ruma::CanonicalJsonValue::Integer(2000.into()));
	let pdu_id_a = services.rooms.timeline.get_pdu_id(&event_a).await.unwrap();
	services
		.rooms
		.timeline
		.replace_pdu(&pdu_id_a, &json_a, &event_a)
		.await
		.unwrap();

	let mut json_b = services
		.rooms
		.timeline
		.get_pdu_json(&event_b)
		.await
		.unwrap();
	json_b.insert("origin_server_ts".to_owned(), ruma::CanonicalJsonValue::Integer(1000.into()));
	let pdu_id_b = services.rooms.timeline.get_pdu_id(&event_b).await.unwrap();
	services
		.rooms
		.timeline
		.replace_pdu(&pdu_id_b, &json_b, &event_b)
		.await
		.unwrap();

	// Check original order (Event A count < Event B count)
	let count_a_before = conduwuit::matrix::pdu::Id::from(
		services.rooms.timeline.get_pdu_id(&event_a).await.unwrap(),
	);
	let count_b_before = conduwuit::matrix::pdu::Id::from(
		services.rooms.timeline.get_pdu_id(&event_b).await.unwrap(),
	);
	assert!(
		count_a_before.shorteventid.into_signed() < count_b_before.shorteventid.into_signed(),
		"Event A should be before Event B initially"
	);

	// Run reorder-timeline
	let res = services
		.admin
		.command_in_place(
			format!("yolo reorder-timeline {room_id} --no-compute-state"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reorder-timeline failed: {res:?}");

	// Run rebuild-state
	let res = services
		.admin
		.command_in_place(
			format!("yolo rebuild-state {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "rebuild-state failed: {res:?}");

	// Verify stream order remains strictly immutable
	let count_a_after = conduwuit::matrix::pdu::Id::from(
		services.rooms.timeline.get_pdu_id(&event_a).await.unwrap(),
	);
	let count_b_after = conduwuit::matrix::pdu::Id::from(
		services.rooms.timeline.get_pdu_id(&event_b).await.unwrap(),
	);
	assert_eq!(
		count_a_before.shorteventid, count_a_after.shorteventid,
		"Event A stream order must not be mutated"
	);
	assert_eq!(
		count_b_before.shorteventid, count_b_after.shorteventid,
		"Event B stream order must not be mutated"
	);

	// Check new order: B (ts=1000) should sort before A (ts=2000) because
	// the topological sort tie-breaks concurrent forks by timestamp
	// (chronological).
	let mut ordered_events = Vec::new();
	use futures::StreamExt;
	let mut stream = Box::pin(services.rooms.timeline.topo_pdus(&room_id, None));
	while let Some(Ok((_, pdu))) = stream.next().await {
		ordered_events.push(pdu.event_id.clone());
	}
	let index_a = ordered_events
		.iter()
		.position(|id| id == &event_a)
		.expect("Event A not found");
	let index_b = ordered_events
		.iter()
		.position(|id| id == &event_b)
		.expect("Event B not found");
	println!("Event A topological index: {}, Event B topological index: {}", index_a, index_b);
	assert!(
		index_b < index_a,
		"Event B (ts=1000) should be before Event A (ts=2000) after reordering because the \
		 topological sort tie-breaks concurrent forks by timestamp (chronological)"
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_yolo_dedup_room_removes_duplicate_topo_entry() {
	use conduwuit::{
		PduCount,
		matrix::pdu::{Id as PduId, RawId as RawPduId},
		pdu::PduBuilder,
	};
	use futures::StreamExt;
	use ruma::{
		RoomId, RoomVersionId,
		events::room::{
			create::RoomCreateEventContent,
			member::{MembershipState, RoomMemberEventContent},
			message::RoomMessageEventContent,
		},
	};

	fn topo_pducount_key(pdu_id: &RawPduId, depth: u64) -> Vec<u8> {
		let mut shorteventid = [0_u8; 8];
		shorteventid.copy_from_slice(&pdu_id.shorteventid());
		let stream_ordering = i64::from_be_bytes(PduCount::offset_binary_encoding(shorteventid));
		let timeline_key = conduwuit::matrix::pdu::TimelineKey::new(depth, stream_ordering);

		let mut topo_key = Vec::with_capacity(24);
		topo_key.extend_from_slice(&pdu_id.shortroomid());
		topo_key.extend_from_slice(&timeline_key.to_be_bytes());
		topo_key
	}

	let (services, _guard) = setup_test_services("dedup").await;

	let room_id = RoomId::new(services.globals.server_name());
	let shortroomid = services
		.rooms
		.short
		.get_or_create_shortroomid(&room_id)
		.await;

	let state_lock = services.rooms.state.mutex.lock(&room_id).await;
	let server_user = services.globals.server_user.as_ref();
	services
		.users
		.create(server_user, None, None)
		.await
		.unwrap();

	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomCreateEventContent {
				federate: true,
				predecessor: None,
				room_version: RoomVersionId::V11,
				..RoomCreateEventContent::new_v11()
			}),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				String::from(server_user),
				&RoomMemberEventContent::new(MembershipState::Join),
			),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	let duplicated_event = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain("duplicate me")),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();
	drop(state_lock);

	let duplicate_count = PduCount::Normal(services.globals.next_count().unwrap());
	let duplicate_pdu_id: RawPduId = PduId {
		shortroomid,
		shorteventid: duplicate_count,
	}
	.into();
	let metadata = services
		.rooms
		.timeline
		.get_event_metadata(&duplicated_event)
		.await
		.unwrap();
	let duplicate_topo_key = topo_pducount_key(&duplicate_pdu_id, metadata.depth.into());

	// Seed the exact corruption that dedup-room repairs: a second timeline
	// index entry for the same event ID, without changing the canonical
	// eventid_pduid mapping.
	services.db["room_pducount_eventid"].insert(&duplicate_pdu_id, duplicated_event.as_bytes());
	services.db["roomid_topologicalorder_pducount"]
		.insert(&duplicate_topo_key, duplicated_event.as_bytes());

	let mut stream_duplicates = 0_usize;
	let mut stream = Box::pin(services.rooms.timeline.all_pdus(&room_id));
	while let Some((_, pdu)) = stream.next().await {
		if pdu.event_id == duplicated_event {
			stream_duplicates = stream_duplicates.saturating_add(1);
		}
	}
	assert_eq!(stream_duplicates, 2, "test setup should create a duplicate stream entry");

	// topo_pdus filters entries whose encoded position doesn't match the
	// event's canonical EventMetadata (see comment on
	// `count_topo_occurrences_for_test`) — exactly the mismatch this test
	// just manufactured — so use a raw column scan here, not topo_pdus,
	// to verify the corrupted entry actually landed in the DB.
	let topo_duplicates =
		count_topo_occurrences_for_test(&services, &room_id, &duplicated_event).await;
	assert_eq!(topo_duplicates, 2, "test setup should create a duplicate topo entry");

	let res = services
		.admin
		.command_in_place(
			format!("yolo dedup-room {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "dedup-room failed: {res:?}");

	let mut stream_duplicates = 0_usize;
	let mut stream = Box::pin(services.rooms.timeline.all_pdus(&room_id));
	while let Some((_, pdu)) = stream.next().await {
		if pdu.event_id == duplicated_event {
			stream_duplicates = stream_duplicates.saturating_add(1);
		}
	}
	assert_eq!(stream_duplicates, 1, "dedup-room should remove duplicate stream entry");

	let topo_duplicates =
		count_topo_occurrences_for_test(&services, &room_id, &duplicated_event).await;
	assert_eq!(topo_duplicates, 1, "dedup-room should remove duplicate topo entry");
}

async fn create_test_room_with_message(
	services: &std::sync::Arc<service::Services>,
	body: &str,
) -> (ruma::OwnedRoomId, ruma::OwnedEventId) {
	use conduwuit::pdu::PduBuilder;
	use ruma::{
		RoomId, RoomVersionId,
		events::room::{
			create::RoomCreateEventContent,
			member::{MembershipState, RoomMemberEventContent},
			message::RoomMessageEventContent,
		},
	};

	let room_id = RoomId::new(services.globals.server_name());
	services
		.rooms
		.short
		.get_or_create_shortroomid(&room_id)
		.await;

	let state_lock = services.rooms.state.mutex.lock(&room_id).await;
	let server_user = services.globals.server_user.as_ref();
	services
		.users
		.create(server_user, None, None)
		.await
		.unwrap();

	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomCreateEventContent {
				federate: true,
				predecessor: None,
				room_version: RoomVersionId::V11,
				..RoomCreateEventContent::new_v11()
			}),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				String::from(server_user),
				&RoomMemberEventContent::new(MembershipState::Join),
			),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	let event_id = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain(body)),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();
	drop(state_lock);

	(room_id, event_id)
}

fn topo_pducount_key_for_test(pdu_id: &conduwuit::matrix::pdu::RawId, depth: u64) -> Vec<u8> {
	let mut shorteventid = [0_u8; 8];
	shorteventid.copy_from_slice(&pdu_id.shorteventid());
	let stream_ordering =
		i64::from_be_bytes(conduwuit::matrix::pdu::Count::offset_binary_encoding(shorteventid));
	let timeline_key = conduwuit::matrix::pdu::TimelineKey::new(depth, stream_ordering);

	let mut topo_key = Vec::with_capacity(24);
	topo_key.extend_from_slice(&pdu_id.shortroomid());
	topo_key.extend_from_slice(&timeline_key.to_be_bytes());
	topo_key
}

async fn seed_stale_topo_entry_for_test(
	services: &std::sync::Arc<service::Services>,
	event_id: &ruma::EventId,
) {
	let pdu_id = services.rooms.timeline.get_pdu_id(event_id).await.unwrap();
	let metadata = services
		.rooms
		.timeline
		.get_event_metadata(event_id)
		.await
		.unwrap();
	let stale_topo_key = topo_pducount_key_for_test(
		&pdu_id,
		metadata.deprecated_local_topo_depth.saturating_add(10_000),
	);
	services.db["roomid_topologicalorder_pducount"].insert(&stale_topo_key, event_id.as_bytes());
}

/// Counts raw entries for `event_id` in the `roomid_topologicalorder_pducount`
/// column, scanning the column directly rather than going through
/// `Service::topo_pdus`.
///
/// `topo_pdus` deliberately filters out any entry whose encoded
/// (depth, pdu_count) doesn't match the event's canonical `EventMetadata`
/// (see `EventMetadata::matches_timeline_position`) — that's the read-time
/// safety property that keeps stale/duplicate index entries from ever being
/// served to clients. The tests using this helper manufacture exactly that
/// kind of mismatched entry on purpose, to verify `yolo dedup-room` /
/// `reindex-short` / `reorder-timeline` clean it up — none of which rely on
/// `topo_pdus` to find what to repair (they scan `all_pdus` or rebuild the
/// column outright), so this helper needs to see the raw, unfiltered count
/// to make that assertion meaningful.
async fn count_topo_occurrences_for_test(
	services: &std::sync::Arc<service::Services>,
	room_id: &ruma::RoomId,
	event_id: &ruma::EventId,
) -> usize {
	use futures::StreamExt;

	let shortroomid = services.rooms.short.get_shortroomid(room_id).await.unwrap();
	let prefix = shortroomid.to_be_bytes();

	let mut count = 0_usize;
	let mut stream =
		Box::pin(services.db["roomid_topologicalorder_pducount"].raw_stream_prefix(&prefix));
	while let Some(item) = stream.next().await {
		let (_, val) = item.unwrap();
		if val == event_id.as_bytes() {
			count = count.saturating_add(1);
		}
	}
	count
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_forward_extremities_excludes_ineligible_candidates() {
	use futures::StreamExt;
	use ruma::OwnedEventId;

	let (services, _guard) =
		setup_test_services("set_forward_extremities_excludes_ineligible").await;
	let (room_id, event_id) = create_test_room_with_message(&services, "eligible tip").await;

	// A candidate we never appended anywhere: mark it rejected via the public
	// pdu_metadata API. Since it has no prior metadata, this also implicitly
	// flags it as an outlier (see `mark_event_rejected`'s doc comment).
	let rejected_event_id: OwnedEventId = "$fake_rejected_extremity_candidate_0000"
		.try_into()
		.unwrap();
	services
		.rooms
		.pdu_metadata
		.mark_event_rejected(&rejected_event_id, "test: never accepted into timeline")
		.await;

	let state_lock = services.rooms.state.mutex.lock(&room_id).await;
	services
		.rooms
		.state
		.set_forward_extremities(
			&room_id,
			vec![event_id.clone(), rejected_event_id.clone()].into_iter(),
			None,
			&state_lock,
		)
		.await;
	drop(state_lock);

	let extremities: Vec<OwnedEventId> = services
		.rooms
		.state
		.get_forward_extremities(&room_id)
		.collect()
		.await;

	assert!(
		extremities.contains(&event_id),
		"the accepted timeline event must remain a forward extremity"
	);
	assert!(
		!extremities.contains(&rejected_event_id),
		"a rejected/outlier candidate must never be persisted as a forward extremity"
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_forward_extremities_all_ineligible_is_noop() {
	use futures::StreamExt;
	use ruma::OwnedEventId;

	let (services, _guard) =
		setup_test_services("set_forward_extremities_all_ineligible_noop").await;
	let (room_id, event_id) =
		create_test_room_with_message(&services, "existing eligible tip").await;

	let baseline: Vec<OwnedEventId> = services
		.rooms
		.state
		.get_forward_extremities(&room_id)
		.collect()
		.await;
	assert_eq!(
		baseline,
		vec![event_id.clone()],
		"test setup should start with exactly one forward extremity"
	);

	let rejected_event_id: OwnedEventId = "$fake_rejected_extremity_candidate_0001"
		.try_into()
		.unwrap();
	services
		.rooms
		.pdu_metadata
		.mark_event_rejected(&rejected_event_id, "test: never accepted into timeline")
		.await;

	// Passing only ineligible candidates must not empty or corrupt the stored
	// extremity set: it should be treated as an anomaly and left unchanged.
	let state_lock = services.rooms.state.mutex.lock(&room_id).await;
	services
		.rooms
		.state
		.set_forward_extremities(&room_id, vec![rejected_event_id].into_iter(), None, &state_lock)
		.await;
	drop(state_lock);

	let extremities: Vec<OwnedEventId> = services
		.rooms
		.state
		.get_forward_extremities(&room_id)
		.collect()
		.await;
	assert_eq!(
		extremities, baseline,
		"an all-ineligible candidate set must leave existing extremities unchanged"
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_yolo_reindex_short_removes_stale_topo_entries() {
	let (services, _guard) = setup_test_services("reindex_short_topo").await;
	let (room_id, event_id) = create_test_room_with_message(&services, "stale topo").await;

	assert_eq!(count_topo_occurrences_for_test(&services, &room_id, &event_id).await, 1);
	seed_stale_topo_entry_for_test(&services, &event_id).await;
	assert_eq!(
		count_topo_occurrences_for_test(&services, &room_id, &event_id).await,
		2,
		"test setup should create a stale duplicate topo entry",
	);

	let res = services
		.admin
		.command_in_place(
			format!("yolo reindex-short {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reindex-short failed: {res:?}");

	assert_eq!(
		count_topo_occurrences_for_test(&services, &room_id, &event_id).await,
		1,
		"reindex-short should rebuild topo index without stale duplicates",
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_yolo_reorder_timeline_removes_stale_topo_entries() {
	let (services, _guard) = setup_test_services("reorder_topo").await;
	let (room_id, event_id) = create_test_room_with_message(&services, "stale topo").await;

	seed_stale_topo_entry_for_test(&services, &event_id).await;
	assert_eq!(
		count_topo_occurrences_for_test(&services, &room_id, &event_id).await,
		2,
		"test setup should create a stale duplicate topo entry",
	);

	let res = services
		.admin
		.command_in_place(
			format!("yolo reorder-timeline {room_id} --no-compute-state"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reorder-timeline failed: {res:?}");

	assert_eq!(
		count_topo_occurrences_for_test(&services, &room_id, &event_id).await,
		1,
		"reorder-timeline should rebuild topo index without stale duplicates",
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_busted_dag_resolution() {
	use std::path::Path;

	use conduwuit::matrix::Event;
	use futures::StreamExt;
	use ruma::RoomId;

	let dag_path_str = std::env::var("CONDUWUIT_TEST_DAG_BUSTED").unwrap_or_default();
	let dag_path = Path::new(&dag_path_str);
	if !dag_path.exists() {
		println!("Skipping test_busted_dag_resolution: test DAG file not found");
		return;
	}
	let (services, _guard) = setup_test_services("busted_dag").await;

	let room_id = RoomId::parse("!L58ME6ufiP49v97UIOBIpvWKEgj4912JmECPuDzlvCI").unwrap();

	// 1. Import the DAG
	let start_import = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!(
				"yolo import-pdus {} --skip-auth --skip-sig-verify --room-version 12",
				dag_path.to_string_lossy()
			),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "import-pdus failed: {res:?}");
	println!("yolo import-pdus took {:?}", start_import.elapsed());

	// Build derived data (auth chain roaring bitmaps, short IDs, metadata)
	println!("Starting reindex-short...");
	let start_reindex = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo reindex-short {room_id} --skip-topo"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reindex-short failed: {res:?}");
	println!("reindex-short took {:?}", start_reindex.elapsed());

	// Run reorder-timeline
	println!("Starting yolo reorder-timeline...");
	let start_reorder = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo reorder-timeline {room_id} --no-compute-state"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reorder-timeline failed: {res:?}");
	println!("yolo reorder-timeline took {:?}", start_reorder.elapsed());

	// Run rebuild-state (compute state hashes from the create event forward)
	println!("Starting rebuild-state...");
	let start_rebuild = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo rebuild-state {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "rebuild-state failed: {res:?}");
	println!("rebuild-state took {:?}", start_rebuild.elapsed());

	// Bootstrap room state hash from the latest PDU
	let latest_pdu = services
		.rooms
		.timeline
		.latest_pdu_in_room(room_id)
		.await
		.unwrap();
	let latest_event_id = latest_pdu.event_id();
	let ssh = services
		.rooms
		.state_accessor
		.pdu_shortstatehash(latest_event_id)
		.await
		.unwrap();
	let state_lock = services.rooms.state.mutex.lock(room_id).await;
	services
		.rooms
		.state
		.set_room_state(room_id, ssh, &state_lock);
	drop(state_lock);

	// Run force-set-state (to trigger re-resolution on local DAG)
	println!("Starting force-set-state...");
	let start_force = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("debug force-set-state {room_id} --event-id {latest_event_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "force-set-state failed: {res:?}");
	println!("force-set-state took {:?}", start_force.elapsed());

	// Run check-rooms (to check sanity)
	println!("Starting check-rooms...");
	let start_check = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			"yolo check-rooms".to_owned(),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "check-rooms failed: {res:?}");
	println!("check-rooms took {:?}", start_check.elapsed());

	// Run audit-membership
	println!("Starting audit-membership...");
	let start_audit = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo audit-membership {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "audit-membership failed: {res:?}");
	println!("audit-membership took {:?}", start_audit.elapsed());

	// Verify forward extremities count is small and not bloated (e.g. 2000 heads)
	let exts_count = services
		.rooms
		.state
		.get_forward_extremities(room_id)
		.count()
		.await;
	println!("Busted DAG resolved. Final forward extremities count: {exts_count}");
	assert!(exts_count < 10, "expected very few forward extremities, got: {exts_count}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unredacted_room_dag_resolution() {
	use std::path::Path;

	use conduwuit::matrix::Event;
	use futures::StreamExt;
	use ruma::RoomId;

	let dag_path_str = std::env::var("CONDUWUIT_TEST_DAG_UNREDACTED_ROOM").unwrap_or_default();
	let dag_path = Path::new(&dag_path_str);
	if !dag_path.exists() {
		println!("Skipping test_unredacted_room_dag_resolution: test DAG file not found");
		return;
	}
	let (services, _guard) = setup_test_services("unredacted_room").await;

	let room_id = RoomId::parse("!BDSybzDpGyDxMHZzpN:unredacted.org").unwrap();

	// 1. Import the DAG
	println!("Starting import-pdus...");
	let start_import = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!(
				"yolo import-pdus {} --skip-auth --skip-sig-verify --room-version 10",
				dag_path.to_string_lossy()
			),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "import-pdus failed: {res:?}");
	println!("import-pdus took {:?}", start_import.elapsed());

	// Build derived data (auth chain roaring bitmaps)
	println!("Starting reindex-short...");
	let start_reindex = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo reindex-short {room_id} --skip-topo"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reindex-short failed: {res:?}");
	println!("reindex-short took {:?}", start_reindex.elapsed());

	// Run reorder-timeline
	println!("Starting reorder-timeline...");
	let start_reorder = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo reorder-timeline {room_id} --no-compute-state"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reorder-timeline failed: {res:?}");
	println!("reorder-timeline took {:?}", start_reorder.elapsed());

	// Run rebuild-state
	println!("Starting rebuild-state...");
	let start_rebuild = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo rebuild-state {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "rebuild-state failed: {res:?}");
	println!("rebuild-state took {:?}", start_rebuild.elapsed());

	// Bootstrap room state hash from the latest PDU
	let latest_pdu = services
		.rooms
		.timeline
		.latest_pdu_in_room(room_id)
		.await
		.unwrap();
	let latest_event_id = latest_pdu.event_id();
	let ssh = services
		.rooms
		.state_accessor
		.pdu_shortstatehash(latest_event_id)
		.await
		.unwrap();
	let state_lock = services.rooms.state.mutex.lock(room_id).await;
	services
		.rooms
		.state
		.set_room_state(room_id, ssh, &state_lock);
	drop(state_lock);

	// Run force-set-state (to trigger re-resolution on local DAG)
	let res = services
		.admin
		.command_in_place(
			format!("debug force-set-state {room_id} --event-id {latest_event_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "force-set-state failed: {res:?}");

	// Run check-rooms (to check sanity)
	let res = services
		.admin
		.command_in_place(
			"yolo check-rooms".to_owned(),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "check-rooms failed: {res:?}");

	// Run audit-membership
	let res = services
		.admin
		.command_in_place(
			format!("yolo audit-membership {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "audit-membership failed: {res:?}");

	// Verify forward extremities count is small and not bloated (e.g. 2000 heads)
	let exts_count = services
		.rooms
		.state
		.get_forward_extremities(room_id)
		.count()
		.await;
	println!("Unredacted Room DAG resolved. Final forward extremities count: {exts_count}");
	assert!(exts_count < 10, "expected very few forward extremities, got: {exts_count}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_unredacted_lounge_dag_resolution() {
	use std::path::Path;

	use conduwuit::matrix::Event;
	use futures::StreamExt;
	use ruma::RoomId;

	let dag_path_str = std::env::var("CONDUWUIT_TEST_DAG_UNREDACTED_LOUNGE").unwrap_or_default();
	let dag_path = Path::new(&dag_path_str);
	if !dag_path.exists() {
		println!("Skipping test_unredacted_lounge_dag_resolution: test DAG file not found");
		return;
	}
	eprintln!("[LOUNGE] setup_test_services...");
	let (services, _guard) = setup_test_services("unredacted_lounge").await;
	eprintln!("[LOUNGE] services ready");

	let room_id = RoomId::parse("!sM2LwqNHGQOgLf35gqxPMy9D7oYde2q9ADg8HPBM3kE").unwrap();

	// 1. Import the DAG
	eprintln!("[LOUNGE] >>> import-pdus");
	let start_import = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!(
				"yolo import-pdus {} --skip-auth --skip-sig-verify --room-version 12",
				dag_path.to_string_lossy()
			),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "import-pdus failed: {res:?}");
	eprintln!("[LOUNGE] <<< import-pdus took {:?}", start_import.elapsed());

	// Build derived data (auth chain roaring bitmaps)
	eprintln!("[LOUNGE] >>> reindex-short");
	let start_reindex = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo reindex-short {room_id} --skip-topo"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reindex-short failed: {res:?}");
	eprintln!("[LOUNGE] <<< reindex-short took {:?}", start_reindex.elapsed());

	// Reorder PDU index by origin_server_ts so rebuild-state processes
	// parents before children (it walks events in pdu_count order)
	eprintln!("[LOUNGE] >>> reorder-timeline");
	let start_reorder = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo reorder-timeline {room_id} --no-compute-state"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reorder-timeline failed: {res:?}");
	eprintln!("[LOUNGE] <<< reorder-timeline took {:?}", start_reorder.elapsed());

	// Run rebuild-state
	eprintln!("[LOUNGE] >>> rebuild-state");
	let start_rebuild = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo rebuild-state {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "rebuild-state failed: {res:?}");
	eprintln!("[LOUNGE] <<< rebuild-state took {:?}", start_rebuild.elapsed());

	// rebuild-state Phase 5+6 already merged extremities and set the room SSH.
	// Just read it back.
	let ssh = services
		.rooms
		.state
		.get_room_shortstatehash(room_id)
		.await
		.expect("rebuild-state should have set room SSH");
	let best_entries = services
		.rooms
		.state_accessor
		.state_full_pdus(ssh)
		.count()
		.await;
	eprintln!("[LOUNGE] Room SSH={ssh}, state entries={best_entries}");

	// Skip force-set-state — it reads room SSH which is stale for merged DAGs
	// with orphan extremities. Just validate rebuild-state's output directly.

	// Run check-rooms (to check sanity)
	eprintln!("[LOUNGE] >>> check-rooms");
	let start_check = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			"yolo check-rooms".to_owned(),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "check-rooms failed: {res:?}");
	eprintln!("[LOUNGE] <<< check-rooms took {:?}", start_check.elapsed());

	// Run audit-membership
	eprintln!("[LOUNGE] >>> audit-membership");
	let start_audit = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo audit-membership {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "audit-membership failed: {res:?}");
	eprintln!("[LOUNGE] <<< audit-membership took {:?}", start_audit.elapsed());

	// Verify forward extremities count is small and not bloated (e.g. 2000 heads)
	let exts_count = services
		.rooms
		.state
		.get_forward_extremities(room_id)
		.count()
		.await;
	eprintln!("[LOUNGE] forward extremities: {exts_count}");
	assert!(exts_count <= 15, "expected few forward extremities, got: {exts_count}");

	let expected_present = [
		"$TN3aSG4dg-NueYfa8FNgOg154yVJlB_g102cf5eQiFY",
		"$x49Eu0L3xnLbMJ1sAJIk8wtj0moDiZyjya_rNh3U2UQ",
		"$xqrfEc0vwvpDFN4laAkpvtniqlv1oV7kb-RfdT7mXCI",
		"$0-Rwh5ycT6Hwr9jkoiSsOSKW7HK_xiSrNyCvzh2Whcs",
		"$4sXgVhE2a85_i94Ul_TvfwKVfpjIHUQKWcuzdw0W8as",
		"$CITU5ramZfoRbG5NuEBd_kMm6f9a1UJB5TKRhMpVT6E",
		"$EhAnh9S3GYGd3tHSsoVhZAGbQt9fPgV_ketRNIQDc0s",
		"$DT2PAjF5OtuocQGMV_ekKgN68M6XaYYsO2TGQPGEZ_c",
	];

	let expected_absent = [
		"$AJsK9SExNlblHbfse7eDhSNISk9E871gJzbkqoTA9Ds",
		"$TtQ6QYSjCphiJuzNiwfINI-ylQQTkBSkWaMydae_nCc",
		"$YlZG-G6Ak3fdjf4TIHEA8oD7C_FHX8EwmwFYL6jXNtg",
		"$heDtrL6Z-AVUZkzEsqtIKLxIQpzhMwcEU4JZ1bRyXSE",
		"$kUBfA5z53UYwkouV54Wq_UgK_8vnszbTp8gflvF3qns",
		"$mK__qhCzbLBUyb4IjkIxXKQpmdBwr8vxWwd40sXn1U4",
		"$rmb6V2Nb_UScP9htYUTPOy9LhbWgxb5wxgMEIfj8aFM",
		"$Hk-xXbs52DhNQI_Ca1E2DkyNMazBITKkepo8IuqC7EI",
	];

	// Collect all resolved state PDUs for diagnostics
	let resolved_state_pdus: Vec<_> = services
		.rooms
		.state_accessor
		.state_full_pdus(ssh)
		.collect()
		.await;

	eprintln!("[LOUNGE] Total resolved state entries: {}", resolved_state_pdus.len());

	let resolved_state_ids: std::collections::HashSet<ruma::OwnedEventId> = resolved_state_pdus
		.iter()
		.map(|pdu| pdu.event_id().to_owned())
		.collect();

	let mut mismatches = 0u32;
	for id in &expected_present {
		let eid = <&ruma::EventId>::try_from(*id).unwrap();
		if !resolved_state_ids.contains(eid) {
			println!("MISMATCH: expected PRESENT but MISSING: {id}");
			// Find what's in the same state key slot
			if let Ok(pdu) = services.rooms.timeline.get_pdu(eid).await {
				let ty = pdu.kind().to_string();
				let sk = pdu.state_key().unwrap_or("(none)");
				println!("  type={ty}, state_key={sk}");
				// Find the actual winner in that slot
				for state_pdu in &resolved_state_pdus {
					if state_pdu.kind().to_string() == ty && state_pdu.state_key() == Some(sk) {
						println!(
							"  actual winner: {} (sender={}, ts={})",
							state_pdu.event_id(),
							state_pdu.sender(),
							u64::from(state_pdu.origin_server_ts().0),
						);
					}
				}
			}
			mismatches += 1;
		}
	}

	for id in &expected_absent {
		let eid = <&ruma::EventId>::try_from(*id).unwrap();
		if resolved_state_ids.contains(eid) {
			println!("MISMATCH: expected ABSENT but PRESENT: {id}");
			mismatches += 1;
		}
	}

	assert!(mismatches == 0, "{mismatches} state resolution mismatches (see above)");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nheko_dag_resolution() {
	use std::path::Path;

	use conduwuit::matrix::Event;
	use futures::StreamExt;
	use ruma::RoomId;

	let dag_path_str = std::env::var("CONDUWUIT_TEST_DAG_FILE").unwrap_or_default();
	let dag_path = Path::new(&dag_path_str);
	if !dag_path.exists() {
		println!("Skipping test_nheko_dag_resolution: test DAG file not found");
		return;
	}
	let (services, _guard) = setup_test_services("nheko_room").await;

	let room_id = RoomId::parse("!UbCmIlGTHNIgIRZcpt:nheko.im").unwrap();

	// 1. Import the DAG
	println!("Starting import-pdus...");
	let start_import = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!(
				"yolo import-pdus {} --skip-auth --skip-sig-verify --room-version 5",
				dag_path.to_string_lossy()
			),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "import-pdus failed: {res:?}");
	println!("import-pdus took {:?}", start_import.elapsed());

	// Build derived data (auth chain roaring bitmaps)
	println!("Starting reindex-short...");
	let start_reindex = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo reindex-short {room_id} --skip-topo"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reindex-short failed: {res:?}");
	println!("reindex-short took {:?}", start_reindex.elapsed());

	// Run reorder-timeline
	println!("Starting reorder-timeline...");
	let start_reorder = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo reorder-timeline {room_id} --no-compute-state"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reorder-timeline failed: {res:?}");
	println!("reorder-timeline took {:?}", start_reorder.elapsed());

	// Run rebuild-state
	println!("Starting rebuild-state...");
	let start_rebuild = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo rebuild-state {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "rebuild-state failed: {res:?}");
	println!("rebuild-state took {:?}", start_rebuild.elapsed());

	// Bootstrap room state hash from the latest PDU
	let latest_pdu = services
		.rooms
		.timeline
		.latest_pdu_in_room(room_id)
		.await
		.unwrap();
	let latest_event_id = latest_pdu.event_id();
	let ssh = services
		.rooms
		.state_accessor
		.pdu_shortstatehash(latest_event_id)
		.await
		.unwrap();
	let state_lock = services.rooms.state.mutex.lock(room_id).await;
	services
		.rooms
		.state
		.set_room_state(room_id, ssh, &state_lock);
	drop(state_lock);

	// Run force-set-state (to trigger re-resolution on local DAG)
	let res = services
		.admin
		.command_in_place(
			format!("debug force-set-state {room_id} --event-id {latest_event_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "force-set-state failed: {res:?}");

	// Run check-rooms (to check sanity)
	let res = services
		.admin
		.command_in_place(
			"yolo check-rooms".to_owned(),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "check-rooms failed: {res:?}");

	// Run audit-membership
	let res = services
		.admin
		.command_in_place(
			format!("yolo audit-membership {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "audit-membership failed: {res:?}");

	// Verify forward extremities count is not bloated (originally 6344 heads)
	let exts_count = services
		.rooms
		.state
		.get_forward_extremities(room_id)
		.count()
		.await;
	println!("Nheko Room DAG resolved. Final forward extremities count: {exts_count}");
	assert!(exts_count < 10, "expected very few forward extremities, got: {exts_count}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_yolo_heal_receipts() {
	use conduwuit_database::Json;
	use futures::StreamExt;
	use ruma::{
		RoomId, UserId,
		events::receipt::{Receipt, ReceiptEvent, ReceiptEventContent, ReceiptType},
	};
	let (services, _guard) = setup_test_services("heal_receipts").await;

	let room_id = RoomId::new(services.globals.server_name());
	let user_id = UserId::parse("@user:test.conduwuit.local").unwrap();

	// 1. Manually insert duplicate receipts into the database
	let mut content1 = ReceiptEventContent(std::collections::BTreeMap::new());
	let mut users1 = std::collections::BTreeMap::new();
	users1
		.insert(user_id.into(), Receipt::new(ruma::MilliSecondsSinceUnixEpoch(1000_u32.into())));
	let mut types1 = std::collections::BTreeMap::new();
	types1.insert(ReceiptType::Read, users1);
	content1
		.0
		.insert(ruma::event_id!("$event1").to_owned(), types1);

	let event1 = ReceiptEvent {
		content: content1,
		room_id: room_id.clone(),
	};

	let mut content2 = ReceiptEventContent(std::collections::BTreeMap::new());
	let mut users2 = std::collections::BTreeMap::new();
	users2
		.insert(user_id.into(), Receipt::new(ruma::MilliSecondsSinceUnixEpoch(2000_u32.into())));
	let mut types2 = std::collections::BTreeMap::new();
	types2.insert(ReceiptType::Read, users2);
	content2
		.0
		.insert(ruma::event_id!("$event2").to_owned(), types2);

	let event2 = ReceiptEvent {
		content: content2,
		room_id: room_id.clone(),
	};

	// Insert in order
	let mut prefix = room_id.as_bytes().to_vec();
	prefix.push(conduwuit_database::SEP);

	let mut key1 = prefix.clone();
	key1.extend_from_slice(&1_u64.to_be_bytes());
	key1.push(conduwuit_database::SEP);
	key1.extend_from_slice(user_id.as_bytes());
	services.db["readreceiptid_readreceipt"].raw_put(&key1, Json(event1));

	let mut key2 = prefix.clone();
	key2.extend_from_slice(&2_u64.to_be_bytes());
	key2.push(conduwuit_database::SEP);
	key2.extend_from_slice(user_id.as_bytes());
	services.db["readreceiptid_readreceipt"].raw_put(&key2, Json(event2));

	assert_eq!(
		services.db["readreceiptid_readreceipt"]
			.raw_stream()
			.count()
			.await,
		2
	);

	let res = services
		.admin
		.command_in_place(
			"yolo heal-receipts".to_owned(),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "heal-receipts failed: {res:?}");

	let count = services.db["readreceiptid_readreceipt"]
		.raw_stream()
		.count()
		.await;
	assert_eq!(count, 1, "Expected exactly 1 receipt remaining, got {count}");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_threaded_receipts_notification_counters() {
	use ruma::{OwnedEventId, RoomId, UserId, events::receipt::ReceiptThread};

	let (services, _guard) = setup_test_services("threaded_receipts").await;

	let room_id = RoomId::new(services.globals.server_name());
	let user_id = UserId::parse("@threaded:test.conduwuit.local").unwrap();
	let thread_a: OwnedEventId = "$thread-a:test.conduwuit.local".try_into().unwrap();
	let thread_b: OwnedEventId = "$thread-b:test.conduwuit.local".try_into().unwrap();

	services.db["userroomid_notificationcount"].put((&user_id, &room_id), 8_u64);
	services.db["userroomid_highlightcount"].put((&user_id, &room_id), 2_u64);
	services.db["userroomid_notificationcount"].put((&user_id, &room_id, &thread_a), 4_u64);
	services.db["userroomid_highlightcount"].put((&user_id, &room_id, &thread_a), 1_u64);
	services.db["userroomid_notificationcount"].put((&user_id, &room_id, &thread_b), 6_u64);

	let thread_counts = services
		.rooms
		.user
		.thread_notification_counts(&user_id, &room_id)
		.await;
	assert_eq!(thread_counts.get(&thread_a).copied(), Some((4, 1)));
	assert_eq!(thread_counts.get(&thread_b).copied(), Some((6, 0)));
	assert_eq!(
		services
			.rooms
			.user
			.notification_count(&user_id, &room_id)
			.await,
		8
	);
	assert_eq!(
		services
			.rooms
			.user
			.highlight_count(&user_id, &room_id)
			.await,
		2
	);

	services
		.rooms
		.user
		.reset_notification_counts_for_thread(
			&user_id,
			&room_id,
			&ReceiptThread::Thread(thread_a.clone()),
		)
		.await;

	let thread_counts = services
		.rooms
		.user
		.thread_notification_counts(&user_id, &room_id)
		.await;
	assert_eq!(
		services
			.rooms
			.user
			.notification_count(&user_id, &room_id)
			.await,
		8
	);
	assert_eq!(
		services
			.rooms
			.user
			.highlight_count(&user_id, &room_id)
			.await,
		2
	);
	assert_eq!(thread_counts.get(&thread_a).copied(), Some((0, 0)));
	assert_eq!(thread_counts.get(&thread_b).copied(), Some((6, 0)));

	services
		.rooms
		.user
		.reset_notification_counts_for_thread(&user_id, &room_id, &ReceiptThread::Main)
		.await;

	let thread_counts = services
		.rooms
		.user
		.thread_notification_counts(&user_id, &room_id)
		.await;
	assert_eq!(
		services
			.rooms
			.user
			.notification_count(&user_id, &room_id)
			.await,
		0
	);
	assert_eq!(
		services
			.rooms
			.user
			.highlight_count(&user_id, &room_id)
			.await,
		0
	);
	assert_eq!(thread_counts.get(&thread_a).copied(), Some((0, 0)));
	assert_eq!(thread_counts.get(&thread_b).copied(), Some((6, 0)));

	services
		.rooms
		.user
		.reset_notification_counts_for_thread(&user_id, &room_id, &ReceiptThread::Unthreaded)
		.await;

	assert_eq!(
		services
			.rooms
			.user
			.notification_count(&user_id, &room_id)
			.await,
		0
	);
	assert_eq!(
		services
			.rooms
			.user
			.highlight_count(&user_id, &room_id)
			.await,
		0
	);
	assert!(
		services
			.rooms
			.user
			.thread_notification_counts(&user_id, &room_id)
			.await
			.is_empty()
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_yolo_rescue_room() {
	use conduwuit::pdu::PduBuilder;
	use ruma::{
		RoomId,
		events::room::{
			create::RoomCreateEventContent,
			member::{MembershipState, RoomMemberEventContent},
		},
	};
	let (services, _guard) = setup_test_services("rescue_room").await;

	let room_id = RoomId::new(services.globals.server_name());
	let server_user = services.globals.server_user.as_ref();
	services
		.users
		.create(server_user, None, None)
		.await
		.unwrap();

	let _admin_room = services.admin.get_admin_room().await.unwrap();

	let state_lock = services.rooms.state.mutex.lock(&room_id).await;

	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomCreateEventContent::new_v11()),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				String::from(server_user),
				&RoomMemberEventContent::new(MembershipState::Join),
			),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();
	drop(state_lock);

	services.db["roomid_shortstatehash"].remove(&room_id);

	let res = services
		.admin
		.command_in_place(
			format!("yolo rescue-room {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "rescue-room failed: {res:?}");

	let res = services
		.admin
		.command_in_place(
			"yolo check-rooms".to_owned(),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	let output = res.unwrap().unwrap().body().to_owned();
	assert!(!output.contains('✗'), "Expected clean state after rescue");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_knocking_dag_resolution() {
	use std::path::Path;

	use ruma::RoomId;

	let dag_path_str = std::env::var("CONDUWUIT_TEST_DAG_KNOCKING").unwrap_or_default();
	let dag_path = Path::new(&dag_path_str);
	if !dag_path.exists() {
		println!("Skipping test_knocking_dag_resolution: test DAG file not found");
		return;
	}
	let (services, _guard) = setup_test_services("knocking_dag").await;

	let room_id = RoomId::parse("!ylRY10DiOcgVxCi0W8f9ztanFl5wdBxYCWQqM45n_Kk").unwrap();

	// 1. Import the DAG
	println!("Starting import-pdus...");
	let res = services
		.admin
		.command_in_place(
			format!(
				"yolo import-pdus {} --skip-auth --skip-sig-verify --room-version 12",
				dag_path.to_string_lossy()
			),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "import-pdus failed: {res:?}");

	// Build derived data (auth chain roaring bitmaps)
	println!("Starting reindex-short...");
	let res = services
		.admin
		.command_in_place(
			format!("yolo reindex-short {room_id} --skip-topo"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reindex-short failed: {res:?}");

	// Reorder PDU index
	println!("Starting reorder-timeline...");
	let res = services
		.admin
		.command_in_place(
			format!("yolo reorder-timeline {room_id} --no-compute-state"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reorder-timeline failed: {res:?}");

	// Run rebuild-state
	println!("Starting rebuild-state...");
	let res = services
		.admin
		.command_in_place(
			format!("yolo rebuild-state {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "rebuild-state failed: {res:?}");

	println!("DAG knocking state resolved successfully without panicking!");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_yolo_reorder_timeline_state_resolution() {
	use conduwuit::pdu::PduBuilder;
	use ruma::{
		RoomId, RoomVersionId,
		events::room::{
			create::RoomCreateEventContent,
			member::{MembershipState, RoomMemberEventContent},
			message::RoomMessageEventContent,
			name::RoomNameEventContent,
		},
	};
	let (services, _guard) = setup_test_services("reorder_state_res").await;

	let room_id = RoomId::new(services.globals.server_name());
	let _short_id = services
		.rooms
		.short
		.get_or_create_shortroomid(&room_id)
		.await;

	let state_lock = services.rooms.state.mutex.lock(&room_id).await;

	// Create bot user
	let server_user = services.globals.server_user.as_ref();
	services
		.users
		.create(server_user, None, None)
		.await
		.unwrap();

	// 1. Create room event
	let _create_event = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomCreateEventContent {
				federate: true,
				predecessor: None,
				room_version: RoomVersionId::V11,
				..RoomCreateEventContent::new_v11()
			}),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	// 2. Bot user joins
	let _join_event = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(
				String::from(server_user),
				&RoomMemberEventContent::new(MembershipState::Join),
			),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	// 3. Set Room Name to "Name A" (base state event)
	let name_a_event = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomNameEventContent::new("Name A".to_owned())),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	// Reset extremities to name_a_event so Branch 1 and Branch 2 fork from it
	services
		.rooms
		.state
		.set_forward_extremities(
			&room_id,
			vec![name_a_event.clone()].into_iter(),
			None,
			&state_lock,
		)
		.await;

	// Ensure Name B gets a strictly later origin_server_ts than Name A.
	// Under parallel test load, both events can land in the same millisecond,
	// making the V2 mainline sort tiebreaker depend on event_id hash ordering.
	tokio::time::sleep(std::time::Duration::from_millis(2)).await;

	// 4. Branch 1: Set Room Name to "Name B" (state event)
	let name_b_event = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::state(String::new(), &RoomNameEventContent::new("Name B".to_owned())),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	// Reset extremities to name_a_event again for Branch 2 fork
	services
		.rooms
		.state
		.set_forward_extremities(
			&room_id,
			vec![name_a_event.clone()].into_iter(),
			None,
			&state_lock,
		)
		.await;

	// 5. Branch 2: Message "Hello C" (non-state event)
	let message_c_event = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain("Hello C")),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	// Reset extremities to merge the two branches at the next event
	services
		.rooms
		.state
		.set_forward_extremities(
			&room_id,
			vec![name_b_event.clone(), message_c_event.clone()].into_iter(),
			None,
			&state_lock,
		)
		.await;

	// 6. Append Merge Event M: "Hello M" (non-state event)
	let merge_event = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder::timeline(&RoomMessageEventContent::text_plain("Hello M")),
			server_user,
			Some(&room_id),
			&state_lock,
		)
		.await
		.unwrap();

	drop(state_lock);

	// Run reorder-timeline (WITH state computation)
	let res = services
		.admin
		.command_in_place(
			format!("yolo reorder-timeline {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reorder-timeline failed: {res:?}");

	// Verify that state snapshot for message_c_event (Branch 2) does NOT leak
	// Branch 1 ("Name B") name change. Its state name must be "Name A".
	let ssh_c = services
		.rooms
		.state_accessor
		.pdu_shortstatehash(&message_c_event)
		.await
		.unwrap();
	let name_c: Option<RoomNameEventContent> = services
		.rooms
		.state_accessor
		.state_get_content(ssh_c, &ruma::events::StateEventType::RoomName, "")
		.await
		.ok();
	assert_eq!(
		name_c.as_ref().map(|c| c.name.as_str()),
		Some("Name A"),
		"Branch 2 message should not leak concurrent Branch 1 state changes"
	);

	// Verify that state snapshot for merge_event (M) correctly resolves conflict to
	// "Name B"
	let ssh_m = services
		.rooms
		.state_accessor
		.pdu_shortstatehash(&merge_event)
		.await
		.unwrap();
	let name_m: Option<RoomNameEventContent> = services
		.rooms
		.state_accessor
		.state_get_content(ssh_m, &ruma::events::StateEventType::RoomName, "")
		.await
		.ok();
	assert_eq!(
		name_m.as_ref().map(|c| c.name.as_str()),
		Some("Name B"),
		"Merge event state snapshot should resolve conflict to Name B"
	);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_janian_dag_reorder_with_state() {
	use std::path::Path;

	use ruma::RoomId;

	let dag_path_str = std::env::var("CONDUWUIT_TEST_DAG_JANIAN").unwrap_or_default();
	let dag_path = Path::new(&dag_path_str);
	if !dag_path.exists() {
		println!("Skipping test_janian_dag_reorder_with_state: test DAG file not found");
		return;
	}
	let (services, _guard) = setup_test_services("janian_dag").await;

	let room_id = RoomId::parse("!hdMhyaHZvjLjagsXsk:janian.de").unwrap();

	// 1. Import the DAG
	let start_import = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!(
				"yolo import-pdus {} --skip-auth --skip-sig-verify --room-version 11",
				dag_path.to_string_lossy()
			),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "import-pdus failed: {res:?}");
	println!("yolo import-pdus took {:?}", start_import.elapsed());

	// Run reorder-timeline WITH state computation!
	println!("Starting yolo reorder-timeline (with state)...");
	let start_reorder = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			format!("yolo reorder-timeline {room_id}"),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "reorder-timeline failed: {res:?}");
	println!("yolo reorder-timeline took {:?}", start_reorder.elapsed());

	// Run check-rooms (to check sanity)
	println!("Starting check-rooms...");
	let start_check = std::time::Instant::now();
	let res = services
		.admin
		.command_in_place(
			"yolo check-rooms".to_owned(),
			None,
			service::admin::InvocationSource::Console,
		)
		.await;
	assert!(res.is_ok(), "check-rooms failed: {res:?}");
	println!("check-rooms took {:?}", start_check.elapsed());
}
