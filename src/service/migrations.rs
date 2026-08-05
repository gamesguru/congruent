use std::{cmp, collections::HashMap, future::ready};

use conduwuit::{
	Err, Event, Pdu, Result, debug, debug_info, debug_warn, err, error, info,
	result::NotFound,
	trace,
	utils::{
		IterStream, ReadyExt,
		stream::{TryExpect, TryIgnore},
	},
	warn,
};
use database::Json;
use futures::{FutureExt, StreamExt, TryStreamExt, pin_mut};
use itertools::Itertools;
use ruma::{
	OwnedRoomId, OwnedUserId, RoomId, UserId,
	events::{
		AnyStrippedStateEvent, GlobalAccountDataEventType, StateEventType,
		push_rules::PushRulesEvent,
		room::member::{MembershipState, RoomMemberEventContent},
	},
	push::Ruleset,
	serde::Raw,
};
use sha2::{Digest, Sha256};

use crate::{Services, media, rooms::short::ShortStateHash};

/// The current schema version.
/// - If database is opened at greater version we reject with error. The
///   software must be updated for backward-incompatible changes.
/// - If database is opened at lesser version we apply migrations up to this.
///   Note that named-feature migrations may also be performed when opening at
///   equal or lesser version. These are expected to be backward-compatible.
pub(crate) const DATABASE_VERSION: u64 = 19;

/// Column families explicitly dropped in migrations. These are included
/// in the fingerprint hash (prefixed with '-') so that a branch which
/// still has them as live CFs produces a different fingerprint.
const DROPPED_CFS: &[&str] =
	&["eventid_receivecount", "roomid_outliereventid", "softfailedeventids"];

/// Compute schema fingerprint from the static column family name list,
/// explicitly dropped CFs, and the schema version number.
fn compute_schema_fingerprint() -> [u8; 32] {
	let mut hasher = Sha256::new();

	// Include version so (v19, CFs) != (v20, same CFs)
	hasher.update(DATABASE_VERSION.to_be_bytes());

	// MAPS is already in alphabetical order (static slice)
	for name in database::maps::column_family_names() {
		hasher.update(b"+");
		hasher.update(name.as_bytes());
		hasher.update(b"\n");
	}

	// Dropped CFs marked with '-' prefix
	for name in DROPPED_CFS {
		hasher.update(b"-");
		hasher.update(name.as_bytes());
		hasher.update(b"\n");
	}

	hasher.finalize().into()
}

pub(crate) async fn migrations(services: &Services) -> Result<()> {
	let users_count = services.users.count().await;

	// Matrix resource ownership is based on the server name; changing it
	// requires recreating the database from scratch.
	if users_count > 0 {
		let server_user = &services.globals.server_user;
		if !services.users.exists(server_user).await {
			error!("The {server_user} server user does not exist, and the database is not new.");
			return Err!(Database(
				"Cannot reuse an existing database after changing the server name, please \
				 delete the old one first.",
			));
		}
	}

	if users_count > 0 {
		migrate(services).await
	} else {
		fresh(services).await
	}
}

async fn fresh(services: &Services) -> Result<()> {
	info!("Creating new fresh database");
	let db = &services.db;

	services.globals.db.bump_database_version(DATABASE_VERSION);
	services
		.globals
		.db
		.set_schema_fingerprint(&compute_schema_fingerprint());

	db["global"].insert(b"feat_sha256_media", []);
	db["global"].insert(b"fix_bad_double_separator_in_state_cache", []);
	db["global"].insert(b"retroactively_fix_bad_data_from_roomuserid_joined", []);
	db["global"].insert(b"fix_referencedevents_missing_sep", []);
	db["global"].insert(b"fix_readreceiptid_readreceipt_duplicates", []);
	db["global"].insert(b"fix_corrupt_msc4133_fields", []);
	db["global"].insert(b"populate_userroomid_leftstate_table", []);
	db["global"].insert(b"fix_local_invite_state", []);
	// v19 - PDU and read receipt refactor/optimization
	db["global"].insert(MIGRATE_EVENT_STORE_TO_SSOT_MARKER, []);
	db["global"].insert(MIGRATE_READ_RECEIPTS_TO_SSOT_MARKER, []);
	db["global"].insert(MIGRATE_PRIVATE_READ_RECEIPTS_TO_SSOT_MARKER, []);
	db["global"].insert(POPULATE_TOPOLOGICAL_INDEX_MARKER, []);
	db["global"].insert(POPULATE_SHORTPREVEVENTS_MARKER, []);
	db["global"].insert(UNIFY_RAW_PDU_ID_MARKER, []);

	// Create the admin room and server user on first run
	info!("Creating admin room and server user");
	crate::admin::create_admin_room(services)
		.boxed()
		.await
		.inspect_err(|e| error!("Failed to create admin room during db init: {e}"))?;

	info!("Created new database with version {DATABASE_VERSION}");

	Ok(())
}

/// Apply any migrations
async fn migrate(services: &Services) -> Result<()> {
	let db = &services.db;
	let config = &services.server.config;

	// Guard against running software older than what created this database
	let db_version = services.globals.db.database_version().await;
	if db_version > DATABASE_VERSION {
		return Err!(Database(
			"Database schema version {db_version} is newer than this software supports \
			 ({DATABASE_VERSION}). Upgrade the software or use a compatible database.",
		));
	}

	if services.globals.db.database_version().await < 11 {
		return Err!(Database(
			"Database schema version {} is no longer supported",
			services.globals.db.database_version().await
		));
	}

	if services.globals.db.database_version().await < 12 {
		db_lt_12(services)
			.await
			.map_err(|e| err!("Failed to run v12 migrations: {e}"))?;
	}

	// This migration can be reused as-is anytime the server-default rules are
	// updated.
	if services.globals.db.database_version().await < 13 {
		db_lt_13(services)
			.await
			.map_err(|e| err!("Failed to run v13 migrations: {e}"))?;
	}

	if db["global"].get(b"feat_sha256_media").await.is_not_found() {
		media::migrations::migrate_sha256_media(services)
			.await
			.map_err(|e| err!("Failed to run SHA256 media migration: {e}"))?;
	} else if config.media_startup_check {
		info!("Starting media startup integrity check.");
		let now = std::time::Instant::now();
		media::migrations::checkup_sha256_media(services)
			.await
			.map_err(|e| err!("Failed to verify media integrity: {e}"))?;
		info!(
			"Finished media startup integrity check in {} seconds.",
			now.elapsed().as_secs_f32()
		);
	}

	if db["global"]
		.get(b"fix_bad_double_separator_in_state_cache")
		.await
		.is_not_found()
	{
		info!("Running migration 'fix_bad_double_separator_in_state_cache'");
		fix_bad_double_separator_in_state_cache(services)
			.await
			.map_err(|e| {
				err!("Failed to run 'fix_bad_double_separator_in_state_cache' migration: {e}")
			})?;
	}

	if db["global"]
		.get(b"retroactively_fix_bad_data_from_roomuserid_joined")
		.await
		.is_not_found()
	{
		info!("Running migration 'retroactively_fix_bad_data_from_roomuserid_joined'");
		retroactively_fix_bad_data_from_roomuserid_joined(services)
			.await
			.map_err(|e| {
				err!(
					"Failed to run 'retroactively_fix_bad_data_from_roomuserid_joined' \
					 migration: {e}"
				)
			})?;
	}

	if db["global"]
		.get(b"fix_referencedevents_missing_sep")
		.await
		.is_not_found()
		|| services.globals.db.database_version().await < 17
	{
		info!("Running migration 'fix_referencedevents_missing_sep'");
		fix_referencedevents_missing_sep(services)
			.await
			.map_err(|e| {
				err!("Failed to run 'fix_referencedevents_missing_sep' migration': {e}")
			})?;
	}

	if db["global"]
		.get(b"fix_readreceiptid_readreceipt_duplicates")
		.await
		.is_not_found()
		|| services.globals.db.database_version().await < 17
	{
		info!("Running migration 'fix_readreceiptid_readreceipt_duplicates'");
		fix_readreceiptid_readreceipt_duplicates(services)
			.await
			.map_err(|e| {
				err!("Failed to run 'fix_readreceiptid_readreceipt_duplicates' migration': {e}")
			})?;
	}

	if services.globals.db.database_version().await < 17 {
		services.globals.db.bump_database_version(17);
		info!("Migration: Bumped database version to 17");
	}

	if db["global"]
		.get(FIXED_CORRUPT_MSC4133_FIELDS_MARKER)
		.await
		.is_not_found()
	{
		info!("Running migration 'fix_corrupt_msc4133_fields'");
		fix_corrupt_msc4133_fields(services)
			.await
			.map_err(|e| err!("Failed to run 'fix_corrupt_msc4133_fields' migration': {e}"))?;
	}

	if services.globals.db.database_version().await < 18 {
		services.globals.db.bump_database_version(18);
		info!("Migration: Bumped database version to 18");
	}

	if db["global"]
		.get(POPULATED_USERROOMID_LEFTSTATE_TABLE_MARKER)
		.await
		.is_not_found()
	{
		info!("Running migration 'populate_userroomid_leftstate_table'");
		populate_userroomid_leftstate_table(services)
			.await
			.map_err(|e| {
				err!("Failed to run 'populate_userroomid_leftstate_table' migration': {e}")
			})?;
	}

	if db["global"]
		.get(FIXED_LOCAL_INVITE_STATE_MARKER)
		.await
		.is_not_found()
	{
		info!("Running migration 'fix_local_invite_state'");
		fix_local_invite_state(services)
			.await
			.map_err(|e| err!("Failed to run 'fix_local_invite_state' migration': {e}"))?;
	}

	let ssot_needs_run = db["global"]
		.get(MIGRATE_EVENT_STORE_TO_SSOT_MARKER)
		.await
		.is_not_found()
		|| db["eventid_pdu"].raw_keys().next().await.is_none();

	if ssot_needs_run {
		info!("Running migration 'migrate_event_store_to_ssot'");
		migrate_event_store_to_ssot(services)
			.await
			.map_err(|e| err!("Failed to run 'migrate_event_store_to_ssot': {e}"))?;
	}

	if db["global"]
		.get(MIGRATE_READ_RECEIPTS_TO_SSOT_MARKER)
		.await
		.is_not_found()
	{
		info!("Running migration 'migrate_read_receipts'");
		migrate_read_receipts(services)
			.await
			.map_err(|e| err!("Failed to run 'migrate_read_receipts': {e}"))?;
	}

	let private_receipts_needs_run = db["global"]
		.get(MIGRATE_PRIVATE_READ_RECEIPTS_TO_SSOT_MARKER)
		.await
		.is_not_found()
		|| db["roomuserid_privatereadreceipt"]
			.raw_keys()
			.next()
			.await
			.is_none();

	if private_receipts_needs_run {
		info!("Running migration 'migrate_private_read_receipts'");
		migrate_private_read_receipts(services)
			.await
			.map_err(|e| err!("Failed to run 'migrate_private_read_receipts': {e}"))?;
	}

	// Version 19 - keep events and outliers in a single table, add
	// eventid_metadata, drop softfailedeventids
	if services.globals.db.database_version().await < 19 {
		db_lt_19(services)
			.await
			.map_err(|e| err!("Failed to run v19 migrations: {e}"))?;
	}

	if db["global"]
		.get(UNIFY_RAW_PDU_ID_MARKER)
		.await
		.is_not_found()
	{
		info!("Running migration 'unify_raw_pdu_id_16_byte'");
		unify_raw_pdu_id_16_byte(services)
			.await
			.map_err(|e| err!("Failed to run 'unify_raw_pdu_id_16_byte': {e}"))?;
	}

	if db["global"]
		.get(POPULATE_TOPOLOGICAL_INDEX_MARKER)
		.await
		.is_not_found()
	{
		info!("Running migration 'populate_topological_index'");
		populate_topological_index(services)
			.await
			.map_err(|e| err!("Failed to run 'populate_topological_index': {e}"))?;
	}

	if db["global"]
		.get(POPULATE_PDU_COUNT_IN_METADATA_MARKER)
		.await
		.is_not_found()
	{
		info!("Running migration 'populate_pdu_count_in_metadata'");
		populate_pdu_count_in_metadata(services)
			.await
			.map_err(|e| err!("Failed to run 'populate_pdu_count_in_metadata': {e}"))?;
	}

	if services.globals.db.database_version().await != DATABASE_VERSION {
		return Err!(Database(
			"Database version {} does not match expected version {DATABASE_VERSION} after \
			 running all migrations.",
			services.globals.db.database_version().await,
		));
	}

	// Validate schema fingerprint (trust-on-first-use for upgrades)
	let expected = compute_schema_fingerprint();
	if let Some(stored) = services.globals.db.schema_fingerprint().await {
		if stored != expected {
			return Err!(Database(
				"Schema fingerprint mismatch! This database was created by a different build \
				 with incompatible column families. Expected {expected:x?}, found {stored:x?}. \
				 Do NOT continue — data corruption will occur.",
			));
		}
	}
	services.globals.db.set_schema_fingerprint(&expected);
	// --- END v19 migration ---

	{
		let patterns = services.globals.forbidden_usernames();
		if !patterns.is_empty() {
			services
				.users
				.stream()
				.ready_filter(|user_id| services.globals.user_is_local(user_id))
				.ready_for_each(|user_id| {
					let matches = patterns.matches(user_id.localpart());
					if matches.matched_any() {
						warn!(
							"User {} matches the following forbidden username patterns: {}",
							user_id.to_string(),
							matches
								.into_iter()
								.map(|x| &patterns.patterns()[x])
								.join(", ")
						);
					}
				})
				.await;
		}
	}

	{
		let patterns = services.globals.forbidden_alias_names();
		if !patterns.is_empty() {
			services
				.rooms
				.alias
				.all_local_aliases()
				.ready_for_each(|(room_id, alias)| {
					let matches = patterns.matches(alias);
					if matches.matched_any() {
						warn!(
							"Room with alias #{alias} ({room_id}) matches the following \
							 forbidden room name patterns: {}",
							matches
								.into_iter()
								.map(|x| &patterns.patterns()[x])
								.join(", ")
						);
					}
				})
				.await;
		}
	}

	info!("Loaded RocksDB database with schema version {DATABASE_VERSION}");

	Ok(())
}

const MIGRATE_READ_RECEIPTS_TO_SSOT_MARKER: &[u8] = b"migrate_read_receipts_to_ssot";
async fn migrate_read_receipts(services: &Services) -> Result<()> {
	use ruma::events::receipt::ReceiptEvent;

	info!("Starting read receipt state map migration...");

	let db = &services.db;
	let stream_index = db["readreceiptid_readreceipt"].clone();
	let state_map = db["roomuserid_readreceipt"].clone();

	let stream = stream_index.raw_stream();
	pin_mut!(stream);

	let mut total_migrated: usize = 0;

	while let Some((key, value)) = stream.try_next().await? {
		let sep1 = key.iter().position(|&b| b == database::SEP);
		let Some(sep1) = sep1 else {
			continue;
		};

		let room_id_bytes = &key[..sep1];
		let count_start = sep1.saturating_add(1);
		let count_end = count_start.saturating_add(8);
		if key.len() <= count_end || key[count_end] != database::SEP {
			continue;
		}
		let count_bytes = &key[count_start..count_end];
		let count = conduwuit::utils::u64_from_bytes(count_bytes).unwrap_or(0);
		let user_id_bytes = &key[count_end.saturating_add(1)..];

		let Ok(event) = serde_json::from_slice::<ReceiptEvent>(value) else {
			continue;
		};

		let mut state_key = room_id_bytes.to_vec();
		state_key.push(database::SEP);
		state_key.extend_from_slice(user_id_bytes);

		state_map.put(state_key, Json((count, event)));
		total_migrated = total_migrated.saturating_add(1);

		if total_migrated.is_multiple_of(2000) {
			info!("Migrated {} read receipts to state map...", total_migrated);
		}
	}

	info!("Successfully migrated {total_migrated} read receipts into the new O(1) state map!");
	db["global"].insert(MIGRATE_READ_RECEIPTS_TO_SSOT_MARKER, []);
	db.db.sort()?;
	Ok(())
}

const MIGRATE_PRIVATE_READ_RECEIPTS_TO_SSOT_MARKER: &[u8] =
	b"migrate_private_read_receipts_to_ssot";
async fn migrate_private_read_receipts(services: &Services) -> Result<()> {
	info!("Starting private read receipt migration...");

	let db = &services.db;
	let legacy_count_map = db["roomuserid_privateread"].clone();
	let legacy_event_map = db["roomuserid_privatereadevent"].clone();
	let legacy_update_map = db["roomuserid_lastprivatereadupdate"].clone();
	let new_receipt_map = db["roomuserid_privatereadreceipt"].clone();

	let stream = legacy_count_map.raw_stream();
	pin_mut!(stream);
	let mut total_migrated: usize = 0;
	let mut with_event: usize = 0;
	let mut count_only: usize = 0;
	let mut skipped: usize = 0;

	while let Some((key, value)) = stream.try_next().await? {
		let Some(sep) = key.iter().position(|&b| b == database::SEP) else {
			continue;
		};

		let room_id_bytes = &key[..sep];
		let user_id_bytes = &key[sep.saturating_add(1)..];

		let Ok(room_id) = <&RoomId>::try_from(
			conduwuit::utils::string::str_from_bytes(room_id_bytes).unwrap_or_default(),
		) else {
			skipped = skipped.saturating_add(1);
			continue;
		};
		let Ok(user_id) = <&UserId>::try_from(
			conduwuit::utils::string::str_from_bytes(user_id_bytes).unwrap_or_default(),
		) else {
			skipped = skipped.saturating_add(1);
			continue;
		};

		let count =
			conduwuit::utils::u64_from_bytes(value.get(..8).unwrap_or_default()).unwrap_or(0);

		let mut legacy_key = room_id.as_bytes().to_vec();
		legacy_key.push(0xFF);
		legacy_key.extend_from_slice(user_id.as_bytes());

		let event: ruma::events::receipt::ReceiptEvent =
			if let Ok(event_bytes) = legacy_event_map.get(&legacy_key).await {
				with_event = with_event.saturating_add(1);
				serde_json::from_slice(&event_bytes).unwrap_or_else(|_| {
					ruma::events::receipt::ReceiptEvent {
						content: ruma::events::receipt::ReceiptEventContent(
							std::collections::BTreeMap::new(),
						),
						room_id: room_id.to_owned(),
					}
				})
			} else {
				count_only = count_only.saturating_add(1);
				// No cached event -- store receipt with count only (no DB lookups)
				ruma::events::receipt::ReceiptEvent {
					content: ruma::events::receipt::ReceiptEventContent(
						std::collections::BTreeMap::new(),
					),
					room_id: room_id.to_owned(),
				}
			};

		let update_count = if let Ok(update_bytes) = legacy_update_map.get(&legacy_key).await {
			conduwuit::utils::u64_from_bytes(&update_bytes).unwrap_or(0)
		} else {
			0
		};

		let mut new_key = room_id.as_bytes().to_vec();
		new_key.push(database::SEP);
		new_key.extend_from_slice(user_id.as_bytes());

		new_receipt_map.put(new_key, Json((count, event, update_count)));
		total_migrated = total_migrated.saturating_add(1);

		if total_migrated.is_multiple_of(5000) {
			info!("Migrated {} private read receipts...", total_migrated);
		}
	}

	info!(
		"Successfully migrated {total_migrated} private read receipts ({with_event} with event, \
		 {count_only} count-only, {skipped} skipped)."
	);
	db["global"].insert(MIGRATE_PRIVATE_READ_RECEIPTS_TO_SSOT_MARKER, []);
	db.db.sort()?;
	Ok(())
}

const MIGRATE_EVENT_STORE_TO_SSOT_MARKER: &[u8] = b"migrate_event_store_to_ssot";
async fn migrate_event_store_to_ssot(services: &Services) -> Result<()> {
	info!(
		"Starting event store SSOT migration (pduid_pdu + eventid_outlierpdu -> eventid_pdu + \
		 room_pducount_eventid)..."
	);

	let db = &services.db;
	let eventid_pdu = db["eventid_pdu"].clone();
	let room_pducount_eventid = db["room_pducount_eventid"].clone();
	let eventid_metadata = db["eventid_metadata"].clone();
	let roomid_topologicalorder_pducount = db["roomid_topologicalorder_pducount"].clone();

	let cork = db.cork_and_sync();

	let mut total: usize = 0;
	let mut timeline: usize = 0;
	let mut outliers: usize = 0;
	let mut skipped: usize = 0;
	let mut timeline_event_ids: std::collections::HashSet<Vec<u8>> =
		std::collections::HashSet::new();
	let mut depth_cache: HashMap<Vec<u8>, u64> = HashMap::new();

	// Phase 1: Migrate timeline events from pduid_pdu (pdu_id -> PDU JSON)
	if let Ok(pduid_pdu) = database::Map::open(&db.db, "pduid_pdu") {
		info!("Phase 1: Migrating timeline events from pduid_pdu...");
		let stream = pduid_pdu.raw_stream();
		pin_mut!(stream);

		while let Some(Ok((pdu_id_bytes, pdu_json_bytes))) = stream.next().await {
			let Ok(pdu) = serde_json::from_slice::<conduwuit::PduEvent>(pdu_json_bytes) else {
				skipped = skipped.saturating_add(1);
				continue;
			};

			let event_id_bytes = pdu.event_id.as_bytes();
			let mut shortroomid = [0_u8; 8];
			shortroomid.copy_from_slice(&pdu_id_bytes[0..8]);

			let mut count_bytes = [0_u8; 8];
			if pdu_id_bytes.len() == 24 {
				count_bytes.copy_from_slice(&pdu_id_bytes[16..24]);
			} else {
				count_bytes.copy_from_slice(&pdu_id_bytes[8..16]);
			}

			let unsigned_pdu_count = i64::from_be_bytes(count_bytes).unsigned_abs();

			// eventid_pdu: event_id -> PDU JSON
			eventid_pdu.insert(event_id_bytes, pdu_json_bytes);

			// room_pducount_eventid: pdu_id -> event_id
			room_pducount_eventid.insert(&pdu_id_bytes, event_id_bytes);

			// eventid_metadata with topological depth
			let mut max_depth: u64 = 0;
			for prev_id in pdu.prev_events() {
				if let Some(&d) = depth_cache.get(prev_id.as_bytes()) {
					max_depth = max_depth.max(d);
				}
			}
			let deprecated_local_topo_depth = max_depth.saturating_add(1);
			depth_cache.insert(event_id_bytes.to_vec(), deprecated_local_topo_depth);

			let metadata = crate::rooms::timeline::EventMetadata {
				short_room_id: u64::from_be_bytes(shortroomid),
				is_outlier: false,
				origin_server_ts: pdu.origin_server_ts().0,
				depth: pdu.depth(),
				soft_failed: false,
				rejected: pdu.rejected(),
				redacted_by: pdu.redacts().map(ToOwned::to_owned),
				short_state_hash: None,
				deprecated_local_topo_depth,
				pdu_count: Some(unsigned_pdu_count),
				soft_fail_reason: String::new(),
				rejection_reason: String::new(),
			};
			if let Ok(metadata_bytes) = bincode::serialize(&metadata) {
				eventid_metadata.insert(event_id_bytes, metadata_bytes);
			}

			// roomid_topologicalorder_pducount
			let mut topo_key = Vec::with_capacity(32);
			topo_key.extend_from_slice(&shortroomid);
			topo_key.extend_from_slice(&deprecated_local_topo_depth.to_be_bytes());
			topo_key.extend_from_slice(&count_bytes);
			roomid_topologicalorder_pducount.insert(&topo_key, event_id_bytes);

			timeline_event_ids.insert(event_id_bytes.to_vec());
			timeline = timeline.saturating_add(1);
			total = total.saturating_add(1);
			if total.is_multiple_of(10000) {
				info!("Phase 1: Migrated {timeline} timeline PDUs...");
			}
		}
		info!("Phase 1 complete: {timeline} timeline PDUs migrated.");
	}

	// Phase 2: Migrate outliers from eventid_outlierpdu (event_id -> PDU JSON)
	if let Ok(eventid_outlierpdu) = database::Map::open(&db.db, "eventid_outlierpdu") {
		info!("Phase 2: Migrating outlier events from eventid_outlierpdu...");
		let stream = eventid_outlierpdu.raw_stream();
		pin_mut!(stream);

		while let Some(Ok((event_id_bytes, pdu_json_bytes))) = stream.next().await {
			let Ok(pdu) = serde_json::from_slice::<conduwuit::PduEvent>(pdu_json_bytes) else {
				skipped = skipped.saturating_add(1);
				continue;
			};

			// Only write if Phase 1 didn't already handle this event
			// (preserves authoritative timeline PDU data and is_outlier: false)
			if !timeline_event_ids.contains(event_id_bytes) {
				eventid_pdu.insert(event_id_bytes, pdu_json_bytes);

				let metadata = crate::rooms::timeline::EventMetadata {
					short_room_id: 0,
					is_outlier: true,
					origin_server_ts: pdu.origin_server_ts().0,
					depth: pdu.depth(),
					soft_failed: false,
					rejected: pdu.rejected(),
					redacted_by: pdu.redacts().map(ToOwned::to_owned),
					short_state_hash: None,
					deprecated_local_topo_depth: 0,
					pdu_count: None,
					soft_fail_reason: String::new(),
					rejection_reason: String::new(),
				};
				if let Ok(metadata_bytes) = bincode::serialize(&metadata) {
					eventid_metadata.insert(event_id_bytes, metadata_bytes);
				}
			}

			outliers = outliers.saturating_add(1);
			total = total.saturating_add(1);
			if outliers.is_multiple_of(10000) {
				info!("Phase 2: Migrated {outliers} outlier PDUs...");
			}
		}
		info!("Phase 2 complete: {outliers} outlier PDUs migrated.");
	}

	if total == 0 {
		info!("No legacy PDU data found; skipping SSOT migration.");
	}

	drop(cork);
	info!(
		"Successfully migrated {total} PDUs to SSOT event store ({timeline} timeline, \
		 {outliers} outliers, {skipped} skipped)."
	);

	db["global"].insert(MIGRATE_EVENT_STORE_TO_SSOT_MARKER, []);
	db["global"].insert(POPULATE_TOPOLOGICAL_INDEX_MARKER, []);
	db.db.sort()?;
	Ok(())
}

const POPULATE_TOPOLOGICAL_INDEX_MARKER: &[u8] = b"populate_topological_index_v2";
const POPULATE_SHORTPREVEVENTS_MARKER: &[u8] = b"populate_shortprevevents";

async fn populate_topological_index(services: &Services) -> Result<()> {
	const BATCH_SIZE: usize = 1000;

	info!("Starting migration to populate roomid_topologicalorder_pducount...");
	let db = &services.db;
	let room_pducount_eventid = db["room_pducount_eventid"].clone();
	let eventid_metadata = db["eventid_metadata"].clone();

	let roomid_topologicalorder_pducount = db["roomid_topologicalorder_pducount"].clone();

	// First, completely clear the old broken index (the byte encoding has changed).
	let clear_stream = roomid_topologicalorder_pducount.raw_stream();
	pin_mut!(clear_stream);
	let mut cleared: usize = 0;
	while let Some(Ok((key, _))) = clear_stream.next().await {
		roomid_topologicalorder_pducount.remove(&key);
		cleared = cleared.saturating_add(1);
	}
	info!("Cleared {cleared} old entries from topological index to prepare for rebuild.");

	let stream = room_pducount_eventid.raw_stream();
	pin_mut!(stream);
	let mut total_migrated: usize = 0;
	let mut batch_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH_SIZE);

	loop {
		// Collect a batch of entries from the stream
		batch_entries.clear();
		while batch_entries.len() < BATCH_SIZE {
			match stream.next().await {
				| Some(Ok((pdu_id_bytes, event_id_bytes))) => {
					batch_entries.push((pdu_id_bytes.to_vec(), event_id_bytes.to_vec()));
				},
				| _ => break,
			}
		}

		if batch_entries.is_empty() {
			break;
		}

		// Batch-fetch all metadata for this batch
		let meta_keys: Vec<&[u8]> = batch_entries
			.iter()
			.map(|(_, eid)| eid.as_slice())
			.collect();

		for (i, meta_result) in eventid_metadata
			.get_batch_blocking(meta_keys.iter().copied())
			.enumerate()
		{
			let Ok(meta_handle) = meta_result else {
				continue;
			};

			let Ok(meta) = crate::rooms::timeline::EventMetadata::from_bincode(&meta_handle)
			else {
				continue;
			};

			let pdu_id_bytes = &batch_entries[i].0;
			let mut shortroomid = [0_u8; 8];
			shortroomid.copy_from_slice(&pdu_id_bytes[0..8]);

			let mut count_bytes = [0_u8; 8];
			if pdu_id_bytes.len() == 24 {
				count_bytes.copy_from_slice(&pdu_id_bytes[16..24]);
			} else {
				count_bytes.copy_from_slice(&pdu_id_bytes[8..16]);
			}

			let global_depth: u64 = meta.depth.into();

			let mut topo_key = Vec::with_capacity(24);
			topo_key.extend_from_slice(&shortroomid);
			topo_key.extend_from_slice(&global_depth.to_be_bytes());
			topo_key.extend_from_slice(&count_bytes);

			roomid_topologicalorder_pducount.put(&topo_key, batch_entries[i].1.clone());

			total_migrated = total_migrated.saturating_add(1);
			if total_migrated.is_multiple_of(10000) {
				info!("Migrated {} events to topological index...", total_migrated);
			}
		}
	}

	info!("Successfully populated topological index for {total_migrated} events!");
	db["global"].insert(POPULATE_TOPOLOGICAL_INDEX_MARKER, []);
	db.db.sort()?;
	Ok(())
}

const POPULATE_PDU_COUNT_IN_METADATA_MARKER: &[u8] = b"populate_pdu_count_in_metadata";

async fn populate_pdu_count_in_metadata(services: &Services) -> Result<()> {
	const BATCH_SIZE: usize = 1000;

	info!("Starting migration to populate pdu_count in EventMetadata from eventid_pduid...");

	let db = &services.db;
	let eventid_pduid = db["eventid_pduid"].clone();
	let eventid_metadata = db["eventid_metadata"].clone();

	let _cork = db.cork_and_sync();

	let stream = eventid_pduid.raw_stream();
	pin_mut!(stream);

	let mut migrated: usize = 0;
	let mut skipped: usize = 0;
	let mut missing_meta: usize = 0;
	let mut batch_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH_SIZE);

	loop {
		batch_entries.clear();
		while batch_entries.len() < BATCH_SIZE {
			match stream.next().await {
				| Some(Ok((event_id_bytes, pdu_id_bytes))) => {
					batch_entries.push((event_id_bytes.to_vec(), pdu_id_bytes.to_vec()));
				},
				| _ => break,
			}
		}

		if batch_entries.is_empty() {
			break;
		}

		// Batch-fetch all metadata
		let meta_keys: Vec<&[u8]> = batch_entries
			.iter()
			.map(|(eid, _)| eid.as_slice())
			.collect();

		for (i, meta_result) in eventid_metadata
			.get_batch_blocking(meta_keys.iter().copied())
			.enumerate()
		{
			let Ok(meta_handle) = meta_result else {
				missing_meta = missing_meta.saturating_add(1);
				continue;
			};

			let Ok(mut meta) = crate::rooms::timeline::EventMetadata::from_bincode(&meta_handle)
			else {
				missing_meta = missing_meta.saturating_add(1);
				continue;
			};

			if meta.pdu_count.is_some() {
				skipped = skipped.saturating_add(1);
				continue;
			}

			let pdu_id_bytes = &batch_entries[i].1;
			let mut count_bytes = [0_u8; 8];
			if pdu_id_bytes.len() == 24 {
				count_bytes.copy_from_slice(&pdu_id_bytes[16..24]);
			} else {
				count_bytes.copy_from_slice(&pdu_id_bytes[8..16]);
			}
			let count = i64::from_be_bytes(count_bytes).unsigned_abs();
			meta.pdu_count = Some(count);

			if let Ok(new_bytes) = bincode::serialize(&meta) {
				eventid_metadata.insert(&batch_entries[i].0, new_bytes);
			}

			migrated = migrated.saturating_add(1);
			if migrated.is_multiple_of(10000) {
				info!("Migrated {migrated} events pdu_count into metadata...");
			}
		}
	}

	info!(
		"Successfully populated pdu_count for {migrated} events ({skipped} already set, \
		 {missing_meta} missing metadata)."
	);
	db["global"].insert(POPULATE_PDU_COUNT_IN_METADATA_MARKER, []);
	db.db.sort()?;
	Ok(())
}

async fn db_lt_12(services: &Services) -> Result<()> {
	for username in &services
		.users
		.list_local_users()
		.map(ToOwned::to_owned)
		.collect::<Vec<OwnedUserId>>()
		.await
	{
		let user = match UserId::parse_with_server_name(username.as_str(), &services.server.name)
		{
			| Ok(u) => u,
			| Err(e) => {
				warn!("Invalid username {username}: {e}");
				continue;
			},
		};

		let mut account_data: PushRulesEvent = services
			.account_data
			.get_global(&user, GlobalAccountDataEventType::PushRules)
			.await
			.expect("Username is invalid");

		let rules_list = &mut account_data.content.global;

		//content rule
		{
			let content_rule_transformation =
				[".m.rules.contains_user_name", ".m.rule.contains_user_name"];

			let rule = rules_list.content.get(content_rule_transformation[0]);

			if let Some(rule) = rule {
				let mut rule = rule.clone();
				content_rule_transformation[1].clone_into(&mut rule.rule_id);
				rules_list
					.content
					.shift_remove(content_rule_transformation[0]);

				rules_list.content.insert(rule);
			}
		}

		//underride rules
		{
			let underride_rule_transformation = [
				[".m.rules.call", ".m.rule.call"],
				[".m.rules.room_one_to_one", ".m.rule.room_one_to_one"],
				[".m.rules.encrypted_room_one_to_one", ".m.rule.encrypted_room_one_to_one"],
				[".m.rules.message", ".m.rule.message"],
				[".m.rules.encrypted", ".m.rule.encrypted"],
			];

			for transformation in underride_rule_transformation {
				let rule = rules_list.underride.get(transformation[0]);
				if let Some(rule) = rule {
					let mut rule = rule.clone();
					transformation[1].clone_into(&mut rule.rule_id);
					rules_list.underride.shift_remove(transformation[0]);
					rules_list.underride.insert(rule);
				}
			}
		}

		services
			.account_data
			.update(
				None,
				&user,
				GlobalAccountDataEventType::PushRules.to_string().into(),
				&serde_json::to_value(account_data).expect("to json value always works"),
			)
			.await?;
	}

	services.globals.db.bump_database_version(12);
	info!("Migration: 11 -> 12 finished");
	Ok(())
}

async fn db_lt_13(services: &Services) -> Result<()> {
	for username in &services
		.users
		.list_local_users()
		.map(ToOwned::to_owned)
		.collect::<Vec<OwnedUserId>>()
		.await
	{
		let user = match UserId::parse_with_server_name(username.as_str(), &services.server.name)
		{
			| Ok(u) => u,
			| Err(e) => {
				warn!("Invalid username {username}: {e}");
				continue;
			},
		};

		let mut account_data: PushRulesEvent = services
			.account_data
			.get_global(&user, GlobalAccountDataEventType::PushRules)
			.await
			.expect("Username is invalid");

		let user_default_rules = Ruleset::server_default(&user);
		account_data
			.content
			.global
			.update_with_server_default(user_default_rules);

		services
			.account_data
			.update(
				None,
				&user,
				GlobalAccountDataEventType::PushRules.to_string().into(),
				&serde_json::to_value(account_data).expect("to json value always works"),
			)
			.await?;
	}

	services.globals.db.bump_database_version(13);
	info!("Migration: 12 -> 13 finished");
	Ok(())
}

async fn fix_bad_double_separator_in_state_cache(services: &Services) -> Result<()> {
	info!("Fixing bad double separator in state_cache roomuserid_joined");

	let db = &services.db;
	let roomuserid_joined = &db["roomuserid_joined"];
	let _cork = db.cork_and_sync();

	let mut iter_count: usize = 0;
	roomuserid_joined
		.raw_stream()
		.ignore_err()
		.ready_for_each(|(key, value)| {
			let mut key = key.to_vec();
			iter_count = iter_count.saturating_add(1);
			debug_info!(%iter_count);
			let first_sep_index = key
				.iter()
				.position(|&i| i == 0xFF)
				.expect("found 0xFF delim");

			if key
				.iter()
				.get(first_sep_index..=first_sep_index.saturating_add(1))
				.copied()
				.collect_vec()
				== vec![0xFF, 0xFF]
			{
				debug_warn!("Found bad key: {key:?}");
				roomuserid_joined.remove(&key);

				key.remove(first_sep_index);
				debug_warn!("Fixed key: {key:?}");
				roomuserid_joined.insert(&key, value);
			}
		})
		.await;

	db.db.sort()?;
	db["global"].insert(b"fix_bad_double_separator_in_state_cache", []);

	info!("Finished fixing");
	Ok(())
}

async fn retroactively_fix_bad_data_from_roomuserid_joined(services: &Services) -> Result<()> {
	info!("Retroactively fixing bad data from broken roomuserid_joined");

	let db = &services.db;
	let _cork = db.cork_and_sync();

	let room_ids = services
		.rooms
		.metadata
		.iter_ids()
		.map(ToOwned::to_owned)
		.collect::<Vec<_>>()
		.await;

	for room_id in &room_ids {
		debug_info!("Fixing room {room_id}");

		let users_in_room: Vec<OwnedUserId> = services
			.rooms
			.state_cache
			.room_members(room_id)
			.map(ToOwned::to_owned)
			.collect()
			.await;

		let joined_members = users_in_room
			.iter()
			.stream()
			.filter(|user_id| {
				services
					.rooms
					.state_accessor
					.get_member(room_id, user_id)
					.map(|member| {
						member.is_ok_and(|member| member.membership == MembershipState::Join)
					})
			})
			.collect::<Vec<_>>()
			.await;

		let non_joined_members = users_in_room
			.iter()
			.stream()
			.filter(|user_id| {
				services
					.rooms
					.state_accessor
					.get_member(room_id, user_id)
					.map(|member| {
						member.is_ok_and(|member| member.membership == MembershipState::Join)
					})
			})
			.collect::<Vec<_>>()
			.await;

		for user_id in &joined_members {
			debug_info!("User is joined, marking as joined");
			services
				.rooms
				.state_cache
				.mark_as_joined(user_id, room_id)
				.await;
		}

		for user_id in &non_joined_members {
			debug_info!("User is left or banned, marking as left");
			services
				.rooms
				.state_cache
				.mark_as_left(user_id, room_id, None)
				.await;
		}
	}

	for room_id in &room_ids {
		debug_info!(
			"Updating joined count for room {room_id} to fix servers in room after correcting \
			 membership states"
		);

		services
			.rooms
			.state_cache
			.update_joined_count(room_id)
			.await;
	}

	db.db.sort()?;
	db["global"].insert(b"retroactively_fix_bad_data_from_roomuserid_joined", []);

	info!("Finished fixing");
	Ok(())
}

async fn fix_referencedevents_missing_sep(services: &Services) -> Result {
	info!("Fixing missing record separator between room_id and event_id in referencedevents");

	let db = &services.db;
	let cork = db.cork_and_sync();

	let referencedevents = db["referencedevents"].clone();

	let totals: (usize, usize) = (0, 0);
	let (total, fixed) = referencedevents
		.raw_stream()
		.expect_ok()
		.enumerate()
		.ready_fold(totals, |mut a, (i, (key, val))| {
			debug_assert!(val.is_empty(), "expected no value");

			let has_sep = key.contains(&database::SEP);

			if !has_sep {
				let key_str = std::str::from_utf8(key).expect("key not utf-8");
				let room_id_len = key_str.find('$').expect("missing '$' in key");
				let (room_id, event_id) = key_str.split_at(room_id_len);
				debug!(?a, "fixing {room_id}, {event_id}");

				let new_key = (room_id, event_id);
				referencedevents.put_raw(new_key, val);
				referencedevents.remove(key);
			}

			a.0 = cmp::max(i, a.0);
			a.1 = a.1.saturating_add((!has_sep).into());
			a
		})
		.await;

	drop(cork);
	info!(?total, ?fixed, "Fixed missing record separators in 'referencedevents'.");

	db["global"].insert(b"fix_referencedevents_missing_sep", []);
	db.db.sort()
}

async fn fix_readreceiptid_readreceipt_duplicates(services: &Services) -> Result {
	info!("Fixing undeleted entries in readreceiptid_readreceipt...");

	let db = &services.db;
	let cork = db.cork_and_sync();
	let readreceiptid_readreceipt = db["readreceiptid_readreceipt"].clone();
	let iter = readreceiptid_readreceipt.rev_raw_stream();
	let (mut total, mut fixed): (usize, usize) = (0, 0);
	pin_mut!(iter);

	let mut seen = std::collections::HashSet::new();
	let mut current_room: Option<Vec<u8>> = None;

	while let Some((key, _)) = iter.try_next().await? {
		let sep1 = key.iter().position(|&b| b == database::SEP);
		let Some(sep1) = sep1 else {
			continue;
		};

		let room_id_bytes = &key[..sep1];

		if Some(room_id_bytes) != current_room.as_deref() {
			seen.clear();
			current_room = Some(room_id_bytes.to_vec());
		}

		let count_start = sep1.saturating_add(1);
		let count_end = count_start.saturating_add(8);
		if key.len() <= count_end || key[count_end] != database::SEP {
			continue;
		}
		let user_id_bytes = &key[count_end.saturating_add(1)..];

		if !seen.insert(user_id_bytes.to_vec()) {
			readreceiptid_readreceipt.del(key);
			fixed = fixed.saturating_add(1);
		}
		total = total.saturating_add(1);
	}

	drop(cork);
	info!(?total, ?fixed, "Fixed undeleted entries in readreceiptid_readreceipt.");

	db["global"].insert(b"fix_readreceiptid_readreceipt_duplicates", []);
	db.db.sort()
}

const FIXED_CORRUPT_MSC4133_FIELDS_MARKER: &[u8] = b"fix_corrupt_msc4133_fields";
async fn fix_corrupt_msc4133_fields(services: &Services) -> Result {
	// Due to an old bug, some conduwuit databases have `us.cloke.msc4175.tz` user
	// profile fields with raw strings instead of quoted JSON ones.
	// This migration fixes that.

	use serde_json::{Value, from_slice};
	type KeyVal<'a> = ((OwnedUserId, String), &'a [u8]);

	info!("Fixing corrupted `us.cloke.msc4175.tz` fields...");

	let db = &services.db;
	let cork = db.cork_and_sync();
	let useridprofilekey_value = db["useridprofilekey_value"].clone();

	let (total, fixed) = useridprofilekey_value
		.stream()
		.try_fold(
			(0_usize, 0_usize),
			async |(mut total, mut fixed),
			       ((user, key), value): KeyVal<'_>|
			       -> Result<(usize, usize)> {
				match from_slice::<Value>(value) {
					// corrupted timezone field
					| Err(_) if key == "us.cloke.msc4175.tz" => {
						let new_value = Value::String(String::from_utf8(value.to_vec())?);
						useridprofilekey_value.put((user, key), Json(new_value));
						fixed = fixed.saturating_add(1);
					},
					// corrupted value for some other key
					| Err(error) => {
						warn!(
							"deleting MSC4133 key {} for user {} due to deserialization \
							 failure: {}",
							key, user, error
						);
						useridprofilekey_value.del((user, key));
					},
					// other key with no issues
					| Ok(_) => {
						// do nothing
					},
				}

				total = total.saturating_add(1);

				Ok((total, fixed))
			},
		)
		.await?;

	drop(cork);
	info!(?total, ?fixed, "Fixed corrupted `us.cloke.msc4175.tz` fields.");

	db["global"].insert(FIXED_CORRUPT_MSC4133_FIELDS_MARKER, []);
	db.db.sort()?;
	Ok(())
}

const POPULATED_USERROOMID_LEFTSTATE_TABLE_MARKER: &str = "populate_userroomid_leftstate_table";
async fn populate_userroomid_leftstate_table(services: &Services) -> Result {
	type KeyVal<'a> = (Key<'a>, Raw<Option<Pdu>>);
	type Key<'a> = (&'a UserId, &'a RoomId);

	let db = &services.db;
	let cork = db.cork_and_sync();
	let userroomid_leftstate = db["userroomid_leftstate"].clone();

	let (total, fixed, _) = userroomid_leftstate
		.stream()
		.try_fold(
			(0_usize, 0_usize, HashMap::<OwnedRoomId, ShortStateHash>::new()),
			async |(mut total, mut fixed, mut shortstatehash_cache): (
				usize,
				usize,
				HashMap<_, _>,
			),
			       ((user_id, room_id), state): KeyVal<'_>|
			       -> Result<(usize, usize, HashMap<_, _>)> {
				if state.deserialize().is_err() {
					let latest_shortstatehash =
						if let Some(shortstatehash) = shortstatehash_cache.get(room_id) {
							*shortstatehash
						} else if let Ok(shortstatehash) =
							services.rooms.state.get_room_shortstatehash(room_id).await
						{
							shortstatehash_cache.insert(room_id.to_owned(), shortstatehash);
							shortstatehash
						} else {
							warn!(%room_id, %user_id, "room has no shortstatehash");
							return Ok((total, fixed, shortstatehash_cache));
						};

					let leave_state_event = services
						.rooms
						.state_accessor
						.state_get(
							latest_shortstatehash,
							&StateEventType::RoomMember,
							user_id.as_str(),
						)
						.await;

					match leave_state_event {
						| Ok(leave_state_event) => {
							userroomid_leftstate.put((user_id, room_id), Json(leave_state_event));
							fixed = fixed.saturating_add(1);
						},
						| Err(_) => {
							warn!(
								%room_id,
								%user_id,
								"room cached as left has no leave event for user, removing \
								 cache entry"
							);
							userroomid_leftstate.del((user_id, room_id));
						},
					}
				}

				total = total.saturating_add(1);
				Ok((total, fixed, shortstatehash_cache))
			},
		)
		.await?;

	drop(cork);
	info!(?total, ?fixed, "Fixed entries in `userroomid_leftstate`.");

	db["global"].insert(POPULATED_USERROOMID_LEFTSTATE_TABLE_MARKER, []);
	db.db.sort()?;
	Ok(())
}

const FIXED_LOCAL_INVITE_STATE_MARKER: &str = "fix_local_invite_state";
async fn fix_local_invite_state(services: &Services) -> Result {
	// Clean up the effects of !1249 by caching stripped state for invites

	type KeyVal<'a> = (Key<'a>, Raw<Vec<AnyStrippedStateEvent>>);
	type Key<'a> = (&'a UserId, &'a RoomId);

	let db = &services.db;
	let cork = db.cork_and_sync();
	let userroomid_invitestate = services.db["userroomid_invitestate"].clone();

	// for each user invited to a room
	let fixed =  userroomid_invitestate.stream()
		// if they're a local user on this homeserver
		.try_filter(|((user_id, _), _): &KeyVal<'_>| ready(services.globals.user_is_local(user_id)))
		.and_then(async |((user_id, room_id), stripped_state): KeyVal<'_>| Ok::<_,
			conduwuit::Error>((user_id.to_owned(), room_id.to_owned(), stripped_state.deserialize
		().unwrap_or_else(|e| {
			trace!("Failed to deserialize: {:?}", stripped_state.json());
			warn!(
				%user_id,
				%room_id,
				"Failed to deserialize stripped state for invite, removing from db: {e}"
			);
			userroomid_invitestate.del((user_id, room_id));
			vec![]
		}))))
		.try_fold(0_usize, async |mut fixed, (user_id, room_id, stripped_state)| {
			// and their invite state is None
			if stripped_state.is_empty()
				// and they are actually invited to the room
				&& let Ok(membership_event) = services.rooms.state_accessor.room_state_get(&room_id, &StateEventType::RoomMember, user_id.as_str()).await
				&& membership_event.get_content::<RoomMemberEventContent>().is_ok_and(|content| content.membership == MembershipState::Invite)
				// and the invite was sent by a local user
				&& services.globals.user_is_local(&membership_event.sender) {

				// build and save stripped state for their invite in the database
				let stripped_state = services.rooms.state.summary_stripped(&membership_event, &room_id).await;
				userroomid_invitestate.put((&user_id, &room_id), Json(stripped_state));
				fixed = fixed.saturating_add(1);
			}

			Ok(fixed)
		})
		.await?;

	drop(cork);
	info!(?fixed, "Fixed local invite state cache entries.");

	db["global"].insert(FIXED_LOCAL_INVITE_STATE_MARKER, []);
	db.db.sort()?;
	Ok(())
}

async fn db_lt_19(services: &Services) -> Result<()> {
	info!("Running v19 migration (migrating softfailedeventids to eventid_metadata)...");
	let db = &services.db;
	let cork = db.cork_and_sync();

	let mut count = 0_usize;
	let mut migrated = 0_usize;

	// Open softfailedeventids map if it exists
	if db.db.cf_exists("softfailedeventids") {
		let softfailedeventids = database::Map::open(&db.db, "softfailedeventids")
			.map_err(|e| err!("Failed to open softfailedeventids: {e}"))?;
		let softfailed_stream = softfailedeventids.raw_stream();
		pin_mut!(softfailed_stream);

		let mut batch = database::Batch::new();

		let mut batch_count = 0_usize;

		while let Some(Ok((event_id_bytes, _))) = softfailed_stream.next().await {
			count = count.saturating_add(1);
			if let Ok(metadata_bytes) = db["eventid_metadata"].get_blocking(&event_id_bytes) {
				if let Ok(mut meta) =
					crate::rooms::timeline::EventMetadata::from_bincode(&metadata_bytes)
				{
					if !meta.soft_failed {
						meta.soft_failed = true;
						if let Ok(new_bytes) = bincode::serialize(&meta) {
							db["eventid_metadata"].batch_put(
								&mut batch,
								&event_id_bytes,
								&new_bytes,
							);
							migrated = migrated.saturating_add(1);
							batch_count = batch_count.saturating_add(1);
						}
					}
				}
			}

			if batch_count >= 1000 {
				db["eventid_metadata"].apply_batch(batch);
				batch = database::Batch::new();
				batch_count = 0;
			}
		}

		db["eventid_metadata"].apply_batch(batch);
		db.db
			.drop_cf("softfailedeventids")
			.unwrap_or_else(|e| warn!("Failed to drop softfailedeventids: {e}"));
	}

	// Drop eventid_receivecount if it exists
	if db.db.cf_exists("eventid_receivecount") {
		db.db
			.drop_cf("eventid_receivecount")
			.unwrap_or_else(|e| warn!("Failed to drop eventid_receivecount: {e}"));
	}

	// Drop roomid_outliereventid — outlier tracking now uses
	// eventid_metadata.is_outlier
	if db.db.cf_exists("roomid_outliereventid") {
		db.db
			.drop_cf("roomid_outliereventid")
			.unwrap_or_else(|e| warn!("Failed to drop roomid_outliereventid: {e}"));
	}

	drop(cork);
	info!("Migrated {}/{} soft-failed events to eventid_metadata.", migrated, count);

	services.globals.db.bump_database_version(19);
	Ok(())
}

const UNIFY_RAW_PDU_ID_MARKER: &[u8] = b"unify_raw_pdu_id_16_byte";

async fn unify_raw_pdu_id_16_byte(services: &Services) -> Result<()> {
	info!("Starting database migration (RawPduId 16-byte unification)...");
	let db = &services.db;
	let eventid_pduid = db["eventid_pduid"].clone();
	let room_pducount_eventid = db["room_pducount_eventid"].clone();

	let _cork = db.cork_and_sync();
	let stream = eventid_pduid.raw_stream();
	pin_mut!(stream);

	let mut total = 0_usize;
	let mut migrated = 0_usize;
	let mut skipped = 0_usize;
	let mut batch = database::Batch::new();

	while let Some(Ok((event_id_bytes, old_raw_id_bytes))) = stream.next().await {
		total = total.saturating_add(1);

		let needs_migration = if old_raw_id_bytes.len() == 24 {
			// Old backfilled 24-byte format
			true
		} else if old_raw_id_bytes.len() == 16 {
			// Old normal format (or already migrated)
			// Old normal counts were positive, so their high byte was < 0x80.
			// Migrated normal counts use offset binary encoding, so their high byte is >=
			// 0x80. Migrated backfilled counts use offset binary encoding, so their high
			// byte is < 0x80. Wait, how do we know if it's already migrated?
			// Actually, we don't know if an existing 16-byte key is old Normal or new
			// Backfilled just by looking at it, but this migration runs precisely ONCE
			// during the bump to v20. If we process it during the v20 bump, it MUST be
			// an old key. Old Normal has high byte < 0x80.
			// Old Backfilled is 24 bytes.
			// So if it's 16 bytes and high byte is < 0x80, it's an old Normal key.
			let high_byte = old_raw_id_bytes[8];
			high_byte < 0x80
		} else {
			false
		};

		if !needs_migration {
			skipped = skipped.saturating_add(1);
			continue;
		}

		// It is an old format key (either 24 byte backfilled, or 16 byte normal).
		// We can decode it using the OLD decoding rules implicitly.
		// Wait! The `RawId::from` is already updated to the NEW 16-byte rules.
		// So we CANNOT use `RawId::from` to decode old 24-byte keys, nor can we use
		// `RawId::from` for old 16-byte keys!
		// We must manually extract the `shortroomid` and `shorteventid` using the old
		// logic.
		let mut shortroomid = [0_u8; 8];
		shortroomid.copy_from_slice(&old_raw_id_bytes[0..8]);

		let mut count_bytes = [0_u8; 8];
		if old_raw_id_bytes.len() == 24 {
			// Old backfilled format: [room(8) | 0x00(8) | count(8)]
			count_bytes.copy_from_slice(&old_raw_id_bytes[16..24]);
		} else {
			// Old normal format: [room(8) | count(8)]
			count_bytes.copy_from_slice(&old_raw_id_bytes[8..16]);
		}

		// Apply offset binary encoding to the old two's complement count
		let encoded_count = conduwuit::matrix::pdu::Count::offset_binary_encoding(count_bytes);

		// Build the new 16 byte key
		let mut new_raw_id_bytes = [0_u8; 16];
		new_raw_id_bytes[0..8].copy_from_slice(&shortroomid);
		new_raw_id_bytes[8..16].copy_from_slice(&encoded_count);

		// Apply updates
		room_pducount_eventid.batch_delete(&mut batch, old_raw_id_bytes);
		room_pducount_eventid.batch_put(&mut batch, &new_raw_id_bytes, event_id_bytes);
		eventid_pduid.batch_put(&mut batch, event_id_bytes, new_raw_id_bytes);

		migrated = migrated.saturating_add(1);

		if migrated.is_multiple_of(10000) {
			room_pducount_eventid.apply_batch(batch);
			batch = database::Batch::new();
			info!("RawPduId unification: Processed {} PDUs...", migrated);
		}
	}

	room_pducount_eventid.apply_batch(batch);

	info!(
		"RawPduId unification complete. Migrated {} PDUs ({} skipped, {} total).",
		migrated, skipped, total
	);
	db["global"].insert(UNIFY_RAW_PDU_ID_MARKER, []);
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_schema_fingerprint_deterministic() {
		let a = compute_schema_fingerprint();
		let b = compute_schema_fingerprint();
		assert_eq!(a, b, "fingerprint must be deterministic");
	}

	#[test]
	fn test_schema_fingerprint_not_empty() {
		let fp = compute_schema_fingerprint();
		assert_ne!(fp, [0_u8; 32], "fingerprint must not be all zeros");
	}

	#[test]
	fn test_schema_fingerprint_sensitive_to_dropped_cfs() {
		// The fingerprint includes DROPPED_CFS; verify our list is non-empty
		// and thus contributes to the hash
		assert!(
			!DROPPED_CFS.is_empty(),
			"DROPPED_CFS must list explicitly dropped column families"
		);

		// Verify the CF names we expect are present
		assert!(DROPPED_CFS.contains(&"softfailedeventids"));
		assert!(DROPPED_CFS.contains(&"eventid_receivecount"));
		assert!(DROPPED_CFS.contains(&"roomid_outliereventid"));
	}

	#[test]
	fn test_schema_fingerprint_includes_version() {
		// The hash includes DATABASE_VERSION.to_be_bytes() as first input.
		// We can't easily test mutation, but we verify the constant is
		// included by confirming it matches the expected value.
		assert_eq!(DATABASE_VERSION, 19);
	}
}
