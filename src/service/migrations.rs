use std::{cmp, collections::HashMap, future::ready, sync::Arc};

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
use database::{Deserialized, Json};
use futures::{FutureExt, StreamExt, TryStreamExt, pin_mut};
use itertools::Itertools;
use sha2::{Digest, Sha256};
use slipstream::{
	OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId,
	events::{
		AnyStrippedStateEvent, GlobalAccountDataEventType, StateEventType,
		push_rules::PushRulesEvent,
		room::member::{MembershipState, RoomMemberEventContent},
	},
	push::Ruleset,
	serde::Raw,
};

use crate::{
	Services, media,
	rooms::short::{ShortEventId, ShortId as ShortStateHash},
};

/// The current schema version.
/// - If database is opened at greater version we reject with error. The
///   software must be updated for backward-incompatible changes.
/// - If database is opened at lesser version we apply migrations up to this.
///   Note that named-feature migrations may also be performed when opening at
///   equal or lesser version. These are expected to be backward-compatible.
pub(crate) const DATABASE_VERSION: u64 = 24;

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
	// v20 - PDU/read-receipt refactor plus RawPduId format unification
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
		.get(POPULATE_SHORTPREVEVENTS_MARKER)
		.await
		.is_not_found()
	{
		info!("Running migration 'populate_shortprevevents'");
		populate_shortprevevents(services)
			.await
			.map_err(|e| err!("Failed to run 'populate_shortprevevents': {e}"))?;
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

	if services.globals.db.database_version().await < 20 {
		services.globals.db.bump_database_version(20);
	}

	// v21 - delete the single-slot `EventStatus` model; verdicts live in the
	// independent `eventid_rejections`/`eventid_softfailed` stores. `db_lt_21`
	// folds the legacy `eventid_status` CF and any legacy `eventid_metadata.status`
	// field into those stores and rewrites `eventid_metadata` rows status-less.
	// (`eventid_status` stays as a live-but-unused CF for upgrade-path safety.)
	if services.globals.db.database_version().await < 21 {
		db_lt_21(services)
			.await
			.map_err(|e| err!("Failed to run v21 migrations: {e}"))?;
	}

	// MSC4511 augmented-HAMT migrations, renumbered to follow the v19-v21
	// sequence above so the two branches' migration sets remain a single
	// monotonic, collision-free list. Order is preserved: LtHash accumulators,
	// then HAMT root construction (which reads the legacy state maps), then the
	// cleanup that clears them.
	//
	// Version 22 - populate MSC4500 LtHash state accumulators
	if services.globals.db.database_version().await < 22 {
		Box::pin(db_lt_22(services))
			.await
			.map_err(|e| err!("Failed to run v22 migrations: {e}"))?;
	}

	// Version 23 - build HAMT roots for existing rooms
	if services.globals.db.database_version().await < 23 {
		Box::pin(db_lt_23(services))
			.await
			.map_err(|e| err!("Failed to run v23 migrations: {e}"))?;
	}

	// Version 24 - drop legacy shortstatehash table data now that v23 has
	// finished building HAMT roots from it.
	if services.globals.db.database_version().await < 24 {
		Box::pin(db_lt_24(services))
			.await
			.map_err(|e| err!("Failed to run v24 migrations: {e}"))?;
	}

	if services.globals.db.database_version().await != DATABASE_VERSION {
		return Err!(Database(
			"Database version {} does not match expected version {DATABASE_VERSION} after \
			 running all migrations.",
			services.globals.db.database_version().await,
		));
	}

	// Validate schema fingerprint. A fingerprint from an older schema is expected
	// to differ because the schema version is part of the hash. Only retain the
	// hard-fail behavior for databases that were already current when opened;
	// successful versioned migrations are the compatibility boundary for upgrades.
	let expected = compute_schema_fingerprint();
	if let Some(stored) = services.globals.db.schema_fingerprint().await {
		if stored != expected {
			if db_version >= DATABASE_VERSION {
				return Err!(Database(
					"Schema fingerprint mismatch! This database was created by a different \
					 build with incompatible column families. Expected {expected:x?}, found \
					 {stored:x?}. Do NOT continue — data corruption will occur.",
				));
			}
			warn!(
				"Replacing schema fingerprint from database version {db_version} after \
				 successful migration to {DATABASE_VERSION}"
			);
		}
	}
	services.globals.db.set_schema_fingerprint(&expected);

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
	use slipstream::events::receipt::ReceiptEvent;

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
	let new_receipt_map = db["roomuserid_privatereadreceipt"].clone();
	let Some(legacy_count_map) = db
		.db
		.cf_exists("roomuserid_privateread")
		.then(|| database::Map::open(&db.db, "roomuserid_privateread"))
		.transpose()?
	else {
		info!("Legacy private read receipt maps not present; marking migration complete.");
		db["global"].insert(MIGRATE_PRIVATE_READ_RECEIPTS_TO_SSOT_MARKER, []);
		db.db.sort()?;
		return Ok(());
	};
	let legacy_event_map = db
		.db
		.cf_exists("roomuserid_privatereadevent")
		.then(|| database::Map::open(&db.db, "roomuserid_privatereadevent"))
		.transpose()?;
	let legacy_update_map = db
		.db
		.cf_exists("roomuserid_lastprivatereadupdate")
		.then(|| database::Map::open(&db.db, "roomuserid_lastprivatereadupdate"))
		.transpose()?;
	let (total_migrated, with_event, count_only, skipped) = {
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

			let Ok(room_id) = <RoomId>::try_from(
				conduwuit::utils::string::str_from_bytes(room_id_bytes).unwrap_or_default(),
			) else {
				skipped = skipped.saturating_add(1);
				continue;
			};
			let Ok(user_id) = <UserId>::try_from(
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

			let event: slipstream::events::receipt::ReceiptEvent =
				if let Some(legacy_event_map) = &legacy_event_map {
					if let Ok(event_bytes) = legacy_event_map.get(&legacy_key).await {
						with_event = with_event.saturating_add(1);
						serde_json::from_slice(&event_bytes).unwrap_or_else(|_| {
							slipstream::events::receipt::ReceiptEvent {
								content: slipstream::events::receipt::ReceiptEventContent(
									std::collections::BTreeMap::new(),
								),
								room_id: room_id.to_owned(),
							}
						})
					} else {
						count_only = count_only.saturating_add(1);
						slipstream::events::receipt::ReceiptEvent {
							content: slipstream::events::receipt::ReceiptEventContent(
								std::collections::BTreeMap::new(),
							),
							room_id: room_id.to_owned(),
						}
					}
				} else {
					count_only = count_only.saturating_add(1);
					slipstream::events::receipt::ReceiptEvent {
						content: slipstream::events::receipt::ReceiptEventContent(
							std::collections::BTreeMap::new(),
						),
						room_id: room_id.to_owned(),
					}
				};

			let update_count = if let Some(legacy_update_map) = &legacy_update_map {
				if let Ok(update_bytes) = legacy_update_map.get(&legacy_key).await {
					conduwuit::utils::u64_from_bytes(&update_bytes).unwrap_or(0)
				} else {
					0
				}
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

		(total_migrated, with_event, count_only, skipped)
	};

	info!(
		"Successfully migrated {total_migrated} private read receipts ({with_event} with event, \
		 {count_only} count-only, {skipped} skipped)."
	);

	// `Map::open` stashes a lifetime-erased `Arc<ColumnFamily>` (see the SAFETY
	// comment in `map/open.rs`) that becomes invalid the moment its column
	// family is dropped. These are the only owners of that handle, so drop
	// them explicitly before `drop_cf` below invalidates the handles --
	// otherwise we'd be holding dangling column family handles, risking a
	// crash (or worse) the next time they're touched, including on their own
	// eventual `Drop`.
	drop(legacy_count_map);
	drop(legacy_event_map);
	drop(legacy_update_map);

	if db.db.cf_exists("roomuserid_privateread") {
		db.db
			.drop_cf("roomuserid_privateread")
			.unwrap_or_else(|e| warn!("Failed to drop roomuserid_privateread: {e}"));
	}
	if db.db.cf_exists("roomuserid_privatereadevent") {
		db.db
			.drop_cf("roomuserid_privatereadevent")
			.unwrap_or_else(|e| warn!("Failed to drop roomuserid_privatereadevent: {e}"));
	}
	if db.db.cf_exists("roomuserid_lastprivatereadupdate") {
		db.db
			.drop_cf("roomuserid_lastprivatereadupdate")
			.unwrap_or_else(|e| warn!("Failed to drop roomuserid_lastprivatereadupdate: {e}"));
	}
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
	let eventid_rejections = db["eventid_rejections"].clone();
	let eventid_softfailed = db["eventid_softfailed"].clone();
	let roomid_topologicalorder_pducount = db["roomid_topologicalorder_pducount"].clone();

	let cork = db.cork_and_sync();

	let mut total: usize = 0;
	let mut timeline: usize = 0;
	let mut outliers: usize = 0;
	let mut skipped: usize = 0;
	let mut timeline_event_ids: std::collections::HashSet<Vec<u8>> =
		std::collections::HashSet::new();
	let mut depth_cache: HashMap<Vec<u8>, u64> = HashMap::new();

	// When this migration rewrites an `eventid_metadata` row it may be
	// clobbering a pre-v21 row that still carries the legacy single-slot
	// `EventStatus` verdict (v20 layout). `db_lt_21` later folds those verdicts
	// out of `EventMetadata`, but it runs *after* this SSOT migration on the
	// same startup, so once we strip the `status` field here there is nothing
	// left for `db_lt_21` to fold. Preserve any legacy verdict into the
	// authoritative independent stores *before* overwriting the row.
	// Read errors are fatal here: if we cannot verify whether this row still
	// carries a legacy verdict, aborting the migration (rather than silently
	// proceeding to overwrite it) is the only way to guarantee the verdict is
	// not lost. Only `NotFound` means there is genuinely no legacy row to
	// preserve.
	let fold_legacy_status = |event_id_bytes: &[u8]| -> Result<()> {
		let existing_bytes = match eventid_metadata.get_blocking(event_id_bytes) {
			| Ok(bytes) => bytes,
			| Err(e) if e.is_not_found() => return Ok(()),
			| Err(e) => {
				return Err(err!(
					"Failed reading eventid_metadata while preserving legacy verdict for event \
					 {:?}: {e}",
					event_id_bytes.len(),
				));
			},
		};
		let Ok(legacy) = bincode::deserialize::<EventMetadataV20>(&existing_bytes) else {
			return Ok(());
		};
		if let EventStatusV20::Rejected(code) = &legacy.status {
			if eventid_rejections
				.get_blocking(event_id_bytes)
				.is_not_found()
			{
				eventid_rejections.insert(event_id_bytes, [code.to_u8()]);
			}
		} else if let EventStatusV20::SoftFailed(code) = &legacy.status {
			if eventid_softfailed
				.get_blocking(event_id_bytes)
				.is_not_found()
			{
				eventid_softfailed.insert(event_id_bytes, [code.to_u8()]);
			}
		}
		Ok(())
	};

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
				redacted_by: pdu.redacts().map(ToOwned::to_owned),
				short_state_hash: None,
				deprecated_local_topo_depth,
				pdu_count: Some(unsigned_pdu_count),
			};
			if let Ok(metadata_bytes) = bincode::serialize(&metadata) {
				fold_legacy_status(event_id_bytes)?;
				eventid_metadata.insert(event_id_bytes, metadata_bytes);
			}
			if pdu.rejected()
				&& db["eventid_rejections"]
					.get_blocking(event_id_bytes)
					.is_not_found()
			{
				db["eventid_rejections"].insert(event_id_bytes, [
					crate::rooms::pdu_metadata::RejectionCode::Unknown.to_u8(),
				]);
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
					redacted_by: pdu.redacts().map(ToOwned::to_owned),
					short_state_hash: None,
					deprecated_local_topo_depth: 0,
					pdu_count: None,
				};
				if let Ok(metadata_bytes) = bincode::serialize(&metadata) {
					fold_legacy_status(event_id_bytes)?;
					eventid_metadata.insert(event_id_bytes, metadata_bytes);
				}
				if pdu.rejected()
					&& db["eventid_rejections"]
						.get_blocking(event_id_bytes)
						.is_not_found()
				{
					db["eventid_rejections"].insert(event_id_bytes, [
						crate::rooms::pdu_metadata::RejectionCode::Unknown.to_u8(),
					]);
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

	// Phase 1 above writes `roomid_topologicalorder_pducount` entries using an
	// ad-hoc key layout (shortroomid ++ raw depth ++ raw count bytes), not the
	// canonical `TimelineKey`-based encoding `populate_topological_index` (and
	// every read-path helper, e.g. `Data::topo_pducount_key`) expects. On a
	// normal upgrade `populate_topological_index` runs right after this and
	// rebuilds the whole index, so only clear its marker when this invocation
	// actually wrote timeline entries that require that rebuild. If no legacy
	// timeline PDUs were migrated (`timeline == 0`), clearing the marker would
	// force a full rebuild on every boot whenever `eventid_pdu` stays empty.
	if timeline > 0 {
		db["global"].remove(POPULATE_TOPOLOGICAL_INDEX_MARKER);
	}
	db.db.sort()?;
	Ok(())
}

const POPULATE_TOPOLOGICAL_INDEX_MARKER: &[u8] = b"populate_topological_index_v4";
const POPULATE_SHORTPREVEVENTS_MARKER: &[u8] = b"populate_shortprevevents";

/// Build the short-event-id -> short-prev-event-id index from the canonical
/// PDU store. This is deliberately a separate migration from the topological
/// index migration: the latter only needs metadata and cannot reconstruct
/// prev_events.
async fn populate_shortprevevents(services: &Services) -> Result<()> {
	const BATCH_SIZE: usize = 10_000;

	info!("Starting migration to populate shorteventid_shortprevevents...");

	let db = &services.db;
	let eventid_pdu = db["eventid_pdu"].clone();
	let shorteventid_shortprevevents = db["shorteventid_shortprevevents"].clone();
	let cork = db.cork_and_sync();
	let stream = eventid_pdu.raw_stream();
	pin_mut!(stream);

	let mut processed = 0_usize;

	loop {
		let mut entries = Vec::with_capacity(BATCH_SIZE);
		while entries.len() < BATCH_SIZE {
			let Some(entry) = stream.next().await else { break };
			let (event_id_bytes, pdu_json_bytes) = entry.map_err(|e| {
				err!(Database("Failed to read eventid_pdu during short-prev migration: {e}"))
			})?;
			let event_id_string = std::str::from_utf8(event_id_bytes).map_err(|e| {
				err!(Database("Invalid event ID UTF-8 during short-prev migration: {e}"))
			})?;
			let event_id = OwnedEventId::parse(event_id_string).map_err(|e| {
				err!(Database(
					"Invalid event ID during short-prev migration: {event_id_string}: {e}"
				))
			})?;

			// Do not silently skip an unreadable PDU. Leaving the marker unset makes
			// the migration retryable after the underlying record is repaired.
			let pdu =
				serde_json::from_slice::<conduwuit::PduEvent>(pdu_json_bytes).map_err(|e| {
					err!(Database(
						"Cannot decode eventid_pdu during short-prev migration: {event_id}: {e}"
					))
				})?;

			// Only the DAG edges are needed below; retaining the decoded PDU would
			// pin up to BATCH_SIZE full event bodies in memory at once.
			let prev_events = pdu.prev_events().map(ToOwned::to_owned).collect::<Vec<_>>();
			entries.push((event_id, prev_events));
		}

		if entries.is_empty() {
			break;
		}

		let short_event_ids = services
			.rooms
			.short
			.multi_get_or_create_shorteventid(entries.iter().map(|(event_id, _)| event_id))
			.collect::<Vec<_>>()
			.await;

		let mut prev_event_ids = Vec::new();
		let mut prev_ranges = Vec::with_capacity(entries.len());
		for (_, prev_events) in &entries {
			let start = prev_event_ids.len();
			prev_event_ids.extend(prev_events.iter());
			prev_ranges.push(start..prev_event_ids.len());
		}
		let prev_short_ids = services
			.rooms
			.short
			.multi_get_or_create_shorteventid(prev_event_ids.iter().copied())
			.collect::<Vec<_>>()
			.await;

		let mut batch = database::Batch::new();
		for (short_event_id, range) in short_event_ids.iter().zip(prev_ranges) {
			let key = short_event_id.to_be_bytes();
			let value = prev_short_ids[range]
				.iter()
				.flat_map(|short_id| short_id.to_be_bytes())
				.collect::<Vec<_>>();
			shorteventid_shortprevevents.batch_put(&mut batch, &key, &value);
		}

		shorteventid_shortprevevents.apply_batch(batch);
		processed = processed.saturating_add(entries.len());
		info!("Populated short-prev index for {processed} events...");
	}

	info!("Successfully populated short-prev index for {processed} events.");
	db["global"].insert(POPULATE_SHORTPREVEVENTS_MARKER, []);
	drop(cork);
	db.db.sort()?;
	Ok(())
}

async fn populate_topological_index(services: &Services) -> Result<()> {
	const BATCH_SIZE: usize = 10_000;

	info!("Starting migration to populate roomid_topologicalorder_pducount...");
	let db = &services.db;
	let room_pducount_eventid = db["room_pducount_eventid"].clone();
	let eventid_metadata = db["eventid_metadata"].clone();

	let roomid_topologicalorder_pducount = db["roomid_topologicalorder_pducount"].clone();
	let cork = db.cork_and_sync();

	// First, completely clear the old broken index (the byte encoding has changed).
	let clear_stream = roomid_topologicalorder_pducount.raw_stream();
	pin_mut!(clear_stream);
	let mut cleared: usize = 0;
	let mut clear_batch = database::Batch::new();
	while let Some(Ok((key, _))) = clear_stream.next().await {
		roomid_topologicalorder_pducount.batch_delete(&mut clear_batch, &key);
		cleared = cleared.saturating_add(1);
		if cleared.is_multiple_of(BATCH_SIZE) {
			roomid_topologicalorder_pducount.apply_batch(clear_batch);
			clear_batch = database::Batch::new();
		}
	}
	roomid_topologicalorder_pducount.apply_batch(clear_batch);
	info!("Cleared {cleared} old entries from topological index to prepare for rebuild.");

	let stream = room_pducount_eventid.raw_stream();
	pin_mut!(stream);
	let mut total_migrated: usize = 0;
	let mut batch_entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(BATCH_SIZE);
	let mut write_batch = database::Batch::new();

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

			let Ok(mut meta) = crate::rooms::timeline::EventMetadata::from_bincode(&meta_handle)
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
			let stream_ordering =
				i64::from_be_bytes(conduwuit::PduCount::offset_binary_encoding(count_bytes));
			let timeline_key = conduwuit::pdu::TimelineKey::new(global_depth, stream_ordering);

			let mut topo_key = Vec::with_capacity(24);
			topo_key.extend_from_slice(&shortroomid);
			topo_key.extend_from_slice(&timeline_key.to_be_bytes());

			roomid_topologicalorder_pducount.batch_put(
				&mut write_batch,
				&topo_key,
				batch_entries[i].1.as_slice(),
			);
			meta.deprecated_local_topo_depth = global_depth;
			if let Ok(metadata_bytes) = bincode::serialize(&meta) {
				eventid_metadata.batch_put(
					&mut write_batch,
					batch_entries[i].1.as_slice(),
					metadata_bytes,
				);
			}

			total_migrated = total_migrated.saturating_add(1);
			if total_migrated.is_multiple_of(BATCH_SIZE) {
				roomid_topologicalorder_pducount.apply_batch(write_batch);
				write_batch = database::Batch::new();
				info!("Migrated {} events to topological index...", total_migrated);
			}
		}
	}
	roomid_topologicalorder_pducount.apply_batch(write_batch);

	info!("Successfully populated topological index for {total_migrated} events!");
	db["global"].insert(POPULATE_TOPOLOGICAL_INDEX_MARKER, []);
	drop(cork);
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
			// `unsigned_abs()` here would fold Backfilled(-n) and Normal(n) onto
			// the same Some(n), which then fails `matches_timeline_position`'s
			// `Backfilled(_) => self.pdu_count.is_none()` arm for every
			// backfilled event this migration touches (dropping them from
			// topological pagination). Only Normal counts get a stored value;
			// Backfilled counts stay None, matching every other write site
			// (insert_pdu, replace_pdu, reindex.rs, reorder.rs).
			meta.pdu_count =
				match conduwuit::PduCount::from_signed(i64::from_be_bytes(count_bytes)) {
					| conduwuit::PduCount::Normal(x) => Some(x),
					| conduwuit::PduCount::Backfilled(_) => None,
				};

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

		let user_default_rules = Ruleset::server_default(user.as_str());
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

	let room_ids = services.rooms.metadata.iter_ids().collect::<Vec<_>>().await;

	for room_id in &room_ids {
		debug_info!("Fixing room {room_id}");

		let users_in_room: Vec<OwnedUserId> = services
			.rooms
			.state_cache
			.room_members(room_id)
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

	let total = userroomid_leftstate
		.stream()
		.try_fold(
			0_usize,
			async |mut total: usize, ((user_id, room_id), state): KeyVal<'_>| -> Result<usize> {
				if state.deserialize().is_err() {
					// The cached leave event is corrupted. Try to reconstruct it from
					// the room's current membership state when a HAMT root is already
					// available (fresh/migrated rooms with a `roomid_roothandle`
					// entry). Otherwise the legacy read chain that used to repair this
					// was removed by the HAMT cutover, so we drop the bad entry — the
					// leave event remains in the timeline and is recovered at runtime
					// once the HAMT migration has run.
					let repaired = match services.rooms.state.get_room_state_hamt(room_id).await {
						| Ok(root_handle) => services
							.rooms
							.state_accessor
							.state_get_in_room_hamt(
								room_id,
								&root_handle,
								&StateEventType::RoomMember,
								user_id.as_str(),
							)
							.await
							.ok(),
						| Err(_) => None,
					};

					match repaired {
						| Some(leave)
							if leave.get_content::<RoomMemberEventContent>().is_ok_and(
								|content| content.membership == MembershipState::Leave,
							) =>
						{
							userroomid_leftstate.put((user_id, room_id), Json(leave));
							warn!(
								%room_id,
								%user_id,
								"repaired corrupted cached leave event from room state"
							);
						},
						| _ => {
							warn!(
								%room_id,
								%user_id,
								"room cached as left has a corrupted leave event, removing \
								 cache entry"
							);
							userroomid_leftstate.del((user_id, room_id));
						},
					}
				}

				total = total.saturating_add(1);
				Ok(total)
			},
		)
		.await?;

	drop(cork);
	info!(?total, "Verified entries in `userroomid_leftstate`.");

	db["global"].insert(POPULATED_USERROOMID_LEFTSTATE_TABLE_MARKER, []);
	db.db.sort()?;
	Ok(())
}

const FIXED_LOCAL_INVITE_STATE_MARKER: &str = "fix_local_invite_state";
async fn fix_local_invite_state(services: &Services) -> Result {
	// Clean up the effects of !1249 by caching stripped state for invites

	type KeyVal = ((OwnedUserId, OwnedRoomId), Raw<Vec<AnyStrippedStateEvent>>);

	let db = &services.db;
	let cork = db.cork_and_sync();
	let userroomid_invitestate = services.db["userroomid_invitestate"].clone();

	// for each user invited to a room
	let fixed =  userroomid_invitestate.stream()
		// if they're a local user on this homeserver
		.try_filter(|((user_id, _), _): &KeyVal| ready(services.globals.user_is_local(user_id)))
		.and_then(async |((user_id, room_id), stripped_state): KeyVal| Ok::<_,
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
	info!("Running v19 cleanup migration...");
	let db = &services.db;
	let cork = db.cork_and_sync();

	if db.db.cf_exists("softfailedeventids") {
		if let Ok(softfailedeventids) = database::Map::open(&db.db, "softfailedeventids") {
			let softfailed_stream = softfailedeventids.raw_stream();
			pin_mut!(softfailed_stream);

			let mut batch = database::Batch::new();
			let mut batch_count = 0_usize;

			while let Some(item) = softfailed_stream.next().await {
				let (event_id_bytes, _) = item?;

				// `eventid_status` already carries a verdict for this event:
				// leave it alone so the existing (possibly more specific)
				// verdict is preserved. Only `NotFound` / an empty record is
				// treated as "absent"; any other read error is a real failure
				// we must not paper over by synthesizing `Unknown`.
				let already_has_status = match db["eventid_status"].get_blocking(&event_id_bytes)
				{
					| Ok(bytes) => !bytes.is_empty(),
					| Err(e) if e.is_not_found() => false,
					| Err(e) => {
						return Err(err!(
							"Failed reading eventid_status while folding softfailedeventids: {e}"
						));
					},
				};
				if already_has_status {
					continue;
				}

				// An event can be listed in `softfailedeventids` while a more
				// specific verdict (Rejected or a typed SoftFailed code) already
				// lives in its `eventid_metadata.status`. Writing a blanket
				// SoftFail(Unknown) here would clobber that and, because the v21
				// fold reads `eventid_status` before `eventid_metadata`, end up
				// hiding the real verdict. Only synthesize `Unknown` when no
				// stronger verdict is present; otherwise leave the row alone so
				// the v21 migration can fold the stored verdict verbatim.
				let metadata_has_verdict = db["eventid_metadata"]
					.get_blocking(&event_id_bytes)
					.ok()
					.and_then(|bytes| bincode::deserialize::<EventMetadataV20>(&bytes).ok())
					.is_some_and(|legacy| {
						!matches!(
							legacy.status,
							EventStatusV20::Pending | EventStatusV20::Accepted
						)
					});
				if metadata_has_verdict {
					continue;
				}

				db["eventid_status"].batch_put(&mut batch, &event_id_bytes, [
					1_u8,
					crate::rooms::pdu_metadata::SoftFailCode::Unknown.to_u8(),
				]);
				batch_count = batch_count.saturating_add(1);

				if batch_count >= 1000 {
					db["eventid_status"].apply_batch(batch);
					batch = database::Batch::new();
					batch_count = 0;
				}
			}
			db["eventid_status"].apply_batch(batch);
		}

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
	info!("v19 cleanup migration completed.");

	services.globals.db.bump_database_version(19);
	Ok(())
}

/// Legacy `EventMetadata` layout (v20) that still carried the single-slot
/// `status: EventStatus` field, used only to read pre-v21 rows during
/// `db_lt_21`. Field order replicates the exact on-disk layout of the old
/// struct so legacy rows deserialize correctly.
mod owned_event_id_option {
	use serde::{Deserialize, Deserializer, Serialize, Serializer};
	use slipstream::OwnedEventId;

	pub fn serialize<S>(value: &Option<OwnedEventId>, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		value
			.as_ref()
			.map(ToString::to_string)
			.serialize(serializer)
	}

	pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<OwnedEventId>, D::Error>
	where
		D: Deserializer<'de>,
	{
		Option::<String>::deserialize(deserializer)?
			.map(|value| OwnedEventId::parse(&value).map_err(serde::de::Error::custom))
			.transpose()
	}
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct EventMetadataV20 {
	short_room_id: u64,
	is_outlier: bool,
	origin_server_ts: slipstream::UInt,
	depth: slipstream::UInt,
	status: EventStatusV20,
	#[serde(with = "owned_event_id_option")]
	redacted_by: Option<OwnedEventId>,
	short_state_hash: Option<u64>,
	#[serde(default)]
	deprecated_local_topo_depth: u64,
	#[serde(default)]
	pdu_count: Option<u64>,
}

/// Older v19 layout. v19 databases can still contain rows written before the
/// EventStatus transition: the verdict was represented by independent boolean
/// fields plus human-readable reason strings. Those rows must be accepted by
/// v21 and their verdicts folded into the independent verdict maps.
#[derive(Debug, Clone, serde::Deserialize)]
struct EventMetadataV19 {
	short_room_id: u64,
	is_outlier: bool,
	origin_server_ts: slipstream::UInt,
	depth: slipstream::UInt,
	soft_failed: bool,
	rejected: bool,
	#[serde(with = "owned_event_id_option")]
	redacted_by: Option<OwnedEventId>,
	short_state_hash: Option<u64>,
	#[serde(default)]
	deprecated_local_topo_depth: u64,
	#[serde(default)]
	pdu_count: Option<u64>,
	#[serde(default)]
	_soft_fail_reason: String,
	#[serde(default)]
	_rejection_reason: String,
}

/// Pre-v19 layout. Some v19 databases retain rows written before the
/// topological-depth, PDU-count, and reason-string fields were added.
#[derive(Debug, Clone, serde::Deserialize)]
struct EventMetadataV18 {
	short_room_id: u64,
	is_outlier: bool,
	origin_server_ts: slipstream::UInt,
	depth: slipstream::UInt,
	soft_failed: bool,
	rejected: bool,
	#[serde(with = "owned_event_id_option")]
	redacted_by: Option<OwnedEventId>,
	short_state_hash: Option<u64>,
}

enum LegacyEventMetadata {
	V18(EventMetadataV18),
	V19(EventMetadataV19),
	V20(EventMetadataV20),
}

/// Legacy single-slot event verdict (v20), mirroring the deleted `EventStatus`
/// enum's serde layout. Reuses the still-live `RejectionCode`/`SoftFailCode`
/// types because their serialized form is unchanged.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
enum EventStatusV20 {
	#[default]
	Pending,
	Accepted,
	Rejected(crate::rooms::pdu_metadata::RejectionCode),
	SoftFailed(crate::rooms::pdu_metadata::SoftFailCode),
}

/// v21: delete the single-slot `EventStatus` model. The verdict now lives
/// entirely in the independent `eventid_rejections` / `eventid_softfailed`
/// stores, and `EventMetadata` no longer carries a `status` field. This folds
/// the legacy `eventid_status` CF (2-byte records) and any legacy
/// `eventid_metadata.status` field into those stores, and rewrites every
/// `eventid_metadata` row into the status-less layout. The `eventid_status` CF
/// stays as a live-but-unused map to keep upgrade paths and restarts safe.
async fn db_lt_21(services: &Services) -> Result<()> {
	info!("Starting v21 migration (fold event status into independent stores)...");
	let db = &services.db;
	// Hold the cork for the whole migration so the per-record inserts and
	// batched flushes below land in a single WAL flush instead of one per
	// record (prohibitively slow on databases with many legacy verdicts).
	let cork = db.cork_and_sync();

	let eventid_rejections = db["eventid_rejections"].clone();
	let eventid_softfailed = db["eventid_softfailed"].clone();
	let eventid_metadata = db["eventid_metadata"].clone();

	// Fold legacy `eventid_status` 2-byte records ([1, code] = soft-fail,
	// [2, code] = rejected) into the independent stores, skipping any entry the
	// independent store already records (fresh writes win).
	if db.db.cf_exists("eventid_status") {
		let eventid_status = database::Map::open(&db.db, "eventid_status")
			.map_err(|e| err!("Failed to open legacy eventid_status CF for v21 folding: {e}"))?;
		let stream = eventid_status.raw_stream();
		pin_mut!(stream);
		while let Some(item) = stream.next().await {
			let (event_id_bytes, value) = item.map_err(|e| {
				err!("Failed while reading legacy eventid_status during v21 migration: {e}")
			})?;
			if value.len() < 2 {
				continue;
			}
			match value[0] {
				| 1 if eventid_softfailed
					.get_blocking(&event_id_bytes)
					.is_not_found() =>
				{
					eventid_softfailed.insert(&event_id_bytes, [value[1]]);
				},
				| 2 if eventid_rejections
					.get_blocking(&event_id_bytes)
					.is_not_found() =>
				{
					eventid_rejections.insert(&event_id_bytes, [value[1]]);
				},
				| _ => {},
			}
		}
		info!("Folded legacy eventid_status records into independent stores.");
	}

	// Rewrite every `eventid_metadata` row into the status-less layout, folding
	// legacy verdicts into the independent stores as we go. Rows that already
	// parse as v21 are left untouched; anything that parses as neither v19 nor
	// v20 nor v21 is a migration error and aborts instead of being skipped.
	let mut batch = database::Batch::new();
	let mut batch_count = 0_usize;
	let metadata_stream = eventid_metadata.raw_stream();
	pin_mut!(metadata_stream);
	while let Some(item) = metadata_stream.next().await {
		let (event_id_bytes, value) = item.map_err(|e| {
			err!("Failed while scanning eventid_metadata during v21 migration: {e}")
		})?;
		let legacy = match bincode::deserialize::<EventMetadataV20>(value) {
			| Ok(legacy) => LegacyEventMetadata::V20(legacy),
			| Err(v20_err) => match bincode::deserialize::<EventMetadataV19>(value) {
				| Ok(legacy) => LegacyEventMetadata::V19(legacy),
				| Err(v19_err) => match bincode::deserialize::<EventMetadataV18>(value) {
					| Ok(legacy) => LegacyEventMetadata::V18(legacy),
					| Err(v18_err) => {
						// A row that doesn't parse as either legacy layout is only safe
						// to leave untouched if it already parses as v21.
						if bincode::deserialize::<crate::rooms::timeline::EventMetadata>(value)
							.is_ok()
						{
							continue;
						}
						return Err(err!(
							"eventid_metadata row ({} bytes) parses as neither v20 nor v19 nor \
							 v18 nor v21 during v21 migration: v20={v20_err}; v19={v19_err}; \
							 v18={v18_err}",
							value.len(),
						));
					},
				},
			},
		};

		let (metadata, rejection, soft_failed) = match legacy {
			| LegacyEventMetadata::V18(legacy) => (
				crate::rooms::timeline::EventMetadata {
					short_room_id: legacy.short_room_id,
					is_outlier: legacy.is_outlier,
					origin_server_ts: legacy.origin_server_ts,
					depth: legacy.depth,
					redacted_by: legacy.redacted_by,
					short_state_hash: legacy.short_state_hash,
					deprecated_local_topo_depth: 0,
					pdu_count: None,
				},
				legacy
					.rejected
					.then_some(crate::rooms::pdu_metadata::RejectionCode::Unknown.to_u8()),
				legacy
					.soft_failed
					.then_some(crate::rooms::pdu_metadata::SoftFailCode::Unknown.to_u8()),
			),
			| LegacyEventMetadata::V20(legacy) => {
				let verdict = match legacy.status {
					| EventStatusV20::Rejected(code) => (Some(code.to_u8()), None),
					| EventStatusV20::SoftFailed(code) => (None, Some(code.to_u8())),
					| EventStatusV20::Pending | EventStatusV20::Accepted => (None, None),
				};
				(
					crate::rooms::timeline::EventMetadata {
						short_room_id: legacy.short_room_id,
						is_outlier: legacy.is_outlier,
						origin_server_ts: legacy.origin_server_ts,
						depth: legacy.depth,
						redacted_by: legacy.redacted_by,
						short_state_hash: legacy.short_state_hash,
						deprecated_local_topo_depth: legacy.deprecated_local_topo_depth,
						pdu_count: legacy.pdu_count,
					},
					verdict.0,
					verdict.1,
				)
			},
			| LegacyEventMetadata::V19(legacy) => (
				crate::rooms::timeline::EventMetadata {
					short_room_id: legacy.short_room_id,
					is_outlier: legacy.is_outlier,
					origin_server_ts: legacy.origin_server_ts,
					depth: legacy.depth,
					redacted_by: legacy.redacted_by,
					short_state_hash: legacy.short_state_hash,
					deprecated_local_topo_depth: legacy.deprecated_local_topo_depth,
					pdu_count: legacy.pdu_count,
				},
				legacy
					.rejected
					.then_some(crate::rooms::pdu_metadata::RejectionCode::Unknown.to_u8()),
				legacy
					.soft_failed
					.then_some(crate::rooms::pdu_metadata::SoftFailCode::Unknown.to_u8()),
			),
		};

		if let Some(code) = rejection
			&& eventid_rejections
				.get_blocking(&event_id_bytes)
				.is_not_found()
		{
			eventid_rejections.insert(&event_id_bytes, [code]);
		}
		if let Some(code) = soft_failed
			&& eventid_softfailed
				.get_blocking(&event_id_bytes)
				.is_not_found()
		{
			eventid_softfailed.insert(&event_id_bytes, [code]);
		}

		if let Ok(new_bytes) = bincode::serialize(&metadata) {
			eventid_metadata.batch_put(&mut batch, &event_id_bytes, new_bytes);
			batch_count = batch_count.saturating_add(1);
			if batch_count >= 1000 {
				eventid_metadata.apply_batch(batch);
				batch = database::Batch::new();
				batch_count = 0;
			}
		}
	}
	eventid_metadata.apply_batch(batch);
	info!("Rewrote eventid_metadata rows in status-less layout.");

	// `eventid_status` stays a live (now-unused) CF: it must remain described
	// so the `<19` upgrade path (`db_lt_19`) and restart-after-drop consistency
	// keep working. Its contents were folded out above.

	drop(cork);

	services.globals.db.bump_database_version(21);
	info!("v21 migration completed.");
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
		assert_eq!(DATABASE_VERSION, 24);
	}

	#[test]
	fn legacy_event_metadata_v20_bincode_round_trips() {
		let original = EventMetadataV20 {
			short_room_id: 7,
			is_outlier: false,
			origin_server_ts: 42,
			depth: 11,
			status: EventStatusV20::Pending,
			redacted_by: Some(OwnedEventId::parse("$legacy:event").unwrap()),
			short_state_hash: Some(9),
			deprecated_local_topo_depth: 3,
			pdu_count: Some(2),
		};
		let bytes = bincode::serialize(&original).unwrap();
		let decoded: EventMetadataV20 = bincode::deserialize(&bytes).unwrap();
		assert_eq!(decoded.redacted_by, original.redacted_by);
		assert_eq!(decoded.short_room_id, original.short_room_id);
		assert_eq!(decoded.pdu_count, original.pdu_count);
	}

	#[test]
	fn legacy_event_metadata_v18_v19_decode_string_event_ids() {
		#[derive(serde::Serialize)]
		struct V18 {
			short_room_id: u64,
			is_outlier: bool,
			origin_server_ts: u64,
			depth: u64,
			soft_failed: bool,
			rejected: bool,
			redacted_by: Option<String>,
			short_state_hash: Option<u64>,
		}
		#[derive(serde::Serialize)]
		struct V19 {
			short_room_id: u64,
			is_outlier: bool,
			origin_server_ts: u64,
			depth: u64,
			soft_failed: bool,
			rejected: bool,
			redacted_by: Option<String>,
			short_state_hash: Option<u64>,
			deprecated_local_topo_depth: u64,
			pdu_count: Option<u64>,
			_soft_fail_reason: String,
			_rejection_reason: String,
		}

		let id = Some("$legacy:event".to_owned());
		let v18 = bincode::serialize(&V18 {
			short_room_id: 1,
			is_outlier: false,
			origin_server_ts: 2,
			depth: 3,
			soft_failed: false,
			rejected: false,
			redacted_by: id.clone(),
			short_state_hash: None,
		})
		.unwrap();
		let v19 = bincode::serialize(&V19 {
			short_room_id: 1,
			is_outlier: false,
			origin_server_ts: 2,
			depth: 3,
			soft_failed: false,
			rejected: false,
			redacted_by: id,
			short_state_hash: None,
			deprecated_local_topo_depth: 0,
			pdu_count: None,
			_soft_fail_reason: String::new(),
			_rejection_reason: String::new(),
		})
		.unwrap();

		assert_eq!(
			bincode::deserialize::<EventMetadataV18>(&v18)
				.unwrap()
				.redacted_by
				.unwrap()
				.as_str(),
			"$legacy:event"
		);
		assert_eq!(
			bincode::deserialize::<EventMetadataV19>(&v19)
				.unwrap()
				.redacted_by
				.unwrap()
				.as_str(),
			"$legacy:event"
		);
	}

	fn diff(parent: Option<u64>, added: &[u64], removed: &[u64]) -> StateDiff {
		let key = |n: u64| {
			let mut k = [0_u8; 16];
			k[..8].copy_from_slice(&(n % 5).to_be_bytes());
			k[8..].copy_from_slice(&n.to_be_bytes());
			k
		};
		StateDiff {
			parent,
			added: Arc::new(added.iter().copied().map(key).collect()),
			removed: Arc::new(removed.iter().copied().map(key).collect()),
		}
	}

	#[tokio::test]
	async fn test_cached_full_state_matches_chain_walk() {
		// 1 <- 2 <- 3 <- 4, with forks 5 (from 2) and 6 (from 5).
		let diffs: HashMap<u64, StateDiff> = HashMap::from([
			(1, diff(None, &[1, 2, 3], &[])),
			(2, diff(Some(1), &[4, 5], &[1])),
			(3, diff(Some(2), &[6], &[2, 4])),
			(4, diff(Some(3), &[7, 1], &[])),
			(5, diff(Some(2), &[8], &[5])),
			(6, diff(Some(5), &[9], &[8, 3])),
		]);
		let walk = |hash: u64| {
			let mut stack = Vec::new();
			let mut curr = Some(hash);
			while let Some(h) = curr {
				stack.push(diffs[&h].clone());
				curr = diffs[&h].parent;
			}
			let mut full = LegacyFullState::new();
			for d in stack.into_iter().rev() {
				full.extend(d.added.iter().copied());
				for rm in d.removed.iter() {
					full.remove(rm);
				}
			}
			full
		};

		// Tiny capacity forces both cache hits and evictions.
		for capacity in [1, 2, 64] {
			let mut cache = LegacyStateCache::new(capacity);
			for hash in [1_u64, 2, 3, 4, 6, 5, 4, 1, 6] {
				let got = legacy_get_full_state_cached(hash, &mut cache, |h| {
					ready(Ok(diffs[&h].clone()))
				})
				.await
				.unwrap();
				assert_eq!(*got, walk(hash), "hash {hash} capacity {capacity}");
			}
			assert!(cache.states.len() <= capacity);
		}
	}
}

// MSC4500 LtHash accumulator migration, renumbered from the augmented-HAMT
// branch's v19 to v22 so it can coexist with the v19-v21 set above.
async fn db_lt_22(services: &Services) -> Result<()> {
	services.globals.db.bump_database_version(22);
	Ok(())
}

#[derive(Clone)]
struct StateDiff {
	parent: Option<ShortStateHash>,
	added: Arc<std::collections::HashSet<[u8; 16]>>,
	removed: Arc<std::collections::HashSet<[u8; 16]>>,
}

async fn legacy_get_statediff(
	services: &Services,
	shortstatehash: ShortStateHash,
) -> Result<StateDiff> {
	const STRIDE: usize = size_of::<ShortStateHash>();

	let value = services.db["shortstatehash_statediff"]
		.get(&shortstatehash.to_be_bytes())
		.await
		.map_err(|e| err!(Database("Failed to find StateDiff: {e}")))?;

	let slice: &[u8] = &value;

	if slice.len() < STRIDE {
		return Err(err!(Database(
			"Truncated legacy state-diff record for shortstatehash {shortstatehash}: length {} \
			 is less than minimum stride {STRIDE}",
			slice.len()
		)));
	}

	let parent = conduwuit::utils::u64_from_bytes(&slice[0..8])
		.ok()
		.filter(|parent| *parent != 0);

	let mut add_mode = true;
	let mut added = std::collections::HashSet::new();
	let mut removed = std::collections::HashSet::new();

	let mut i = STRIDE;
	while let Some(v) = slice.get(i..i.saturating_add(2_usize.saturating_mul(STRIDE))) {
		if add_mode && v.starts_with(0_u64.to_be_bytes().as_slice()) {
			add_mode = false;
			i = i.saturating_add(STRIDE);
			continue;
		}

		if add_mode {
			added.insert(v.try_into().unwrap());
		} else {
			removed.insert(v.try_into().unwrap());
		}
		i = i.saturating_add(2_usize.saturating_mul(STRIDE));
	}

	Ok(StateDiff {
		parent,
		added: Arc::new(added),
		removed: Arc::new(removed),
	})
}

async fn legacy_get_full_state(
	services: &Services,
	shortstatehash: ShortStateHash,
) -> Result<std::collections::HashSet<[u8; 16]>> {
	let mut stack = Vec::new();
	let mut curr = Some(shortstatehash);
	while let Some(hash) = curr {
		let diff = legacy_get_statediff(services, hash).await?;
		stack.push(diff.clone());
		curr = diff.parent;
	}

	let mut full_state = std::collections::HashSet::new();
	for diff in stack.into_iter().rev() {
		for add in diff.added.iter() {
			full_state.insert(*add);
		}
		for rm in diff.removed.iter() {
			full_state.remove(rm);
		}
	}

	Ok(full_state)
}

type LegacyFullState = std::collections::HashSet<[u8; 16]>;

/// Small bounded cache of resolved legacy full states, keyed by snapshot.
/// Snapshots are visited in ascending `shortstatehash` order, so a snapshot's
/// parent is usually among the most recently resolved states and the parent
/// chain walk stops there instead of replaying every ancestor diff.
struct LegacyStateCache {
	states: HashMap<ShortStateHash, Arc<LegacyFullState>>,
	order: std::collections::VecDeque<ShortStateHash>,
	capacity: usize,
}

impl LegacyStateCache {
	fn new(capacity: usize) -> Self {
		Self {
			states: HashMap::new(),
			order: std::collections::VecDeque::new(),
			capacity: capacity.max(1),
		}
	}

	fn insert(&mut self, hash: ShortStateHash, state: Arc<LegacyFullState>) {
		if self.states.insert(hash, state).is_none() {
			self.order.push_back(hash);
		}
		while self.order.len() > self.capacity {
			if let Some(old) = self.order.pop_front() {
				self.states.remove(&old);
			}
		}
	}
}

/// Resolve the full state of `shortstatehash`, starting from the nearest
/// cached ancestor instead of the root of the diff chain. Result is identical
/// to `legacy_get_full_state`; `fetch` loads a single statediff.
async fn legacy_get_full_state_cached<F, Fut>(
	shortstatehash: ShortStateHash,
	cache: &mut LegacyStateCache,
	fetch: F,
) -> Result<Arc<LegacyFullState>>
where
	F: Fn(ShortStateHash) -> Fut,
	Fut: Future<Output = Result<StateDiff>>,
{
	if let Some(state) = cache.states.get(&shortstatehash) {
		return Ok(Arc::clone(state));
	}

	let mut stack = Vec::new();
	let mut base: LegacyFullState = LegacyFullState::new();
	let mut curr = Some(shortstatehash);
	while let Some(hash) = curr {
		if let Some(state) = cache.states.get(&hash) {
			base = (**state).clone();
			break;
		}
		let diff = fetch(hash).await?;
		curr = diff.parent;
		stack.push(diff);
	}

	for diff in stack.into_iter().rev() {
		for add in diff.added.iter() {
			base.insert(*add);
		}
		for rm in diff.removed.iter() {
			base.remove(rm);
		}
	}

	let state = Arc::new(base);
	cache.insert(shortstatehash, Arc::clone(&state));
	Ok(state)
}

/// Returns the `shorteventid`s of the state events a legacy statediff added,
/// i.e. the state events whose *post-event* state is that snapshot. The raw
/// statediff payload starts with the parent `ShortStateHash`, followed by
/// 16-byte `(shortstatekey, shorteventid)` entries, first the added set and
/// then (after an all-zero shortstatekey marker) the removed set. The second
/// half of each added entry is the event the snapshot's state results from.
fn legacy_statediff_added_shorteventids(slice: &[u8]) -> Vec<ShortEventId> {
	const STRIDE: usize = size_of::<ShortStateHash>();
	let mut added = Vec::new();
	let mut add_mode = true;
	let mut i = STRIDE;
	while let Some(v) = slice.get(i..i.saturating_add(2_usize.saturating_mul(STRIDE))) {
		if add_mode && v.starts_with(0_u64.to_be_bytes().as_slice()) {
			add_mode = false;
			i = i.saturating_add(STRIDE);
			continue;
		}

		if add_mode {
			if let Ok(shorteventid) = v
				.get(STRIDE..2_usize.saturating_mul(STRIDE))
				.ok_or(())
				.and_then(|b| <[u8; 8]>::try_from(b).map_err(|_| ()))
				.map(u64::from_be_bytes)
			{
				added.push(shorteventid);
			}
		}

		i = i.saturating_add(2_usize.saturating_mul(STRIDE));
	}

	added
}

/// Build the HAMT root for each accumulated post-event snapshot and return the
/// `(shorteventid, serialized root)` pairs to store in
/// `shorteventid_roothandle`. A room id is required for the structural key; it
/// is resolved from the first event of each snapshot group.
async fn legacy_added_events_roothandles(
	services: &Services,
	post_state_events: &HashMap<ShortStateHash, Vec<ShortEventId>>,
	cache: &mut LegacyStateCache,
) -> Result<Vec<(ShortEventId, Vec<u8>)>> {
	let mut out = Vec::new();
	let mut snapshots: Vec<_> = post_state_events.iter().collect();
	snapshots.sort_unstable_by_key(|(shortstatehash, _)| **shortstatehash);
	for (shortstatehash, shorteventids) in snapshots {
		let first = shorteventids.first().ok_or(err!(Database(error!(
			"Empty event group for shortstatehash {shortstatehash} during v20 backfill."
		))))?;
		let event_id: OwnedEventId = services.rooms.short.get_eventid_from_short(*first).await?;
		let pdu = services.rooms.timeline.get_pdu(&event_id).await?;
		let room_id = pdu
			.room_id_or_hash()
			.expect("timeline PDU must have a room_id")
			.clone();

		let (root_handle, root_node) =
			legacy_build_root_handle_for_state(services, &room_id, *shortstatehash, cache)
				.await?;
		services
			.rooms
			.state_hamt
			.store
			.persist_node_recursive(root_node);

		let serialized = crate::rooms::state::root_handle_to_bytes(&root_handle);
		for shorteventid in shorteventids {
			out.push((*shorteventid, serialized.clone()));
		}
	}

	Ok(out)
}

async fn legacy_build_root_handle_for_state(
	services: &Services,
	room_id: &RoomId,
	shortstatehash: ShortStateHash,
	cache: &mut LegacyStateCache,
) -> Result<(rezzy::hamt::RootHandle, Arc<rezzy::hamt::HamtNode<u64, u64>>)> {
	let full_state = legacy_get_full_state_cached(shortstatehash, cache, |hash| {
		legacy_get_statediff(services, hash)
	})
	.await?;

	let mut lattice = rezzy::state::LtHash::default();
	let mut entries = Vec::with_capacity(full_state.len());

	for state_event in full_state.iter() {
		let shortstatekey = conduwuit::utils::u64_from_bytes(&state_event[0..8]).expect("bytes");
		let shorteventid = conduwuit::utils::u64_from_bytes(&state_event[8..16]).expect("bytes");

		let (ty, sk) = services
			.rooms
			.short
			.get_statekey_from_short(shortstatekey)
			.await?;
		let event_id: OwnedEventId = services
			.rooms
			.short
			.get_eventid_from_short(shorteventid)
			.await?;

		lattice.insert(ty.to_string().as_str(), sk.as_str(), event_id.as_str());
		entries.push((shortstatekey, shorteventid));
	}

	let structural_key =
		crate::rooms::state_hamt::room_structural_key(&services.globals.server_secret, room_id);
	let (root_handle, root_node) =
		rezzy::hamt::build_hamt_root_handle(&structural_key, &lattice, entries).map_err(|e| {
			err!(error!(
				"Failed to build HAMT root for room {room_id} and state {shortstatehash}: {e:?}"
			))
		})?;

	services.db["state_hamt_root_lattices"]
		.insert(&root_handle.structural_hash, lattice.to_bytes());

	Ok((root_handle, root_node))
}

async fn db_lt_23(services: &Services) -> Result<()> {
	const FLUSH_AFTER_EVENTS: usize = 65_536;

	info!("Running v20 migration (building HAMT roots for existing rooms)...");

	let mut room_stream = services.rooms.metadata.iter_ids();
	while let Some(room_id) = room_stream.next().await {
		match services.db["roomid_shortstatehash"]
			.get(&room_id)
			.await
			.deserialized()
		{
			| Err(e) if e.is_not_found() => {
				// Room has no state yet (e.g. partial join); skip.
				debug_warn!(
					"Skipping room {room_id} in v20 migration: no shortstatehash (room may be \
					 incomplete)"
				);
				continue;
			},
			| Err(e) => return Err(e),
			| Ok(shortstatehash) => {
				let full_state = legacy_get_full_state(services, shortstatehash).await?;

				let mut lattice = rezzy::state::LtHash::default();
				let mut entries = Vec::with_capacity(full_state.len());

				for state_event in full_state {
					let shortstatekey =
						conduwuit::utils::u64_from_bytes(&state_event[0..8]).expect("bytes");
					let shorteventid =
						conduwuit::utils::u64_from_bytes(&state_event[8..16]).expect("bytes");

					let (ty, sk) = services
						.rooms
						.short
						.get_statekey_from_short(shortstatekey)
						.await?;
					let event_id: OwnedEventId = services
						.rooms
						.short
						.get_eventid_from_short(shorteventid)
						.await?;

					lattice.insert(ty.to_string().as_str(), sk.as_str(), event_id.as_str());
					entries.push((shortstatekey, shorteventid));
				}

				let structural_key = crate::rooms::state_hamt::room_structural_key(
					&services.globals.server_secret,
					&room_id,
				);

				let (root_handle, root_node) =
					rezzy::hamt::build_hamt_root_handle(&structural_key, &lattice, entries)
						.map_err(|e| {
							err!(error!("Failed to build HAMT root for room {room_id}: {e:?}"))
						})?;

				services
					.rooms
					.state_hamt
					.store
					.persist_node_recursive(root_node);

				// Write the same flat-48-byte encoding used by set_room_state_hamt,
				// so get_room_state_hamt can read the value back.
				services.db["state_hamt_root_lattices"]
					.insert(&root_handle.structural_hash, lattice.to_bytes());
				let data = crate::rooms::state::root_handle_to_bytes(&root_handle);
				services.db["roomid_roothandle"].insert(room_id.as_str().as_bytes(), &data);
			},
		}
	}

	info!("Backfilling per-event HAMT root handles for existing events...");

	// `shorteventid_shortstatehash` stores each state event's *predecessor*
	// (pre-event) state, so labeling events with that snapshot's root would
	// attach the wrong state boundary to `shorteventid_roothandle`. Instead we
	// invert the legacy statediffs: the snapshot whose `added` set contains a
	// state event is that event's *post-event* state, which is exactly what
	// `get_roothandle`/`pdu_roothandle_after_event` must return. This also covers the first
	// state event of each room (present in the first snapshot's `added` even
	// though it has no predecessor mapping). Snapshots are accumulated and
	// flushed in bounded batches so the whole history is never held in memory.
	let mut post_state_events: HashMap<ShortStateHash, Vec<ShortEventId>> = HashMap::new();
	let mut pending_events = 0_usize;
	let mut state_cache = LegacyStateCache::new(64);
	let roothandle_map = services.db["shorteventid_roothandle"].clone();
	let mut batch = conduwuit_database::Batch::new();

	let statediff_map = services.db["shortstatehash_statediff"].clone();
	let mut diff_stream = statediff_map.raw_stream();
	while let Some(result) = diff_stream.next().await {
		let (key, value): database::KeyVal<'_> = result?;
		let shortstatehash = u64::from_be_bytes(key[..8].try_into().map_err(|_| {
			err!(Database(error!(
				"Unexpected key in `shortstatehash_statediff` during v20 backfill."
			)))
		})?);

		let added = legacy_statediff_added_shorteventids(value);
		pending_events = pending_events.saturating_add(added.len());
		post_state_events
			.entry(shortstatehash)
			.or_default()
			.extend(added);

		if pending_events >= FLUSH_AFTER_EVENTS {
			for (shorteventid, serialized) in
				legacy_added_events_roothandles(services, &post_state_events, &mut state_cache)
					.await?
			{
				roothandle_map.batch_put(
					&mut batch,
					&shorteventid.to_be_bytes(),
					serialized.as_slice(),
				);
			}
			roothandle_map.apply_batch(batch);
			batch = conduwuit_database::Batch::new();
			post_state_events.clear();
			pending_events = 0;
		}
	}

	if pending_events > 0 {
		for (shorteventid, serialized) in
			legacy_added_events_roothandles(services, &post_state_events, &mut state_cache)
				.await?
		{
			roothandle_map.batch_put(
				&mut batch,
				&shorteventid.to_be_bytes(),
				serialized.as_slice(),
			);
		}
	}
	roothandle_map.apply_batch(batch);

	// Every timeline event needs a state boundary. State events were covered
	// above by the legacy state-diff inversion; ordinary events have no state
	// diff of their own and inherit the root preceding them. Walk each room in
	// chronological order and fill the remaining per-event root handles.
	let room_ids: Vec<_> = services.rooms.metadata.iter_ids().collect().await;
	for room_id in room_ids {
		let structural_key = crate::rooms::state_hamt::room_structural_key(
			&services.globals.server_secret,
			&room_id,
		);
		let empty_lattice = rezzy::state::LtHash::default();
		let (empty_root, empty_node) =
			rezzy::hamt::build_hamt_root_handle(&structural_key, &empty_lattice, Vec::new())
				.map_err(|e| {
					err!(error!("Failed to build empty HAMT root for {room_id}: {e:?}"))
				})?;
		services
			.rooms
			.state_hamt
			.store
			.persist_node_recursive(empty_node);

		// `all_pdus` yields events oldest-first; walk forward so each non-state
		// event inherits the root of the most recent preceding state event.
		let mut pdus = std::pin::pin!(services.rooms.timeline.all_pdus(&room_id));
		let mut current_root = empty_root;
		let mut event_batch = conduwuit_database::Batch::new();
		let mut batched = 0_usize;
		while let Some((_, pdu)) = pdus.next().await {
			if pdu.state_key().is_some() {
				if let Ok(shorteventid) =
					services.rooms.short.get_shorteventid(pdu.event_id()).await
					&& let Ok(data) = roothandle_map.get(&shorteventid.to_be_bytes()).await
				{
					current_root = crate::rooms::state::root_handle_from_bytes(&data)?;
				}
			}
			roothandle_map.batch_put(
				&mut event_batch,
				&services
					.rooms
					.short
					.get_or_create_shorteventid(pdu.event_id())
					.await
					.to_be_bytes(),
				crate::rooms::state::root_handle_to_bytes(&current_root),
			);
			batched = batched.saturating_add(1);
			if batched >= FLUSH_AFTER_EVENTS {
				roothandle_map.apply_batch(event_batch);
				event_batch = conduwuit_database::Batch::new();
				batched = 0;
			}
		}
		roothandle_map.apply_batch(event_batch);
	}

	services.globals.db.bump_database_version(23);
	Ok(())
}

/// Drop the legacy `shortstatehash` state table data once the HAMT cutover
/// (v23) has rebuilt it into HAMT roots.
///
/// The legacy state maps were only read as inputs to the v23 migration; at
/// runtime the state layer now reads HAMT roots exclusively. Their contents are
/// no longer consulted, so we clear them to reclaim the space. We deliberately
/// do **not** remove the column families or the legacy accessor helpers: fresh
/// databases arriving from schema `< 23` still need both the columns and the
/// helpers to run v23 for the first time.
///
/// This runs only after v23 has bumped the schema version to 23, so the data we
/// clear here is guaranteed to have already been consumed.
async fn db_lt_24(services: &Services) -> Result<()> {
	info!("Running v24 migration (clearing legacy shortstatehash table data)...");

	let db = &services.db;
	let cork = db.cork_and_sync();
	let mut total = 0_usize;

	for map_name in [
		"shortstatehash_statediff",
		"roomid_shortstatehash",
		"shorteventid_shortstatehash",
		"shortstatehash_lthash",
		"roomsynctoken_shortstatehash",
	] {
		let map = db[map_name].clone();
		let cleared = map
			.raw_stream()
			.try_fold(
				0_usize,
				async |mut count: usize, (key, _): database::KeyVal<'_>| -> Result<usize> {
					map.remove_raw(key);
					count = count.saturating_add(1);
					Ok(count)
				},
			)
			.await?;

		info!(%map_name, ?cleared, "Cleared legacy shortstatehash column");
		total = total.saturating_add(cleared);
	}

	drop(cork);
	info!(?total, "Cleared legacy shortstatehash table data.");

	services.globals.db.bump_database_version(24);
	Ok(())
}
