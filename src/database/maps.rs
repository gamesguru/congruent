use std::{collections::BTreeMap, sync::Arc};

use conduwuit::Result;

use crate::{
	Engine, Map,
	engine::descriptor::{self, CacheDisp, Descriptor},
};

pub(super) type Maps = BTreeMap<MapsKey, MapsVal>;
pub(super) type MapsKey = &'static str;
pub(super) type MapsVal = Arc<Map>;

pub(super) fn open(db: &Arc<Engine>) -> Result<Maps> { open_list(db, MAPS) }

#[tracing::instrument(name = "maps", level = "debug", skip_all)]
pub(super) fn open_list(db: &Arc<Engine>, maps: &[Descriptor]) -> Result<Maps> {
	maps.iter()
		.map(|desc| Ok((desc.name, Map::open(db, desc.name)?)))
		.collect()
}

pub(super) static MAPS: &[Descriptor] = &[
	Descriptor {
		name: "alias_roomid",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "alias_userid",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "aliasid_alias",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "backupid_algorithm",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "backupid_etag",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "backupkeyid_backup",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "bannedroomids",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "deviceleftid_userid",
		..descriptor::SEQUENTIAL
	},
	Descriptor {
		name: "disabledroomids",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "email_localpart",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "eventid_metadata",
		val_size_hint: Some(128),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "eventid_pdu",
		val_size_hint: Some(1520),
		block_size: 2048,
		index_size: 512,
		..descriptor::RANDOM
	},
	Descriptor {
		name: "eventid_pduid",
		cache_disp: CacheDisp::Unique,
		key_size_hint: Some(48),
		val_size_hint: Some(16),
		block_size: 512,
		index_size: 512,
		..descriptor::RANDOM
	},
	Descriptor {
		name: "eventid_shorteventid",
		cache_disp: CacheDisp::Unique,
		key_size_hint: Some(48),
		val_size_hint: Some(8),
		block_size: 512,
		index_size: 512,
		..descriptor::RANDOM
	},
	Descriptor {
		name: "federation_outbound_to_device",
		val_size_hint: Some(128),
		..descriptor::RANDOM
	},
	Descriptor {
		name: "global",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "id_appserviceregistrations",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "keychangeid_userid",
		..descriptor::RANDOM
	},
	Descriptor {
		name: "keyid_key",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "lazyloadedids",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "localpart_email",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "logintoken_expiresatuserid",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "mediaid_file",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "mediaid_user",
		..descriptor::RANDOM_SMALL
	},
	// MSC2836 threading: key = (parent_event_id, child_event_id), value =
	// rel_type. Populated whenever an event's content carries an
	// `m.relationship` pointing at a parent; queried by prefix on
	// parent_event_id to answer "what are this event's children" for
	// /event_relationships and unsigned.children/children_hash.
	Descriptor {
		name: "msc2836_children",
		..descriptor::RANDOM
	},
	// MSC2836 threading: key = event_id, value = bincode
	// Msc2836ReportedChildren (counts + children_hash) received from a
	// remote server's /event_relationships response for this event. Kept
	// separate from msc2836_children (our own directly-known child edges)
	// since a remote server may know about children we haven't fetched
	// ourselves yet; see pdu_metadata::msc2836_children_unsigned.
	Descriptor {
		name: "msc2836_reported_children",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "onetimekeyid_onetimekeys",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "openidtoken_expiresatuserid",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "passwordresettoken_info",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "presenceid_presence",
		..descriptor::SEQUENTIAL_SMALL
	},
	Descriptor {
		name: "publicroomids",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "pushkey_deviceid",
		..descriptor::RANDOM_SMALL
	},
	// Stream index (room+count+user) for since-token scans; see readreceipts_since().
	Descriptor {
		name: "readreceiptid_readreceipt",
		..descriptor::RANDOM
	},
	// State index (room+user) for O(1) point lookups of a user's current public
	// read receipt; see readreceipt_get()/readreceipt_update(). Dual-indexed
	// alongside readreceiptid_readreceipt above, not a replacement for it.
	Descriptor {
		name: "roomuserid_readreceipt",
		val_size_hint: Some(1024),
		..descriptor::RANDOM
	},
	Descriptor {
		name: "referencedevents",
		..descriptor::RANDOM
	},
	Descriptor {
		name: "registrationtoken_info",
		..descriptor::RANDOM_SMALL
	},
	// TODO: Legacy Conduit table, superseded by eventid_metadata.rejected
	// and eventid_metadata.soft_failed fields. No service code references
	// this CF. Remove in a future schema version bump.
	Descriptor {
		name: "rejectedeventids",
		key_size_hint: Some(48),
		..descriptor::RANDOM_SMALL
	},
	// Primary timeline index, keyed by stream order (monotonic server-local
	// counter from globals.next_count()). Used by /sync, read receipts, and
	// notification counting. Key: (shortroomid: u64, pdu_count: u64) -> event_id.
	// Migrated from pduid_pdu as of v19 (migrate_event_store_to_ssot).
	// See also: roomid_topologicalorder_pducount for DAG-based ordering.
	Descriptor {
		name: "room_pducount_eventid",
		key_size_hint: Some(16),
		val_size_hint: Some(32),
		..descriptor::SEQUENTIAL_SMALL
	},
	// DEPRECATED: This map is no longer used. Stream order is immutable and
	// the reorder/heal code no longer backs up old stream order entries.
	// Retained to avoid a schema migration; will be removed in a future
	// database version bump.
	Descriptor {
		name: "room_pducount_eventid_backup",
		key_size_hint: Some(16),
		val_size_hint: Some(32),
		..descriptor::SEQUENTIAL_SMALL
	},
	Descriptor {
		name: "roomid_invitedcount",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomid_inviteviaservers",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomid_joinedcount",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomid_pduleaves",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomid_roomversion",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomid_shortroomid",
		val_size_hint: Some(8),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomid_shortstatehash",
		val_size_hint: Some(8),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomid_topologicalorder_pducount",
		key_size_hint: Some(32),
		val_size_hint: Some(32),
		..descriptor::SEQUENTIAL_SMALL
	},
	// Tier 3 backfill perf work (see
	// docs/development-gg/backfill-extremities-write-time-design.md):
	// backward extremities (missing prev_events), indexed by depth so
	// `backfill_if_required` can range-scan near a position instead of
	// walking the timeline. Not yet wired into the read path -- currently
	// written at insert time only, pending the migration for existing
	// rooms. Key: (shortroomid: u64, depth: u64, event_id) -> ().
	// See also: roomid_missingeventid_depth, the delete-path companion index.
	Descriptor {
		name: "roomid_depth_missingeventid",
		key_size_hint: Some(32),
		val_size_hint: Some(0),
		..descriptor::SEQUENTIAL_SMALL
	},
	// Companion to roomid_depth_missingeventid: keyed by event_id so
	// arrival of a previously-missing event can find (and delete) its
	// depth-indexed entry in O(1) instead of a scan.
	// Key: (shortroomid: u64, event_id) -> depth: u64.
	Descriptor {
		name: "roomid_missingeventid_depth",
		key_size_hint: Some(32),
		val_size_hint: Some(8),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomid_timestamp_pducount",
		key_size_hint: Some(25),
		..descriptor::SEQUENTIAL
	},
	Descriptor {
		name: "roomserverids",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomsynctoken_shortstatehash",
		file_shape: 3,
		val_size_hint: Some(8),
		block_size: 512,
		compression_level: 3,
		bottommost_level: Some(6),
		..descriptor::SEQUENTIAL
	},
	Descriptor {
		name: "roomuserdataid_accountdata",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomuserid_invitecount",
		val_size_hint: Some(8),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomuserid_joined",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomuserid_knockedcount",
		val_size_hint: Some(8),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomuserid_lastnotificationread",
		val_size_hint: Some(8),
		..descriptor::RANDOM_SMALL
	},
	// TODO: legacy, superseded by roomuserid_privatereadreceipt. No longer written;
	// only read as a fallback in read_receipt/data.rs. Remove once a migration
	// backfills roomuserid_privatereadreceipt from this map.
	Descriptor {
		name: "roomuserid_lastprivatereadupdate",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomuserid_leftcount",
		val_size_hint: Some(8),
		..descriptor::RANDOM
	},
	Descriptor {
		name: "roomuserid_forgotten",
		val_size_hint: Some(0),
		..descriptor::RANDOM_SMALL
	},
	// TODO: legacy, superseded by roomuserid_privatereadreceipt. No longer written;
	// only read as a fallback in read_receipt/data.rs. Remove once a migration
	// backfills roomuserid_privatereadreceipt from this map.
	Descriptor {
		name: "roomuserid_privateread",
		..descriptor::RANDOM_SMALL
	},
	// TODO: legacy, superseded by roomuserid_privatereadreceipt. No longer written;
	// only read as a fallback in read_receipt/data.rs. Remove once a migration
	// backfills roomuserid_privatereadreceipt from this map.
	Descriptor {
		name: "roomuserid_privatereadevent",
		..descriptor::RANDOM_SMALL
	},
	// Consolidated, thread-aware private read receipt map (MSC4102). Supersedes
	// roomuserid_privateread, roomuserid_privatereadevent, and
	// roomuserid_lastprivatereadupdate above.
	Descriptor {
		name: "roomuserid_privatereadreceipt",
		val_size_hint: Some(1024),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "roomuseroncejoinedids",
		..descriptor::RANDOM
	},
	Descriptor {
		name: "roomusertype_roomuserdataid",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "senderkey_pusher",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "server_signingkeys",
		..descriptor::RANDOM
	},
	Descriptor {
		name: "servercurrentevent_data",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "servername_destination",
		..descriptor::RANDOM_SMALL_CACHE
	},
	Descriptor {
		name: "servername_educount",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "servername_override",
		..descriptor::RANDOM_SMALL_CACHE
	},
	Descriptor {
		name: "servernameevent_data",
		cache_disp: CacheDisp::Unique,
		val_size_hint: Some(128),
		..descriptor::RANDOM
	},
	Descriptor {
		name: "serverroomids",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "shorteventid_authchain",
		cache_disp: CacheDisp::Unique,
		key_size_hint: Some(8),
		..descriptor::SEQUENTIAL
	},
	Descriptor {
		name: "shorteventid_shortauthevents",
		key_size_hint: Some(8),
		cache_disp: CacheDisp::Unique,
		..descriptor::SEQUENTIAL
	},
	Descriptor {
		name: "shorteventid_shortprevevents",
		key_size_hint: Some(8),
		cache_disp: CacheDisp::Unique,
		..descriptor::SEQUENTIAL
	},
	Descriptor {
		name: "shorteventid_eventid",
		cache_disp: CacheDisp::Unique,
		key_size_hint: Some(8),
		val_size_hint: Some(48),
		..descriptor::SEQUENTIAL_SMALL
	},
	Descriptor {
		name: "shorteventid_shortstatehash",
		key_size_hint: Some(8),
		val_size_hint: Some(8),
		block_size: 512,
		index_size: 512,
		..descriptor::SEQUENTIAL
	},
	Descriptor {
		name: "shortstatehash_statediff",
		key_size_hint: Some(8),
		..descriptor::SEQUENTIAL_SMALL
	},
	Descriptor {
		name: "shortstatekey_statekey",
		cache_disp: CacheDisp::Unique,
		key_size_hint: Some(8),
		val_size_hint: Some(1016),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "statehash_shortstatehash",
		val_size_hint: Some(8),
		..descriptor::RANDOM
	},
	Descriptor {
		name: "statekey_shortstatekey",
		cache_disp: CacheDisp::Unique,
		key_size_hint: Some(1016),
		val_size_hint: Some(8),
		..descriptor::RANDOM
	},
	Descriptor {
		name: "threadid_userids",
		..descriptor::SEQUENTIAL_SMALL
	},
	Descriptor {
		name: "todeviceid_events",
		..descriptor::RANDOM
	},
	Descriptor {
		name: "tofrom_relation",
		key_size_hint: Some(8),
		val_size_hint: Some(8),
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "token_userdeviceid",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "tokenids",
		block_size: 512,
		..descriptor::RANDOM
	},
	Descriptor {
		name: "url_previews",
		..descriptor::RANDOM
	},
	Descriptor {
		name: "userdeviceid_metadata",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userdeviceid_token",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userdevicesessionid_uiaainfo",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userdevicetxnid_response",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userfilterid_filter",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_avatarurl",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_blurhash",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_dehydrateddevice",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_devicelistversion",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_displayname",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_lastonetimekeyupdate",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_lock",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_logindisabled",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_masterkeyid",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_lastremotedeviceliststreamid",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_origin",
		..descriptor::RANDOM
	},
	Descriptor {
		name: "userid_password",
		..descriptor::RANDOM
	},
	Descriptor {
		name: "userid_presenceid",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_selfsigningkeyid",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_suspension",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userid_usersigningkeyid",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "useridprofilekey_value",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userroomid_highlightcount",
		..descriptor::RANDOM
	},
	Descriptor {
		name: "userroomid_invitesender",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userroomid_invitestate",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userroomid_joined",
		..descriptor::RANDOM
	},
	Descriptor {
		name: "userroomid_knockedstate",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userroomid_leftstate",
		..descriptor::RANDOM
	},
	Descriptor {
		name: "userroomid_notificationcount",
		..descriptor::RANDOM
	},
	Descriptor {
		name: "delayid_scheduleddelayedevent",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "delayid_finalizeddelayedevent",
		..descriptor::RANDOM_SMALL
	},
	Descriptor {
		name: "userroomdelayid",
		..descriptor::RANDOM_SMALL
	},
];

/// Returns an iterator of column family names from the static MAPS list.
/// Used for schema fingerprinting across crate boundaries.
pub fn column_family_names() -> impl Iterator<Item = &'static str> {
	MAPS.iter().map(|desc| desc.name)
}
