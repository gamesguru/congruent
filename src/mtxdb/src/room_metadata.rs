//! Matrix room metadata stored in the event collection's genesis record.

use mtxdb::ShardType;
use rezzy::json::Value;

use crate::{
	StorageError,
	keys::{derive_id, room_event_collection_id},
	store::MtxdbStore,
};

const ROOM_VERSION_DOMAIN: &[u8] = b"matrix-room-version-v1";
const ROOM_VERSION_COLLECTION: &[u8] = b"matrix-room-version-record-v1";
const ROOM_VERSION_NODE: &[u8] = b"room-version";
const ROOM_VERSION_MAGIC: &[u8; 4] = b"MRV1";
const ROOM_VERSION_HEADER_LEN: usize = 8;
const MAX_STALE_READ_RETRIES: usize = 8;

fn version_collection(room_id: &[u8]) -> [u8; 16] { derive_id(ROOM_VERSION_DOMAIN, room_id) }

fn version_node() -> [u8; 16] { derive_id(ROOM_VERSION_COLLECTION, ROOM_VERSION_NODE) }

/// Encode the room ID and version, rejecting lengths that cannot fit in the
/// record with `StorageError::Corrupt`.
fn encode_record(room_id: &[u8], version: &str) -> Result<Vec<u8>, StorageError> {
	let room_len = u32::try_from(room_id.len()).map_err(|_| {
		StorageError::Corrupt("room id is too long for room-version record".into())
	})?;
	let capacity = ROOM_VERSION_HEADER_LEN
		.checked_add(room_id.len())
		.and_then(|size| size.checked_add(version.len()))
		.ok_or_else(|| StorageError::Corrupt("room-version record length overflow".into()))?;
	let mut bytes = Vec::with_capacity(capacity);
	bytes.extend_from_slice(ROOM_VERSION_MAGIC);
	bytes.extend_from_slice(&room_len.to_be_bytes());
	bytes.extend_from_slice(room_id);
	bytes.extend_from_slice(version.as_bytes());
	Ok(bytes)
}

/// Decode a version for the requested room, returning `None` for another room.
/// Malformed records and empty or non-UTF-8 versions return `StorageError::Corrupt`.
fn decode_record(bytes: &[u8], room_id: &[u8]) -> Result<Option<String>, StorageError> {
	if bytes.len() < ROOM_VERSION_HEADER_LEN
		|| bytes.get(..ROOM_VERSION_MAGIC.len()) != Some(ROOM_VERSION_MAGIC.as_slice())
	{
		return Err(StorageError::Corrupt("malformed room-version record".into()));
	}
	let room_len = u32::from_be_bytes(
		bytes[ROOM_VERSION_MAGIC.len()..ROOM_VERSION_HEADER_LEN]
			.try_into()
			.expect("fixed-size slice"),
	);
	let room_len = usize::try_from(room_len)
		.map_err(|_| StorageError::Corrupt("room-version room id length overflow".into()))?;
	let room_end = ROOM_VERSION_HEADER_LEN
		.checked_add(room_len)
		.ok_or_else(|| StorageError::Corrupt("room-version record length overflow".into()))?;
	let stored_room = bytes
		.get(ROOM_VERSION_HEADER_LEN..room_end)
		.ok_or_else(|| StorageError::Corrupt("truncated room-version record".into()))?;
	if stored_room != room_id {
		return Ok(None);
	}
	let version = bytes
		.get(room_end..)
		.ok_or_else(|| StorageError::Corrupt("missing room version".into()))?;
	let version = std::str::from_utf8(version)
		.map_err(|_| StorageError::Corrupt("room version is not UTF-8".into()))?;
	if version.is_empty() {
		return Err(StorageError::Corrupt("empty room version".into()));
	}
	Ok(Some(version.to_owned()))
}

/// Read the authoritative version record for a room, if present.
///
/// A record for a different room is a miss. Malformed records return
/// `StorageError::Corrupt`; storage read errors propagate.
pub fn get_room_version(
	store: &MtxdbStore,
	room_id: &[u8],
) -> Result<Option<String>, StorageError> {
	let collection = version_collection(room_id);
	store
		.get(ShardType::ServerInfo, &collection, version_node())?
		.map_or(Ok(None), |record| decode_record(&record, room_id))
}

/// Persist an immutable room version. Repeated writes of the same value are
/// idempotent; conflicting versions fail rather than silently changing room
/// validation rules.
///
/// Returns `StorageError::Collision` for a conflicting version or room ID,
/// `StorageError::StaleRead` after repeated concurrent write contention,
/// and propagates record encoding, decoding, and storage errors.
pub fn set_room_version(
	store: &MtxdbStore,
	room_id: &[u8],
	version: &str,
) -> Result<(), StorageError> {
	let collection = version_collection(room_id);
	let node = version_node();
	let record = encode_record(room_id, version)?;

	let mut attempts = 0_usize;
	loop {
		let tx = store.begin_transaction();
		let result = (|| {
			let (values, collection_version) =
				tx.get_with_collection_version(ShardType::ServerInfo, collection, &[node])?;
			if let Some(existing) = values.into_iter().next().flatten() {
				return match decode_record(&existing, room_id)? {
					| Some(existing) if existing == version => Ok(()),
					| Some(existing) => Err(StorageError::Collision(format!(
						"room {room_id:?} already has room version {existing}, cannot change it \
						 to {version}"
					))),
					| None => Err(StorageError::Collision(
						"room-version collection key collision".into(),
					)),
				};
			}
			tx.put(ShardType::ServerInfo, collection, node, &record)?;
			tx.expect_collection_version(ShardType::ServerInfo, collection, collection_version)?;
			tx.commit()
		})();
		match result {
			| Err(error) if error.is_stale_read() && attempts < MAX_STALE_READ_RETRIES => {
				attempts = attempts.saturating_add(1);
			},
			| result => return result,
		}
	}
}

/// Read a room version from the collection header written by the mtxdb importer.
///
/// This is a metadata-only lookup: it does not read or parse the create event.
/// Collections created by older server code may not have this header. Missing,
/// malformed, or unrecognized extensions yield `None`; storage errors propagate.
pub fn room_version_from_collection(
	store: &MtxdbStore,
	room_id: &[u8],
) -> Result<Option<String>, StorageError> {
	let collection = room_event_collection_id(room_id);
	let Some(metadata) = store.get_collection_metadata(ShardType::EventDag, &collection)? else {
		return Ok(None);
	};
	let Some(extension) = metadata.extension else {
		return Ok(None);
	};
	let Ok(value) = Value::parse_bytes(&extension) else {
		return Ok(None);
	};
	if value.get("ext").and_then(Value::as_str) != Some("matrix.room")
		|| value.get("fmt").and_then(Value::as_u64) != Some(1)
	{
		return Ok(None);
	}

	Ok(value
		.get("room_version")
		.and_then(Value::as_str)
		.map(str::to_owned))
}
