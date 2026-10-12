//! Stable identifiers used at the mtxdb boundary.

use mtxdb::{
	MEMBER_NAMESPACE_AUTH, MEMBER_NAMESPACE_EVNT, MEMBER_NAMESPACE_PREV, MEMBER_NAMESPACE_STAT,
	MEMBER_NAMESPACE_STGP, derive_group_full_id, derive_member_collection_id_from_group,
};

/// Derive an mtxdb identifier from a domain and logical identifier.
///
/// The domain is length-delimited by the separator, so identifiers from
/// different namespaces cannot accidentally share the same input stream.
/// Callers should retain the original logical identifier where collision
/// detection is required; mtxdb identifiers are deliberately only 128 bits.
#[must_use]
pub fn derive_id(domain: &[u8], identifier: &[u8]) -> [u8; 16] {
	use sha2::{Digest, Sha256};

	let mut hasher = Sha256::new();
	hasher.update(domain);
	hasher.update([0]);
	hasher.update(identifier);

	let mut id = [0; 16];
	id.copy_from_slice(&hasher.finalize()[..16]);
	id
}

/// Derive the full 256-bit logical identity for a room's collection group.
///
/// Formula: `BLAKE3-256("mtxdb/group/v1" || room_id)`
#[must_use]
pub fn room_group_full_id(room_id: &[u8]) -> [u8; 32] { derive_group_full_id(room_id) }

/// Derive a 128-bit collection ID for a member collection within a room's group.
///
/// Formula: `BLAKE3-256("mtxdb/member/v1" || member_namespace || group_full_id)[0..16]`
#[must_use]
pub fn room_collection_id(member_namespace: [u8; 4], group_full_id: &[u8; 32]) -> [u8; 16] {
	derive_member_collection_id_from_group(member_namespace, *group_full_id)
		.expect("valid member namespace")
}

/// Derive the State (STAT) collection ID for a room (HAMT nodes + roots).
#[must_use]
pub fn room_state_collection_id(room_id: &[u8]) -> [u8; 16] {
	let group_full = room_group_full_id(room_id);
	room_collection_id(MEMBER_NAMESPACE_STAT, &group_full)
}

/// Derive the Event (EVNT) collection ID for a room (PDU JSON).
#[must_use]
pub fn room_event_collection_id(room_id: &[u8]) -> [u8; 16] {
	let group_full = room_group_full_id(room_id);
	room_collection_id(MEMBER_NAMESPACE_EVNT, &group_full)
}

/// Derive the Prev-edges (PREV) collection ID for a room.
#[must_use]
pub fn room_prev_collection_id(room_id: &[u8]) -> [u8; 16] {
	let group_full = room_group_full_id(room_id);
	room_collection_id(MEMBER_NAMESPACE_PREV, &group_full)
}

/// Derive the Auth-edges (AUTH) collection ID for a room.
#[must_use]
pub fn room_auth_collection_id(room_id: &[u8]) -> [u8; 16] {
	let group_full = room_group_full_id(room_id);
	room_collection_id(MEMBER_NAMESPACE_AUTH, &group_full)
}

/// Derive the room roots collection ID (room-root + event-roots).
///
/// Uses STGP namespace (state-group) for roots per sithnapse convention.
#[must_use]
pub fn room_roots_collection_id(room_id: &[u8]) -> [u8; 16] {
	let group_full = room_group_full_id(room_id);
	room_collection_id(MEMBER_NAMESPACE_STGP, &group_full)
}

/// Derive a HAMT node ID from a 32-byte structural hash.
///
/// Truncates to first 16 bytes. The full hash must be supplied at read time
/// for verification via `decode_v1_verified`.
#[must_use]
pub fn hamt_node_id(structural_hash: &[u8; 32]) -> [u8; 16] {
	let mut id = [0_u8; 16];
	id.copy_from_slice(&structural_hash[..16]);
	id
}

/// Derive an event record ID from an event ID.
///
/// Uses SHA-256 for domain separation from mtxdb's BLAKE3-based collections.
#[must_use]
pub fn event_record_id(event_id: &[u8]) -> [u8; 16] { derive_id(b"event", event_id) }

/// Derive the room current-root node ID (fixed tag within room's STAT collection).
#[must_use]
pub fn room_root_node_id() -> [u8; 16] { derive_id(b"root", b"current") }

/// Derive an event-root node ID within a room's STAT collection.
#[must_use]
pub fn event_root_node_id(event_id: &[u8]) -> [u8; 16] { derive_id(b"event-root", event_id) }

/// Derive the event-locator collection ID (maps event_id -> room_id).
///
/// Global collection (not per-room) so events can be looked up by event_id alone.
#[must_use]
pub fn event_locator_collection_id() -> [u8; 16] { derive_id(b"sys", b"event-locator") }

/// Encode a Matrix state tuple without delimiter ambiguity.
pub fn encode_state_key_v1(event_type: &str, state_key: &str) -> Result<Vec<u8>, &'static str> {
	let event_type_len = u32::try_from(event_type.len()).map_err(|_| "event type is too long")?;
	let state_key_len = u32::try_from(state_key.len()).map_err(|_| "state key is too long")?;

	let capacity = 1_usize
		.checked_add(4)
		.and_then(|size| size.checked_add(event_type.len()))
		.and_then(|size| size.checked_add(4))
		.and_then(|size| size.checked_add(state_key.len()))
		.ok_or("state key is too large")?;

	let mut encoded = Vec::with_capacity(capacity);
	encoded.push(1);
	encoded.extend_from_slice(&event_type_len.to_le_bytes());
	encoded.extend_from_slice(event_type.as_bytes());
	encoded.extend_from_slice(&state_key_len.to_le_bytes());
	encoded.extend_from_slice(state_key.as_bytes());
	Ok(encoded)
}
