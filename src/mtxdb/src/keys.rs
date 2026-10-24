//! Stable identifiers used at the mtxdb boundary.
//!
//! The byte-exact derivations are owned by [`mtxdb::interop`]; this module
//! re-exports them under the names the server already uses, so the server
//! and the importer can never drift apart.

pub use mtxdb::interop::{
	EVENT_LOCATOR_BUCKETS, derive_id, event_locator_collection_id, event_record_id,
	event_state_root_node_id as event_root_node_id, event_verdict_collection_id,
	room_auth_collection_id, room_event_collection_id, room_prev_collection_id,
	room_state_root_node_id as room_root_node_id, server_keys_collection_id,
};
use mtxdb::{
	MEMBER_NAMESPACE_STAT, MEMBER_NAMESPACE_STGP, derive_group_full_id,
	derive_member_collection_id_from_group,
};

/// Derive a signing-key record id for a particular record kind and origin.
/// The Notary id retains mtxdb's established derivation; the other records
/// use disjoint domains in the shared server-keys collection.
#[must_use]
pub fn server_key_record_id(kind: crate::server_keys::KeyRecordKind, origin: &[u8]) -> [u8; 16] {
	use crate::server_keys::KeyRecordKind;
	match kind {
		| KeyRecordKind::Notary => mtxdb::interop::notary_record_id(origin),
		| KeyRecordKind::Historical => derive_id(b"server-key-historical", origin),
		| KeyRecordKind::Provisional => derive_id(b"server-key-provisional", origin),
		| KeyRecordKind::Corroborated => derive_id(b"server-key-corroborated", origin),
	}
}

/// Derive the pre-native-layout signing-key record ID. Legacy IDs used the
/// shared `server-key` domain and `kind NUL origin` as their input.
#[must_use]
pub fn legacy_server_key_record_id(
	kind: crate::server_keys::KeyRecordKind,
	origin: &[u8],
) -> [u8; 16] {
	let mut input = Vec::with_capacity(
		kind.as_str()
			.len()
			.saturating_add(1)
			.saturating_add(origin.len()),
	);
	input.extend_from_slice(kind.as_str().as_bytes());
	input.push(0);
	input.extend_from_slice(origin);
	derive_id(b"server-key", &input)
}

/// Derive the full 256-bit logical identity for a room's collection group.
///
/// Formula: `BLAKE3-256("mtxdb/group/v1" || room_id)`
#[must_use]
pub fn room_group_full_id(room_id: &[u8]) -> [u8; 32] { derive_group_full_id(room_id) }

/// Derive a 128-bit collection ID for a member collection within a room's group.
///
/// Formula: `wrapping_add_le(group_full_id, NAMESPACE_BIAS[member_namespace])[0..16]`
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn room_collection_ids_are_deterministic() {
		let room = b"!room:example.com";
		assert_eq!(room_state_collection_id(room), room_state_collection_id(room));
		assert_eq!(room_roots_collection_id(room), room_roots_collection_id(room));
		assert_eq!(room_event_collection_id(room), room_event_collection_id(room));
		assert_eq!(room_prev_collection_id(room), room_prev_collection_id(room));
		assert_eq!(room_auth_collection_id(room), room_auth_collection_id(room));
	}

	#[test]
	fn room_collection_kinds_do_not_alias() {
		let room = b"!room:example.com";
		let ids = [
			room_state_collection_id(room),
			room_roots_collection_id(room),
			room_event_collection_id(room),
			room_prev_collection_id(room),
			room_auth_collection_id(room),
		];
		for (i, a) in ids.iter().enumerate() {
			for b in ids.iter().skip(i + 1) {
				assert_ne!(a, b, "two collection kinds for one room share an id");
			}
		}
	}

	#[test]
	fn room_collection_ids_differ_across_rooms() {
		let a = b"!a:example.com";
		let b = b"!b:example.com";
		assert_ne!(room_state_collection_id(a), room_state_collection_id(b));
		assert_ne!(room_roots_collection_id(a), room_roots_collection_id(b));
		assert_ne!(room_event_collection_id(a), room_event_collection_id(b));
		assert_ne!(room_prev_collection_id(a), room_prev_collection_id(b));
		assert_ne!(room_auth_collection_id(a), room_auth_collection_id(b));
	}

	#[test]
	fn hamt_node_id_is_the_structural_hash_prefix() {
		let mut hash = [0_u8; 32];
		for (i, byte) in hash.iter_mut().enumerate() {
			*byte = u8::try_from(i).unwrap();
		}
		assert_eq!(hamt_node_id(&hash), hash[..16]);

		// Hashes that share a 16-byte prefix collide; the read path must
		// verify the full hash, so make the aliasing explicit here.
		let mut other = hash;
		other[31] ^= 0xFF;
		assert_eq!(hamt_node_id(&hash), hamt_node_id(&other));
	}

	#[test]
	fn root_node_ids_are_distinct() {
		let event = b"$event:example.com";
		assert_ne!(room_root_node_id(), event_root_node_id(event));
		assert_ne!(event_root_node_id(event), event_root_node_id(b"$other:example.com"));
		assert_ne!(event_record_id(event), event_root_node_id(event));
	}

	#[test]
	fn derive_id_separates_domains() {
		assert_ne!(derive_id(b"event", b"x"), derive_id(b"root", b"x"));
		assert_eq!(derive_id(b"event", b"x"), derive_id(b"event", b"x"));
	}

	#[test]
	fn legacy_server_key_ids_use_kind_null_origin() {
		use crate::server_keys::KeyRecordKind;

		for kind in [
			KeyRecordKind::Historical,
			KeyRecordKind::Provisional,
			KeyRecordKind::Corroborated,
			KeyRecordKind::Notary,
		] {
			let mut legacy_input = kind.as_str().as_bytes().to_vec();
			legacy_input.push(0);
			legacy_input.extend_from_slice(b"example.org");
			assert_eq!(
				legacy_server_key_record_id(kind, b"example.org"),
				derive_id(b"server-key", &legacy_input)
			);
		}
	}

	#[test]
	fn event_locator_buckets_are_stable_and_bounded() {
		let node = event_record_id(b"$event:example.com");
		assert_eq!(event_locator_collection_id(&node), event_locator_collection_id(&node));

		let mut seen = std::collections::HashSet::new();
		for i in 0_u32..4096 {
			let node = event_record_id(format!("$event{i}:example.com").as_bytes());
			seen.insert(event_locator_collection_id(&node));
		}
		let buckets = usize::try_from(EVENT_LOCATOR_BUCKETS).unwrap();
		assert!(seen.len() <= buckets, "more locator collections than buckets");
		assert!(seen.len() > buckets / 2, "locator ids should spread across buckets");
	}
}
