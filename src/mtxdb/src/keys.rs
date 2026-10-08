//! Stable identifiers used at the mtxdb boundary.

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
