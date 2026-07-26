use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use rezzy::LtHash;

/// Converts an LtHash into a little-endian byte vector.
#[must_use]
#[inline]
pub fn lthash_to_bytes(lthash: &LtHash) -> Vec<u8> {
	let mut bytes = vec![0_u8; lthash.0.len().saturating_mul(2)];
	for (i, val) in lthash.0.iter().enumerate() {
		let le = val.to_le_bytes();
		let idx = i.saturating_mul(2);
		bytes[idx] = le[0];
		bytes[idx.saturating_add(1)] = le[1];
	}
	bytes
}

/// Restores an LtHash from a little-endian byte slice.
#[must_use]
#[inline]
pub fn lthash_from_bytes(bytes: &[u8]) -> Option<LtHash> {
	if bytes.len() != 2048 {
		return None;
	}
	let mut arr = [0_u16; 1024];
	for (i, chunk) in bytes.chunks_exact(2).enumerate() {
		arr[i] = u16::from_le_bytes([chunk[0], chunk[1]]);
	}
	Some(LtHash(arr))
}

/// Serializes an LtHash into a `(lattice, digest)` pair.
#[must_use]
pub fn serialize_lthash(lthash: &LtHash) -> (String, String) {
	let bytes = lthash_to_bytes(lthash);
	let lattice = URL_SAFE_NO_PAD.encode(&bytes);

	let mut digest = String::with_capacity(64);
	for b in lthash.checksum() {
		use std::fmt::Write;
		let _ = write!(&mut digest, "{b:02x}");
	}

	(lattice, digest)
}
