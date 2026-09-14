use super::{Count, Id, ShortEventId, ShortId, ShortRoomId};

/// A raw database key representation of a PduId.
/// Uses offset binary encoding to ensure that Backfilled (negative) counts
/// lexicographically sort before Normal (positive) counts in unsigned byte
/// comparisons. This guarantees a uniform 16-byte layout for all PDU IDs.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RawId(pub [u8; 16]);

const INT_LEN: usize = size_of::<ShortId>();

impl RawId {
	pub const LEN: usize = size_of::<ShortRoomId>() + size_of::<ShortEventId>();

	#[inline]
	#[must_use]
	pub fn pdu_count(&self) -> Count {
		let id: Id = (*self).into();
		id.shorteventid
	}

	#[inline]
	#[must_use]
	pub fn shortroomid(self) -> [u8; INT_LEN] {
		self.0[0..INT_LEN]
			.try_into()
			.expect("shortroomid array from slice")
	}

	#[inline]
	#[must_use]
	pub fn shorteventid(self) -> [u8; INT_LEN] {
		self.0[INT_LEN..INT_LEN * 2]
			.try_into()
			.expect("shorteventid array from slice")
	}

	/// Returns the canonical 16-byte key [shortroomid(8) |
	/// offset_binary_encoded_count(8)].
	#[inline]
	#[must_use]
	pub fn to_short_key(self) -> [u8; 16] { self.0 }

	#[inline]
	#[must_use]
	pub fn as_bytes(&self) -> &[u8] { &self.0 }
}

impl AsRef<[u8]> for RawId {
	#[inline]
	fn as_ref(&self) -> &[u8] { &self.0 }
}

impl From<&[u8]> for RawId {
	#[inline]
	fn from(id: &[u8]) -> Self {
		assert_eq!(id.len(), Self::LEN, "RawId must be exactly {} bytes", Self::LEN);
		Self(id.try_into().expect("RawId from [u8]"))
	}
}

impl From<Id> for RawId {
	#[inline]
	fn from(id: Id) -> Self {
		let mut key = [0_u8; 16];
		key[..8].copy_from_slice(&id.shortroomid.to_be_bytes());
		key[8..].copy_from_slice(&Count::offset_binary_encoding(
			id.shorteventid.into_signed().to_be_bytes(),
		));
		Self(key)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::matrix::pdu::ShortRoomId;

	#[test]
	fn test_offset_binary_encoding_sorting() {
		// We want to verify that backfilled counts sort before normal counts
		// lexicographically
		let room_id: ShortRoomId = 42;

		let backfill_earlier = Id {
			shortroomid: room_id,
			shorteventid: Count::Backfilled(-100),
		};
		let backfill_later = Id {
			shortroomid: room_id,
			shorteventid: Count::Backfilled(-1),
		};
		let norm_earlier = Id {
			shortroomid: room_id,
			shorteventid: Count::Normal(1),
		};
		let norm_later = Id {
			shortroomid: room_id,
			shorteventid: Count::Normal(100),
		};

		let raw_bf_far: RawId = backfill_earlier.into();
		let raw_bf_rec: RawId = backfill_later.into();
		let raw_norm_first: RawId = norm_earlier.into();
		let raw_norm_later: RawId = norm_later.into();

		assert!(raw_bf_far.as_bytes() < raw_bf_rec.as_bytes());
		assert!(raw_bf_rec.as_bytes() < raw_norm_first.as_bytes());
		assert!(raw_norm_first.as_bytes() < raw_norm_later.as_bytes());

		// Verify sizes
		assert_eq!(raw_bf_far.as_bytes().len(), 16);
		assert_eq!(raw_norm_later.as_bytes().len(), 16);
	}

	#[test]
	fn test_roundtrip() {
		let room_id: ShortRoomId = 12345;

		let backfill = Id {
			shortroomid: room_id,
			shorteventid: Count::Backfilled(-42),
		};
		let normal = Id {
			shortroomid: room_id,
			shorteventid: Count::Normal(42),
		};

		let raw_backfill: RawId = backfill.into();
		let raw_normal: RawId = normal.into();

		let id_backfill: Id = raw_backfill.into();
		let id_normal: Id = raw_normal.into();

		assert_eq!(id_backfill.shorteventid, backfill.shorteventid);
		assert_eq!(id_normal.shorteventid, normal.shorteventid);
	}
}
