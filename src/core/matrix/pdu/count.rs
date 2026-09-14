#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss, clippy::as_conversions)]

use std::{
	cmp::Ordering,
	fmt::{self, Display},
	str::FromStr,
};

use ruma::api::Direction;

use crate::{Error, Result, err};

#[derive(Hash, PartialEq, Eq, Clone, Copy, Debug)]
pub enum Count {
	Normal(u64),
	Backfilled(i64),
}

impl Count {
	#[inline]
	#[must_use]
	pub fn from_unsigned(unsigned: u64) -> Self { Self::from_signed(unsigned as i64) }

	#[inline]
	#[must_use]
	pub fn from_signed(signed: i64) -> Self {
		match signed {
			| i64::MIN..=0 => Self::Backfilled(signed),
			| _ => Self::Normal(signed as u64),
		}
	}

	#[inline]
	#[must_use]
	pub fn into_unsigned(self) -> u64 {
		self.debug_assert_valid();
		match self {
			| Self::Normal(i) => i,
			| Self::Backfilled(i) => i as u64,
		}
	}

	#[inline]
	#[must_use]
	pub fn into_signed(self) -> i64 {
		self.debug_assert_valid();
		match self {
			| Self::Normal(i) => i as i64,
			| Self::Backfilled(i) => i,
		}
	}

	#[inline]
	#[must_use]
	pub fn into_normal(self) -> Self {
		self.debug_assert_valid();
		match self {
			| Self::Normal(i) => Self::Normal(i),
			| Self::Backfilled(_) => Self::Normal(0),
		}
	}

	#[inline]
	pub fn checked_inc(self, dir: Direction) -> Result<Self, Error> {
		match dir {
			| Direction::Forward => self.checked_add(1),
			| Direction::Backward => self.checked_sub(1),
		}
	}

	#[inline]
	pub fn checked_add(self, add: u64) -> Result<Self, Error> {
		Ok(match self {
			| Self::Normal(i) => Self::Normal(
				i.checked_add(add)
					.ok_or_else(|| err!(Arithmetic("Count::Normal overflow")))?,
			),
			| Self::Backfilled(i) => Self::Backfilled(
				i.checked_add(add as i64)
					.ok_or_else(|| err!(Arithmetic("Count::Backfilled overflow")))?,
			),
		})
	}

	#[inline]
	pub fn checked_sub(self, sub: u64) -> Result<Self, Error> {
		Ok(match self {
			| Self::Normal(i) => Self::Normal(
				i.checked_sub(sub)
					.ok_or_else(|| err!(Arithmetic("Count::Normal underflow")))?,
			),
			| Self::Backfilled(i) => Self::Backfilled(
				i.checked_sub(sub as i64)
					.ok_or_else(|| err!(Arithmetic("Count::Backfilled underflow")))?,
			),
		})
	}

	#[inline]
	#[must_use]
	pub fn saturating_inc(self, dir: Direction) -> Self {
		match dir {
			| Direction::Forward => self.saturating_add(1),
			| Direction::Backward => self.saturating_sub(1),
		}
	}

	#[inline]
	#[must_use]
	pub fn saturating_add(self, add: u64) -> Self {
		match self {
			| Self::Normal(i) => Self::Normal(i.saturating_add(add).min(i64::MAX as u64)),
			| Self::Backfilled(i) => {
				let add = i64::try_from(add).unwrap_or(i64::MAX);
				Self::from_signed(i.saturating_add(add))
			},
		}
	}

	#[inline]
	#[must_use]
	pub fn saturating_sub(self, sub: u64) -> Self {
		match self {
			| Self::Normal(i) =>
				if let Some(res) = i.checked_sub(sub) {
					Self::Normal(res)
				} else {
					Self::Backfilled(0_i64.saturating_sub(sub.saturating_sub(i) as i64))
				},
			| Self::Backfilled(i) => Self::Backfilled(i.saturating_sub(sub as i64)),
		}
	}

	#[inline]
	#[must_use]
	pub const fn min() -> Self { Self::Backfilled(i64::MIN) }

	#[inline]
	#[must_use]
	pub const fn max() -> Self { Self::Normal(i64::MAX as u64) }

	#[inline]
	pub(crate) fn debug_assert_valid(&self) {
		if let Self::Backfilled(i) = self {
			debug_assert!(*i <= 0, "Backfilled sequence must be negative");
		}
	}

	/// Applies offset binary encoding (flips the sign bit) to a byte
	/// representation of an i64 or u64 so that it sorts correctly in RocksDB
	/// unsigned byte comparison. This operation is its own inverse, so it is
	/// used for both encoding and decoding.
	#[inline]
	#[must_use]
	pub fn offset_binary_encoding(mut count_bytes: [u8; 8]) -> [u8; 8] {
		count_bytes[0] ^= 0x80;
		count_bytes
	}
}

impl Display for Count {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
		self.debug_assert_valid();
		match self {
			| Self::Normal(i) => write!(f, "{i}"),
			| Self::Backfilled(i) => write!(f, "{i}"),
		}
	}
}

impl From<i64> for Count {
	#[inline]
	fn from(signed: i64) -> Self { Self::from_signed(signed) }
}

impl From<u64> for Count {
	#[inline]
	fn from(unsigned: u64) -> Self { Self::from_unsigned(unsigned) }
}

impl FromStr for Count {
	type Err = Error;

	fn from_str(token: &str) -> Result<Self, Self::Err> { Ok(Self::from_signed(token.parse()?)) }
}

impl PartialOrd for Count {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}

impl Ord for Count {
	fn cmp(&self, other: &Self) -> Ordering { self.into_signed().cmp(&other.into_signed()) }
}

impl Default for Count {
	fn default() -> Self { Self::Normal(0) }
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Backfilled events must always sort before Normal events in the
	/// timeline ordering.  The sync early-return in `load_timeline` relies on
	/// `last_timeline_count <= starting_count` to skip rooms with no new
	/// activity.  If `last_timeline_count` returns a Backfilled count, it
	/// must be less than any Normal sync token so the room is skipped.
	#[test]
	fn backfilled_is_less_than_normal() {
		assert!(Count::Backfilled(-1) < Count::Normal(0));
		assert!(Count::Backfilled(-1) < Count::Normal(1));
		assert!(Count::Backfilled(0) < Count::Normal(1));
		assert!(Count::Backfilled(i64::MIN) < Count::Normal(0));
	}

	/// `Count::min()` must be strictly less than any realistic Normal sync
	/// token so that `last_timeline_count` returning `min()` for empty rooms
	/// always triggers the sync early-return path.
	#[test]
	fn min_is_less_than_any_normal_token() {
		assert!(Count::min() < Count::Normal(0));
		assert!(Count::min() < Count::Normal(1));
		assert!(Count::min() < Count::Normal(u64::MAX / 2));
		assert!(Count::min() <= Count::Backfilled(-1));
	}

	/// `Count::max()` must be strictly greater than any realistic Normal sync
	/// token.  Previously `last_timeline_count` incorrectly returned `max()`
	/// for backfilled-only rooms, which defeated the sync early-return check
	/// and caused massive log spam.
	#[test]
	fn max_is_greater_than_any_normal_token() {
		assert!(Count::max() > Count::Normal(0));
		assert!(Count::max() > Count::Normal(26_400_000));
		assert!(Count::max() > Count::Backfilled(-1));
		assert!(Count::max() > Count::min());
	}

	/// Verify the sync early-return invariant directly:
	/// `last_timeline_count <= starting_count` must be true when the room's
	/// latest event is Backfilled and the client's sync token is Normal.
	#[test]
	fn sync_early_return_skips_backfilled_rooms() {
		let starting_count = Count::Normal(26_400_689); // typical sync token
		let last_backfilled = Count::Backfilled(-100); // room with only backfilled events
		let last_empty = Count::min(); // room with no events at all

		assert!(
			last_backfilled <= starting_count,
			"backfilled-only rooms must trigger sync early return"
		);
		assert!(last_empty <= starting_count, "empty rooms must trigger sync early return");
	}

	/// Verify that a room with recent Normal activity is NOT skipped.
	#[test]
	fn sync_early_return_does_not_skip_active_rooms() {
		let starting_count = Count::Normal(26_400_689);
		let last_active = Count::Normal(26_400_692); // newer than sync token

		assert!(last_active > starting_count, "active rooms must NOT trigger sync early return");
	}
}

use std::array::TryFromSliceError;

/// Unified timeline coordinate representing both live and backfill events.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimelineKey {
	/// True Matrix DAG depth (`topological_ordering`).
	pub depth: u64,
	/// Monotonic sequence (`stream_ordering`): positive for live, negative for
	/// backfill.
	pub stream_ordering: i64,
}

impl TimelineKey {
	#[inline]
	#[must_use]
	pub fn new(depth: u64, stream_ordering: i64) -> Self { Self { depth, stream_ordering } }

	/// Encodes into a 16-byte big-endian array optimized for lexicographical
	/// range scans (`ORDER BY depth ASC, stream_ordering ASC`).
	#[inline]
	#[must_use]
	pub fn to_be_bytes(&self) -> [u8; 16] {
		let mut buf = [0_u8; 16];

		// Primary Axis: Depth (ASC)
		buf[0..8].copy_from_slice(&self.depth.to_be_bytes());

		// Secondary Axis: Offset-Binary Stream Ordering (ASC)
		let offset_binary = (self.stream_ordering as u64) ^ 0x8000_0000_0000_0000;
		buf[8..16].copy_from_slice(&offset_binary.to_be_bytes());

		buf
	}

	/// Decodes a 16-byte KV slice back into a TimelineKey during pagination.
	#[inline]
	#[must_use]
	pub fn from_bytes(bytes: &[u8; 16]) -> Self {
		let mut depth_bytes = [0_u8; 8];
		depth_bytes.copy_from_slice(&bytes[0..8]);
		let depth = u64::from_be_bytes(depth_bytes);

		let mut stream_bytes = [0_u8; 8];
		stream_bytes.copy_from_slice(&bytes[8..16]);
		let offset_binary = u64::from_be_bytes(stream_bytes);

		let stream_ordering = (offset_binary ^ 0x8000_0000_0000_0000) as i64;

		Self { depth, stream_ordering }
	}
}

impl TryFrom<&[u8]> for TimelineKey {
	type Error = TryFromSliceError;

	fn try_from(slice: &[u8]) -> Result<Self, Self::Error> {
		let bytes: &[u8; 16] = slice.try_into()?;
		Ok(Self::from_bytes(bytes))
	}
}
