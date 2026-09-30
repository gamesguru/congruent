use super::{Count, RawId};
pub type ShortRoomId = ShortId;
pub type ShortEventId = ShortId;
pub type ShortStateKey = ShortId;
pub type ShortId = u64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Id {
	pub shortroomid: ShortRoomId,
	pub shorteventid: Count,
}

impl From<RawId> for Id {
	#[inline]
	fn from(raw: RawId) -> Self {
		let shortroomid = u64::from_be_bytes(raw.shortroomid());
		let count_bytes = Count::offset_binary_encoding(raw.shorteventid());
		let shorteventid = Count::from_signed(i64::from_be_bytes(count_bytes));

		Self { shortroomid, shorteventid }
	}
}
