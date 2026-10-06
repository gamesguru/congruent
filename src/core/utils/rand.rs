use std::{
	ops::Range,
	time::{Duration, SystemTime},
};

use arrayvec::ArrayString;
use rand::{RngExt, seq::SliceRandom};

pub fn shuffle<T>(vec: &mut [T]) {
	let mut rng = rand::rng();
	vec.shuffle(&mut rng);
}

pub fn string(length: usize) -> String {
	rand::rng()
		.sample_iter(&rand::distr::Alphanumeric)
		.take(length)
		.map(char::from)
		.collect()
}

/// Generates a fresh random legacy (room version 1-11) room ID,
/// `!<opaque_id>:<server_name>`, where `opaque_id` is a random string.
/// Room version 12+ IDs are derived from the create event hash instead.
#[must_use]
pub fn room_id_v11(server_name: &slipstream::OwnedServerName) -> slipstream::OwnedRoomId {
	slipstream::OwnedRoomId::parse(format!("!{}:{server_name}", string(18)))
		.expect("generated room ID must be valid")
}

#[inline]
pub fn string_array<const LENGTH: usize>() -> ArrayString<LENGTH> {
	let mut ret = ArrayString::<LENGTH>::new();
	rand::rng()
		.sample_iter(&rand::distr::Alphanumeric)
		.take(LENGTH)
		.map(char::from)
		.for_each(|c| ret.push(c));

	ret
}

#[inline]
#[must_use]
pub fn time_from_now_secs(range: Range<u64>) -> SystemTime {
	SystemTime::now()
		.checked_add(secs(range))
		.expect("range does not overflow SystemTime")
}

#[must_use]
pub fn secs(range: Range<u64>) -> Duration { Duration::from_secs(rand::random_range(range)) }
