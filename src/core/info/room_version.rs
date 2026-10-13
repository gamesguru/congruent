//! Room version support

use std::{iter::once, sync::LazyLock};

use slipstream::{RoomVersionId, api::client::discovery::get_capabilities::RoomVersionStability};

use crate::{at, is_equal_to};

/// Supported and stable room versions
pub const STABLE_ROOM_VERSIONS: &[RoomVersionId] = &[
	RoomVersionId::V2,
	RoomVersionId::V6,
	RoomVersionId::V7,
	RoomVersionId::V8,
	RoomVersionId::V9,
	RoomVersionId::V10,
	RoomVersionId::V11,
	RoomVersionId::V12,
];

pub static MSC3389_ROOM_VERSION: LazyLock<RoomVersionId> = LazyLock::new(|| {
	RoomVersionId::try_from("org.matrix.msc3389.10").expect("valid room version")
});

pub static MSC4311_ROOM_VERSION: LazyLock<RoomVersionId> = LazyLock::new(|| {
	RoomVersionId::try_from("org.matrix.msc4311.10").expect("valid room version")
});

/// Experimental, partially supported room versions
pub static UNSTABLE_ROOM_VERSIONS: LazyLock<Vec<RoomVersionId>> = LazyLock::new(|| {
	vec![
		RoomVersionId::V3,
		RoomVersionId::V4,
		RoomVersionId::V5,
		MSC3389_ROOM_VERSION.clone(),
		MSC4311_ROOM_VERSION.clone(),
	]
});

type RoomVersion = (RoomVersionId, RoomVersionStability);

#[inline]
#[must_use]
pub fn is_msc3389(version: &RoomVersionId) -> bool {
	version == &*MSC3389_ROOM_VERSION || version.as_str() == "org.matrix.msc3389.10"
}

#[inline]
#[must_use]
pub fn is_msc4311(version: &RoomVersionId) -> bool { version == &*MSC4311_ROOM_VERSION }

#[inline]
#[must_use]
pub fn has_msc4311_stripped_state_validation(version: &RoomVersionId) -> bool {
	is_msc4311(version) || *version == RoomVersionId::V12
}

impl crate::Server {
	#[inline]
	pub fn supported_room_version(&self, version: &RoomVersionId) -> bool {
		self.supported_room_versions().any(is_equal_to!(*version))
	}

	#[inline]
	pub fn supported_room_versions(&self) -> impl Iterator<Item = RoomVersionId> + '_ {
		Self::available_room_versions()
			.filter(|(_, stability)| self.supported_stability(*stability))
			.map(at!(0))
	}

	#[inline]
	pub fn available_room_versions() -> impl Iterator<Item = RoomVersion> {
		available_room_versions()
	}

	#[inline]
	fn supported_stability(&self, stability: RoomVersionStability) -> bool {
		self.config.allow_unstable_room_versions || stability == RoomVersionStability::Stable
	}
}

pub fn available_room_versions() -> impl Iterator<Item = RoomVersion> {
	let unstable_room_versions = UNSTABLE_ROOM_VERSIONS
		.iter()
		.cloned()
		.zip(once(RoomVersionStability::Unstable).cycle());

	STABLE_ROOM_VERSIONS
		.iter()
		.cloned()
		.zip(once(RoomVersionStability::Stable).cycle())
		.chain(unstable_room_versions)
}
