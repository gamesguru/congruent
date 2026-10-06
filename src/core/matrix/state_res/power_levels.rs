use std::collections::BTreeMap;

use slipstream::{
	Int, OwnedUserId, UserId,
	codec::DeError,
	events::{TimelineEventType, room::power_levels::RoomPowerLevelsEventContent},
	json::Value,
	power_levels::{NotificationPowerLevels, default_power_level},
};

use super::{
	Result, RoomVersion,
	content::{decode, object},
};
use crate::error;

/// How strictly power level values are read.
#[derive(Clone, Copy)]
enum Mode {
	/// Integers only (room versions with integer power levels).
	Integer,
	/// Integers or integer strings (legacy room versions).
	Legacy,
}

fn level(value: &Value, mode: Mode) -> Result<Int, DeError> {
	match mode {
		| Mode::Integer => value.as_i64(),
		| Mode::Legacy => value
			.as_i64()
			.or_else(|| value.as_str().and_then(|value| value.parse().ok())),
	}
	.ok_or_else(|| DeError::expected("power level"))
}

/// A power level that takes `default` when the field is absent.
fn level_or(content: &Value, key: &str, default: Int, mode: Mode) -> Result<Int, DeError> {
	content
		.get(key)
		.map_or(Ok(default), |value| level(value, mode))
}

/// The `users` map as a vec sorted by user ID. Values are always read
/// leniently, as strings were accepted here even for integer power levels.
fn users(content: &Value) -> Result<Vec<(OwnedUserId, Int)>, DeError> {
	let Some(users) = content.get("users") else {
		return Ok(Vec::new());
	};
	let object = users
		.as_object()
		.ok_or_else(|| DeError::expected("power-level object"))?;

	object
		.iter()
		.map(|(user, value)| {
			Ok((OwnedUserId::from(user.as_str()), level(value, Mode::Legacy)?))
		})
		.collect()
}

fn map_or_default<K, V>(content: &Value, key: &str) -> Result<BTreeMap<K, V>, DeError>
where
	K: slipstream::codec::Deserialize + Ord,
	V: slipstream::codec::Deserialize,
{
	content.get(key).map_or_else(|| Ok(BTreeMap::new()), decode)
}

fn int_room_power_levels(content: &str) -> Result<RoomPowerLevelsEventContent, DeError> {
	let content = object(content)?;
	let mode = Mode::Integer;

	let notifications = match content.get("notifications") {
		| None => default_power_level(),
		| Some(notifications) => {
			if notifications.as_object().is_none() {
				return Err(DeError::expected("notifications object"));
			}
			level_or(notifications, "room", default_power_level(), mode)?
		},
	};

	let mut pl = RoomPowerLevelsEventContent::new();
	pl.ban = level_or(&content, "ban", default_power_level(), mode)?;
	pl.events = map_or_default::<TimelineEventType, Int>(&content, "events")?;
	pl.events_default = level_or(&content, "events_default", 0, mode)?;
	pl.invite = level_or(&content, "invite", 0, mode)?;
	pl.kick = level_or(&content, "kick", default_power_level(), mode)?;
	pl.redact = level_or(&content, "redact", default_power_level(), mode)?;
	pl.state_default = level_or(&content, "state_default", default_power_level(), mode)?;
	pl.users = map_or_default::<OwnedUserId, Int>(&content, "users")?;
	pl.users_default = level_or(&content, "users_default", 0, mode)?;

	let mut notif = NotificationPowerLevels::new();
	notif.room = notifications;
	pl.notifications = notif;

	Ok(pl)
}

#[inline]
pub(crate) fn deserialize_power_levels(
	content: &str,
	room_version: &RoomVersion,
) -> Option<RoomPowerLevelsEventContent> {
	if room_version.integer_power_levels {
		deserialize_integer_power_levels(content)
	} else {
		deserialize_legacy_power_levels(content)
	}
}

fn deserialize_integer_power_levels(content: &str) -> Option<RoomPowerLevelsEventContent> {
	match int_room_power_levels(content) {
		| Ok(content) => Some(content),
		| Err(_) => {
			error!("m.room.power_levels event is not valid with integer values");
			None
		},
	}
}

fn deserialize_legacy_power_levels(content: &str) -> Option<RoomPowerLevelsEventContent> {
	match slipstream::codec::from_str(content) {
		| Ok(content) => Some(content),
		| Err(_) => {
			error!(
				"m.room.power_levels event is not valid with integer or string integer values"
			);
			None
		},
	}
}

pub(crate) struct PowerLevelsContentFields {
	pub(crate) users: Vec<(OwnedUserId, Int)>,

	pub(crate) users_default: Int,
}

impl PowerLevelsContentFields {
	/// Reads the fields leniently (integers or integer strings).
	pub(crate) fn parse(content: &str) -> Result<Self, DeError> {
		Self::from_content(&object(content)?, Mode::Legacy)
	}

	fn from_content(content: &Value, mode: Mode) -> Result<Self, DeError> {
		Ok(Self {
			users: users(content)?,
			users_default: level_or(content, "users_default", 0, mode)?,
		})
	}

	pub(crate) fn get_user_power(&self, user_id: &UserId) -> Option<&Int> {
		let comparator = |item: &(OwnedUserId, Int)| {
			let item: &UserId = &item.0;
			item.cmp(user_id)
		};

		self.users
			.binary_search_by(comparator)
			.ok()
			.and_then(|idx| self.users.get(idx).map(|item| &item.1))
	}
}

#[inline]
pub(crate) fn deserialize_power_levels_content_fields(
	content: &str,
	room_version: &RoomVersion,
) -> Result<PowerLevelsContentFields, DeError> {
	let mode = if room_version.integer_power_levels { Mode::Integer } else { Mode::Legacy };
	PowerLevelsContentFields::from_content(&object(content)?, mode)
}

pub(crate) struct PowerLevelsContentInvite {
	pub(crate) invite: Int,
}

pub(crate) fn deserialize_power_levels_content_invite(
	content: &str,
	room_version: &RoomVersion,
) -> Result<PowerLevelsContentInvite, DeError> {
	let mode = if room_version.integer_power_levels { Mode::Integer } else { Mode::Legacy };
	Ok(PowerLevelsContentInvite {
		invite: level_or(&object(content)?, "invite", 0, mode)?,
	})
}

pub(crate) struct PowerLevelsContentRedact {
	pub(crate) redact: Int,
}

pub(crate) fn deserialize_power_levels_content_redact(
	content: &str,
	room_version: &RoomVersion,
) -> Result<PowerLevelsContentRedact, DeError> {
	let mode = if room_version.integer_power_levels { Mode::Integer } else { Mode::Legacy };
	Ok(PowerLevelsContentRedact {
		redact: level_or(&object(content)?, "redact", default_power_level(), mode)?,
	})
}
