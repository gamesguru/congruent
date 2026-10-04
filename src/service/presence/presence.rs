use conduwuit::{Error, Result, utils};
use slipstream::{
	UInt, UserId,
	codec::{Deserialize, Serialize},
	events::presence::{PresenceEvent, PresenceEventContent},
	presence::PresenceState,
};

use crate::users;

/// Represents data required to be kept in order to implement the presence
/// specification.
#[derive(Debug, Clone)]
pub(super) struct Presence {
	pub(super) state: PresenceState,
	pub(super) currently_active: bool,
	pub(super) last_active_ts: u64,
	pub(super) status_msg: Option<String>,
}

impl Serialize for Presence {
	fn to_json(&self) -> slipstream::json::Value {
		slipstream::json::Value::Object(
			[
				("state".into(), self.state.to_json()),
				("currently_active".into(), self.currently_active.to_json()),
				("last_active_ts".into(), self.last_active_ts.to_json()),
				("status_msg".into(), self.status_msg.to_json()),
			]
			.into_iter()
			.collect(),
		)
	}
}

impl Deserialize for Presence {
	fn from_json(value: &slipstream::json::Value) -> Result<Self, slipstream::codec::DeError> {
		let object = value
			.as_object()
			.ok_or_else(|| slipstream::codec::DeError::expected("object"))?;
		Ok(Self {
			state: PresenceState::from_json(
				object
					.get("state")
					.ok_or_else(|| slipstream::codec::DeError::expected("state"))?,
			)?,
			currently_active: bool::from_json(
				object
					.get("currently_active")
					.ok_or_else(|| slipstream::codec::DeError::expected("currently_active"))?,
			)?,
			last_active_ts: u64::from_json(
				object
					.get("last_active_ts")
					.ok_or_else(|| slipstream::codec::DeError::expected("last_active_ts"))?,
			)?,
			status_msg: Option::<String>::from_json(
				object
					.get("status_msg")
					.unwrap_or(&slipstream::json::Value::Null),
			)?,
		})
	}
}

impl Presence {
	#[must_use]
	pub(super) fn new(
		state: PresenceState,
		currently_active: bool,
		last_active_ts: u64,
		status_msg: Option<String>,
	) -> Self {
		Self {
			state,
			currently_active,
			last_active_ts,
			status_msg,
		}
	}

	pub(super) fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
		slipstream::codec::from_str(
			std::str::from_utf8(bytes)
				.map_err(|_| Error::bad_database("Invalid presence data in database"))?,
		)
		.map_err(|_| Error::bad_database("Invalid presence data in database"))
	}

	/// Creates a PresenceEvent from available data.
	pub(super) async fn to_presence_event(
		&self,
		user_id: &UserId,
		users: &users::Service,
	) -> PresenceEvent {
		let now = utils::millis_since_unix_epoch();
		let last_active_ago = Some(now.saturating_sub(self.last_active_ts));

		PresenceEvent {
			sender: user_id.to_owned(),
			content: PresenceEventContent {
				presence: self.state.clone(),
				status_msg: self.status_msg.clone(),
				currently_active: Some(self.currently_active),
				last_active_ago,
				displayname: users.displayname(user_id).await.ok(),
				avatar_url: users.avatar_url(user_id).await.ok(),
			},
			origin_server_ts: None,
		}
	}
}
