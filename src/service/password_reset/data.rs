use std::{
	sync::Arc,
	time::{Duration, SystemTime},
};

use conduwuit::utils::{ReadyExt, stream::TryExpect};
use database::{Database, Deserialized, Json, Map};
use slipstream::{
	OwnedUserId, UserId,
	codec::{Deserialize, Serialize},
};

pub(super) struct Data {
	passwordresettoken_info: Arc<Map>,
}

#[derive(Debug)]
pub struct ResetTokenInfo {
	pub user: OwnedUserId,
	pub issued_at: SystemTime,
}

impl Serialize for ResetTokenInfo {
	fn to_json(&self) -> slipstream::json::Value {
		slipstream::json::Value::Object(
			[
				("user".into(), self.user.to_json()),
				(
					"issued_at".into(),
					slipstream::json::Value::Number(slipstream::json::Number::from(
						self.issued_at
							.duration_since(SystemTime::UNIX_EPOCH)
							.unwrap_or_default()
							.as_millis() as u64,
					)),
				),
			]
			.into_iter()
			.collect(),
		)
	}
}

impl Deserialize for ResetTokenInfo {
	fn from_json(value: &slipstream::json::Value) -> Result<Self, slipstream::codec::DeError> {
		let object = value
			.as_object()
			.ok_or_else(|| slipstream::codec::DeError::expected("object"))?;
		let issued_at = object
			.get("issued_at")
			.and_then(slipstream::json::Value::as_u64)
			.ok_or_else(|| slipstream::codec::DeError::expected("issued_at"))?;
		Ok(Self {
			user: OwnedUserId::from_json(
				object
					.get("user")
					.ok_or_else(|| slipstream::codec::DeError::expected("user"))?,
			)?,
			issued_at: SystemTime::UNIX_EPOCH + Duration::from_millis(issued_at),
		})
	}
}

impl ResetTokenInfo {
	// one hour
	const MAX_TOKEN_AGE: Duration = Duration::from_hours(1);

	pub fn is_valid(&self) -> bool {
		let now = SystemTime::now();

		now.duration_since(self.issued_at)
			.is_ok_and(|duration| duration < Self::MAX_TOKEN_AGE)
	}
}

impl Data {
	pub(super) fn new(db: &Arc<Database>) -> Self {
		Self {
			passwordresettoken_info: db["passwordresettoken_info"].clone(),
		}
	}

	/// Associate a reset token with its info in the database.
	pub(super) fn save_token(&self, token: &str, info: &ResetTokenInfo) {
		self.passwordresettoken_info.raw_put(token, Json(info));
	}

	/// Lookup the info for a reset token.
	pub(super) async fn lookup_token_info(&self, token: &str) -> Option<ResetTokenInfo> {
		self.passwordresettoken_info
			.get(token)
			.await
			.deserialized()
			.ok()
	}

	/// Find a user's existing reset token, if any.
	pub(super) async fn find_token_for_user(
		&self,
		user: &UserId,
	) -> Option<(String, ResetTokenInfo)> {
		self.passwordresettoken_info
			.stream::<'_, String, ResetTokenInfo>()
			.expect_ok()
			.ready_find(|(_, info)| info.user == user)
			.await
	}

	/// Remove a reset token.
	pub(super) fn remove_token(&self, token: &str) { self.passwordresettoken_info.remove(token); }
}

database::codec_value_impls!(ResetTokenInfo);
