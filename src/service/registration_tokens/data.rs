use std::{sync::Arc, time::SystemTime};

use conduwuit::utils::{
	self,
	stream::{ReadyExt, TryIgnore},
};
use database::{Database, Deserialized, Json, Map};
use futures::Stream;
use slipstream::{
	OwnedUserId,
	codec::{DeError, Deserialize, Serialize},
	endpoint::body_field,
	json::{Object, Value},
};

pub(super) struct Data {
	registrationtoken_info: Arc<Map>,
}

/// Metadata of a registration token.
#[derive(Debug)]
pub struct DatabaseTokenInfo {
	/// The admin user who created this token.
	pub creator: OwnedUserId,
	/// The number of times this token has been used to create an account.
	pub uses: u64,
	/// When this token will expire, if it expires.
	pub expires: Option<TokenExpires>,
}

impl DatabaseTokenInfo {
	pub(super) fn new(creator: OwnedUserId, expires: Option<TokenExpires>) -> Self {
		Self { creator, uses: 0, expires }
	}

	/// Determine whether this token info represents a valid token, i.e. one
	/// that has not expired according to its [`Self::expires`] property. If
	/// [`Self::expires`] is [`None`], this function will always return `true`.
	#[must_use]
	pub fn is_valid(&self) -> bool {
		match self.expires {
			| Some(TokenExpires::AfterUses(max_uses)) => self.uses < max_uses,
			| Some(TokenExpires::AfterTime(expiry_time)) => {
				let now = SystemTime::now();

				expiry_time >= now
			},
			| None => true,
		}
	}
}

impl std::fmt::Display for DatabaseTokenInfo {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "Token created by {} and used {} times. ", self.creator, self.uses)?;
		if let Some(expires) = &self.expires {
			write!(f, "{expires}.")?;
		} else {
			write!(f, "Never expires.")?;
		}

		Ok(())
	}
}

#[derive(Debug)]
pub enum TokenExpires {
	AfterUses(u64),
	AfterTime(SystemTime),
}

impl Serialize for DatabaseTokenInfo {
	fn to_json(&self) -> Value {
		let mut obj = Object::new();
		obj.insert("creator".into(), self.creator.to_json());
		obj.insert("uses".into(), self.uses.to_json());
		obj.insert(
			"expires".into(),
			self.expires
				.as_ref()
				.map_or(Value::Null, Serialize::to_json),
		);
		Value::Object(obj)
	}
}

impl Deserialize for DatabaseTokenInfo {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		let expires = value
			.as_object()
			.and_then(|o| o.get("expires"))
			.filter(|v| !v.is_null());
		Ok(Self {
			creator: body_field(Some(value), "creator")?,
			uses: body_field(Some(value), "uses")?,
			expires: expires.map(TokenExpires::from_json).transpose()?,
		})
	}
}

// Encoded like serde's externally tagged enum: {"AfterUses": n} or
// {"AfterTime": {"secs_since_epoch": s, "nanos_since_epoch": n}}.
impl Serialize for TokenExpires {
	fn to_json(&self) -> Value {
		let mut obj = Object::new();
		match self {
			| Self::AfterUses(uses) => {
				obj.insert("AfterUses".into(), uses.to_json());
			},
			| Self::AfterTime(time) => {
				let since = time
					.duration_since(SystemTime::UNIX_EPOCH)
					.unwrap_or_default();
				let mut t = Object::new();
				t.insert("secs_since_epoch".into(), since.as_secs().to_json());
				t.insert("nanos_since_epoch".into(), since.subsec_nanos().to_json());
				obj.insert("AfterTime".into(), Value::Object(t));
			},
		}
		Value::Object(obj)
	}
}

impl Deserialize for TokenExpires {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		let obj = value
			.as_object()
			.ok_or_else(|| DeError("expected object".into()))?;
		if let Some(uses) = obj.get("AfterUses") {
			return Ok(Self::AfterUses(u64::from_json(uses)?));
		}
		let time = obj
			.get("AfterTime")
			.ok_or_else(|| DeError("unknown TokenExpires variant".into()))?;
		let secs: u64 = body_field(Some(time), "secs_since_epoch")?;
		let nanos: u32 = body_field(Some(time), "nanos_since_epoch")?;
		SystemTime::UNIX_EPOCH
			.checked_add(std::time::Duration::new(secs, nanos))
			.map(Self::AfterTime)
			.ok_or_else(|| DeError::expected("time"))
	}
}

database::codec_value_impls!(DatabaseTokenInfo);

impl std::fmt::Display for TokenExpires {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			| Self::AfterUses(max_uses) => write!(f, "Expires after {max_uses} uses"),
			| Self::AfterTime(max_age) => {
				let now = SystemTime::now();
				let formatted_expiry = utils::time::format(*max_age, "%+");

				match max_age.duration_since(now) {
					| Ok(duration) => write!(
						f,
						"Expires in {} ({formatted_expiry})",
						utils::time::pretty(duration)
					),
					| Err(_) => write!(f, "Expired at {formatted_expiry}"),
				}
			},
		}
	}
}

impl Data {
	pub(super) fn new(db: &Arc<Database>) -> Self {
		Self {
			registrationtoken_info: db["registrationtoken_info"].clone(),
		}
	}

	/// Associate a registration token with its metadata in the database.
	pub(super) fn save_token(&self, token: &str, info: &DatabaseTokenInfo) {
		self.registrationtoken_info.raw_put(token, Json(info));
	}

	/// Delete a registration token.
	pub(super) fn revoke_token(&self, token: &str) { self.registrationtoken_info.remove(token); }

	/// Look up a registration token's metadata.
	pub(super) async fn lookup_token_info(&self, token: &str) -> Option<DatabaseTokenInfo> {
		self.registrationtoken_info
			.get(token)
			.await
			.deserialized()
			.ok()
	}

	/// Iterate over all valid tokens and delete expired ones.
	pub(super) fn iterate_and_clean_tokens(
		&self,
	) -> impl Stream<Item = (&str, DatabaseTokenInfo)> + Send + '_ {
		self.registrationtoken_info
			.stream()
			.ignore_err()
			.ready_filter_map(|item: (&str, DatabaseTokenInfo)| {
				if item.1.is_valid() {
					Some(item)
				} else {
					self.registrationtoken_info.remove(item.0);
					None
				}
			})
	}
}
