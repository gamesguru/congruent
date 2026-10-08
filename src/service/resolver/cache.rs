use std::{
	collections::BTreeMap,
	net::IpAddr,
	sync::Arc,
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use conduwuit::{
	Error, Result,
	arrayvec::ArrayVec,
	err, implement,
	utils::{math::Expected, rand, stream::TryIgnore},
};
use database::{Deserialized, Json, Map};
use futures::{Stream, StreamExt, future::join};
use slipstream::{
	OwnedServerName, ServerName,
	codec::{self, DeError, Deserialize, Serialize},
	json::Value,
};

use super::fed::FedDest;

pub struct Cache {
	destinations: Arc<Map>,
	overrides: Arc<Map>,
}

#[derive(Clone, Debug)]
pub struct CachedDest {
	pub dest: FedDest,
	pub host: String,
	pub expire: SystemTime,
}

#[derive(Clone, Debug)]
pub struct CachedOverride {
	pub ips: IpAddrs,
	pub port: u16,
	pub expire: SystemTime,
	pub overriding: Option<String>,
}

pub type IpAddrs = ArrayVec<IpAddr, MAX_IPS>;
pub(crate) const MAX_IPS: usize = 3;

impl Cache {
	pub(super) fn new(args: &crate::Args<'_>) -> Arc<Self> {
		Arc::new(Self {
			destinations: args.db["servername_destination"].clone(),
			overrides: args.db["servername_override"].clone(),
		})
	}
}

#[implement(Cache)]
pub async fn clear(&self) { join(self.clear_destinations(), self.clear_overrides()).await; }

#[implement(Cache)]
pub async fn clear_destinations(&self) { self.destinations.clear().await; }

#[implement(Cache)]
pub async fn clear_overrides(&self) { self.overrides.clear().await; }

#[implement(Cache)]
pub fn del_destination(&self, name: &ServerName) { self.destinations.remove(name); }

#[implement(Cache)]
pub fn del_override(&self, name: &ServerName) { self.overrides.remove(name); }

#[implement(Cache)]
pub fn set_destination(&self, name: &ServerName, dest: &CachedDest) {
	self.destinations.raw_put(name, Json(codec::to_value(dest)));
}

#[implement(Cache)]
pub fn set_override(&self, name: &str, over: &CachedOverride) {
	self.overrides.raw_put(name, Json(codec::to_value(over)));
}

#[implement(Cache)]
#[must_use]
pub async fn has_destination(&self, destination: &ServerName) -> bool {
	self.get_destination(destination).await.is_ok()
}

#[implement(Cache)]
#[must_use]
pub async fn has_override(&self, destination: &str) -> bool {
	self.get_override(destination)
		.await
		.iter()
		.any(CachedOverride::valid)
}

#[implement(Cache)]
pub async fn get_destination(&self, name: &ServerName) -> Result<CachedDest> {
	self.destinations
		.get(name)
		.await
		.deserialized::<Json<_>>()
		.and_then(|json| {
			codec::from_value::<CachedDest>(&json.0)
				.map_err(|error| Error::SerdeDe(error.to_string().into()))
		})
		.and_then(|dest| {
			dest.valid()
				.then_some(dest)
				.ok_or(err!(Request(NotFound("Expired from cache"))))
		})
}

#[implement(Cache)]
pub async fn get_override(&self, name: &str) -> Result<CachedOverride> {
	self.overrides
		.get(name)
		.await
		.deserialized::<Json<_>>()
		.and_then(|json| {
			codec::from_value::<CachedOverride>(&json.0)
				.map_err(|error| Error::SerdeDe(error.to_string().into()))
		})
}

#[implement(Cache)]
pub fn destinations(&self) -> impl Stream<Item = (OwnedServerName, CachedDest)> + Send + '_ {
	self.destinations.stream().ignore_err().filter_map(
		|item: (OwnedServerName, Json<Value>)| async move {
			codec::from_value(&item.1.0)
				.ok()
				.map(|value| (item.0, value))
		},
	)
}

#[implement(Cache)]
pub fn overrides(&self) -> impl Stream<Item = (OwnedServerName, CachedOverride)> + Send + '_ {
	self.overrides.stream().ignore_err().filter_map(
		|item: (OwnedServerName, Json<Value>)| async move {
			codec::from_value(&item.1.0)
				.ok()
				.map(|value| (item.0, value))
		},
	)
}

impl CachedDest {
	#[inline]
	#[must_use]
	pub fn valid(&self) -> bool { self.expire > SystemTime::now() }

	#[must_use]
	pub(crate) fn default_expire(expire_secs: u64) -> SystemTime {
		rand::time_from_now_secs(expire_secs..expire_secs.saturating_mul(2).max(1))
	}

	#[inline]
	#[must_use]
	pub fn size(&self) -> usize {
		self.dest
			.size()
			.expected_add(self.host.len())
			.expected_add(size_of_val(&self.expire))
	}
}

impl Serialize for CachedDest {
	fn to_json(&self) -> Value {
		let mut object = BTreeMap::new();
		object.insert("dest".to_owned(), self.dest.uri_string().to_json());
		object.insert("host".to_owned(), self.host.to_json());
		object.insert("expire".to_owned(), expiry_to_json(self.expire));
		Value::Object(object)
	}
}

impl Deserialize for CachedDest {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		let object = value
			.as_object()
			.ok_or_else(|| DeError::expected("object"))?;
		let dest_string = String::from_json(field(object, "dest")?)?;
		let dest = super::fed::get_ip_with_port(&dest_string)
			.unwrap_or_else(|| super::fed::add_port_to_hostname(&dest_string));
		Ok(Self {
			dest,
			host: String::from_json(field(object, "host")?)?,
			expire: expiry_from_json(field(object, "expire")?)?,
		})
	}
}

impl CachedOverride {
	#[inline]
	#[must_use]
	pub fn valid(&self) -> bool { self.expire > SystemTime::now() }

	#[must_use]
	pub(crate) fn default_expire(expire_secs: u64) -> SystemTime {
		rand::time_from_now_secs(expire_secs..expire_secs.saturating_mul(2).max(1))
	}

	#[inline]
	#[must_use]
	pub fn is_overriding(&self) -> bool { self.overriding.is_some() }

	#[inline]
	#[must_use]
	pub fn size(&self) -> usize { size_of_val(self) }
}

impl Serialize for CachedOverride {
	fn to_json(&self) -> Value {
		let mut object = BTreeMap::new();
		object.insert(
			"ips".to_owned(),
			Value::Array(self.ips.iter().map(|ip| ip.to_string().to_json()).collect()),
		);
		object.insert("port".to_owned(), self.port.to_json());
		object.insert("expire".to_owned(), expiry_to_json(self.expire));
		object.insert(
			"overriding".to_owned(),
			self.overriding
				.as_ref()
				.map_or(Value::Null, String::to_json),
		);
		Value::Object(object)
	}
}

impl Deserialize for CachedOverride {
	fn from_json(value: &Value) -> Result<Self, DeError> {
		let object = value
			.as_object()
			.ok_or_else(|| DeError::expected("object"))?;
		let mut ips = IpAddrs::new();
		for value in field(object, "ips")?
			.as_array()
			.ok_or_else(|| DeError::expected("array"))?
		{
			let ip: IpAddr = String::from_json(value)?
				.parse::<IpAddr>()
				.map_err(|error| DeError(error.to_string()))?;
			ips.try_push(ip)
				.map_err(|_| DeError::expected("at most three IPs"))?;
		}
		Ok(Self {
			ips,
			port: u16::from_json(field(object, "port")?)?,
			expire: expiry_from_json(field(object, "expire")?)?,
			overriding: match field(object, "overriding")? {
				| Value::Null => None,
				| value => Some(String::from_json(value)?),
			},
		})
	}
}

fn field<'a>(object: &'a BTreeMap<String, Value>, name: &str) -> Result<&'a Value, DeError> {
	object
		.get(name)
		.ok_or_else(|| DeError(format!("missing field {name}")))
}

fn expiry_to_json(expire: SystemTime) -> Value {
	let millis = expire
		.duration_since(UNIX_EPOCH)
		.unwrap_or_default()
		.as_millis()
		.try_into()
		.unwrap_or(u64::MAX);
	millis.to_json()
}

fn expiry_from_json(value: &Value) -> Result<SystemTime, DeError> {
	Ok(UNIX_EPOCH + Duration::from_millis(u64::from_json(value)?))
}
