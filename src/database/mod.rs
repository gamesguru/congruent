#![type_length_limit = "3072"]
#![allow(clippy::disallowed_macros)]

extern crate conduwuit_core as conduwuit;

conduwuit_macros::introspect_crate! {}

conduwuit::mod_ctor! {}
conduwuit::mod_dtor! {}

#[cfg(test)]
mod benches;
mod cork;
pub mod dbkey;
mod de;
mod deserialized;
mod engine;
mod handle;
pub mod keyval;
mod map;
pub mod maps;
mod ser;
#[cfg(test)]
mod tests;
pub(crate) mod util;
mod watchers;

use std::{ops::Index, sync::Arc};

use conduwuit::{Result, Server, err};

pub(crate) use self::engine::Engine;
pub use self::{
	dbkey::from_json_slice,
	de::{Ignore, IgnoreAll},
	deserialized::Deserialized,
	handle::Handle,
	keyval::{KeyVal, Slice, serialize_key, serialize_val},
	map::{Batch, Get, Map, Qry, RecursiveGetOutput, compact},
	ser::{Cbor, Interfix, Json, SEP, Separator, serialize, serialize_to, serialize_to_vec},
};
use crate::maps::{Maps, MapsKey, MapsVal};

pub struct Database {
	maps: Maps,
	pub db: Arc<Engine>,
}

impl Database {
	/// Load an existing database or create a new one.
	pub fn open(server: &Arc<Server>) -> Result<Arc<Self>> {
		let db = Arc::new(Engine::open(&server.config.database_path)?);
		Ok(Arc::new(Self { maps: maps::open(&db), db }))
	}

	#[inline]
	pub fn get(&self, name: &str) -> Result<&Arc<Map>> {
		self.maps
			.get(name)
			.ok_or_else(|| err!(Request(NotFound("column not found"))))
	}

	#[inline]
	pub fn iter(&self) -> impl Iterator<Item = (&MapsKey, &MapsVal)> + Send + '_ {
		self.maps.iter()
	}

	#[inline]
	pub fn keys(&self) -> impl Iterator<Item = &MapsKey> + Send + '_ { self.maps.keys() }
}

impl Index<&str> for Database {
	type Output = Arc<Map>;

	fn index(&self, name: &str) -> &Self::Output {
		self.maps
			.get(name)
			.expect("column in database does not exist")
	}
}
pub mod batch;
