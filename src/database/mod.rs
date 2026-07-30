#![type_length_limit = "3072"]

extern crate conduwuit_core as conduwuit;
extern crate rust_rocksdb as rocksdb;

conduwuit_macros::introspect_crate! {}

conduwuit::mod_ctor! {}
conduwuit::mod_dtor! {}

#[cfg(test)]
mod benches;
mod cork;
mod de;
mod deserialized;
mod engine;
mod handle;
pub mod keyval;
mod map;
pub mod maps;
mod pool;
mod ser;
mod stream;
#[cfg(test)]
mod tests;
pub(crate) mod util;
mod watchers;

use std::{ops::Index, sync::Arc};

use conduwuit::{Result, Server, err};

pub use self::{
	de::{Ignore, IgnoreAll},
	deserialized::Deserialized,
	handle::Handle,
	keyval::{KeyVal, Slice, serialize_key, serialize_val},
	map::{Get, Map, Qry, compact},
	ser::{Cbor, Interfix, Json, SEP, Separator, serialize, serialize_to, serialize_to_vec},
};
pub(crate) use self::{
	engine::{Engine, context::Context},
	util::or_else,
};
use crate::maps::{Maps, MapsKey, MapsVal};

pub struct Database {
	maps: Maps,
	pub db: Arc<Engine>,
	pub(crate) _ctx: Arc<Context>,
}

impl Database {
	/// Load an existing database or create a new one.
	pub async fn open(server: &Arc<Server>) -> Result<Arc<Self>> {
		let ctx = Context::new(server)?;
		let db = Engine::open(ctx.clone(), maps::MAPS).await?;
		Ok(Arc::new(Self {
			maps: maps::open(&db)?,
			db: db.clone(),
			_ctx: ctx,
		}))
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

	#[tracing::instrument(skip(self))]
	pub async fn flush_and_close(self) {
		conduwuit::info!("Exclusive database lock acquired. Flushing to disk...");
		let Ok((sort_result, sync_result)) = tokio::task::spawn_blocking(move || {
			let sort_result = self.db.sort();
			let sync_result = self.db.sync();
			(sort_result, sync_result)
		})
		.await
		else {
			conduwuit::error!("spawn_blocking failed during database flush");
			return;
		};

		if let Err(error) = sort_result {
			conduwuit::error!("Failed to sort database during shutdown flush: {error}");
		}

		if let Err(error) = sync_result {
			conduwuit::error!("Failed to sync database during shutdown flush: {error}");
		}
	}

	/// Best-effort flush via a shared reference. This does **not** close or
	/// tear down the database engine—it only sorts and syncs the WAL to disk.
	/// Used as a fallback when exclusive ownership (`try_unwrap`) could not be
	/// obtained due to dangling `Arc` references.
	#[tracing::instrument(skip(self))]
	pub async fn force_flush(&self) {
		conduwuit::warn!("Force flushing database via shared reference...");
		let db = self.db.clone();
		let Ok((sort_result, sync_result)) = tokio::task::spawn_blocking(move || {
			let sort_result = db.sort();
			let sync_result = db.sync();
			(sort_result, sync_result)
		})
		.await
		else {
			conduwuit::error!("spawn_blocking failed during forced database flush");
			return;
		};

		if let Err(error) = sort_result {
			conduwuit::error!("Failed to sort database during forced flush: {error}");
		}

		if let Err(error) = sync_result {
			conduwuit::error!("Failed to sync database during forced flush: {error}");
		}
	}
}

impl Index<&str> for Database {
	type Output = Arc<Map>;

	fn index(&self, name: &str) -> &Self::Output {
		self.maps
			.get(name)
			.expect("column in database does not exist")
	}
}
