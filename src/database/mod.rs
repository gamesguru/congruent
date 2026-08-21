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
pub mod transaction;
pub(crate) mod util;
mod watchers;

use std::{future::Future, ops::Index, sync::Arc};

use conduwuit::{Result, Server, SyncMutex, err};

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

	/// Executes a block of database operations within an atomic transaction.
	/// Automatically commits the operations to the database upon completion.
	pub async fn transaction<F, Fut, R>(&self, f: F) -> Result<R>
	where
		F: FnOnce() -> Fut,
		Fut: Future<Output = Result<R>>,
	{
		let batch = Arc::new(SyncMutex::new(transaction::TransactionContext::new()));

		let res = transaction::TRANSACTION_BATCH
			.scope(batch.clone(), async { f().await })
			.await?;

		let mut batch_guard = batch.lock();
		let batch = std::mem::take(&mut batch_guard.batch);
		let wake_closures = std::mem::take(&mut batch_guard.wake_closures);
		drop(batch_guard);

		let write_options = map::write_options_default(&self.db);
		self.db
			.db
			.write_opt(&batch, &write_options)
			.or_else(or_else)?;

		for wake_closure in wake_closures {
			wake_closure();
		}

		Ok(res)
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
