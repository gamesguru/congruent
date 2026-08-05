//! Type-safe write batch: pairs every key written into the batch with the
//! `Map` it belongs to, so `apply()` can wake the right watchers for every
//! key once the batch commits. This replaces the old pattern of building a
//! raw `rocksdb::WriteBatch` directly and relying on callers to remember to
//! call `.wake()` afterward -- that discipline is exactly what produced the
//! wake-gap bug class (batched membership/PDU writes landing without ever
//! notifying an in-flight `/sync` long-poll).

use conduwuit::implement;
use rocksdb::WriteBatchWithTransaction;
use serde::Serialize;

use super::Map;
use crate::{keyval::ValBuf, ser, util::or_else};

/// A write batch that remembers which `Map`+key pairs it touched, so the
/// corresponding watchers get woken automatically when the batch is
/// applied. Build with `Batch::new()`, populate via `Map::batch_put` /
/// `Map::batch_raw_put` / `Map::batch_delete`, then commit with
/// `Map::apply_batch`.
pub struct Batch<'a> {
	pub(super) inner: WriteBatchWithTransaction<false>,
	wakes: Vec<(&'a Map, Vec<u8>)>,
}

impl Batch<'_> {
	#[must_use]
	pub fn new() -> Self {
		Self {
			inner: WriteBatchWithTransaction::default(),
			wakes: Vec::new(),
		}
	}

	#[must_use]
	pub fn len(&self) -> usize { self.wakes.len() }

	#[must_use]
	pub fn is_empty(&self) -> bool { self.wakes.is_empty() }
}

impl Default for Batch<'_> {
	fn default() -> Self { Self::new() }
}

#[implement(Map)]
/// Record a raw put into `batch`. Key and value are both already bytes.
pub fn batch_put<'a, K, V>(&'a self, batch: &mut Batch<'a>, key: &K, val: V)
where
	K: AsRef<[u8]> + ?Sized,
	V: AsRef<[u8]>,
{
	batch.inner.put_cf(&self.cf(), key.as_ref(), val.as_ref());
	batch.wakes.push((self, key.as_ref().to_vec()));
}

#[implement(Map)]
/// Record a put into `batch` with a raw key and a value to be serialized.
pub fn batch_raw_put<'a, K, V>(&'a self, batch: &mut Batch<'a>, key: K, val: V)
where
	K: AsRef<[u8]>,
	V: Serialize,
{
	let mut val_buf = ValBuf::new();
	let val = ser::serialize(&mut val_buf, val).expect("failed to serialize batch insertion val");
	self.batch_put(batch, key.as_ref(), val);
}

#[implement(Map)]
/// Record a delete into `batch`.
pub fn batch_delete<'a, K>(&'a self, batch: &mut Batch<'a>, key: &K)
where
	K: AsRef<[u8]> + ?Sized,
{
	batch.inner.delete_cf(&self.cf(), key.as_ref());
	batch.wakes.push((self, key.as_ref().to_vec()));
}

#[implement(Map)]
/// Commit `batch` to the database, then wake every watcher for every key
/// the batch touched. Takes `batch` by value (not `&Batch`) and destructures
/// it so it can't be applied twice by accident.
pub fn apply_batch(&self, batch: Batch<'_>) {
	let Batch { inner, wakes } = batch;

	let write_options = &self.write_options;
	self.db
		.db
		.write_opt(&inner, write_options)
		.or_else(or_else)
		.expect("database apply batch error");

	if !self.db.corked() {
		self.db.flush().expect("database flush error");
	}

	for (map, key) in &wakes {
		map.watchers.wake(key);
	}
}
