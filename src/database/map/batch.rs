//! Type-safe write batch: pairs every key written into the batch with the
//! `Map` it belongs to, so `apply()` can wake the right watchers for every
//! key once the batch commits. This replaces the old pattern of building a
//! raw `rocksdb::WriteBatch` directly and relying on callers to remember to
//! call `.wake()` afterward -- that discipline is exactly what produced the
//! wake-gap bug class (batched membership/PDU writes landing without ever
//! notifying an in-flight `/sync` long-poll).

use conduwuit::implement;

use super::Map;
use crate::{dbkey::DbKey, keyval::ValBuf, ser};

pub(crate) enum DbOp {
	Insert {
		map_name: &'static str,
		key: Vec<u8>,
		value: Vec<u8>,
	},
	Remove {
		map_name: &'static str,
		key: Vec<u8>,
	},
}

/// A write batch that remembers which `Map`+key pairs it touched, so the
/// corresponding watchers get woken automatically when the batch is
/// applied. Build with `Batch::new()`, populate via `Map::batch_put` /
/// `Map::batch_raw_put` / `Map::batch_delete`, then commit with
/// `Map::apply_batch`.
pub struct Batch<'a> {
	pub(super) ops: Vec<DbOp>,
	wakes: Vec<(&'a Map, Vec<u8>)>,
}

impl Batch<'_> {
	#[must_use]
	pub fn new() -> Self { Self { ops: Vec::new(), wakes: Vec::new() } }

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
	batch.ops.push(DbOp::Insert {
		map_name: self.name,
		key: key.as_ref().to_vec(),
		value: val.as_ref().to_vec(),
	});
	batch.wakes.push((self, key.as_ref().to_vec()));
}

#[implement(Map)]
/// Record a put into `batch` with a raw key and a value to be serialized.
pub fn batch_raw_put<'a, K, V>(&'a self, batch: &mut Batch<'a>, key: K, val: V)
where
	K: AsRef<[u8]>,
	V: DbKey,
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
	batch.ops.push(DbOp::Remove {
		map_name: self.name,
		key: key.as_ref().to_vec(),
	});
	batch.wakes.push((self, key.as_ref().to_vec()));
}

#[implement(Map)]
/// Commit `batch` to the database, then wake every watcher for every key
/// the batch touched. Takes `batch` by value (not `&Batch`) and destructures
/// it so it can't be applied twice by accident.
pub fn apply_batch(&self, batch: Batch<'_>) {
	let Batch { ops, wakes } = batch;

	self.db
		.commit_batch(ops)
		.expect("database apply batch error");

	if !self.db.corked() {
		self.db.flush().expect("database flush error");
	}

	for (map, key) in &wakes {
		map.watchers.wake(key);
	}
}
