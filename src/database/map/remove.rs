use std::{convert::AsRef, fmt::Debug};

use conduwuit::implement;
use serde::Serialize;

use crate::{keyval::KeyBuf, ser, util::or_else};

#[implement(super::Map)]
#[inline]
pub fn del<K>(&self, key: K)
where
	K: Serialize + Debug,
{
	let mut buf = KeyBuf::new();
	let key = ser::serialize(&mut buf, key).expect("failed to serialize deletion key");
	self.remove_raw(key);
}

#[implement(super::Map)]
#[inline]
pub fn remove<K>(&self, key: &K)
where
	K: AsRef<[u8]> + ?Sized + Debug,
{
	self.remove_raw(key.as_ref());
}

#[implement(super::Map)]
#[inline]
pub fn remove_raw(&self, key: &[u8]) {
	let mut batch = rocksdb::WriteBatch::default();
	batch.delete_cf(&self.cf(), key);

	let write_options = &self.write_options;
	self.db
		.db
		.write_opt(&batch, write_options)
		.or_else(or_else)
		.expect("database remove error");

	// Honor corking semantics for remove operations: when not corked,
	// ensure the delete is flushed in the same way as inserts.
	if !self.db.corked() {
		self.db.flush().expect("database flush error after remove");
	}

	self.watchers.wake(key);
}

#[implement(super::Map)]
#[inline]
pub fn remove_from_batch(&self, batch: &mut rocksdb::WriteBatch, key: &[u8]) {
	batch.delete_cf(&self.cf(), key);
	self.watchers.wake(key);
}
