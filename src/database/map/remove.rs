use std::{convert::AsRef, fmt::Debug, io::Write};

use conduwuit::{arrayvec::ArrayVec, implement};
use serde::Serialize;

use crate::{
	keyval::KeyBuf,
	ser,
	util::{RawRef, or_else},
};

#[implement(super::Map)]
#[inline]
pub fn del<K>(&self, key: K)
where
	K: Serialize + Debug,
{
	let mut buf = KeyBuf::new();
	self.bdel(key, &mut buf);
}

#[implement(super::Map)]
#[inline]
pub fn adel<const MAX: usize, K>(&self, key: K)
where
	K: Serialize + Debug,
{
	let mut buf = ArrayVec::<u8, MAX>::new();
	self.bdel(key, &mut buf);
}

#[implement(super::Map)]
#[tracing::instrument(skip(self, buf), level = "trace")]
pub fn bdel<K, B>(&self, key: K, buf: &mut B)
where
	K: Serialize + Debug,
	B: Write + AsRef<[u8]>,
{
	let key = ser::serialize(buf, key).expect("failed to serialize deletion key");
	self.remove_raw(key);
}

#[implement(super::Map)]
#[inline]
pub fn remove<K>(&self, key: &K)
where
	K: AsRef<[u8]> + ?Sized + Debug,
{
	self.remove_raw(key.as_raw());
}

#[implement(super::Map)]
#[tracing::instrument(skip(self, key), fields(%self), level = "trace")]
pub fn remove_raw(&self, key: &[u8]) {
	let write_options = &self.write_options;

	let appended_to_txn = crate::transaction::TRANSACTION_BATCH
		.try_with(|batch| {
			let mut batch_guard = batch.lock();
			batch_guard.batch.delete_cf(&self.cf(), key);
			let watchers = self.watchers.clone();
			let key_owned = key.to_vec();
			batch_guard
				.wake_closures
				.push(Box::new(move || watchers.wake(&key_owned)));
			true
		})
		.unwrap_or(false);

	if !appended_to_txn {
		self.db
			.db
			.delete_cf_opt(&self.cf(), key, write_options)
			.or_else(or_else)
			.expect("database remove error");

		// Honor corking semantics for remove operations: when not corked,
		// ensure the delete is flushed in the same way as inserts.
		if !self.db.corked() {
			self.db.flush().expect("database flush error after remove");
		}

		self.watchers.wake(key);
	}
}
