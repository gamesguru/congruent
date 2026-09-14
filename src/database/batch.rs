use std::sync::Arc;

use rocksdb::{WriteBatchWithTransaction, WriteOptions};

use crate::{Engine, Map};

pub struct Batch<'a> {
	batch: WriteBatchWithTransaction<false>,
	db: &'a Arc<Engine>,
	write_options: WriteOptions,
}

impl<'a> Batch<'a> {
	pub fn new(map: &'a Map) -> Self {
		Self {
			batch: WriteBatchWithTransaction::<false>::default(),
			db: map.db(),
			write_options: crate::map::write_options_default(map.db()),
		}
	}

	pub fn insert<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, map: &Map, key: K, val: V) {
		assert!(Arc::ptr_eq(self.db, map.db()), "map belongs to a different database engine");
		self.batch.put_cf(&map.cf(), key.as_ref(), val.as_ref());
	}

	pub fn commit(self) {
		self.db
			.db
			.write_opt(&self.batch, &self.write_options)
			.expect("database insert batch error");
	}
}
