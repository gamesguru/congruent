use std::sync::Arc;

use crate::{Database, Engine};

pub struct Cork {
	db: Arc<Engine>,
	flush: bool,
	sync: bool,
}

impl Database {
	#[inline]
	#[must_use]
	pub fn cork(&self) -> Cork { Cork::new(&self.db, false, false) }

	#[inline]
	#[must_use]
	pub fn cork_and_flush(&self) -> Cork { Cork::new(&self.db, true, false) }

	#[inline]
	#[must_use]
	pub fn cork_and_sync(&self) -> Cork { Cork::new(&self.db, true, true) }

	/// Briefly lift an outer cork so unrelated flushes aren't held back
	/// while awaiting long-running I/O (e.g. federation round-trips) in the
	/// middle of an existing corked write phase. The outer cork is restored
	/// when the returned guard drops. Safe to nest; corking is refcounted.
	#[inline]
	#[must_use]
	pub fn uncork_briefly(&self) -> Uncork { Uncork::new(&self.db) }
}

impl Cork {
	#[inline]
	pub(super) fn new(db: &Arc<Engine>, flush: bool, sync: bool) -> Self {
		db.cork();
		Self { db: db.clone(), flush, sync }
	}
}

impl Drop for Cork {
	fn drop(&mut self) {
		self.db.uncork();
		if self.flush {
			self.db.flush().ok();
		}
		if self.sync {
			self.db.sync().ok();
		}
	}
}

pub struct Uncork {
	db: Arc<Engine>,
}

impl Uncork {
	#[inline]
	fn new(db: &Arc<Engine>) -> Self {
		db.uncork();
		Self { db: db.clone() }
	}
}

impl Drop for Uncork {
	#[inline]
	fn drop(&mut self) { self.db.cork(); }
}
