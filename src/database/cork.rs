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

	/// Briefly lift an outer cork so unrelated flushes aren't held back while
	/// awaiting long-running I/O (e.g. federation fetches in the middle of a
	/// corked write phase). The outer cork is restored when the returned
	/// `Uncork` guard is dropped. Harmless to call when no cork is held.
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
	lifted: bool,
}

impl Uncork {
	fn new(db: &Arc<Engine>) -> Self {
		let lifted = db.has_corks();
		if lifted {
			db.lift();
		}
		Self { db: db.clone(), lifted }
	}
}

impl Drop for Uncork {
	fn drop(&mut self) {
		if self.lifted {
			self.db.unlift();
		}
	}
}
