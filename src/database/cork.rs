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
	/// when the returned guard drops.
	///
	/// The cork count is a single counter shared by every concurrent caller
	/// in the process, not a per-task stack, so this can't simply subtract
	/// one now and unconditionally add one back later -- if two `Uncork`s
	/// are alive at once (e.g. from a `for_each_concurrent` federation fetch
	/// fan-out) and only one outer cork is actually held, a second
	/// unconditional decrement would take the unsigned counter below zero.
	/// Instead each `Uncork` only ever decrements if it can observe a
	/// nonzero count to take from, and only restores what it actually took.
	/// That makes it safe under arbitrary concurrency, but also means it's a
	/// best-effort lift: if N `Uncork`s are alive against fewer than N held
	/// corks, only some of them actually see the uncorked state at a time.
	/// Callers that need every concurrent branch of a fan-out to actually
	/// run uncorked should wrap the whole fan-out in one `uncork_briefly`
	/// rather than call it per-branch.
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
	// Whether this guard actually took a decrement it's now responsible for
	// restoring. False means there was nothing to lift (count was already
	// zero) or nothing needs restoring -- drop is then a no-op.
	lifted: bool,
}

impl Uncork {
	#[inline]
	fn new(db: &Arc<Engine>) -> Self {
		let lifted = db.try_uncork_one();
		Self { db: db.clone(), lifted }
	}
}

impl Drop for Uncork {
	#[inline]
	fn drop(&mut self) {
		if self.lifted {
			self.db.cork();
		}
	}
}
