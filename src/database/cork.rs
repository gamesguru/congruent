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
	/// in the process, not a per-task stack, so a lift can't be tied to any
	/// particular `Cork` -- `Uncork` tracks its own "currently lifted" count
	/// in a separate counter (`Engine::lifts`) instead of borrowing against
	/// `corks` directly. That means it never has to reject a concurrent
	/// lift: any number of `Uncork`s can be alive at once, each cheaply
	/// incrementing/decrementing its own counter, with no risk of the
	/// unsigned `corks` counter itself underflowing (only `Cork::new`/
	/// `Cork::drop` ever touch that one, always in balanced pairs). The
	/// tradeoff is that `corked()` becomes a heuristic comparison of two
	/// independently-read counters (`corks > lifts`) rather than an exact
	/// "is anything currently un-suppressed" answer: with N `Uncork`s alive
	/// against fewer than N held corks, `corked()` reads `false` (i.e.
	/// every write flushes eagerly) for all of them at once, rather than
	/// just the "fair share" -- extra flushing, never suppressed durability.
	/// Callers that need every concurrent branch of a fan-out to actually
	/// run uncorked should still prefer wrapping the whole fan-out in one
	/// `uncork_briefly` rather than call it per-branch, since one lift is
	/// cheaper than many and avoids that extra-flush skew.
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
	#[inline]
	fn new(db: &Arc<Engine>) -> Self {
		let lifted = db.corked();
		if lifted {
			db.lift();
		}
		Self { db: db.clone(), lifted }
	}
}

impl Drop for Uncork {
	#[inline]
	fn drop(&mut self) {
		if self.lifted {
			self.db.unlift();
		}
	}
}
