mod backup;
mod cf_opts;
pub(crate) mod context;
mod db_opts;
pub(crate) mod descriptor;
mod files;
mod logger;
mod memory_usage;
mod open;
mod repair;

use std::{
	ffi::CStr,
	sync::{
		Arc,
		atomic::{AtomicU32, Ordering},
	},
};

use conduwuit::{Err, Result, debug, info, trace, warn};
use rocksdb::{
	AsColumnFamilyRef, BoundColumnFamily, DBCommon, DBWithThreadMode, MultiThreaded,
	WaitForCompactOptions,
};

use crate::{
	Context,
	pool::Pool,
	util::{map_err, result},
};

pub struct Engine {
	pub(crate) db: Db,
	pub(crate) pool: Arc<Pool>,
	pub(crate) ctx: Arc<Context>,
	pub(crate) checksums: bool,
	corks: AtomicU32,
	// Number of currently-active temporary lifts (`Uncork`, see `cork.rs`).
	// Deliberately a *separate* counter from `corks` rather than borrowing
	// against it: `corks` only ever moves in balanced +1/-1 pairs from
	// `Cork::new`/`Cork::drop`, so it can never underflow regardless of what
	// concurrent `Uncork`s are doing. If `Uncork` instead decremented
	// `corks` directly (as an earlier version of this code did), a
	// concurrent, *unrelated* `Cork::drop`'s unconditional decrement could
	// fire while that unit was "on loan" to an `Uncork`, wrapping the
	// unsigned counter and leaving `corked()` stuck permanently `true` --
	// i.e. writes silently stop flushing forever. With two independent
	// counters that failure mode is unreachable: the worst outcome of any
	// interleaving is `lifts` transiently exceeding `corks`, which just
	// makes `corked()` read `false` a little more often than strictly
	// necessary (extra flushes, never lost ones).
	lifts: AtomicU32,
}

pub(crate) type Db = DBWithThreadMode<MultiThreaded>;

impl Engine {
	#[tracing::instrument(
		level = "info",
		skip_all,
		fields(
			sequence = ?self.current_sequence(),
		),
	)]
	pub fn wait_compactions_blocking(&self) -> Result {
		info!("Waiting for database compactions to finish... This may block for a while.");
		let mut opts = WaitForCompactOptions::default();
		opts.set_abort_on_pause(true);
		opts.set_flush(false);
		opts.set_timeout(0);

		self.db.wait_for_compact(&opts).map_err(map_err)
	}

	#[tracing::instrument(
		level = "info",
		skip_all,
		fields(
			sequence = ?self.current_sequence(),
		),
	)]
	pub fn sort(&self) -> Result {
		info!("Flushing database... This may block temporarily.");
		let flushoptions = rocksdb::FlushOptions::default();
		result(DBCommon::flush_opt(&self.db, &flushoptions))
	}

	#[tracing::instrument(
		level = "debug",
		skip_all,
		fields(
			sequence = ?self.current_sequence(),
		),
	)]
	pub fn update(&self) -> Result {
		info!("Catching up with primary... This may block.");
		self.db.try_catch_up_with_primary().map_err(map_err)
	}

	pub fn sync(&self) -> Result {
		trace!("Syncing database WAL...");
		result(DBCommon::flush_wal(&self.db, true))
	}

	pub fn flush(&self) -> Result {
		trace!("Flushing database WAL...");
		result(DBCommon::flush_wal(&self.db, false))
	}

	#[inline]
	pub(crate) fn cork(&self) { self.corks.fetch_add(1, Ordering::Relaxed); }

	#[inline]
	pub(crate) fn uncork(&self) { self.corks.fetch_sub(1, Ordering::Relaxed); }

	/// Mark one more temporary lift as active -- see `Uncork` in `cork.rs`.
	/// Always succeeds and is always paired with a later `unlift()`; unlike
	/// the `corks` counter, this never needs to be conditional because it
	/// doesn't borrow against another counter's balance.
	#[inline]
	pub(crate) fn lift(&self) { self.lifts.fetch_add(1, Ordering::Relaxed); }

	#[inline]
	pub(crate) fn unlift(&self) { self.lifts.fetch_sub(1, Ordering::Relaxed); }

	/// True while at least one held cork is not currently lifted. Both
	/// loads are independent and not synchronized against each other, so
	/// this is a best-effort/racy read by design (matches `Uncork`'s
	/// best-effort semantics, see its doc comment) -- never treat it as
	/// more than a heuristic for deciding whether to flush eagerly.
	#[inline]
	pub fn corked(&self) -> bool {
		self.corks.load(Ordering::Relaxed) > self.lifts.load(Ordering::Relaxed)
	}

	/// Query for database property by null-terminated name which is expected to
	/// have a result with an integer representation. This is intended for
	/// low-overhead programmatic use.
	pub(crate) fn property_integer(
		&self,
		cf: &impl AsColumnFamilyRef,
		name: &CStr,
	) -> Result<u64> {
		result(self.db.property_int_value_cf(cf, name))
			.and_then(|val| val.map_or_else(|| Err!("Property {name:?} not found."), Ok))
	}

	/// Query for database property by name receiving the result in a string.
	pub(crate) fn property(&self, cf: &impl AsColumnFamilyRef, name: &str) -> Result<String> {
		result(self.db.property_value_cf(cf, name))
			.and_then(|val| val.map_or_else(|| Err!("Property {name:?} not found."), Ok))
	}

	pub(crate) fn cf(&self, name: &str) -> Arc<BoundColumnFamily<'_>> {
		self.db
			.cf_handle(name)
			.expect("column must be described prior to database open")
	}

	pub fn cf_exists(&self, name: &str) -> bool { self.db.cf_handle(name).is_some() }

	#[inline]
	#[must_use]
	#[tracing::instrument(name = "sequence", level = "debug", skip_all, fields(sequence))]
	pub fn current_sequence(&self) -> u64 {
		let sequence = self.db.latest_sequence_number();

		#[cfg(debug_assertions)]
		tracing::Span::current().record("sequence", sequence);

		sequence
	}

	pub fn drop_cf(&self, name: &str) -> Result<()> { result(self.db.drop_cf(name)) }
}

impl Drop for Engine {
	#[cold]
	fn drop(&mut self) {
		const BLOCKING: bool = true;

		debug!("Waiting for background tasks to finish...");
		self.db.cancel_all_background_work(BLOCKING);

		info!(
			sequence = %self.current_sequence(),
			"Closing database..."
		);
	}
}
