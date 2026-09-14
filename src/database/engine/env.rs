use std::{
	ptr::eq as ptr_eq,
	sync::{Arc, Mutex, MutexGuard, PoisonError, Weak},
};

use conduwuit::{Result, Server, debug};
use rocksdb::Env as RocksEnv;

use crate::or_else;

/// The shared rocksdb environment.
///
/// The inner mutex guards the handle itself, which the engine needs while
/// opening a database or a backup. Releasing the last reference shuts down and
/// joins the environment's background threads, so an engine must hold one for
/// as long as it is open.
pub(crate) struct Env(Mutex<RocksEnv>);

/// The process-global rocksdb environment, held weakly so it lives exactly as
/// long as some context needs it.
///
/// `RocksEnv::new` returns rocksdb's default environment singleton, so every
/// handle addresses the same object and its thread pools are shared by every
/// engine open in this process. Locking this slot across both acquisition and
/// teardown is what keeps the two mutually exclusive.
static ENV: Mutex<Weak<Env>> = Mutex::new(Weak::new());

impl Env {
	/// Take a reference to the shared environment, creating one when no context
	/// currently holds it.
	///
	/// The slot is held for the whole body so an acquisition cannot interleave
	/// with the teardown in [`Drop for Env`]. The priority knobs apply to the
	/// environment rather than to any one context, so they are read from the
	/// config of whichever server first needs it.
	pub(super) fn acquire(server: &Server) -> Result<Arc<Self>> {
		let mut slot = ENV.lock().expect("environment slot locked");

		if let Some(env) = slot.upgrade() {
			return Ok(env);
		}

		let config = &server.config;
		let mut env = RocksEnv::new().or_else(or_else)?;

		if config.rocksdb_compaction_prio_idle {
			env.lower_thread_pool_cpu_priority();
		}

		if config.rocksdb_compaction_ioprio_idle {
			env.lower_thread_pool_io_priority();
		}

		let env = Arc::new(Self(env.into()));
		*slot = Arc::downgrade(&env);

		Ok(env)
	}

	#[inline]
	pub(super) fn lock(&self) -> MutexGuard<'_, RocksEnv> {
		self.0.lock().unwrap_or_else(PoisonError::into_inner)
	}
}

impl Drop for Env {
	#[cold]
	fn drop(&mut self) {
		let mut slot = ENV.lock().expect("environment slot locked");

		// A context which acquired after our last strong reference went away
		// owns the same environment now, so the shutdown is its job.
		if !ptr_eq(slot.as_ptr(), self) {
			return;
		}

		*slot = Weak::new();

		let env = self.0.get_mut().unwrap_or_else(PoisonError::into_inner);

		debug!("Shutting down background threads");
		env.set_high_priority_background_threads(0);
		env.set_low_priority_background_threads(0);
		env.set_bottom_priority_background_threads(0);
		env.set_background_threads(0);

		debug!("Joining background threads...");
		env.join_all_threads();
	}
}
