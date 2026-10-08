use std::{
	future::Future,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::SystemTime,
};

use slipstream::OwnedServerName;
use tokio::sync::broadcast::{self, Sender};

use crate::{Err, Result, config, config::Config, log::Log, metrics::Metrics};

#[derive(Clone, Copy, Debug, Default)]
pub struct RuntimeHandle;

impl RuntimeHandle {
	pub fn spawn<F>(&self, future: F) -> smol::Task<F::Output>
	where
		F: Future + Send + 'static,
		F::Output: Send + 'static,
	{
		smol::spawn(future)
	}
}

/// Server runtime state; public portion
pub struct Server {
	/// Configured name of server. This is the same as the one in the config
	/// but developers can (and should) reference this string instead.
	pub name: OwnedServerName,

	/// Server-wide configuration instance
	pub config: config::Manager,

	/// Timestamp server was started; used for uptime.
	pub started: SystemTime,

	/// Reload/shutdown pending indicator; server is shutting down. This is an
	/// observable used on shutdown and should not be modified.
	pub stopping: AtomicBool,

	/// Reload/shutdown desired indicator; when false, shutdown is desired. This
	/// is an observable used on shutdown and modifying is not recommended.
	pub reloading: AtomicBool,

	/// Restart desired; when true, restart it desired after shutdown.
	pub restarting: AtomicBool,

	/// Handle to the runtime
	pub runtime: RuntimeHandle,

	/// Reload/shutdown signal
	pub signal: Sender<&'static str>,

	/// Logging subsystem state
	pub log: Log,

	/// Metrics subsystem state
	pub metrics: Metrics,
}

impl Server {
	#[must_use]
	pub fn new<T>(config: Config, runtime: Option<&T>, log: Log) -> Self {
		let (signal, _) = broadcast::channel(16);
		Self {
			name: config.server_name.clone(),
			config: config::Manager::new(config),
			started: SystemTime::now(),
			stopping: AtomicBool::new(false),
			reloading: AtomicBool::new(false),
			restarting: AtomicBool::new(false),
			runtime: RuntimeHandle,
			signal,
			log,
			metrics: Metrics::new(runtime),
		}
	}

	pub fn reload(&self) -> Result<()> {
		if cfg!(any(not(conduwuit_mods), not(feature = "conduwuit_mods"))) {
			return Err!("Reloading not enabled");
		}

		if self.reloading.swap(true, Ordering::AcqRel) {
			return Err!("Reloading already in progress");
		}

		if self.stopping.swap(true, Ordering::AcqRel) {
			return Err!("Shutdown already in progress");
		}

		self.signal("SIGINT").inspect_err(|_| {
			self.stopping.store(false, Ordering::Release);
			self.reloading.store(false, Ordering::Release);
		})
	}

	pub fn restart(&self) -> Result {
		if self.restarting.swap(true, Ordering::AcqRel) {
			return Err!("Restart already in progress");
		}

		self.shutdown().inspect_err(|_| {
			self.restarting.store(false, Ordering::Release);
		})
	}

	pub fn shutdown(&self) -> Result {
		if self.stopping.swap(true, Ordering::AcqRel) {
			return Err!("Shutdown already in progress");
		}

		self.signal("SIGTERM").inspect_err(|_| {
			self.stopping.store(false, Ordering::Release);
		})
	}

	pub fn signal(&self, sig: &'static str) -> Result<()> {
		if let Err(error) = self.signal.send(sig) {
			return Err!("Failed to send signal: {error}");
		}

		Ok(())
	}

	#[inline]
	pub async fn until_shutdown(self: &Arc<Self>) {
		let mut signal = self.signal.subscribe();
		while self.running() {
			signal.recv().await.ok();
		}
	}

	#[inline]
	pub fn runtime(&self) -> &RuntimeHandle { &self.runtime }

	#[inline]
	pub fn check_running(&self) -> Result {
		use std::{io, io::ErrorKind::Interrupted};

		self.running()
			.then_some(())
			.ok_or_else(|| io::Error::new(Interrupted, "Server shutting down"))
			.map_err(Into::into)
	}

	#[inline]
	pub fn running(&self) -> bool { !self.is_stopping() }

	/// Whether the server is running without accepting network traffic.
	///
	/// This is the effective maintenance/offline state used by background
	/// workers which must not perform network-facing work or emit periodic
	/// operational noise while the server is being operated offline.
	#[inline]
	pub fn is_maintenance(&self) -> bool { !self.config.listening }

	#[inline]
	pub fn is_stopping(&self) -> bool { self.stopping.load(Ordering::Relaxed) }

	#[inline]
	pub fn is_reloading(&self) -> bool { self.reloading.load(Ordering::Relaxed) }

	#[inline]
	pub fn is_restarting(&self) -> bool { self.restarting.load(Ordering::Relaxed) }

	#[inline]
	pub fn is_ours(&self, name: &str) -> bool { name == self.config.server_name }

	/// Returns a concurrency limit scaled to the number of Tokio worker
	/// threads. Use this instead of hardcoded constants for federation
	/// fan-out, fetch parallelism, etc. so that small boxes (2 cores)
	/// automatically get lower limits while large boxes still saturate.
	///
	/// `multiplier` controls how aggressive the scaling is:
	///   - `1` = one task per worker (conservative)
	///   - `2` = two tasks per worker (default for I/O-bound federation)
	#[inline]
	pub fn concurrency_scaled(&self, multiplier: usize) -> usize {
		let workers = self.config.worker_threads.unwrap_or_else(|| {
			std::thread::available_parallelism().map_or(4, std::num::NonZero::get)
		});
		workers.saturating_mul(multiplier).max(2)
	}
}
