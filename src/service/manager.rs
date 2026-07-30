use std::{panic::AssertUnwindSafe, sync::Arc, time::Duration};

use conduwuit::{Err, Error, Result, Server, debug, error, info, trace, utils::time, warn};
use futures::{FutureExt, TryFutureExt};
use tokio::{
	sync::{Mutex, MutexGuard},
	task::{JoinHandle, JoinSet},
	time::sleep,
};

use crate::{Services, service, service::Service};

pub(crate) struct Manager {
	manager: Mutex<Option<JoinHandle<Result<()>>>>,
	workers: Mutex<Workers>,
	server: Arc<Server>,
	service: Arc<service::Map>,
}

type Workers = JoinSet<WorkerResult>;
type WorkerResult = (Arc<dyn Service>, Result<()>);
type WorkersLocked<'a> = MutexGuard<'a, Workers>;

const RESTART_DELAY_MS: u64 = 2500;

impl Manager {
	pub(super) fn new(services: &Services) -> Arc<Self> {
		Arc::new(Self {
			manager: Mutex::new(None),
			workers: Mutex::new(JoinSet::new()),
			server: services.server.clone(),
			service: services.service.clone(),
		})
	}

	pub(super) async fn poll(&self) -> Result<()> {
		let manager = self.manager.lock().await.take();
		if let Some(mut manager) = manager {
			trace!("Polling service manager...");
			let res = (&mut manager).await;
			return res.map_err(Error::from).and_then(|res| res);
		}

		Ok(())
	}

	pub(super) async fn start(self: Arc<Self>) -> Result<()> {
		let mut workers = self.workers.lock().await;

		info!("Starting service manager...");
		let self_ = self.clone();
		_ = self.manager.lock().await.insert(
			self.server
				.runtime()
				.spawn(async move { self_.worker().await }),
		);

		// we can't hold the lock during the iteration with start_worker so the values
		// are snapshotted here
		let services: Vec<Arc<dyn Service>> = self
			.service
			.read()
			.values()
			.map(|val| val.0.upgrade())
			.map(|arc| arc.expect("services available for manager startup"))
			.collect();

		info!("Starting service workers...");
		for service in services {
			self.start_worker(&mut workers, &service).await?;
		}

		Ok(())
	}

	pub(super) async fn stop(&self) {
		if let Some(manager) = self.manager.lock().await.take() {
			debug!("Waiting for service manager...");

			// Workers already implement graceful shutdown via interrupt()
			// (channel close) and their own internal drain timeouts (e.g.
			// sender_shutdown_timeout). We must NOT hard-abort them or we
			// lose in-flight transactions. A generous safety-net timeout
			// guards against workers that never exit; on expiry we warn
			// and let the caller proceed to drop Services and force-flush.
			match tokio::time::timeout(Duration::from_secs(30), manager).await {
				| Ok(Ok(Ok(()))) => {
					info!("All workers shut down gracefully.");
				},
				| Ok(Ok(Err(e))) => {
					error!("Manager shutdown error: {e:?}");
				},
				| Ok(Err(e)) => {
					error!("Manager task error: {e:?}");
				},
				| Err(_) => {
					warn!(
						"Shutdown safety timeout reached; some workers may still be running. \
						 Proceeding with database flush."
					);
				},
			}
		}
	}

	async fn worker(&self) -> Result<()> {
		loop {
			let mut workers = self.workers.lock().await;
			tokio::select! {
				result = workers.join_next() => match result {
					Some(Ok(result)) => self.handle_result(&mut workers, result).await?,
					Some(Err(error)) => self.handle_abort(&mut workers, Error::from(error)).await?,
					None => break,
				}
			}
		}

		debug!("Worker manager finished");
		Ok(())
	}

	async fn handle_abort(&self, _workers: &mut WorkersLocked<'_>, error: Error) -> Result<()> {
		// not supported until service can be associated with abort
		unimplemented!("unexpected worker task abort {error:?}");
	}

	async fn handle_result(
		&self,
		workers: &mut WorkersLocked<'_>,
		result: WorkerResult,
	) -> Result<()> {
		let (service, result) = result;
		match result {
			| Ok(()) => self.handle_finished(workers, &service).await,
			| Err(error) => self.handle_error(workers, &service, error).await,
		}
	}

	async fn handle_finished(
		&self,
		_workers: &mut WorkersLocked<'_>,
		service: &Arc<dyn Service>,
	) -> Result<()> {
		debug!("service {:?} worker finished", service.name());
		Ok(())
	}

	async fn handle_error(
		&self,
		workers: &mut WorkersLocked<'_>,
		service: &Arc<dyn Service>,
		error: Error,
	) -> Result<()> {
		let name = service.name();
		error!("service {name:?} aborted: {error}");

		if !self.server.running() {
			warn!("service {name:?} error ignored on shutdown.");
			return Ok(());
		}

		if !error.is_panic() {
			return Err(error);
		}

		let delay = Duration::from_millis(RESTART_DELAY_MS);
		warn!("service {name:?} worker restarting after {} delay", time::pretty(delay));
		sleep(delay).await;

		self.start_worker(workers, service).await
	}

	/// Start the worker in a task for the service.
	async fn start_worker(
		&self,
		workers: &mut WorkersLocked<'_>,
		service: &Arc<dyn Service>,
	) -> Result<()> {
		if !self.server.running() {
			return Err!(
				"Service {:?} worker not starting during server shutdown.",
				service.name()
			);
		}

		info!("Service {:?} worker starting...", service.name());
		workers.spawn_on(worker(service.clone()), self.server.runtime());

		Ok(())
	}
}

/// Base frame for service worker. This runs in a tokio::task. All errors and
/// panics from the worker are caught and returned cleanly. The JoinHandle
/// should never error with a panic, and if so it should propagate, but it may
/// error with an Abort which the manager should handle along with results to
/// determine if the worker should be restarted.
#[tracing::instrument(
	parent = None,
	level = "trace",
	skip_all,
	fields(service = %service.name()),
)]
async fn worker(service: Arc<dyn Service>) -> WorkerResult {
	let service_ = Arc::clone(&service);
	let result = AssertUnwindSafe(service_.worker())
		.catch_unwind()
		.map_err(Error::from_panic);

	let result = if service.unconstrained() {
		tokio::task::unconstrained(result).await
	} else {
		result.await
	};

	// flattens JoinError for panic into worker's Error
	(service, result.unwrap_or_else(Err))
}
