extern crate conduwuit_admin as admin;
extern crate conduwuit_core as conduwuit;
extern crate conduwuit_service as service;

use std::{
	sync::{Arc, atomic::Ordering},
	time::Duration,
};

use axum_server::{Address, Handle as ServerHandle};
use conduwuit::{Error, Result, Server, debug, debug_info, error, info, warn};
use futures::FutureExt;
use service::Services;
use tokio::{
	sync::broadcast::{self, Sender},
	task::JoinHandle,
};

use crate::serve;

/// Main loop base
#[tracing::instrument(skip_all, level = "info")]
pub(crate) async fn run(services: Arc<Services>) -> Result<()> {
	let server = &services.server;
	debug!("Start");

	// Install the admin room callback here for now
	admin::init(&services.admin).await;

	// Setup shutdown/signal handling
	let handle = ServerHandle::new();
	let (tx, _) = broadcast::channel::<()>(1);
	let sigs = server
		.runtime()
		.spawn(signal(services.clone(), tx.clone(), handle.clone()));

	let mut listener =
		server
			.runtime()
			.spawn(serve::serve(services.clone(), handle.clone(), tx.subscribe()));

	// Run startup admin commands.
	// This has to be done after the admin service is initialized otherwise it
	// panics.
	services.admin.startup_execute().await?;

	// Print first-run banner if necessary. This needs to be done after the startup
	// admin commands are run in case one of them created the first user.
	services.firstrun.print_first_run_banner();

	debug!("Running");
	let res = tokio::select! {
		res = &mut listener => res.map_err(Error::from).unwrap_or_else(Err),
		res = services.poll() => handle_services_poll(server, res, listener).await,
	};

	// Join the signal handler before we leave.
	sigs.abort();
	_ = sigs.await;

	// Remove the admin room callback
	admin::fini(&services.admin).await;

	debug_info!("Finish");
	res
}

/// Async initializations
#[tracing::instrument(skip_all, level = "info")]
pub(crate) async fn start(server: Arc<Server>) -> Result<Arc<Services>> {
	debug!("Starting...");

	let services = Services::build(server).await?.start().await?;

	#[cfg(all(feature = "systemd", target_os = "linux"))]
	sd_notify::notify(&[sd_notify::NotifyState::Ready])
		.expect("failed to notify systemd of ready state");

	debug!("Started");
	Ok(services)
}

/// Async destructions
#[tracing::instrument(skip_all, level = "info")]
pub(crate) async fn stop(services: Arc<Services>) -> Result<()> {
	debug!("Shutting down...");

	#[cfg(all(feature = "systemd", target_os = "linux"))]
	sd_notify::notify(&[sd_notify::NotifyState::Stopping])
		.expect("failed to notify systemd of stopping state");

	// Fire the global cancellation token (tell cooperative workers to stop)
	services.shutdown.cancel();

	// Wait for all completions before dropping or we'll lose them to the module
	// unload and explode. This executes the 10-second timeout.
	services.stop().await;

	// Disentangle the Arcs!
	// We extract a clone of the DB, then explicitly DROP the `Services` wrapper.
	// This destroys all channels, manager structs, and internal worker state,
	// guaranteeing their internal `Arc<Database>` references are dropped.
	let db_arc = services.db.clone();
	drop(services);

	// Assert Exclusive Database Ownership and Flush
	match Arc::try_unwrap(db_arc) {
		| Ok(db) => {
			info!("Exclusive database lock acquired. Flushing to disk...");
			// Explicitly flush the WAL and close
			db.flush_and_close().await;
			info!("Database safely closed.");
		},
		| Err(dangling_arc) => {
			let count = Arc::strong_count(&dangling_arc).saturating_sub(1);
			warn!(
				"{count} dangling database reference(s) remain (likely from an in-flight admin \
				 command or untracked task). Forcing WAL flush via shared reference..."
			);
			// Best-effort flush via the shared reference to protect the WAL
			dangling_arc.force_flush().await;
		},
	}

	warn!("Shutdown complete.");
	Ok(())
}

#[tracing::instrument(skip_all, level = "info")]
async fn signal<A: Address>(
	services: Arc<Services>,
	tx: Sender<()>,
	handle: axum_server::Handle<A>,
) {
	services
		.server
		.clone()
		.until_shutdown()
		.then(move |()| handle_shutdown(services, tx, handle))
		.await;
}

async fn handle_shutdown<A: Address>(
	services: Arc<Services>,
	tx: Sender<()>,
	handle: axum_server::Handle<A>,
) {
	let server = &services.server;
	if let Err(e) = tx.send(()) {
		error!("failed sending shutdown transaction to channel: {e}");
	}

	services.interrupt();

	let timeout = server.config.client_shutdown_timeout;
	let timeout = Duration::from_secs(timeout);
	debug!(
		?timeout,
		handle_active = %server.metrics.requests_handle_active.load(Ordering::Relaxed),
		"Notifying for graceful shutdown"
	);

	handle.graceful_shutdown(Some(timeout));
}

async fn handle_services_poll(
	server: &Arc<Server>,
	result: Result<()>,
	listener: JoinHandle<Result<()>>,
) -> Result<()> {
	debug!("Service manager finished: {result:?}");

	if server.running() {
		if let Err(e) = server.shutdown() {
			error!("Failed to send shutdown signal: {e}");
		}
	}

	if let Err(e) = listener.await {
		error!("Client listener task finished with error: {e}");
	}

	result
}
