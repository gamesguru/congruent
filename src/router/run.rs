extern crate conduwuit_admin as admin;
extern crate conduwuit_core as conduwuit;
extern crate conduwuit_service as service;

use std::{
	sync::{Arc, Weak, atomic::Ordering},
	time::Duration,
};

use conduwuit::{Result, Server, debug, debug_error, debug_info, info, warn};
use futures::future::{Either, select};
use service::Services;

use crate::serve;

pub(crate) async fn run(services: Arc<Services>) -> Result<()> {
	let server = &services.server;
	debug!("Start");
	admin::init(&services.admin).await;
	let mut listener = server.runtime().spawn(serve::serve(services.clone()));
	services.admin.startup_execute().await?;
	services.firstrun.print_first_run_banner();
	debug!("Running");
	let result = match select(Box::pin(&mut listener), Box::pin(services.poll())).await {
		| Either::Left((result, _)) => result.map_err(conduwuit::Error::from).unwrap_or_else(Err),
		| Either::Right((result, _)) => {
			if server.running() {
				let _ = server.shutdown();
			}
			if let Err(error) = listener.await {
				debug_error!(%error, "listener task failed");
			}
			result
		},
	};
	admin::fini(&services.admin).await;
	debug_info!("Finish");
	result
}

pub(crate) async fn start(server: Arc<Server>) -> Result<Arc<Services>> {
	debug!("Starting...");
	let services = Services::build(server)?.start().await?;
	services.rooms.outlier.startup_janitor().await;
	#[cfg(all(feature = "systemd", target_os = "linux"))]
	sd_notify::notify(&[sd_notify::NotifyState::Ready]).expect("failed to notify systemd");
	debug!("Started");
	Ok(services)
}

pub(crate) async fn stop(services: Arc<Services>) -> Result<()> {
	info!("Shutting down...");
	services.stop().await;
	let db = Arc::downgrade(&services.db);
	if let Err(services) = Arc::try_unwrap(services) {
		debug_error!(
			"{} dangling references to Services after shutdown",
			Arc::strong_count(&services)
		);
	}
	let mut remaining = Weak::strong_count(&db);
	if remaining > 0 {
		let _ = conduwuit::timeout(Duration::from_secs(5), async {
			while Weak::strong_count(&db) > 0 {
				smol::Timer::after(Duration::from_millis(25)).await;
			}
		})
		.await;
		remaining = Weak::strong_count(&db);
	}
	if remaining > 0 {
		warn!("{remaining} database connections remain during shutdown");
	}
	warn!("Shutdown complete.");
	Ok(())
}
