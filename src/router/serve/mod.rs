mod plain;
mod unix;

use std::sync::Arc;

use conduwuit::{Result, err};
use conduwuit_service::Services;
use tokio::sync::broadcast;

use super::layers;

/// Serve clients
pub(super) async fn serve(
	services: Arc<Services>,
	mut shutdown: broadcast::Receiver<()>,
) -> Result {
	let server = &services.server;
	let config = &server.config;
	if !config.listening {
		return shutdown
			.recv()
			.await
			.map_err(|e| err!(error!("channel error: {e}")));
	}

	let addrs = config.get_bind_addrs();
	let (app, _guard) = layers::build(&services)?;
	if cfg!(unix) && config.unix_socket_path.is_some() {
		unix::serve(server, app, shutdown).await
	} else if config.tls.certs.is_some() {
		#[cfg(feature = "direct_tls")]
		return conduwuit_direct_tls::serve(server, app, addrs, shutdown).await;

		#[cfg(not(feature = "direct_tls"))]
		Err(err!(Config(
			"tls",
			"conduwuit was not built with direct TLS support (\"direct_tls\")",
		)))
	} else {
		plain::serve(server, app, addrs, shutdown).await
	}
}
