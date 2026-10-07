use std::{net::SocketAddr, sync::Arc};

use axum::Router;
use axum_server::{Handle as ServerHandle, tls_rustls::RustlsConfig};
use axum_server_dual_protocol::{ServerExt, axum_server::bind_rustls};
use conduwuit_core::{Result, Server, debug, info, warn};
use tokio::{sync::broadcast, task::JoinSet};

pub async fn serve(
	server: &Arc<Server>,
	app: Router,
	addrs: Vec<SocketAddr>,
	mut shutdown: broadcast::Receiver<()>,
) -> Result<()> {
	let tls = &server.config.tls;
	let certs = tls.certs.as_ref().ok_or_else(|| {
		conduwuit_core::err!(Config("tls.certs", "Missing required value in tls config section"))
	})?;
	let key = tls.key.as_ref().ok_or_else(|| {
		conduwuit_core::err!(Config("tls.key", "Missing required value in tls config section"))
	})?;

	info!("Direct TLS is enabled; a reverse proxy is recommended for production deployments.");
	debug!("Using direct TLS. Certificate path {certs} and certificate private key path {key}");
	let config = RustlsConfig::from_pem_file(certs, key).await.map_err(|e| {
		conduwuit_core::err!(Config("tls", "Failed to load certificates or key: {e}"))
	})?;
	let handle = ServerHandle::new();
	let shutdown_handle = handle.clone();
	let timeout = std::time::Duration::from_secs(server.config.client_shutdown_timeout);
	let shutdown_task = server.runtime().spawn(async move {
		let _ = shutdown.recv().await;
		shutdown_handle.graceful_shutdown(Some(timeout));
	});

	let app = app.into_make_service_with_connect_info::<SocketAddr>();
	let mut tasks = JoinSet::new();
	if tls.dual_protocol {
		for addr in addrs {
			tasks.spawn_on(
				axum_server_dual_protocol::bind_dual_protocol(addr, config.clone())
					.set_upgrade(false)
					.handle(handle.clone())
					.serve(app.clone()),
				server.runtime(),
			);
		}
		warn!("Listening with TLS and plain HTTP connections too (insecure!)");
	} else {
		for addr in addrs {
			tasks.spawn_on(
				bind_rustls(addr, config.clone())
					.handle(handle.clone())
					.serve(app.clone()),
				server.runtime(),
			);
		}
		info!("Listening with TLS");
	}

	while tasks.join_next().await.is_some() {}
	shutdown_task.abort();
	Ok(())
}
