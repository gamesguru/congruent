#![cfg(unix)]

use std::{
	fs::Permissions,
	net::{IpAddr, Ipv4Addr, SocketAddr},
	os::unix::{fs::PermissionsExt, net::UnixListener},
	path::Path,
	sync::Arc,
};

use async_io::Async;
use conduwuit::{Result, Server, debug, info, warn};
use conduwuit_api::hyper_router::MinimalRouter;
use conduwuit_core::SmolIo;
use conduwuit_service::{Services, state::State};
use futures::FutureExt;
use hyper::{body::Incoming, server::conn::http1, service::service_fn};

use crate::request;

const NULL_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);

pub(super) async fn serve(
	server: &Arc<Server>,
	services: &Arc<Services>,
	router: MinimalRouter,
	state: State,
) -> Result<()> {
	let path = server
		.config
		.unix_socket_path
		.as_ref()
		.expect("unix socket path must be configured");
	init(path)?;

	let listener = Async::<UnixListener>::bind(path)?;
	std::fs::set_permissions(path, Permissions::from_mode(server.config.unix_socket_perms))?;
	info!(?path, "Listening");
	loop {
		let (stream, _) = futures::select! {
			connection = listener.accept().fuse() => match connection {
				| Ok(connection) => connection,
				| Err(error) => {
					warn!(%error, ?path, "accept failed; retrying");
					smol::Timer::after(std::time::Duration::from_millis(50)).await;
					continue;
				},
			},
			_ = server.until_shutdown().fuse() => break,
		};

		let services = Arc::clone(services);
		let router = router.clone();
		drop(server.runtime().spawn(async move {
			let service = service_fn(move |request: http::Request<Incoming>| {
				let services = Arc::clone(&services);
				let router = router.clone();
				async move {
					Ok::<_, std::convert::Infallible>(
						request::handle(router, services, state, NULL_ADDR, request).await,
					)
				}
			});
			if let Err(error) = http1::Builder::new()
				.serve_connection(SmolIo(stream), service)
				.await
			{
				debug!(%error, "UNIX socket connection closed with error");
			}
		}));
	}

	if let Err(error) = std::fs::remove_file(path) {
		warn!(%error, ?path, "Failed to remove UNIX socket");
	}
	Ok(())
}

fn init(path: &Path) -> Result {
	if path.exists() {
		warn!(?path, "Removing existing UNIX socket (unclean shutdown?)");
		std::fs::remove_file(path)?;
	}

	if let Some(parent) = path.parent() {
		std::fs::create_dir_all(parent)?;
	}

	Ok(())
}
