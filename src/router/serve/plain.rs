use std::{net::SocketAddr, sync::Arc, time::Duration};

use conduwuit::{JoinSet, Result, Server, debug, debug_error, err, info, warn};
use conduwuit_api::hyper_router::MinimalRouter;
use conduwuit_core::SmolIo;
use conduwuit_service::{Services, state::State};
use futures::FutureExt;
use hyper::{body::Incoming, server::conn::http1, service::service_fn};

use crate::request;

pub(super) async fn serve(
	server: &Arc<Server>,
	services: &Arc<Services>,
	router: MinimalRouter,
	state: State,
	addrs: Vec<SocketAddr>,
) -> Result<()> {
	let mut listeners = Vec::with_capacity(addrs.len());
	for addr in addrs.iter().copied() {
		let listener = async_net::TcpListener::bind(addr)
			.await
			.map_err(|error| err!(error!("failed to bind listener on {addr}: {error}")))?;
		listeners.push((addr, listener));
	}
	info!(?addrs, "Listening");

	let mut tasks = JoinSet::new();
	for (addr, listener) in listeners {
		tasks.spawn_on(
			accept_loop(
				Arc::clone(server),
				Arc::clone(services),
				router.clone(),
				state,
				listener,
				addr,
			),
			server.runtime(),
		);
	}

	while let Some(result) = tasks.join_next().await {
		match result {
			| Ok(Ok(())) => {},
			| Ok(Err(error)) => warn!(%error, "listener terminated with error"),
			| Err(error) => warn!(%error, "listener task failed"),
		}
	}
	Ok(())
}

async fn accept_loop(
	server: Arc<Server>,
	services: Arc<Services>,
	router: MinimalRouter,
	state: State,
	listener: async_net::TcpListener,
	addr: SocketAddr,
) -> Result<()> {
	let mut connections = JoinSet::new();
	loop {
		let (stream, peer) = futures::select! {
			() = server.until_shutdown().fuse() => break,
			() = reap(&mut connections).fuse() => continue,
			accepted = listener.accept().fuse() => match accepted {
				| Ok(connection) => connection,
				| Err(error) => {
					warn!(%error, %addr, "accept failed; retrying");
					smol::Timer::after(Duration::from_millis(50)).await;
					continue;
				},
			},
		};
		let services = Arc::clone(&services);
		let router = router.clone();
		connections.spawn_on(
			async move {
				let service = service_fn(move |request: http::Request<Incoming>| {
					let router = router.clone();
					let services = services.clone();
					async move {
						Ok::<_, std::convert::Infallible>(
							request::handle(
								router.clone(),
								services.clone(),
								state,
								peer,
								request,
							)
							.await,
						)
					}
				});
				if let Err(error) = http1::Builder::new()
					.serve_connection(SmolIo(stream), service)
					.await
				{
					debug!(%error, %peer, "connection closed with error");
				}
			},
			server.runtime(),
		);
	}

	drain(&server, &mut connections).await;
	Ok(())
}

async fn reap(connections: &mut JoinSet<()>) {
	if connections.is_empty() {
		futures::future::pending::<()>().await;
	} else {
		let _ = connections.join_next().await;
	}
}

async fn drain(server: &Server, connections: &mut JoinSet<()>) {
	let timeout = Duration::from_secs(server.config.client_shutdown_timeout);
	let drained =
		conduwuit::timeout(timeout, async { while connections.join_next().await.is_some() {} })
			.await;
	if drained.is_err() {
		debug_error!(
			remaining = connections.len(),
			"timed out draining client connections; cancelling remaining connections"
		);
	}
	connections.abort_all();
}
