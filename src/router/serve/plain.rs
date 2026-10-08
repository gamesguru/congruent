use std::{net::SocketAddr, sync::Arc, time::Duration};

use conduwuit::{Result, Server, debug, info, warn};
use conduwuit_api::hyper_router::MinimalRouter;
use conduwuit_core::SmolIo;
use conduwuit_service::{Services, state::State};
use hyper::{body::Incoming, server::conn::http1, service::service_fn};

use crate::request;

pub(super) async fn serve(
	server: &Arc<Server>,
	services: &Arc<Services>,
	router: MinimalRouter,
	state: State,
	addrs: Vec<SocketAddr>,
) -> Result<()> {
	let mut listeners = Vec::new();
	for addr in addrs.iter().copied() {
		listeners.push(server.runtime().spawn(listener(
			Arc::clone(server),
			Arc::clone(services),
			router.clone(),
			state,
			addr,
		)));
	}
	info!(?addrs, "Listening");
	server.until_shutdown().await;
	for task in &mut listeners {
		task.abort();
	}
	Ok(())
}

async fn listener(
	server: Arc<Server>,
	services: Arc<Services>,
	router: MinimalRouter,
	state: State,
	addr: SocketAddr,
) -> Result<()> {
	let listener = async_net::TcpListener::bind(addr).await?;
	loop {
		let (stream, peer) = match listener.accept().await {
			| Ok(connection) => connection,
			| Err(error) => {
				warn!(%error, %addr, "accept failed; retrying");
				smol::Timer::after(Duration::from_millis(50)).await;
				continue;
			},
		};
		let server = Arc::clone(&server);
		let services = Arc::clone(&services);
		let router = router.clone();
		drop(server.runtime().spawn(async move {
			let service = service_fn(move |request: http::Request<Incoming>| {
				let router = router.clone();
				let services = services.clone();
				async move {
					Ok::<_, std::convert::Infallible>(
						request::handle(router.clone(), services.clone(), state, peer, request)
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
		}));
	}
}
