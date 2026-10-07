use std::{
	net::SocketAddr,
	sync::{Arc, atomic::Ordering},
};

use axum::Router;
use conduwuit::{Result, Server, debug_info, info};
use tokio::{net::TcpListener, sync::broadcast, task::JoinSet};

pub(super) async fn serve(
	server: &Arc<Server>,
	app: Router,
	addrs: Vec<SocketAddr>,
	shutdown: broadcast::Receiver<()>,
) -> Result<()> {
	let app = app.into_make_service_with_connect_info::<SocketAddr>();
	let mut join_set = JoinSet::new();
	for addr in &addrs {
		let listener = TcpListener::bind(addr).await?;
		let app = app.clone();
		let mut shutdown = shutdown.resubscribe();
		join_set.spawn_on(
			async move {
				axum::serve(listener, app)
					.with_graceful_shutdown(async move {
						let _ = shutdown.recv().await;
					})
					.await
			},
			server.runtime(),
		);
	}

	info!("Listening on {addrs:?}");
	while join_set.join_next().await.is_some() {}

	let handle_active = server
		.metrics
		.requests_handle_active
		.load(Ordering::Relaxed);
	debug_info!(
		handle_finished = server
			.metrics
			.requests_handle_finished
			.load(Ordering::Relaxed),
		panics = server.metrics.requests_panic.load(Ordering::Relaxed),
		handle_active,
		"Stopped listening on {addrs:?}",
	);

	debug_assert!(handle_active == 0, "active request handles still pending");

	Ok(())
}
