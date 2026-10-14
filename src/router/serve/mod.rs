mod plain;
#[cfg(unix)]
mod unix;

use std::sync::Arc;

use conduwuit::Result;
use conduwuit_service::{Services, state::State};

use super::layers;

pub(super) async fn serve(services: Arc<Services>) -> Result {
	let server = &services.server;
	if !server.config.listening {
		server.until_shutdown().await;
		return Ok(());
	}
	let addrs = server.config.get_bind_addrs();
	let (app, guard, state) = layers::build(&services)?;
	let result = if cfg!(unix) && server.config.unix_socket_path.is_some() {
		#[cfg(unix)]
		{
			unix::serve(server, &services, app, state).await
		}
		#[cfg(not(unix))]
		{
			let _ = (app, state, addrs);
			Err(conduwuit::err!(Config(
				"unix_socket_path",
				"Unix socket serving is only available on Unix"
			)))
		}
	} else if server.config.tls.certs.is_some() {
		#[cfg(feature = "direct_tls")]
		{
			let services = Arc::clone(&services);
			let router = app.clone();
			let service_factory = move |peer| {
				let router = router.clone();
				let services = Arc::clone(&services);
				tower::service_fn(move |request| {
					let router = router.clone();
					let services = Arc::clone(&services);
					async move {
						Ok::<_, std::convert::Infallible>(
							crate::request::handle(router, services, state, peer, request).await,
						)
					}
				})
			};
			conduwuit_direct_tls::serve(server, addrs, service_factory).await
		}
		#[cfg(not(feature = "direct_tls"))]
		{
			let _ = (app, state, addrs);
			Err(conduwuit::err!(Config(
				"tls",
				"conduwuit was not built with direct TLS support (\"direct_tls\")"
			)))
		}
	} else {
		plain::serve(server, &services, app, state, addrs).await
	};
	drop(guard);
	result
}

#[allow(dead_code)]
fn _state_marker(_: State) {}
