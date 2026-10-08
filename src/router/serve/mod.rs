mod plain;

use std::sync::Arc;

use conduwuit::{Result, err};
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
		Err(err!(Config(
			"unix_socket_path",
			"Unix socket serving is not yet available in the smol listener"
		)))
	} else if server.config.tls.certs.is_some() {
		let _ = (app, state, addrs);
		Err(err!(Config("tls", "direct TLS listener migration is not yet available")))
	} else {
		plain::serve(server, &services, app, state, addrs).await
	};
	drop(guard);
	result
}

#[allow(dead_code)]
fn _state_marker(_: State) {}
