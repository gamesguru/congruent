//! Minimal HTTP server for route tests.

use std::sync::Arc;

use bytes::Bytes;
use conduwuit_core::{RuntimeHandle, SmolIo};
use http_body_util::{BodyExt, Full};
use hyper::{Request, body::Incoming, client::conn::http1, service::service_fn};
use tower::Service;

pub(super) struct TestServer {
	pub(super) addr: std::net::SocketAddr,
	pub(super) task: conduwuit_core::JoinHandle<()>,
}

impl TestServer {
	pub(super) async fn spawn(
		router: crate::hyper_router::MinimalRouter,
		state: crate::State,
	) -> Self {
		let listener = async_net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let addr = listener.local_addr().unwrap();
		let task = RuntimeHandle::new().spawn(async move {
			let router = Arc::new(std::sync::Mutex::new(router));
			while let Ok((stream, _)) = listener.accept().await {
				let router = Arc::clone(&router);
				drop(RuntimeHandle::new().spawn(async move {
					let service = service_fn(move |mut request: Request<Incoming>| {
						request.extensions_mut().insert(state);
						router.lock().unwrap().call(request)
					});
					let _ = hyper::server::conn::http1::Builder::new()
						.serve_connection(SmolIo(stream), service)
						.await;
				}));
			}
		});
		Self { addr, task }
	}

	pub(super) async fn send(
		&self,
		request: Request<Bytes>,
	) -> conduwuit::Result<http::Response<Bytes>> {
		let stream = async_net::TcpStream::connect(self.addr).await?;
		let (mut sender, connection) =
			http1::handshake(SmolIo(stream)).await.map_err(|error| {
				conduwuit::err!(Request(Unknown("test HTTP handshake failed: {error}")))
			})?;
		drop(RuntimeHandle::new().spawn(async move {
			let _ = connection.await;
		}));
		let (parts, body) = request.into_parts();
		let response = sender
			.send_request(Request::from_parts(parts, Full::new(body)))
			.await
			.map_err(|error| {
				conduwuit::err!(Request(Unknown("test HTTP request failed: {error}")))
			})?;
		let (parts, body) = response.into_parts();
		let body = body
			.collect()
			.await
			.map_err(|error| {
				conduwuit::err!(Request(Unknown("test HTTP response failed: {error}")))
			})?
			.to_bytes();
		Ok(http::Response::from_parts(parts, body))
	}
}
