use std::{error::Error, net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use conduwuit_core::{JoinHandle, Result, Server, SmolIo, debug, err, info, warn};
use futures::{FutureExt, future::Either};
use futures_rustls::{TlsAcceptor, rustls::ServerConfig};
use http::{Request, Response};
use http_body::Body;
use hyper::{body::Incoming, server::conn::http1, service::service_fn};
use rustls_pemfile::{certs, private_key};
use tokio::sync::broadcast;
use tower::{Service, ServiceExt};

const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(50);

async fn load_tls_config(certs_path: &str, key_path: &str) -> Result<Arc<ServerConfig>> {
	let certificate_data = async_fs::read(certs_path).await.map_err(|e| {
		err!(Config("tls.certs", "Failed to read certificate file {certs_path}: {e}"))
	})?;
	let key_data = async_fs::read(key_path).await.map_err(|e| {
		err!(Config("tls.key", "Failed to read private key file {key_path}: {e}"))
	})?;

	let certificates = certs(&mut certificate_data.as_slice())
		.collect::<std::result::Result<Vec<_>, _>>()
		.map_err(|e| err!(Config("tls.certs", "Failed to parse certificates: {e}")))?;
	let key = private_key(&mut key_data.as_slice())
		.map_err(|e| err!(Config("tls.key", "Failed to parse private key: {e}")))?
		.ok_or_else(|| err!(Config("tls.key", "No private key found in key file")))?;

	let config = ServerConfig::builder()
		.with_no_client_auth()
		.with_single_cert(certificates, key)
		.map_err(|e| err!(Config("tls", "Failed to configure TLS: {e}")))?;

	Ok(Arc::new(config))
}

async fn accept_tls(
	acceptor: TlsAcceptor,
	stream: async_net::TcpStream,
) -> Option<futures_rustls::server::TlsStream<async_net::TcpStream>> {
	match futures::future::select(
		Box::pin(acceptor.accept(stream)),
		Box::pin(smol::Timer::after(TLS_HANDSHAKE_TIMEOUT)),
	)
	.await
	{
		| Either::Left((Ok(stream), _)) => Some(stream),
		| Either::Left((Err(error), _)) => {
			debug!(%error, "TLS handshake failed");
			None
		},
		| Either::Right((..)) => {
			debug!("TLS handshake timed out");
			None
		},
	}
}

async fn listener<S, B>(
	server: Arc<Server>,
	app: S,
	addr: SocketAddr,
	config: Arc<ServerConfig>,
	mut shutdown: broadcast::Receiver<()>,
) -> Result<()>
where
	S: Service<Request<Incoming>, Response = Response<B>> + Clone + Send + 'static,
	S::Future: Send + 'static,
	S::Error: Error + Send + Sync + 'static,
	B: Body<Data = Bytes> + Send + 'static,
	B::Error: Error + Send + Sync + 'static,
{
	let listener = async_net::TcpListener::bind(addr)
		.await
		.map_err(|e| err!(error!("Failed to bind direct TLS listener on {addr}: {e}")))?;
	let acceptor = TlsAcceptor::from(config);
	let runtime = server.runtime().clone();
	let mut connections: Vec<JoinHandle<()>> = Vec::new();

	loop {
		futures::select! {
			_ = shutdown.recv().fuse() => break,
			connection = listener.accept().fuse() => {
				let (stream, peer) = match connection {
					Ok(connection) => connection,
					Err(error) => {
						warn!(%error, %addr, "Direct TLS accept failed; retrying");
						smol::Timer::after(ACCEPT_RETRY_DELAY).await;
						continue;
					},
				};

				let acceptor = acceptor.clone();
				let app = app.clone();
				connections.push(runtime.spawn(async move {
					let Some(stream) = accept_tls(acceptor, stream).await else { return };
					let service = service_fn(move |request| app.clone().oneshot(request));
					if let Err(error) = http1::Builder::new()
						.serve_connection(SmolIo(stream), service)
						.await
					{
						debug!(%error, %peer, "Direct TLS connection closed with error");
					}
				}));
			}
		}
	}

	for mut connection in connections {
		connection.abort();
	}
	Ok(())
}

pub async fn serve<S, B>(
	server: &Arc<Server>,
	app: S,
	addrs: Vec<SocketAddr>,
	mut shutdown: broadcast::Receiver<()>,
) -> Result<()>
where
	S: Service<Request<Incoming>, Response = Response<B>> + Clone + Send + 'static,
	S::Future: Send + 'static,
	S::Error: Error + Send + Sync + 'static,
	B: Body<Data = Bytes> + Send + 'static,
	B::Error: Error + Send + Sync + 'static,
{
	let tls = &server.config.tls;
	let certs_path = tls.certs.as_deref().ok_or_else(|| {
		err!(Config("tls.certs", "Missing required value in tls config section"))
	})?;
	let key_path = tls
		.key
		.as_deref()
		.ok_or_else(|| err!(Config("tls.key", "Missing required value in tls config section")))?;

	info!("Direct TLS is enabled; a reverse proxy is recommended for production deployments.");
	debug!(
		"Using direct TLS. Certificate path {certs_path} and certificate private key path \
		 {key_path}"
	);
	let config = load_tls_config(certs_path, key_path).await?;

	if tls.dual_protocol {
		warn!("dual_protocol is not supported by the smol direct TLS listener; serving TLS only");
	}

	let mut listeners = Vec::with_capacity(addrs.len());
	for addr in addrs {
		listeners.push(server.runtime().spawn(listener(
			Arc::clone(server),
			app.clone(),
			addr,
			Arc::clone(&config),
			shutdown.resubscribe(),
		)));
	}

	let _ = shutdown.recv().await;
	for mut listener in listeners {
		listener.abort();
	}
	Ok(())
}
