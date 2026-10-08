use bytes::Bytes;
pub use conduwuit_core::utils::SmolIo;
use futures_io::{AsyncRead, AsyncWrite};
use http_body_util::Full;
use hyper::client::conn::http1;

/// Performs an HTTP/1.1 Hyper handshake and drives the connection on smol.
pub async fn connect_http1<T>(stream: T) -> conduwuit::Result<http1::SendRequest<Full<Bytes>>>
where
	T: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
{
	let (sender, connection) = http1::handshake(SmolIo(stream))
		.await
		.map_err(|error| conduwuit::Error::HttpClient(error.to_string().into()))?;

	smol::spawn(async move {
		if let Err(error) = connection.await {
			log::debug!("HTTP/1 connection closed with error: {error}");
		}
	})
	.detach();

	Ok(sender)
}
