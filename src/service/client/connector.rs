use std::{
	io,
	pin::Pin,
	task::{Context, Poll},
};

use bytes::Bytes;
use futures_io::{AsyncRead, AsyncWrite};
use http_body_util::Full;
use hyper::{
	client::conn::http1,
	rt::{Read, ReadBufCursor, Write},
};

/// Adapts smol/futures-io transports to Hyper's runtime-neutral I/O traits.
pub struct SmolIo<T>(pub T);

impl<T> Read for SmolIo<T>
where
	T: AsyncRead + Unpin,
{
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		mut buf: ReadBufCursor<'_>,
	) -> Poll<io::Result<()>> {
		let mut scratch = [0_u8; 16 * 1024];
		match Pin::new(&mut self.0).poll_read(cx, &mut scratch) {
			| Poll::Ready(Ok(read)) => {
				buf.put_slice(&scratch[..read]);
				Poll::Ready(Ok(()))
			},
			| Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
			| Poll::Pending => Poll::Pending,
		}
	}
}

impl<T> Write for SmolIo<T>
where
	T: AsyncWrite + Unpin,
{
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		Pin::new(&mut self.0).poll_write(cx, buf)
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Pin::new(&mut self.0).poll_flush(cx)
	}

	fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Pin::new(&mut self.0).poll_close(cx)
	}
}

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
