use std::{
	io,
	pin::Pin,
	task::{Context, Poll},
};

use futures_io::{AsyncRead, AsyncWrite};
use hyper::rt::{Read, ReadBufCursor, Write};

/// Adapts futures-io transports to Hyper's runtime-neutral I/O traits.
pub struct SmolIo<T>(pub T);

impl<T: AsyncRead + Unpin> Read for SmolIo<T> {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		mut buf: ReadBufCursor<'_>,
	) -> Poll<io::Result<()>> {
		let mut scratch = vec![0_u8; 8192].into_boxed_slice();
		let capacity = buf.remaining().min(scratch.len());
		if capacity == 0 {
			return Poll::Ready(Ok(()));
		}

		match Pin::new(&mut self.0).poll_read(cx, &mut scratch[..capacity]) {
			| Poll::Ready(Ok(read)) => {
				buf.put_slice(&scratch[..read]);
				Poll::Ready(Ok(()))
			},
			| Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
			| Poll::Pending => Poll::Pending,
		}
	}
}

impl<T: AsyncWrite + Unpin> Write for SmolIo<T> {
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
