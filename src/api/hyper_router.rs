//! Incremental replacement boundary for the Axum router.

use std::{convert::Infallible, future::Future, pin::Pin};

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming};
use matchit::Router;

pub(crate) struct MinimalRouter {
	pub(crate) inner: Router<()>,
}

impl hyper::service::Service<Request<Incoming>> for MinimalRouter {
	type Error = Infallible;
	type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
	type Response = Response<Full<Bytes>>;

	fn call(&self, _request: Request<Incoming>) -> Self::Future {
		Box::pin(async {
			Ok(Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.body(Full::default())
				.expect("static 404 response is valid"))
		})
	}
}
