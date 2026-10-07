//! Incremental replacement boundary for the Axum router.

use std::{collections::HashMap, convert::Infallible, future::Future, pin::Pin, sync::Arc};

use bytes::Bytes;
use http::Method;
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming};
use matchit::Router;

pub type BoxedHandler = Arc<
	dyn Fn(
			Request<Incoming>,
			HashMap<String, String>,
		) -> Pin<Box<dyn Future<Output = Response<Full<Bytes>>> + Send>>
		+ Send
		+ Sync,
>;

#[derive(Clone)]
pub struct MinimalRouter {
	pub get: Router<BoxedHandler>,
	pub post: Router<BoxedHandler>,
	pub put: Router<BoxedHandler>,
	pub delete: Router<BoxedHandler>,
}

impl Default for MinimalRouter {
	fn default() -> Self { Self::new() }
}

impl MinimalRouter {
	#[must_use]
	pub fn new() -> Self {
		Self {
			get: Router::new(),
			post: Router::new(),
			put: Router::new(),
			delete: Router::new(),
		}
	}

	pub fn register(
		&mut self,
		method: Method,
		path: &str,
		handler: BoxedHandler,
	) -> Result<(), String> {
		let router = match method {
			| Method::GET => &mut self.get,
			| Method::POST => &mut self.post,
			| Method::PUT => &mut self.put,
			| Method::DELETE => &mut self.delete,
			| method => return Err(format!("unsupported method {method}")),
		};

		router
			.insert(path, handler)
			.map_err(|error| error.to_string())
	}
}

impl hyper::service::Service<Request<Incoming>> for MinimalRouter {
	type Error = Infallible;
	type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
	type Response = Response<Full<Bytes>>;

	fn call(&self, request: Request<Incoming>) -> Self::Future {
		let router = match *request.method() {
			| Method::GET => &self.get,
			| Method::POST => &self.post,
			| Method::PUT => &self.put,
			| Method::DELETE => &self.delete,
			| _ => {
				return Box::pin(async {
					Ok(Response::builder()
						.status(http::StatusCode::METHOD_NOT_ALLOWED)
						.body(Full::default())
						.expect("static 405 response is valid"))
				});
			},
		};

		match router.at(request.uri().path()) {
			| Ok(matched) => {
				let handler = matched.value.clone();
				let params = matched
					.params
					.iter()
					.map(|(key, value)| (key.to_owned(), value.to_owned()))
					.collect();
				Box::pin(async move { Ok(handler(request, params).await) })
			},
			| Err(_) => Box::pin(async {
				Ok(Response::builder()
					.status(http::StatusCode::NOT_FOUND)
					.body(Full::default())
					.expect("static 404 response is valid"))
			}),
		}
	}
}
