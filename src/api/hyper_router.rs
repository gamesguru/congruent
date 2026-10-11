//! Incremental replacement boundary for the Axum router.

use std::{
	collections::HashMap,
	convert::Infallible,
	future::Future,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
};

use bytes::Bytes;
use http::Method;
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming};
use matchit::Router;
use percent_encoding::percent_decode_str;

fn unrecognized_response(status: http::StatusCode) -> Response<Full<Bytes>> {
	Response::builder()
		.status(status)
		.header(http::header::CONTENT_TYPE, "application/json")
		.body(Full::from(Bytes::from_static(
			br#"{"errcode":"M_UNRECOGNIZED","error":"Unrecognized request"}"#,
		)))
		.expect("static unrecognized response is valid")
}

fn bad_request_response() -> Response<Full<Bytes>> {
	Response::builder()
		.status(http::StatusCode::BAD_REQUEST)
		.header(http::header::CONTENT_TYPE, "application/json")
		.body(Full::from(Bytes::from_static(
			br#"{"errcode":"M_BAD_JSON","error":"Invalid percent-encoded path parameter"}"#,
		)))
		.expect("static bad request response is valid")
}

#[derive(Clone)]
pub struct RouteManifestEntry {
	pub method: Method,
	pub path: String,
	pub handler: &'static str,
}

static ROUTE_MANIFEST: std::sync::OnceLock<std::sync::Mutex<Vec<RouteManifestEntry>>> =
	std::sync::OnceLock::new();

fn manifest() -> &'static std::sync::Mutex<Vec<RouteManifestEntry>> {
	ROUTE_MANIFEST.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

pub fn record_route(method: Method, path: &str, handler: &'static str) {
	manifest()
		.lock()
		.expect("route manifest mutex poisoned")
		.push(RouteManifestEntry { method, path: path.to_owned(), handler });
}

#[must_use]
pub fn route_manifest() -> Vec<RouteManifestEntry> {
	manifest()
		.lock()
		.expect("route manifest mutex poisoned")
		.clone()
}

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
	pub routes: Vec<(Method, String, BoxedHandler)>,
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
			routes: Vec::new(),
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
			.insert(path, handler.clone())
			.map_err(|error| error.to_string())?;
		self.routes.push((method, path.to_owned(), handler));
		Ok(())
	}

	#[must_use]
	pub(crate) fn route(
		mut self,
		path: &'static str,
		spec: crate::router::handler::RouteSpec,
	) -> Self {
		for builder in spec.builders {
			let (method, _, handler) = builder(path);
			self.register(method, path, handler).expect("valid route");
		}
		self
	}

	#[must_use]
	pub fn merge(mut self, other: Self) -> Self {
		for (method, path, handler) in other.routes {
			self.register(method, &path, handler)
				.expect("valid merged route");
		}
		self
	}

	#[must_use]
	pub fn map_response<F, Fut>(self, f: F) -> Self
	where
		F: Fn(Response<Full<Bytes>>) -> Fut + Clone + Send + Sync + 'static,
		Fut: Future<Output = Response<Full<Bytes>>> + Send + 'static,
	{
		let mut mapped = Self::new();
		for (method, path, handler) in self.routes {
			let inner = handler.clone();
			let f = f.clone();
			let handler: BoxedHandler = Arc::new(move |request, params| {
				let future = inner(request, params);
				let f = f.clone();
				Box::pin(async move { f(future.await).await })
			});
			mapped
				.register(method, &path, handler)
				.expect("mapped route is valid");
		}
		mapped
	}

	#[must_use]
	pub fn map_request<F>(self, f: F) -> Self
	where
		F: Fn(Request<Incoming>) -> Request<Incoming> + Clone + Send + Sync + 'static,
	{
		let mut mapped = Self::new();
		for (method, path, handler) in self.routes {
			let inner = handler.clone();
			let f = f.clone();
			let handler: BoxedHandler =
				Arc::new(move |request, params| inner(f(request), params));
			mapped
				.register(method, &path, handler)
				.expect("mapped route is valid");
		}
		mapped
	}
}

impl tower::Service<Request<Incoming>> for MinimalRouter {
	type Error = Infallible;
	type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
	type Response = Response<Full<Bytes>>;

	fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn call(&mut self, request: Request<Incoming>) -> Self::Future {
		let router = match *request.method() {
			| Method::GET | Method::HEAD => &self.get,
			| Method::POST => &self.post,
			| Method::PUT => &self.put,
			| Method::DELETE => &self.delete,
			| _ => {
				return Box::pin(async {
					Ok(unrecognized_response(http::StatusCode::METHOD_NOT_ALLOWED))
				});
			},
		};

		match router.at(request.uri().path()) {
			| Ok(matched) => {
				let handler = matched.value.clone();
				let params: HashMap<String, String> = match matched
					.params
					.iter()
					.map(|(key, value)| {
						percent_decode_str(value)
							.decode_utf8()
							.map(|value| (key.to_owned(), value.into_owned()))
					})
					.collect()
				{
					| Ok(params) => params,
					| Err(_) => return Box::pin(async { Ok(bad_request_response()) }),
				};
				// Positional args must follow the order of the path template, which a
				// HashMap does not preserve; matchit yields them in path order.
				let path = matched
					.params
					.iter()
					.filter_map(|(_, value)| {
						percent_decode_str(value)
							.decode_utf8()
							.ok()
							.map(std::borrow::Cow::into_owned)
					})
					.collect::<Vec<_>>();
				let mut request = request;
				request.extensions_mut().insert(path);
				Box::pin(async move { Ok(handler(request, params).await) })
			},
			| Err(_) => {
				// A known path under a different method is a 405, not a 404.
				let path = request.uri().path();
				let known = [&self.get, &self.post, &self.put, &self.delete]
					.into_iter()
					.any(|router| router.at(path).is_ok());
				let status = if known {
					http::StatusCode::METHOD_NOT_ALLOWED
				} else {
					http::StatusCode::NOT_FOUND
				};
				Box::pin(async move { Ok(unrecognized_response(status)) })
			},
		}
	}
}
