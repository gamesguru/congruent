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

fn unrecognized_response(status: http::StatusCode) -> Response<Full<Bytes>> {
	Response::builder()
		.status(status)
		.header(http::header::CONTENT_TYPE, "application/json")
		.body(Full::from(Bytes::from_static(
			br#"{"errcode":"M_UNRECOGNIZED","error":"Unrecognized request"}"#,
		)))
		.expect("static unrecognized response is valid")
}

#[derive(Clone)]
pub struct RouteManifestEntry {
	pub method: Method,
	pub path: String,
	pub matchit_path: String,
	pub handler: &'static str,
}

static ROUTE_MANIFEST: std::sync::OnceLock<std::sync::Mutex<Vec<RouteManifestEntry>>> =
	std::sync::OnceLock::new();

fn manifest() -> &'static std::sync::Mutex<Vec<RouteManifestEntry>> {
	ROUTE_MANIFEST.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

#[must_use]
pub fn matchit_path(path: &str) -> String {
	let mut converted = String::with_capacity(path.len());
	let mut parameter = false;
	for character in path.chars() {
		match character {
			| '{' => {
				converted.push(':');
				parameter = true;
			},
			| '}' if parameter => parameter = false,
			| _ => converted.push(character),
		}
	}
	converted
}

pub fn record_route(method: Method, path: &str, handler: &'static str) {
	manifest()
		.lock()
		.expect("route manifest mutex poisoned")
		.push(RouteManifestEntry {
			method,
			path: path.to_owned(),
			matchit_path: matchit_path(path),
			handler,
		});
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

impl tower::Service<Request<Incoming>> for MinimalRouter {
	type Error = Infallible;
	type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
	type Response = Response<Full<Bytes>>;

	fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
		Poll::Ready(Ok(()))
	}

	fn call(&mut self, request: Request<Incoming>) -> Self::Future {
		let router = match *request.method() {
			| Method::GET => &self.get,
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
				let params = matched
					.params
					.iter()
					.map(|(key, value)| (key.to_owned(), value.to_owned()))
					.collect();
				let path = params.values().cloned().collect::<Vec<_>>();
				let mut request = request;
				request.extensions_mut().insert(path);
				Box::pin(async move { Ok(handler(request, params).await) })
			},
			| Err(_) =>
				Box::pin(async { Ok(unrecognized_response(http::StatusCode::NOT_FOUND)) }),
		}
	}
}
