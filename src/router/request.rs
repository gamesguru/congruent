use std::{
	net::SocketAddr,
	sync::{Arc, atomic::Ordering},
};

use bytes::Bytes;
use conduwuit::{debug_warn, trace, warn};
use conduwuit_api::hyper_router::MinimalRouter;
use conduwuit_service::{Services, state::State};
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::Full;
use hyper::body::Incoming;
use tower::Service;

pub(crate) async fn handle(
	mut router: MinimalRouter,
	services: Arc<Services>,
	state: State,
	peer: SocketAddr,
	mut request: Request<Incoming>,
) -> Response<Full<Bytes>> {
	if !services.server.running() {
		debug_warn!(method = %request.method(), uri = %request.uri(), "unavailable pending shutdown");
		return error_response(
			StatusCode::SERVICE_UNAVAILABLE,
			"M_UNAVAILABLE",
			"Server is shutting down",
		);
	}
	if request.method() == Method::OPTIONS {
		return cors_response();
	}

	request.extensions_mut().insert(state);
	request.extensions_mut().insert(peer);
	let method = request.method().clone();
	let uri = request.uri().clone();
	let start = std::time::Instant::now();
	#[cfg(debug_assertions)]
	services
		.server
		.metrics
		.requests_handle_active
		.fetch_add(1, Ordering::Relaxed);

	let mut response = match router.call(request).await {
		| Ok(response) => response,
		| Err(error) => match error {},
	};

	#[cfg(debug_assertions)]
	{
		services
			.server
			.metrics
			.requests_handle_finished
			.fetch_add(1, Ordering::Relaxed);
		services
			.server
			.metrics
			.requests_handle_active
			.fetch_sub(1, Ordering::Relaxed);
	}
	if response.status() == StatusCode::METHOD_NOT_ALLOWED {
		response = error_response(
			StatusCode::METHOD_NOT_ALLOWED,
			"M_UNRECOGNIZED",
			"Method not allowed",
		);
	}
	if method == Method::HEAD {
		let (parts, _) = response.into_parts();
		response = Response::from_parts(parts, Full::new(Bytes::new()));
	}
	add_headers(&mut response);

	services.server.metrics.requests_time.fetch_add(
		u64::try_from(start.elapsed().as_micros()).unwrap_or(u64::MAX),
		Ordering::Relaxed,
	);
	if response.status().is_server_error() {
		services
			.server
			.metrics
			.requests_fail
			.fetch_add(1, Ordering::Relaxed);
		warn!(%method, %uri, status = %response.status(), "request failed");
	} else {
		services
			.server
			.metrics
			.requests_success
			.fetch_add(1, Ordering::Relaxed);
		trace!(%method, %uri, status = %response.status(), "request complete");
	}
	response
}

fn cors_response() -> Response<Full<Bytes>> {
	Response::builder()
		.status(StatusCode::NO_CONTENT)
		.header(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")
		.header(
			header::ACCESS_CONTROL_ALLOW_METHODS,
			"GET, HEAD, PATCH, POST, PUT, DELETE, OPTIONS",
		)
		.header(
			header::ACCESS_CONTROL_ALLOW_HEADERS,
			"Origin, X-Requested-With, Content-Type, Accept, Authorization",
		)
		.body(Full::new(Bytes::new()))
		.expect("static CORS response is valid")
}

fn add_headers(response: &mut Response<Full<Bytes>>) {
	let headers = response.headers_mut();
	let _ = headers.insert("origin-agent-cluster", header::HeaderValue::from_static("?1"));
	let _ = headers
		.insert(header::X_CONTENT_TYPE_OPTIONS, header::HeaderValue::from_static("nosniff"));
	let _ = headers.insert(header::X_FRAME_OPTIONS, header::HeaderValue::from_static("DENY"));
	let _ = headers
		.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, header::HeaderValue::from_static("*"));
}

fn error_response(status: StatusCode, code: &str, message: &str) -> Response<Full<Bytes>> {
	let body = format!(r#"{{"errcode":"{code}","error":"{message}"}}"#);
	Response::builder()
		.status(status)
		.header(header::CONTENT_TYPE, "application/json")
		.body(Full::from(body))
		.expect("error response is valid")
}
