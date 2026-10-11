use std::{
	any::Any,
	net::SocketAddr,
	panic::AssertUnwindSafe,
	sync::{Arc, atomic::Ordering},
	time::Duration,
};

use bytes::Bytes;
use conduwuit::{debug_warn, error, trace, warn};
use conduwuit_api::hyper_router::MinimalRouter;
use conduwuit_service::{Services, state::State};
use futures::FutureExt;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::Full;
use hyper::body::Incoming;
use tower::Service;

const CONDUWUIT_CSP: &str =
	"default-src 'none';frame-ancestors 'none';form-action 'none';base-uri 'none';sandbox";
const CONDUWUIT_PERMISSIONS_POLICY: &str = "interest-cohort=(),browsing-topics=()";

type CallResult = Result<Response<Full<Bytes>>, std::convert::Infallible>;

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

	let call = AssertUnwindSafe(router.call(request)).catch_unwind();
	let shutdown = async {
		services.server.until_shutdown().await;
		smol::Timer::after(Duration::from_secs(services.server.config.client_shutdown_timeout))
			.await;
		Ok::<CallResult, Box<dyn Any + Send + 'static>>(Ok(error_response(
			StatusCode::SERVICE_UNAVAILABLE,
			"M_UNAVAILABLE",
			"Server is shutting down",
		)))
	};
	let combined = async {
		futures::select! {
			result = call.fuse() => result,
			result = shutdown.fuse() => result,
		}
	};
	let request_timeout = Duration::from_secs(services.server.config.client_request_timeout);
	let mut response = match conduwuit::timeout(request_timeout, combined).await {
		| Ok(result) => match result {
			| Ok(Ok(response)) => response,
			| Ok(Err(error)) => match error {},
			| Err(panic) => catch_panic(panic, services.as_ref()),
		},
		| Err(_) => error_response(StatusCode::REQUEST_TIMEOUT, "M_UNKNOWN", "Request timed out"),
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
			.fetch_sub(1, Ordering::Relaxed)
	};
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

#[allow(clippy::needless_pass_by_value)]
fn catch_panic(
	panic: Box<dyn Any + Send + 'static>,
	services: &Services,
) -> Response<Full<Bytes>> {
	services
		.server
		.metrics
		.requests_panic
		.fetch_add(1, Ordering::Release);

	let details = panic_details(&*panic);
	error!("{details:#}");
	panic_response()
}

fn panic_details(panic: &(dyn Any + Send)) -> String {
	match panic.downcast_ref::<String>() {
		| Some(details) => details.clone(),
		| None => match panic.downcast_ref::<&str>() {
			| Some(details) => (*details).to_owned(),
			| None => "Unknown internal server error occurred.".to_owned(),
		},
	}
}

fn panic_response() -> Response<Full<Bytes>> {
	let mut body = slipstream::ObjectBuilder::new();
	body.field("errcode", "M_UNKNOWN");
	body.field("error", "M_UNKNOWN: Internal server error occurred");
	let body = body.finish();

	Response::builder()
		.status(StatusCode::INTERNAL_SERVER_ERROR)
		.header(header::CONTENT_TYPE, "application/json")
		.body(Full::from(body.to_string()))
		.expect("Failed to create response for our panic catcher?")
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
	let _ = headers.insert(header::X_XSS_PROTECTION, header::HeaderValue::from_static("0"));
	let _ = headers.insert(header::X_FRAME_OPTIONS, header::HeaderValue::from_static("DENY"));
	let _ = headers.insert(
		"permissions-policy",
		header::HeaderValue::from_static(CONDUWUIT_PERMISSIONS_POLICY),
	);
	let _ = headers
		.insert(header::CONTENT_SECURITY_POLICY, header::HeaderValue::from_static(CONDUWUIT_CSP));
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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn panic_response_does_not_expose_payload() {
		let payload: Box<dyn Any + Send> = Box::new("secret-token-123");
		assert!(panic_details(&*payload).contains("secret-token-123"));

		let response = panic_response();
		let body = response.into_body().into_inner().expect("response body");
		let body = String::from_utf8_lossy(&body);

		assert!(!body.contains("secret-token-123"));
		assert!(body.contains("Internal server error occurred"));
	}
}
