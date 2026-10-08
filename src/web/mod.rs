use std::{future::Future, sync::Arc};

use bytes::Bytes;
use conduwuit_api::hyper_router::{BoxedHandler, MinimalRouter};
use conduwuit_service::state;
use http::{StatusCode, header::CONTENT_TYPE};
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming};

mod pages;

type State = state::State;

#[derive(Debug, thiserror::Error)]
enum WebError {
	#[error("{0}")]
	BadRequest(String),

	#[error("This page does not exist.")]
	NotFound,

	#[error("{0}")]
	InternalError(#[from] conduwuit_core::Error),
	#[error("Request handler panicked! {0}")]
	Panic(String),
}

impl WebError {
	fn into_response(self) -> Response<Full<Bytes>> {
		let status = match &self {
			| Self::BadRequest(_) => StatusCode::BAD_REQUEST,
			| Self::NotFound => StatusCode::NOT_FOUND,
			| _ => StatusCode::INTERNAL_SERVER_ERROR,
		};

		let error = html_escape(&self.to_string());
		let body = Full::from(Bytes::from(format!(
			"<!doctype html><meta name=\"robots\" \
			 content=\"noindex\"><title>{status}</title><h1>{status}</h1><pre>{error}</pre>"
		)));
		Response::builder()
			.status(status)
			.header(CONTENT_TYPE, "text/html; charset=utf-8")
			.body(body)
			.expect("web error response is valid")
	}
}

fn html_escape(input: &str) -> String {
	input
		.replace('&', "&amp;")
		.replace('<', "&lt;")
		.replace('>', "&gt;")
		.replace('"', "&quot;")
		.replace('\'', "&#39;")
}

fn handler<F, Fut>(state: State, function: F) -> BoxedHandler
where
	F: Fn(Request<Incoming>, State) -> Fut + Send + Sync + 'static,
	Fut: Future<Output = Result<Response<Full<Bytes>>, WebError>> + Send + 'static,
{
	let function = Arc::new(function);
	Arc::new(move |request, _params| {
		let function = Arc::clone(&function);
		Box::pin(async move {
			match function(request, state).await {
				| Ok(response) => response,
				| Err(error) => error.into_response(),
			}
		})
	})
}

pub fn build(state: State) -> MinimalRouter {
	let mut router = MinimalRouter::new();
	let register = |router: &mut MinimalRouter, method, path, handler| {
		router
			.register(method, path, handler)
			.expect("web route is valid");
	};
	register(
		&mut router,
		http::Method::GET,
		"/",
		Arc::new(|request, _| Box::pin(pages::index::index(request))),
	);
	register(
		&mut router,
		http::Method::GET,
		"/_continuwuity/",
		Arc::new(|request, _| Box::pin(pages::index::index(request))),
	);
	register(
		&mut router,
		http::Method::GET,
		"/_continuwuity/_debug/panic",
		handler(state, |request, _state| async move { Ok(pages::debug::panic(request).await) }),
	);
	register(
		&mut router,
		http::Method::GET,
		"/_continuwuity/_debug/error",
		handler(state, |request, _state| async move { Ok(pages::debug::error(request).await) }),
	);
	register(
		&mut router,
		http::Method::GET,
		"/_continuwuity/account/reset_password",
		handler(state, pages::password_reset::get_password_reset),
	);
	register(
		&mut router,
		http::Method::POST,
		"/_continuwuity/account/reset_password",
		handler(state, pages::password_reset::post_password_reset),
	);
	register(
		&mut router,
		http::Method::GET,
		"/_continuwuity/3pid/email/validate",
		handler(state, pages::threepid::threepid_validation),
	);
	router
}
