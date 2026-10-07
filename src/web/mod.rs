#![allow(clippy::disallowed_macros)]

use std::any::Any;

use axum::{
	Router,
	extract::rejection::QueryRejection,
	http::{HeaderValue, StatusCode, header},
	response::{Html, IntoResponse, Response},
};
use conduwuit_service::state;
use tower_http::{catch_panic::CatchPanicLayer, set_header::SetResponseHeaderLayer};
use tower_sec_fetch::SecFetchLayer;

mod pages;

type State = state::State;

#[derive(Debug, thiserror::Error)]
enum WebError {
	#[error("{0}")]
	QueryRejection(#[from] QueryRejection),
	#[error("{0}")]
	BadRequest(String),

	#[error("This page does not exist.")]
	NotFound,

	#[error("{0}")]
	InternalError(#[from] conduwuit_core::Error),
	#[error("Request handler panicked! {0}")]
	Panic(String),
}

impl IntoResponse for WebError {
	fn into_response(self) -> Response {
		let status = match &self {
			| Self::BadRequest(_) | Self::QueryRejection(_) => StatusCode::BAD_REQUEST,
			| Self::NotFound => StatusCode::NOT_FOUND,
			| _ => StatusCode::INTERNAL_SERVER_ERROR,
		};

		let error = html_escape(&self.to_string());
		let body = format!(
			"<!doctype html><meta name=\"robots\" content=\"noindex\"><title>{status}</title>\
			 <h1>{status}</h1><pre>{error}</pre>"
		);
		(status, Html(body)).into_response()
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

pub fn build() -> Router<state::State> {
	#[allow(clippy::wildcard_imports)]
	use pages::*;

	Router::new()
		.merge(index::build())
		.nest(
			"/_continuwuity/",
			Router::new()
				.merge(debug::build())
				.merge(threepid::build())
				.fallback(async || WebError::NotFound),
		)
		.layer(CatchPanicLayer::custom(|panic: Box<dyn Any + Send + 'static>| {
			let details = if let Some(s) = panic.downcast_ref::<String>() {
				s.clone()
			} else if let Some(s) = panic.downcast_ref::<&str>() {
				(*s).to_owned()
			} else {
				"(opaque panic payload)".to_owned()
			};

			WebError::Panic(details).into_response()
		}))
		.layer(SetResponseHeaderLayer::if_not_present(
			header::CONTENT_SECURITY_POLICY,
			HeaderValue::from_static("default-src 'self'; img-src 'self' data:;"),
		))
		.layer(SecFetchLayer::new(|policy| {
			policy.allow_safe_methods().reject_missing_metadata();
		}))
}
