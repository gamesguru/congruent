use axum::{Router, routing::get};

pub(crate) fn build() -> Router<crate::State> {
	Router::new()
		.route("/", get(index))
		.route("/_continuwuity/", get(index))
}

async fn index() -> http::Response<axum::body::Body> {
	http::Response::builder()
		.header(http::header::CONTENT_TYPE, "text/html; charset=utf-8")
		.body(axum::body::Body::from(
			"<!doctype html><html><head><meta \
			 charset=\"utf-8\"><title>Continuwuity</title></head><body><h1>Continuwuity</\
			 h1><p>Matrix homeserver running.</p></body></html>",
		))
		.expect("static response headers are valid")
}
