use axum::{Router, response::Html, routing::get};

pub(crate) fn build() -> Router<crate::State> {
	Router::new()
		.route("/", get(index))
		.route("/_continuwuity/", get(index))
}

async fn index() -> Html<&'static str> {
	Html(
		"<!doctype html><html><head><meta \
		 charset=\"utf-8\"><title>Continuwuity</title></head><body><h1>Continuwuity</\
		 h1><p>Matrix homeserver running.</p></body></html>",
	)
}
