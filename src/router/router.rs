use std::sync::Arc;

use axum::{Router, body::Body, response::IntoResponse};
use conduwuit_service::{Services, state, state::Guard};
use http::{StatusCode, Uri};

pub(crate) fn build(services: &Arc<Services>) -> (Router, Guard) {
	let router = Router::<state::State>::new();
	let (state, guard) = state::create(services.clone());
	let router = conduwuit_api::router::build(router, &services.server)
		.merge(conduwuit_web::build())
		.fallback(not_found)
		.with_state(state);

	(router, guard)
}

async fn not_found(_uri: Uri) -> impl IntoResponse {
	axum::response::Response::builder()
		.status(StatusCode::NOT_FOUND)
		.body(Body::from("not found :("))
		.expect("static 404 response is valid")
}
