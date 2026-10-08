use std::sync::Arc;

use axum::{
	Router, body::Body, extract::State as AxumState, middleware::Next, response::Response,
};
use conduwuit_service::{Services, state, state::Guard};
use http::Request;

pub(crate) fn build(services: &Arc<Services>) -> (Router, Guard) {
	let router = Router::<state::State>::new();
	let (state, guard) = state::create(services.clone());
	let router = conduwuit_api::router::build(router, &services.server)
		.merge(conduwuit_web::build())
		.fallback_service(conduwuit_api::hyper_router::MinimalRouter::new())
		.layer(axum::middleware::from_fn_with_state(state, insert_state_extension))
		.with_state(state);

	(router, guard)
}

async fn insert_state_extension(
	AxumState(state): AxumState<state::State>,
	mut request: Request<Body>,
	next: Next,
) -> Response {
	request.extensions_mut().insert(state);
	next.run(request).await
}
