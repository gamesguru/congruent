use std::sync::Arc;

use axum::Router;
use conduwuit_service::{Services, state, state::Guard};

pub(crate) fn build(services: &Arc<Services>) -> (Router, Guard) {
	let router = Router::<state::State>::new();
	let (state, guard) = state::create(services.clone());
	let router = conduwuit_api::router::build(router, &services.server)
		.merge(conduwuit_web::build())
		.fallback_service(conduwuit_api::hyper_router::MinimalRouter::new())
		.with_state(state);

	(router, guard)
}
