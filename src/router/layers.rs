use std::sync::Arc;

use conduwuit::Result;
use conduwuit_service::{
	Services,
	state::{Guard, State},
};

use crate::router::Router;

pub(crate) fn build(services: &Arc<Services>) -> Result<(Router, Guard, State)> {
	let (state, guard) = conduwuit_service::state::create(services.clone());
	let router =
		crate::router::build(Router::new(), &services.server).merge(conduwuit_web::build(state));
	Ok((router, guard, state))
}
