use conduwuit_api::hyper_router::MinimalRouter;

pub(crate) type Router = MinimalRouter;

pub(crate) fn build(router: Router, server: &conduwuit_core::Server) -> Router {
	conduwuit_api::router::build(router, server)
}
