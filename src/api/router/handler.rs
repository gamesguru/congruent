use std::any::type_name;

use axum::{
	Router,
	extract::FromRequestParts,
	routing::{MethodFilter, on},
};
use conduwuit::Result;
use futures::{Future, TryFutureExt};
use http::Method;
use slipstream::api::{EndpointRequest, IncomingRequest};

use super::{Ruma, RumaResponse, State, response::ApiError};

pub(in super::super) trait RumaHandler<T> {
	fn add_route(&'static self, router: Router<State>, path: &str) -> Router<State>;
	fn add_routes(&'static self, router: Router<State>) -> Router<State>;
}

pub(in super::super) trait RouterExt {
	fn ruma_route<H, T>(self, handler: &'static H) -> Self
	where
		H: RumaHandler<T>;
}

impl RouterExt for Router<State> {
	fn ruma_route<H, T>(self, handler: &'static H) -> Self
	where
		H: RumaHandler<T>,
	{
		handler.add_routes(self)
	}
}

macro_rules! ruma_handler {
	( $($tx:ident),* $(,)? ) => {
		#[allow(non_snake_case)]
		impl<Err, Req, Fut, Fun, $($tx,)*> RumaHandler<($($tx,)* Ruma<Req>,)> for Fun
		where
			Fun: Fn($($tx,)* Ruma<Req>,) -> Fut + Send + Sync + 'static,
			Fut: Future<Output = Result<Req::OutgoingResponse, Err>> + Send,
			Req: EndpointRequest + IncomingRequest + Send + Sync + 'static,
			Err: Into<ApiError> + Send,
			<Req as IncomingRequest>::OutgoingResponse: Send,
			$( $tx: FromRequestParts<State> + Send + Sync + 'static, )*
		{
			fn add_routes(&'static self, router: Router<State>) -> Router<State> {
				let router = self.add_route(router, Req::METADATA.path);
				// Alias paths declared by the endpoint (for example a stable path for an
				// endpoint whose canonical path is still the unstable one).
				let router = Req::METADATA
					.aliases
					.iter()
					.fold(router, |router, alias| self.add_route(router, alias));
				if let Some((prefix, suffix)) = Req::METADATA.path.split_once("/_matrix/client/v3/") {
					let legacy = format!("{prefix}/_matrix/client/r0/{suffix}");
					self.add_route(router, &legacy)
				} else {
					router
				}
			}

			fn add_route(&'static self, router: Router<State>, path: &str) -> Router<State> {
				let metadata_method = Req::METADATA
					.method
					.parse()
					.expect("endpoint metadata contains a valid HTTP method");
				crate::hyper_router::record_route(metadata_method.clone(), path, type_name::<Fun>());

				let action = |$($tx,)* req| {
					self($($tx,)* req)
						.map_ok(RumaResponse)
						.map_err(Into::into)
				};
				let method = method_to_filter(
					&metadata_method,
				);
				router.route(path, on(method, action))
			}
		}
	}
}
ruma_handler!();
ruma_handler!(T1);
ruma_handler!(T1, T2);
ruma_handler!(T1, T2, T3);
ruma_handler!(T1, T2, T3, T4);

const fn method_to_filter(method: &Method) -> MethodFilter {
	match *method {
		| Method::DELETE => MethodFilter::DELETE,
		| Method::GET => MethodFilter::GET,
		| Method::HEAD => MethodFilter::HEAD,
		| Method::OPTIONS => MethodFilter::OPTIONS,
		| Method::PATCH => MethodFilter::PATCH,
		| Method::POST => MethodFilter::POST,
		| Method::PUT => MethodFilter::PUT,
		| Method::TRACE => MethodFilter::TRACE,
		| _ => panic!("Unsupported HTTP method"),
	}
}
