use std::{any::type_name, collections::HashMap, future::Future, sync::Arc};

use conduwuit::Result;
use futures::future::BoxFuture;
use http::Method;
use hyper::{Request, body::Incoming};
use slipstream::{
	api::{EndpointRequest, IncomingRequest},
	codec::Deserialize,
};

use super::{
	Ruma, RumaResponse, State, extract,
	response::{ApiError, IntoResponse},
};
use crate::hyper_router::{BoxedHandler, MinimalRouter};

struct Context {
	request: Option<Request<Incoming>>,
	params: HashMap<String, String>,
}

trait ExtractArg: Sized {
	fn extract<'a>(
		context: &'a mut Context,
		state: &'a State,
	) -> BoxFuture<'a, Result<Self, ApiError>>;
}

impl ExtractArg for extract::State<State> {
	fn extract<'a>(
		_context: &'a mut Context,
		state: &'a State,
	) -> BoxFuture<'a, Result<Self, ApiError>> {
		Box::pin(async move { Ok(Self(*state)) })
	}
}

impl ExtractArg for extract::ClientIp {
	fn extract<'a>(
		context: &'a mut Context,
		_state: &'a State,
	) -> BoxFuture<'a, Result<Self, ApiError>> {
		Box::pin(async move {
			let request = context
				.request
				.as_ref()
				.expect("request exists while extracting");
			Ok(extract::client_ip(request))
		})
	}
}

impl<T> ExtractArg for extract::Path<T>
where
	T: Deserialize + Send + 'static,
{
	fn extract<'a>(
		context: &'a mut Context,
		_state: &'a State,
	) -> BoxFuture<'a, Result<Self, ApiError>> {
		Box::pin(async move {
			let values = context.params.values().cloned().collect::<Vec<_>>();
			let value = slipstream::json::Value::Array(
				values
					.into_iter()
					.map(slipstream::json::Value::String)
					.collect(),
			);
			slipstream::codec::from_value(&value)
				.map(Self)
				.map_err(|e| {
					conduwuit::err!(Request(InvalidParam("Invalid path parameter: {e}"))).into()
				})
		})
	}
}

impl<T> ExtractArg for extract::Query<T>
where
	T: serde::de::DeserializeOwned + Send + 'static,
{
	fn extract<'a>(
		context: &'a mut Context,
		_state: &'a State,
	) -> BoxFuture<'a, Result<Self, ApiError>> {
		Box::pin(async move {
			let request = context
				.request
				.as_ref()
				.expect("request exists while extracting");
			serde_urlencoded::from_str(request.uri().query().unwrap_or_default())
				.map(Self)
				.map_err(|e| conduwuit::err!(Request(InvalidParam("Invalid query: {e}"))).into())
		})
	}
}

impl<Req> ExtractArg for Ruma<Req>
where
	Req: EndpointRequest + IncomingRequest + Send + Sync + 'static,
{
	fn extract<'a>(
		context: &'a mut Context,
		state: &'a State,
	) -> BoxFuture<'a, Result<Self, ApiError>> {
		Box::pin(async move {
			let request = context
				.request
				.take()
				.expect("Ruma body extractor is unique");
			<Self as extract::FromRequest<State, Incoming>>::from_request(request, state).await
		})
	}
}

impl ExtractArg
	for Option<
		extract::TypedHeader<
			extract::headers::Authorization<extract::headers::authorization::Bearer>,
		>,
	>
{
	fn extract<'a>(
		context: &'a mut Context,
		_state: &'a State,
	) -> BoxFuture<'a, Result<Self, ApiError>> {
		Box::pin(async move {
			let request = context
				.request
				.as_ref()
				.expect("request exists while extracting");
			let header = request
				.headers()
				.get(http::header::AUTHORIZATION)
				.and_then(|value| value.to_str().ok())
				.and_then(|value| value.strip_prefix("Bearer "))
				.map(|value| {
					extract::TypedHeader(extract::headers::Authorization(
						extract::headers::authorization::Bearer(value.to_owned()),
					))
				});
			Ok(header)
		})
	}
}

pub(in super::super) trait RumaHandler<T> {
	fn add_routes(&'static self, router: &mut MinimalRouter);
}

pub(in super::super) trait RouterExt {
	fn ruma_route<H, T>(self, handler: &'static H) -> Self
	where
		H: RumaHandler<T>;
}

impl RouterExt for MinimalRouter {
	fn ruma_route<H, T>(mut self, handler: &'static H) -> Self
	where
		H: RumaHandler<T>,
	{
		handler.add_routes(&mut self);
		self
	}
}

macro_rules! ruma_handler {
	( $($tx:ident),* $(,)? ) => {
		#[allow(non_snake_case)]
		impl<Err, Req, Fut, Fun, $($tx,)*> RumaHandler<($($tx,)* Ruma<Req>,)> for Fun
		where
			Fun: Fn($($tx,)* Ruma<Req>,) -> Fut + Send + Sync + 'static,
			Fut: Future<Output = Result<Req::OutgoingResponse, Err>> + Send + 'static,
			Req: EndpointRequest + IncomingRequest + Send + Sync + 'static,
			Err: Into<ApiError> + Send,
			Req::OutgoingResponse: Send,
			$( $tx: ExtractArg + Send + 'static, )*
		{
			fn add_routes(&'static self, router: &mut MinimalRouter) {
				let metadata = &Req::METADATA;
				let method: Method = metadata.method.parse().expect("valid endpoint method");
				let paths = std::iter::once(metadata.path)
					.chain(metadata.aliases.iter().copied())
					.chain(metadata.path.split_once("/_matrix/client/v3/").map(|(prefix, suffix)| {
						Box::leak(format!("{prefix}/_matrix/client/r0/{suffix}").into_boxed_str())
					}))
					.collect::<Vec<_>>();
				for path in paths {
					crate::hyper_router::record_route(method.clone(), path, type_name::<Fun>());
					let handler: BoxedHandler = Arc::new(move |request, params| {
						let state = request.extensions().get::<State>().copied().expect("router state extension");
						let mut context = Context { request: Some(request), params };
						Box::pin(async move {
							$(let $tx = match $tx::extract(&mut context, &state).await {
								Ok(value) => value,
								Err(error) => return error.into().into_response(),
							};)*
							let body = match Ruma::<Req>::extract(&mut context, &state).await {
								Ok(value) => value,
								Err(error) => return error.into().into_response(),
							};
							match self($($tx,)* body).await {
								Ok(response) => RumaResponse(response).into_response(),
								Err(error) => error.into().into_response(),
							}
						})
					});
					router.register(method.clone(), path, handler).expect("valid endpoint route");
				}
			}
		}
	}
}

ruma_handler!();
ruma_handler!(T1);
ruma_handler!(T1, T2);
ruma_handler!(T1, T2, T3);
ruma_handler!(T1, T2, T3, T4);
