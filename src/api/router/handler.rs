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

trait HandlerResult {
	fn into_response(self) -> crate::router::response::Response;
}

impl HandlerResult for crate::router::response::Response {
	fn into_response(self) -> crate::router::response::Response { self }
}

impl HandlerResult for ApiError {
	fn into_response(self) -> crate::router::response::Response {
		IntoResponse::into_response(self)
	}
}

impl<T, E> HandlerResult for std::result::Result<T, E>
where
	T: IntoResponse,
	E: IntoResponse,
{
	fn into_response(self) -> crate::router::response::Response {
		match self {
			| Ok(value) => IntoResponse::into_response(value),
			| Err(error) => IntoResponse::into_response(error),
		}
	}
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
			let value = slipstream::json::Value::Array(
				context
					.params
					.values()
					.cloned()
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
		let state = *state;
		Box::pin(async move {
			let request = context
				.request
				.take()
				.expect("Ruma body extractor is unique");
			<Self as extract::FromRequest<State, Incoming>>::from_request(request, &state).await
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

impl ExtractArg for http::Uri {
	fn extract<'a>(
		context: &'a mut Context,
		_state: &'a State,
	) -> BoxFuture<'a, Result<Self, ApiError>> {
		Box::pin(async move {
			Ok(context
				.request
				.as_ref()
				.expect("request exists while extracting")
				.uri()
				.clone())
		})
	}
}

impl ExtractArg for extract::RawQuery {
	fn extract<'a>(
		context: &'a mut Context,
		_state: &'a State,
	) -> BoxFuture<'a, Result<Self, ApiError>> {
		Box::pin(async move {
			Ok(Self(
				context
					.request
					.as_ref()
					.expect("request exists while extracting")
					.uri()
					.query()
					.map(str::to_owned),
			))
		})
	}
}

impl ExtractArg for Request<Incoming> {
	fn extract<'a>(
		context: &'a mut Context,
		_state: &'a State,
	) -> BoxFuture<'a, Result<Self, ApiError>> {
		Box::pin(async move { Ok(context.request.take().expect("request extractor is unique")) })
	}
}

impl ExtractArg for crate::client::delayed_events::DelayedEventUser {
	fn extract<'a>(
		context: &'a mut Context,
		state: &'a State,
	) -> BoxFuture<'a, Result<Self, ApiError>> {
		Box::pin(async move {
			let request = context.request.take().expect("request extractor is unique");
			let user_id = crate::router::authenticate_user(
				request,
				state,
				&crate::client::delayed_events::GetDelayedEventRequest::METADATA,
			)
			.await?;
			Ok(Self { user_id })
		})
	}
}

impl ExtractArg for crate::client::delayed_events::AllDelayedEventsUser {
	fn extract<'a>(
		context: &'a mut Context,
		state: &'a State,
	) -> BoxFuture<'a, Result<Self, ApiError>> {
		Box::pin(async move {
			let request = context.request.take().expect("request extractor is unique");
			let user_id = crate::router::authenticate_user(
				request,
				state,
				&crate::client::delayed_events::GetAllDelayedEventsRequest::METADATA,
			)
			.await?;
			Ok(Self { user_id })
		})
	}
}

impl ExtractArg
	for extract::TypedHeader<
		extract::headers::Authorization<slipstream::api::federation::authentication::XMatrix>,
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
			let value = request
				.headers()
				.get(http::header::AUTHORIZATION)
				.and_then(slipstream::api::federation::authentication::XMatrix::decode)
				.ok_or_else(|| {
					conduwuit::err!(Request(Forbidden("Invalid X-Matrix authorization")))
				})?;
			Ok(Self(extract::headers::Authorization(value)))
		})
	}
}

pub(crate) trait RouteHandler<T> {
	fn boxed(
		&'static self,
		method: Method,
		path: &'static str,
	) -> (Method, &'static str, BoxedHandler);
}

type RouteBuilder =
	Arc<dyn Fn(&'static str) -> (Method, &'static str, BoxedHandler) + Send + Sync>;

pub(crate) struct RouteSpec {
	pub(crate) builders: Vec<RouteBuilder>,
}

impl RouteSpec {
	fn add<H, T>(mut self, method: Method, handler: H) -> Self
	where
		H: RouteHandler<T> + Copy + Sync + 'static,
	{
		let handler: &'static H = Box::leak(Box::new(handler));
		self.builders
			.push(Arc::new(move |path| handler.boxed(method.clone(), path)));
		self
	}

	pub(crate) fn put<H, T>(self, handler: H) -> Self
	where
		H: RouteHandler<T> + Copy + Sync + 'static,
	{
		self.add(Method::PUT, handler)
	}

	pub(crate) fn delete<H, T>(self, handler: H) -> Self
	where
		H: RouteHandler<T> + Copy + Sync + 'static,
	{
		self.add(Method::DELETE, handler)
	}
}

pub(crate) fn get<H, T>(handler: H) -> RouteSpec
where
	H: RouteHandler<T> + Copy + Sync + 'static,
{
	RouteSpec { builders: Vec::new() }.add(Method::GET, handler)
}

pub(crate) fn post<H, T>(handler: H) -> RouteSpec
where
	H: RouteHandler<T> + Copy + Sync + 'static,
{
	RouteSpec { builders: Vec::new() }.add(Method::POST, handler)
}

pub(crate) fn put<H, T>(handler: H) -> RouteSpec
where
	H: RouteHandler<T> + Copy + Sync + 'static,
{
	RouteSpec { builders: Vec::new() }.add(Method::PUT, handler)
}

pub(crate) fn delete<H, T>(handler: H) -> RouteSpec
where
	H: RouteHandler<T> + Copy + Sync + 'static,
{
	RouteSpec { builders: Vec::new() }.add(Method::DELETE, handler)
}

pub(crate) fn any<H, T>(handler: H) -> RouteSpec
where
	H: RouteHandler<T> + Copy + Sync + 'static,
{
	RouteSpec { builders: Vec::new() }
		.add(Method::GET, handler)
		.add(Method::POST, handler)
		.add(Method::PUT, handler)
		.add(Method::DELETE, handler)
}

macro_rules! route_handler {
	( $($tx:ident),* $(,)? ) => {
		impl<Fun, Fut, Output, $($tx,)*> RouteHandler<($($tx,)*)> for Fun
		where
			Fun: Fn($($tx,)*) -> Fut + Send + Sync + 'static,
			Fut: Future<Output = Output> + Send + 'static,
			Output: HandlerResult + Send + 'static,
			$( $tx: ExtractArg + Send + 'static, )*
		{
			#[allow(non_snake_case)]
			fn boxed(&'static self, method: Method, path: &'static str) -> (Method, &'static str, BoxedHandler) {
				let handler: BoxedHandler = Arc::new(move |request, params| {
					let state = request.extensions().get::<State>().copied().expect("router state extension");
					#[allow(unused_mut)]
					let mut context = Context { request: Some(request), params };
					let _ = (&state, &context);
					Box::pin(async move {
						$(let $tx = match $tx::extract(&mut context, &state).await {
							Ok(value) => value,
								Err(error) => return HandlerResult::into_response(error),
						};)*
						self($($tx,)*).await.into_response()
					})
				});
				(method, path, handler)
			}
		}
	}
}

route_handler!();
route_handler!(T1);
route_handler!(T1, T2);
route_handler!(T1, T2, T3);
route_handler!(T1, T2, T3, T4);
route_handler!(T1, T2, T3, T4, T5);

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
			Req::OutgoingResponse: Send + 'static,
			Err: Into<ApiError> + Send,
			$( $tx: ExtractArg + Send + 'static, )*
		{
			fn add_routes(&'static self, router: &mut MinimalRouter) {
				let metadata = &Req::METADATA;
				let method: Method = metadata.method.parse().expect("valid endpoint method");
				let paths = std::iter::once(metadata.path)
					.chain(metadata.aliases.iter().copied())
					.chain(metadata.path.split_once("/_matrix/client/v3/").map(|(prefix, suffix)| {
						let path: &'static str = Box::leak(
							format!("{prefix}/_matrix/client/r0/{suffix}").into_boxed_str(),
						);
						path
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
								Err(error) => return HandlerResult::into_response(error),
							};)*
							let body = match Ruma::<Req>::extract(&mut context, &state).await {
								Ok(value) => value,
								Err(error) => return HandlerResult::into_response(error),
							};
							match self($($tx,)* body).await {
								Ok(response) => IntoResponse::into_response(RumaResponse(response)),
								Err(error) => HandlerResult::into_response(error.into()),
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
