//! Small request wrappers used by the endpoint functions.

use std::{net::IpAddr, str::FromStr};

use http::Request as HttpRequest;
use hyper::body::Incoming;

#[derive(Debug)]
pub(crate) struct Path<T>(pub(crate) T);
#[derive(Debug)]
pub(crate) struct Query<T>(pub(crate) T);
#[derive(Debug)]
pub(crate) struct RawQuery(pub(crate) Option<String>);
#[derive(Debug)]
pub(crate) struct State<T>(pub(crate) T);
#[derive(Debug)]
pub(crate) struct ClientIp(pub(crate) IpAddr);
#[derive(Debug)]
pub(crate) struct TypedHeader<T>(pub(crate) T);

pub(crate) mod headers {
	#[derive(Debug)]
	pub(crate) struct Authorization<T>(pub(crate) T);

	pub(crate) mod authorization {
		#[derive(Debug)]
		pub(crate) struct Bearer(pub(crate) String);
		impl Bearer {
			pub(crate) fn token(&self) -> &str { &self.0 }
		}
	}
}

pub(crate) trait FromRequest<S, B = Incoming>: Sized {
	type Rejection;
	async fn from_request(request: HttpRequest<B>, state: &S) -> Result<Self, Self::Rejection>;
}

pub(crate) fn client_ip<B>(request: &HttpRequest<B>) -> ClientIp {
	let forwarded = request
		.headers()
		.get("x-forwarded-for")
		.and_then(|value| value.to_str().ok())
		.and_then(|value| value.split(',').next())
		.and_then(|value| IpAddr::from_str(value.trim()).ok());
	let real = request
		.headers()
		.get("x-real-ip")
		.and_then(|value| value.to_str().ok())
		.and_then(|value| IpAddr::from_str(value.trim()).ok());
	ClientIp(forwarded.or(real).unwrap_or(IpAddr::from([127, 0, 0, 1])))
}
