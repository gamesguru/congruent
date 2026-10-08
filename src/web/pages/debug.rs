use bytes::Bytes;
use conduwuit_core::Error;
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming};

use crate::WebError;

pub(crate) async fn panic(_request: Request<Incoming>) -> Response<Full<Bytes>> {
	panic!("Guru meditation error")
}

pub(crate) async fn error(_request: Request<Incoming>) -> Response<Full<Bytes>> {
	WebError::from(Error::Err(std::borrow::Cow::Borrowed("Guru meditation error")))
		.into_response()
}
