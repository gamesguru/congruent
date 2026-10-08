use bytes::Bytes;
use http_body_util::Full;
use hyper::{Request, Response, body::Incoming};
use slipstream::OwnedSessionId;

use crate::WebError;

fn query_value(raw_query: Option<&str>, wanted: &str) -> Option<String> {
	raw_query?.split('&').find_map(|pair| {
		let (name, value) = pair.split_once('=')?;
		(name == wanted).then_some(value.to_owned())
	})
}

pub(crate) async fn threepid_validation(
	request: Request<Incoming>,
	services: crate::State,
) -> Result<Response<Full<Bytes>>, WebError> {
	let session = query_value(request.uri().query(), "session")
		.ok_or_else(|| WebError::BadRequest("missing session".to_owned()))?;
	let token = query_value(request.uri().query(), "token")
		.ok_or_else(|| WebError::BadRequest("missing token".to_owned()))?;

	let session = OwnedSessionId::parse(&session)
		.map_err(|_| WebError::BadRequest("invalid session".to_owned()))?;

	services
		.threepid
		.try_validate_session(&session, &token)
		.await
		.map_err(|message| WebError::BadRequest(message.into_owned()))?;

	Ok(Response::builder()
		.header(http::header::CONTENT_TYPE, "text/html; charset=utf-8")
		.body(Full::from(Bytes::from_static(
			"<!doctype html><title>Email verified</title><h1>Email verified</h1><p>Your email \
			 address has been verified. Return to your Matrix client.</p>",
		)))
		.expect("static response headers are valid"))
}
