use axum::{
	Router,
	extract::{RawQuery, State},
	response::Html,
	routing::get,
};
use slipstream::OwnedSessionId;

use crate::WebError;

pub(crate) fn build() -> Router<crate::State> {
	Router::new().route("/3pid/email/validate", get(threepid_validation))
}

fn query_value(raw_query: Option<String>, wanted: &str) -> Option<String> {
	raw_query?.split('&').find_map(|pair| {
		let (name, value) = pair.split_once('=')?;
		(name == wanted).then_some(value.to_owned())
	})
}

async fn threepid_validation(
	State(services): State<crate::State>,
	RawQuery(query): RawQuery,
) -> Result<Html<&'static str>, WebError> {
	let session = query_value(query.clone(), "session")
		.ok_or_else(|| WebError::BadRequest("missing session".to_owned()))?;
	let token = query_value(query, "token")
		.ok_or_else(|| WebError::BadRequest("missing token".to_owned()))?;

	let session = OwnedSessionId::parse(&session)
		.map_err(|_| WebError::BadRequest("invalid session".to_owned()))?;

	services
		.threepid
		.try_validate_session(&session, &token)
		.await
		.map_err(|message| WebError::BadRequest(message.into_owned()))?;

	Ok(Html(
		"<!doctype html><title>Email verified</title><h1>Email verified</h1><p>Your email \
		 address has been verified. Return to your Matrix client.</p>",
	))
}
