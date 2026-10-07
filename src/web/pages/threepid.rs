use axum::{
	Router,
	extract::{Query, State, rejection::QueryRejection},
	response::Html,
	routing::get,
};
use serde::Deserialize;
use slipstream::OwnedSessionId;

use crate::WebError;

pub(crate) fn build() -> Router<crate::State> {
	Router::new().route("/3pid/email/validate", get(threepid_validation))
}

#[derive(Deserialize)]
struct ThreepidValidationQuery {
	// slipstream IDs have no serde impl, so take the raw string.
	session: String,
	token: String,
}

async fn threepid_validation(
	State(services): State<crate::State>,
	query: Result<Query<ThreepidValidationQuery>, QueryRejection>,
) -> Result<Html<&'static str>, WebError> {
	let Query(query) = query?;

	let session = OwnedSessionId::parse(&query.session)
		.map_err(|_| WebError::BadRequest("invalid session".to_owned()))?;

	services
		.threepid
		.try_validate_session(&session, &query.token)
		.await
		.map_err(|message| WebError::BadRequest(message.into_owned()))?;

	Ok(Html(
		"<!doctype html><title>Email verified</title><h1>Email verified</h1>\
		 <p>Your email address has been verified. Return to your Matrix client.</p>",
	))
}
