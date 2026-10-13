use std::fmt::Debug;

use bytes::BytesMut;
use conduwuit::{Err, Result, debug_error, err, utils, warn};
use slipstream::api::{IncomingResponse, MatrixVersion, OutgoingRequest, SendAccessToken};

use crate::client::HttpClient;

/// Sends a request to an antispam service
pub(crate) async fn send_antispam_request<T>(
	client: &HttpClient,
	base_url: &str,
	secret: &str,
	request: T,
) -> Result<T::IncomingResponse>
where
	T: OutgoingRequest + Debug + Send,
{
	const VERSIONS: [MatrixVersion; 1] = [MatrixVersion::V1_15];
	let http_request = request
		.try_into_http_request::<BytesMut>(base_url, SendAccessToken::Always(secret), &VERSIONS)?
		.map(BytesMut::freeze);
	let response = client.execute(http_request).await.map_err(|e| {
		warn!("Could not send request to antispam: {e:?}");
		e
	})?;

	let status = response.status();
	let body = response.into_body();

	if !status.is_success() {
		debug_error!("Antispam response bytes: {:?}", utils::string_from_bytes(&body));
		return match status {
			| http::StatusCode::FORBIDDEN => {
				Err!(Request(Forbidden("Request was rejected by antispam service.",)))
			},
			| _ => Err!(BadServerResponse(warn!(
				"Antispam returned unsuccessful HTTP response {status}",
			))),
		};
	}

	let response = T::IncomingResponse::try_from_http_response(
		http::Response::builder().status(status).body(body)?,
	);

	response.map_err(|e| {
		err!(BadServerResponse(warn!(
			"Antispam returned invalid/malformed response bytes: {e}",
		)))
	})
}
