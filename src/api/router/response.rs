use axum::response::{IntoResponse, Response};
use bytes::BytesMut;
use conduwuit::{Error, error};
use http::StatusCode;
use http_body_util::Full;
use slipstream::api::{OutgoingResponse, client::uiaa::UiaaResponse};

pub(crate) struct ApiError(pub(crate) Error);

impl From<Error> for ApiError {
	fn from(error: Error) -> Self { Self(error) }
}

impl From<slipstream::codec::DeError> for ApiError {
	fn from(error: slipstream::codec::DeError) -> Self { Self(error.into()) }
}

impl From<slipstream::api::error::IntoHttpError> for ApiError {
	fn from(error: slipstream::api::error::IntoHttpError) -> Self { Self(error.into()) }
}

impl IntoResponse for ApiError {
	fn into_response(self) -> Response {
		let status = self.0.status_code();
		if status == StatusCode::INTERNAL_SERVER_ERROR {
			conduwuit::warn!(
				error = %self.0,
				error_debug = ?self.0,
				kind = ?self.0.kind(),
				status = %status,
				"Server error"
			);
		} else if status.is_server_error() {
			conduwuit::warn!(
				error = %self.0,
				kind = ?self.0.kind(),
				status = %status,
				"Server error"
			);
		} else if status.is_client_error() {
			conduwuit::debug_error!(
				error = %self.0,
				kind = ?self.0.kind(),
				status = %status,
				"Client error"
			);
		}

		let response: UiaaResponse = self.0.into();
		response
			.try_into_http_response::<BytesMut>()
			.inspect_err(|e| error!("error response error: {e}"))
			.map_or_else(
				|_| StatusCode::INTERNAL_SERVER_ERROR.into_response(),
				|r| r.map(BytesMut::freeze).map(Full::new).into_response(),
			)
	}
}

pub(crate) struct RumaResponse<T>(pub(crate) T)
where
	T: OutgoingResponse;

impl From<Error> for RumaResponse<UiaaResponse> {
	fn from(t: Error) -> Self { Self(t.into()) }
}

impl<T> IntoResponse for RumaResponse<T>
where
	T: OutgoingResponse,
{
	fn into_response(self) -> Response {
		self.0
			.try_into_http_response::<BytesMut>()
			.inspect_err(|e| error!("response error: {e}"))
			.map_or_else(
				|_| StatusCode::INTERNAL_SERVER_ERROR.into_response(),
				|r| r.map(BytesMut::freeze).map(Full::new).into_response(),
			)
	}
}
