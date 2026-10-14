use http::StatusCode;
use slipstream::api::client::{
	error::{ErrorBody, ErrorKind},
	uiaa::UiaaResponse,
};

use super::Error;
impl From<Error> for UiaaResponse {
	#[inline]
	fn from(error: Error) -> Self {
		if let Error::Uiaa(uiaainfo) = error {
			return Self::AuthResponse(uiaainfo);
		}

		let message = error.message();
		let status_code = error.status_code();
		let kind = error.into_kind();
		let body = ErrorBody::Standard { kind, message };

		Self::MatrixError(slipstream::api::client::error::Error { status_code, body })
	}
}

pub(super) fn status_code(kind: &ErrorKind, hint: StatusCode) -> StatusCode {
	if hint == StatusCode::BAD_REQUEST {
		bad_request_code(kind)
	} else {
		hint
	}
}

pub(super) fn bad_request_code(kind: &ErrorKind) -> StatusCode {
	use ErrorKind::*;

	match kind {
		// 429
		| LimitExceeded { .. } => StatusCode::TOO_MANY_REQUESTS,

		// 413
		| TooLarge => StatusCode::PAYLOAD_TOO_LARGE,

		// 405
		| Unrecognized => StatusCode::METHOD_NOT_ALLOWED,

		// 404
		| NotFound | NotImplemented | FeatureDisabled | SenderIgnored { .. } =>
			StatusCode::NOT_FOUND,

		// 409
		| CannotOverwriteMedia => StatusCode::CONFLICT,

		// 504
		| NotYetUploaded => StatusCode::GATEWAY_TIMEOUT,

		// 403
		| GuestAccessForbidden
		| ThreepidAuthFailed
		| UserDeactivated
		| ThreepidDenied
		| InviteBlocked
		| WrongRoomKeysVersion { .. }
		| UserSuspended
		| Forbidden { .. } => StatusCode::FORBIDDEN,

		// 401
		| UnknownToken { .. } | MissingToken | Unauthorized | UserLocked =>
			StatusCode::UNAUTHORIZED,

		// 400
		| _ => StatusCode::BAD_REQUEST,
	}
}

pub(super) fn ruma_error_message(error: &slipstream::api::client::error::Error) -> String {
	if let ErrorBody::Standard { message, .. } = &error.body {
		return message.clone();
	}

	format!("{error}")
}

pub(super) fn ruma_error_kind(e: &slipstream::api::client::error::Error) -> &ErrorKind {
	e.error_kind().unwrap_or(&ErrorKind::Unknown)
}

pub(super) fn io_error_code(kind: std::io::ErrorKind) -> StatusCode {
	use std::io::ErrorKind;

	match kind {
		| ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
		| ErrorKind::PermissionDenied => StatusCode::FORBIDDEN,
		| ErrorKind::NotFound => StatusCode::NOT_FOUND,
		| ErrorKind::TimedOut => StatusCode::GATEWAY_TIMEOUT,
		| ErrorKind::FileTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
		| ErrorKind::StorageFull => StatusCode::INSUFFICIENT_STORAGE,
		| ErrorKind::Interrupted => StatusCode::SERVICE_UNAVAILABLE,
		| _ => StatusCode::INTERNAL_SERVER_ERROR,
	}
}
