mod err;
mod log;
mod panic;
mod response;
mod serde;

use std::{any::Any, borrow::Cow, convert::Infallible, error::Error as _, sync::PoisonError};

pub use self::log::*;

#[derive(thiserror::Error)]
pub enum Error {
	#[error("PANIC!")]
	PanicAny(Box<dyn Any + Send>),
	#[error("PANIC! {0}")]
	Panic(&'static str, Box<dyn Any + Send + 'static>),

	// std
	#[error(transparent)]
	Fmt(#[from] std::fmt::Error),
	#[error(transparent)]
	FromUtf8(#[from] std::string::FromUtf8Error),
	#[error("I/O error: {0}")]
	Io(#[from] std::io::Error),
	#[error(transparent)]
	ParseFloat(#[from] std::num::ParseFloatError),
	#[error(transparent)]
	ParseInt(#[from] std::num::ParseIntError),
	#[error(transparent)]
	Std(#[from] Box<dyn std::error::Error + Send>),
	#[error(transparent)]
	ThreadAccessError(#[from] std::thread::AccessError),
	#[error(transparent)]
	TryFromInt(#[from] std::num::TryFromIntError),
	#[error(transparent)]
	TryFromSlice(#[from] std::array::TryFromSliceError),
	#[error(transparent)]
	Utf8(#[from] std::str::Utf8Error),

	// third-party
	#[error(transparent)]
	CapacityError(#[from] arrayvec::CapacityError),
	#[error(transparent)]
	CargoToml(#[from] cargo_toml::Error),
	#[error(transparent)]
	Clap(#[from] clap::error::Error),
	#[error(transparent)]
	Http(#[from] http::Error),
	#[error(transparent)]
	HttpHeader(#[from] http::header::InvalidHeaderValue),
	#[error("Join error: {0}")]
	JoinError(#[from] tokio::task::JoinError),
	#[error(transparent)]
	JsParseInt(#[from] slipstream::JsParseIntError), // js_int re-export
	#[error(transparent)]
	JsTryFromInt(#[from] slipstream::JsTryFromIntError), // js_int re-export
	#[error("Mutex poisoned: {0}")]
	Poison(Cow<'static, str>),
	#[error("Regex error: {0}")]
	Regex(#[from] regex::Error),
	#[error("{0}")]
	Reqwest(#[source] FormattedReqwestError),
	#[error("{0}")]
	SerdeDe(Cow<'static, str>),
	#[error("{0}")]
	SerdeSer(Cow<'static, str>),
	#[error(transparent)]
	TomlDe(#[from] toml::de::Error),
	#[error(transparent)]
	TomlSer(#[from] toml::ser::Error),
	// slipstream/conduwuit
	#[error("Arithmetic operation failed: {0}")]
	Arithmetic(Cow<'static, str>),
	#[error("{0}: {1}")]
	BadRequest(slipstream::api::client::error::ErrorKind, &'static str), //TODO: remove
	#[error("{0}")]
	BadServerResponse(Cow<'static, str>),
	#[error(transparent)]
	CanonicalJson(#[from] slipstream::CanonicalJsonError),
	#[error(transparent)]
	DeserializeError(#[from] slipstream::codec::DeError),
	#[error("There was a problem with the '{0}' directive in your configuration: {1}")]
	Config(&'static str, Cow<'static, str>),
	#[error("Conflict: {0}")]
	Conflict(Cow<'static, str>), // This is only needed for when a room alias already exists
	#[error("Missing auth events required for event validation: {0:?}")]
	MissingAuthEvents(Vec<slipstream::OwnedEventId>),
	#[error("State resolution failed but the event's prev events were all present: {0}")]
	StateResolutionWithPrevsPresent(Cow<'static, str>),
	#[error(transparent)]
	ContentDisposition(#[from] slipstream::http_headers::ContentDispositionParseError),
	#[error("{0}")]
	Database(Cow<'static, str>),
	#[error("Feature '{0}' is not available on this server.")]
	FeatureDisabled(Cow<'static, str>),
	#[error("Remote server {0} responded with: {1}")]
	Federation(slipstream::OwnedServerName, slipstream::api::client::error::Error),
	#[error("{0} in {1}")]
	InconsistentRoomState(&'static str, slipstream::OwnedRoomId),
	#[error(transparent)]
	IntoHttp(#[from] slipstream::api::error::IntoHttpError),
	#[error("{0}")]
	Ldap(Cow<'static, str>),
	#[error(transparent)]
	Mxc(#[from] slipstream::MxcUriError),
	#[error(transparent)]
	Mxid(#[from] slipstream::IdParseError),
	#[error(transparent)]
	MatrixIdParse(#[from] slipstream::MatrixIdParseError),
	#[error("from {0}: {1}")]
	Redaction(slipstream::OwnedServerName, slipstream::canonical_json::RedactionError),
	#[error("{0}: {1}")]
	Request(slipstream::api::client::error::ErrorKind, Cow<'static, str>, http::StatusCode),
	#[error(transparent)]
	Ruma(#[from] slipstream::api::client::error::Error),
	#[error(transparent)]
	Signatures(#[from] slipstream::signatures::Error),
	#[error(transparent)]
	StateRes(#[from] crate::state_res::Error),
	#[error("uiaa")]
	Uiaa(slipstream::api::client::uiaa::UiaaInfo),

	// federation / remote
	#[error("Federation timeout: {0}")]
	FederationTimeout(slipstream::OwnedServerName),
	#[error("Federation connection error: {0}")]
	FederationConnection(slipstream::OwnedServerName),

	// unique / untyped
	#[error("{0}")]
	Err(Cow<'static, str>),
}

impl Error {
	#[inline]
	#[must_use]
	pub fn from_errno() -> Self { Self::Io(std::io::Error::last_os_error()) }

	//#[deprecated]
	#[must_use]
	pub fn bad_database(message: &'static str) -> Self {
		crate::err!(Database(error!("{message}")))
	}

	/// Sanitizes public-facing errors that can leak sensitive information.
	#[must_use]
	pub fn sanitized_message(&self) -> String {
		match self {
			| Self::Database(..) => String::from("Database error occurred."),
			| Self::Io(..) => String::from("I/O error occurred."),
			| _ => self.message(),
		}
	}

	/// Generate the error message string.
	#[must_use]
	pub fn message(&self) -> String {
		match self {
			| Self::Federation(origin, error) => format!("Answer from {origin}: {error}"),
			| Self::FederationTimeout(origin) => format!("Federation timeout for {origin}"),
			| Self::FederationConnection(origin) => {
				format!("Federation connection error for {origin}")
			},
			| Self::Ruma(error) => response::ruma_error_message(error),
			| _ => format!("{self}"),
		}
	}

	/// Returns the Matrix error code / error kind
	#[inline]
	#[must_use]
	pub fn kind(&self) -> &slipstream::api::client::error::ErrorKind {
		use slipstream::api::client::error::ErrorKind::{FeatureDisabled, Unknown};

		match self {
			| Self::Federation(_, error) | Self::Ruma(error) => response::ruma_error_kind(error),
			| Self::BadRequest(kind, ..) | Self::Request(kind, ..) => kind,
			| Self::FeatureDisabled(..) => &FeatureDisabled,
			| _ => &Unknown,
		}
	}

	#[must_use]
	pub fn into_kind(self) -> slipstream::api::client::error::ErrorKind {
		use slipstream::api::client::error::{ErrorBody, ErrorKind};

		match self {
			| Self::Federation(_, error) | Self::Ruma(error) => match error.body {
				| ErrorBody::Standard { kind, .. } => kind,
				| ErrorBody::Other => ErrorKind::Unknown,
			},
			| Self::BadRequest(kind, ..) | Self::Request(kind, ..) => kind,
			| Self::FeatureDisabled(..) => ErrorKind::FeatureDisabled,
			| _ => ErrorKind::Unknown,
		}
	}

	/// Returns the HTTP error code or closest approximation based on error
	/// variant.
	#[must_use]
	pub fn status_code(&self) -> http::StatusCode {
		use http::StatusCode;

		match self {
			| Self::Federation(_, error) | Self::Ruma(error) => error.status_code,
			| Self::Request(kind, _, code) => response::status_code(kind, *code),
			| Self::BadRequest(kind, ..) => response::bad_request_code(kind),
			| Self::FeatureDisabled(..) => response::bad_request_code(self.kind()),
			| Self::Reqwest(error) => error.status().unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
			| Self::Conflict(_) => StatusCode::CONFLICT,
			| Self::Io(error) => response::io_error_code(error.kind()),
			| Self::FederationTimeout(_) => StatusCode::GATEWAY_TIMEOUT,
			| Self::FederationConnection(_) => StatusCode::BAD_GATEWAY,
			| Self::Uiaa(_) => StatusCode::UNAUTHORIZED,
			| _ => StatusCode::INTERNAL_SERVER_ERROR,
		}
	}

	/// Returns true for "not found" errors. This means anything that qualifies
	/// as a "not found" from any variant's contained error type. This call is
	/// often used as a special case to eliminate a contained Option with a
	/// Result where Ok(None) is instead Err(e) if e.is_not_found().
	#[inline]
	#[must_use]
	pub fn is_not_found(&self) -> bool { self.status_code() == http::StatusCode::NOT_FOUND }
}

impl std::fmt::Debug for Error {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.message())
	}
}

impl<T> From<PoisonError<T>> for Error {
	#[cold]
	#[inline(never)]
	fn from(e: PoisonError<T>) -> Self { Self::Poison(e.to_string().into()) }
}

#[allow(clippy::fallible_impl_from)]
impl From<Infallible> for Error {
	#[cold]
	#[inline(never)]
	fn from(_e: Infallible) -> Self {
		panic!("infallible error should never exist");
	}
}

#[cold]
#[inline(never)]
pub fn infallible(_e: &Infallible) {
	panic!("infallible error should never exist");
}

/// Convenience functor for fundamental Error::sanitized_message(); see member.
#[inline]
#[must_use]
#[allow(clippy::needless_pass_by_value)]
pub fn sanitized_message(e: Error) -> String { e.sanitized_message() }

#[derive(Debug)]
pub struct FormattedReqwestError(reqwest::Error);

impl std::ops::Deref for FormattedReqwestError {
	type Target = reqwest::Error;

	fn deref(&self) -> &Self::Target { &self.0 }
}

impl std::error::Error for FormattedReqwestError {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { self.0.source() }
}

impl std::fmt::Display for FormattedReqwestError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		if let Some(hyper_error) = self.0.source()
			&& hyper_error.is::<hyper_util::client::legacy::Error>()
			&& let Some(real_error) = hyper_error.source()
		{
			if let Some(real_reason) = real_error.source() {
				write!(f, "{real_error}: {real_reason}")
			} else {
				write!(f, "{real_error}")
			}
		} else {
			write!(f, "Request error: {}", self.0)
		}
	}
}

impl From<reqwest::Error> for FormattedReqwestError {
	fn from(err: reqwest::Error) -> Self { Self(err) }
}

impl From<reqwest::Error> for Error {
	fn from(err: reqwest::Error) -> Self { Self::Reqwest(err.into()) }
}
