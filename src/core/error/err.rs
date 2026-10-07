//! Error construction macros
//!
//! These are specialized macros specific to this project's patterns for
//! throwing Errors; they make Error construction succinct and reduce clutter.
//! They are developed from folding existing patterns into the macro while
//! fixing several anti-patterns in the codebase.
//!
//! - The primary macros `Err!` and `err!` are provided. `Err!` simply wraps
//!   `err!` in the Result variant to reduce `Err(err!(...))` boilerplate, thus
//!   `err!` can be used in any case.
//!
//! 1. The macro makes the general Error construction easy: `return
//!    Err!("something went wrong")` replaces the prior `return
//!    Err(Error::Err("something went wrong".to_owned()))`.
//!
//! 2. The macro integrates format strings automatically: `return
//!    Err!("something bad: {msg}")` replaces the prior `return
//!    Err(Error::Err(format!("something bad: {msg}")))`.
//!
//! 3. The macro scopes variants of Error: `return Err!(Database("problem with
//!    bad database."))` replaces the prior `return Err(Error::Database("problem
//!    with bad database."))`.
//!
//! 4. The macro matches and scopes some special-case sub-variants, for example
//!    with slipstream ErrorKind: `return Err!(Request(MissingToken("you must provide
//!    an access token")))`.
//!
//! 5. The macro fixes the anti-pattern of repeating messages in an error! log
//!    and then again in an Error construction, often slightly different due to
//!    the Error variant not supporting a format string. Instead `return
//!    Err(Database(error!("problem with db: {msg}")))` logs the error at the
//!    callsite and then returns the error with the same string. Caller has the
//!    option of replacing `error!` with `debug_error!`.

#[macro_export]
#[collapse_debuginfo(yes)]
macro_rules! Err {
	($($args:tt)*) => {
		Err($crate::err!($($args)*))
	};
}

#[macro_export]
#[collapse_debuginfo(yes)]
macro_rules! err {
	(Request(Forbidden($level:ident!($($args:tt)+)))) => {{
		let mut buf = String::new();
		$crate::error::Error::Request(
			$crate::slipstream::api::client::error::ErrorKind::forbidden(),
			$crate::err_log!(buf, $level, $($args)+),
			$crate::http::StatusCode::BAD_REQUEST
		)
	}};

	(Request(Forbidden($($args:tt)+))) => {
		$crate::error::Error::Request(
			$crate::slipstream::api::client::error::ErrorKind::forbidden(),
			$crate::format_maybe!($($args)+),
			$crate::http::StatusCode::BAD_REQUEST
		)
	};

	(Request($variant:ident($level:ident!($($args:tt)+)))) => {{
		let mut buf = String::new();
		$crate::error::Error::Request(
			$crate::slipstream::api::client::error::ErrorKind::$variant,
			$crate::err_log!(buf, $level, $($args)+),
			$crate::http::StatusCode::BAD_REQUEST
		)
	}};

	(Request($variant:ident($($args:tt)+))) => {
		$crate::error::Error::Request(
			$crate::slipstream::api::client::error::ErrorKind::$variant,
			$crate::format_maybe!($($args)+),
			$crate::http::StatusCode::BAD_REQUEST
		)
	};

	(Request($variant:ident($($args:tt)+), $status_code:ident)) => {
		$crate::error::Error::Request(
			$crate::slipstream::api::client::error::ErrorKind::$variant,
			$crate::format_maybe!($($args)+),
			$crate::http::StatusCode::$status_code,
		)
	};

	(Config($item:literal, $($args:tt)+)) => {{
		let mut buf = String::new();
		$crate::error::Error::Config($item, $crate::err_log!(buf, error, config = %$item, $($args)+))
	}};

	($variant:ident($level:ident!($($args:tt)+))) => {{
		let mut buf = String::new();
		$crate::error::Error::$variant($crate::err_log!(buf, $level, $($args)+))
	}};

	($variant:ident($($args:ident),+)) => {
		$crate::error::Error::$variant($($args),+)
	};

	($variant:ident($($args:tt)+)) => {
		$crate::error::Error::$variant($crate::format_maybe!($($args)+))
	};

	($level:ident!($($args:tt)+)) => {{
		let mut buf = String::new();
		$crate::error::Error::Err($crate::err_log!(buf, $level, $($args)+))
	}};

	($($args:tt)+) => {
		$crate::error::Error::Err($crate::format_maybe!($($args)+))
	};
}

/// A trinity of integration between tracing, logging, and Error. This is a
/// customization of tracing::event! with the primary purpose of sharing the
/// error string, fieldset parsing and formatting. An added benefit is that we
/// can share the same callsite metadata for the source of our Error and the
/// associated logging and tracing event dispatches.
#[macro_export]
#[collapse_debuginfo(yes)]
macro_rules! err_log {
	($out:ident, $level:ident, $($fields:tt)+) => {{
		($out).push_str(stringify!($($fields)+));
		($out).into()
	}}
}

#[macro_export]
#[collapse_debuginfo(yes)]
macro_rules! err_lev {
	(debug_warn) => {
		if $crate::debug::logging() {
			$crate::log::Level::WARN
		} else {
			$crate::log::Level::DEBUG
		}
	};

	(debug_error) => {
		if $crate::debug::logging() {
			$crate::log::Level::ERROR
		} else {
			$crate::log::Level::DEBUG
		}
	};

	(warn) => {
		$crate::log::Level::WARN
	};

	(info) => {
		$crate::log::Level::INFO
	};

	(error) => {
		$crate::log::Level::ERROR
	};

	(info) => {
		$crate::log::Level::INFO
	};

	(debug) => {
		$crate::log::Level::DEBUG
	};

	(trace) => {
		$crate::log::Level::TRACE
	};
}
