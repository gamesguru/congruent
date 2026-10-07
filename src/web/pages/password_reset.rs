use axum::{
	Router,
	body::Bytes,
	extract::{RawQuery, State},
	response::Html,
	routing::get,
};

use crate::WebError;

const INVALID_TOKEN_ERROR: &str = "Invalid reset token. Your reset link may have expired.";

pub(crate) fn build() -> Router<crate::State> {
	Router::new()
		.route("/account/reset_password", get(get_password_reset).post(post_password_reset))
}

fn form_page(message: Option<&str>) -> Html<String> {
	let message =
		message.map_or(String::new(), |message| format!("<p><strong>{message}</strong></p>"));
	Html(format!(
		"<!doctype html><title>Reset password</title><h1>Reset password</h1>{message}<form \
		 method=\"post\"><label>New password <input type=\"password\" name=\"new_password\" \
		 required></label><label>Confirm password <input type=\"password\" \
		 name=\"confirm_new_password\" required></label><button type=\"submit\">Reset \
		 password</button></form>"
	))
}

fn percent_decode(value: &str) -> Option<String> {
	let mut decoded = Vec::with_capacity(value.len());
	let mut bytes = value.bytes();
	while let Some(byte) = bytes.next() {
		match byte {
			| b'+' => decoded.push(b' '),
			| b'%' => {
				let high = hex_value(bytes.next()?)?;
				let low = hex_value(bytes.next()?)?;
				decoded.push((high << 4) | low);
			},
			| byte => decoded.push(byte),
		}
	}
	String::from_utf8(decoded).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
	match byte {
		| b'0'..=b'9' => Some(byte - b'0'),
		| b'a'..=b'f' => Some(byte - b'a' + 10),
		| b'A'..=b'F' => Some(byte - b'A' + 10),
		| _ => None,
	}
}

fn field(body: &[u8], wanted: &str) -> Option<String> {
	let body = std::str::from_utf8(body).ok()?;
	body.split('&').find_map(|pair| {
		let (name, value) = pair.split_once('=')?;
		(percent_decode(name)? == wanted)
			.then(|| percent_decode(value))
			.flatten()
	})
}

fn token(raw_query: Option<String>) -> Result<String, WebError> {
	raw_query
		.and_then(|query| field(query.as_bytes(), "token"))
		.filter(|token| !token.is_empty())
		.ok_or_else(|| WebError::BadRequest(INVALID_TOKEN_ERROR.to_owned()))
}

async fn get_password_reset(
	State(services): State<crate::State>,
	RawQuery(query): RawQuery,
) -> Result<Html<String>, WebError> {
	let token = token(query)?;
	if services.password_reset.check_token(&token).await.is_none() {
		return Err(WebError::BadRequest(INVALID_TOKEN_ERROR.to_owned()));
	}

	Ok(form_page(None))
}

async fn post_password_reset(
	State(services): State<crate::State>,
	RawQuery(query): RawQuery,
	body: Bytes,
) -> Result<Html<String>, WebError> {
	let token = token(query)?;
	let Some(new_password) = field(&body, "new_password") else {
		return Ok(form_page(Some("Password cannot be empty.")));
	};
	let Some(confirm_new_password) = field(&body, "confirm_new_password") else {
		return Ok(form_page(Some("Passwords must match.")));
	};

	if new_password.is_empty() {
		return Ok(form_page(Some("Password cannot be empty.")));
	}
	if new_password != confirm_new_password {
		return Ok(form_page(Some("Passwords must match.")));
	}

	let Some(token) = services.password_reset.check_token(&token).await else {
		return Err(WebError::BadRequest(INVALID_TOKEN_ERROR.to_owned()));
	};
	services
		.password_reset
		.consume_token(token, &new_password)
		.await?;

	Ok(Html(
		"<!doctype html><title>Password reset</title><h1>Password reset</h1><p>Your password \
		 has been reset successfully.</p>"
			.to_owned(),
	))
}
