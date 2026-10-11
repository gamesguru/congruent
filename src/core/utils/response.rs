use bytes::Bytes;

use crate::Err;

/// Reads the response body while enforcing a maximum size limit to prevent
/// memory exhaustion.
pub async fn limit_read(
	response: http::Response<Bytes>,
	max_size: u64,
) -> crate::Result<Vec<u8>> {
	if response
		.headers()
		.get(http::header::CONTENT_LENGTH)
		.and_then(|value| value.to_str().ok())
		.and_then(|value| value.parse::<u64>().ok())
		.is_some_and(|len| len > max_size)
	{
		return Err!(BadServerResponse("Response too large"));
	}

	let body = response.into_body();
	if body.len() > usize::try_from(max_size).expect("max_size must fit in usize") {
		return Err!(BadServerResponse("Response too large"));
	}

	Ok(body.into())
}

/// Reads the response body as text while enforcing a maximum size limit to
/// prevent memory exhaustion.
pub async fn limit_read_text(
	response: http::Response<Bytes>,
	max_size: u64,
) -> crate::Result<String> {
	let text = String::from_utf8(limit_read(response, max_size).await?)?;
	Ok(text)
}

#[allow(async_fn_in_trait)]
pub trait LimitReadExt {
	fn error_for_status(self) -> crate::Result<Self>
	where
		Self: Sized;
	async fn limit_read(self, max_size: u64) -> crate::Result<Vec<u8>>;
	async fn limit_read_text(self, max_size: u64) -> crate::Result<String>;
}

impl LimitReadExt for http::Response<Bytes> {
	fn error_for_status(self) -> crate::Result<Self> {
		if self.status().is_client_error() || self.status().is_server_error() {
			return Err(crate::Error::HttpClient(
				format!("HTTP status {}", self.status()).into(),
			));
		}
		Ok(self)
	}

	async fn limit_read(self, max_size: u64) -> crate::Result<Vec<u8>> {
		limit_read(self, max_size).await
	}

	async fn limit_read_text(self, max_size: u64) -> crate::Result<String> {
		limit_read_text(self, max_size).await
	}
}
