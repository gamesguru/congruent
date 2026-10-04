use std::{fmt::Debug, time::Duration};

use conduwuit::{
	Err, Error, Result, debug_warn, err, implement,
	utils::{content_disposition::make_content_disposition, response::LimitReadExt},
};
use http::{
	StatusCode,
	header::{CONTENT_DISPOSITION, CONTENT_TYPE, HeaderValue},
};
use slipstream::{
	Mxc, ServerName, UserId,
	api::{
		client::{
			error::ErrorKind,
			media::{
				get_content::v3 as client_get_content,
				get_content_thumbnail::v3 as client_get_thumbnail,
			},
		},
		federation::authenticated_media::{
			Content, FileOrLocation, get_content::v1 as fed_get_content,
			get_content_thumbnail::v1 as fed_get_thumbnail,
		},
	},
	endpoint::OutgoingRequest,
	http_headers::{ContentDisposition, ContentDispositionType},
};

use super::{Dim, FileMeta};

#[implement(super::Service)]
pub async fn fetch_remote_thumbnail(
	&self,
	mxc: &Mxc<'_>,
	user: Option<&UserId>,
	server: Option<&ServerName>,
	timeout_ms: Duration,
	dim: &Dim,
) -> Result<FileMeta> {
	self.check_fetch_authorized(mxc)?;

	let result = self
		.fetch_thumbnail_authenticated(mxc, user, server, timeout_ms, dim)
		.await;

	if should_fallback_to_unauthenticated(&result, user.is_none()) {
		return self
			.fetch_thumbnail_unauthenticated(mxc, user, server, timeout_ms, dim)
			.await;
	}

	result
}

#[implement(super::Service)]
pub async fn fetch_remote_content(
	&self,
	mxc: &Mxc<'_>,
	user: Option<&UserId>,
	server: Option<&ServerName>,
	timeout_ms: Duration,
) -> Result<FileMeta> {
	self.check_fetch_authorized(mxc)?;

	let result = self
		.fetch_content_authenticated(mxc, user, server, timeout_ms)
		.await
		.inspect_err(|error| {
			debug_warn!(
				%mxc,
				?user,
				?server,
				?error,
				"Authenticated fetch of remote content failed"
			);
		});

	if should_fallback_to_unauthenticated(&result, user.is_none()) {
		return self
			.fetch_content_unauthenticated(mxc, user, server, timeout_ms)
			.await;
	}

	result
}

fn should_fallback_to_unauthenticated(
	result: &Result<FileMeta>,
	allow_broad_fallback: bool,
) -> bool {
	match result {
		| Err(Error::Request(
			ErrorKind::Unrecognized
			| ErrorKind::NotFound
			| ErrorKind::Forbidden { .. }
			| ErrorKind::Unauthorized,
			..,
		)) => true,
		| Err(error) if allow_broad_fallback =>
			error.status_code().is_server_error()
				|| matches!(
					error.status_code(),
					StatusCode::REQUEST_TIMEOUT | StatusCode::GATEWAY_TIMEOUT
				)
				|| matches!(
					error,
					Error::Reqwest(_)
						| Error::Federation(_, _)
						| Error::FederationTimeout(_)
						| Error::FederationConnection(_)
				),
		| _ => false,
	}
}

#[implement(super::Service)]
async fn fetch_thumbnail_authenticated(
	&self,
	mxc: &Mxc<'_>,
	user: Option<&UserId>,
	server: Option<&ServerName>,
	timeout_ms: Duration,
	dim: &Dim,
) -> Result<FileMeta> {
	let request = fed_get_thumbnail::Request {
		media_id: mxc.media_id.into(),
		method: dim.method.clone().into(),
		width: dim.width.into(),
		height: dim.height.into(),
		animated: true.into(),
		timeout_ms,
	};

	let response: fed_get_thumbnail::Response =
		self.federation_request(mxc, server, request).await?;

	match response.content {
		| FileOrLocation::File(content) =>
			self.handle_thumbnail_file(mxc, user, dim, content).await,
		| FileOrLocation::Location(location) => self.handle_location(mxc, user, &location).await,
	}
}

#[implement(super::Service)]
async fn fetch_content_authenticated(
	&self,
	mxc: &Mxc<'_>,
	user: Option<&UserId>,
	server: Option<&ServerName>,
	timeout_ms: Duration,
) -> Result<FileMeta> {
	let request = fed_get_content::Request {
		media_id: mxc.media_id.into(),
		timeout_ms,
	};

	let response: fed_get_content::Response =
		self.federation_request(mxc, server, request).await?;

	match response.content {
		| FileOrLocation::File(content) => self.handle_content_file(mxc, user, content).await,
		| FileOrLocation::Location(location) => self.handle_location(mxc, user, &location).await,
	}
}

#[allow(deprecated)]
#[implement(super::Service)]
async fn fetch_thumbnail_unauthenticated(
	&self,
	mxc: &Mxc<'_>,
	user: Option<&UserId>,
	server: Option<&ServerName>,
	timeout_ms: Duration,
	dim: &Dim,
) -> Result<FileMeta> {
	let request = client_get_thumbnail::Request {
		allow_remote: true,
		allow_redirect: true,
		animated: true.into(),
		method: dim.method.clone().into(),
		width: dim.width.into(),
		height: dim.height.into(),
		server_name: mxc.server_name.into(),
		media_id: mxc.media_id.into(),
		timeout_ms,
	};

	let response: client_get_thumbnail::Response =
		self.federation_request(mxc, server, request).await?;

	self.handle_thumbnail_file(mxc, user, dim, Content {
		file: response.file,
		content_type: response.content_type.map(Into::into),
		content_disposition: response.content_disposition,
	})
	.await
}

#[allow(deprecated)]
#[implement(super::Service)]
async fn fetch_content_unauthenticated(
	&self,
	mxc: &Mxc<'_>,
	user: Option<&UserId>,
	server: Option<&ServerName>,
	timeout_ms: Duration,
) -> Result<FileMeta> {
	let request = client_get_content::Request {
		allow_remote: true,
		allow_redirect: true,
		server_name: mxc.server_name.into(),
		media_id: mxc.media_id.into(),
		timeout_ms,
	};

	let response: client_get_content::Response =
		self.federation_request(mxc, server, request).await?;

	self.handle_content_file(mxc, user, Content {
		file: response.file,
		content_type: response.content_type.map(Into::into),
		content_disposition: response.content_disposition,
	})
	.await
}

#[implement(super::Service)]
async fn handle_thumbnail_file(
	&self,
	mxc: &Mxc<'_>,
	user: Option<&UserId>,
	dim: &Dim,
	content: Content,
) -> Result<FileMeta> {
	let content_disposition = make_content_disposition(
		content.content_disposition.as_ref(),
		content.content_type.as_deref(),
		None,
	);

	self.upload_thumbnail(
		mxc,
		user,
		Some(&content_disposition),
		content.content_type.as_deref(),
		dim,
		&content.file,
	)
	.await
	.map(|()| FileMeta {
		content: Some(content.file),
		content_type: content.content_type.map(Into::into),
		content_disposition: Some(content_disposition),
	})
}

#[implement(super::Service)]
async fn handle_content_file(
	&self,
	mxc: &Mxc<'_>,
	user: Option<&UserId>,
	content: Content,
) -> Result<FileMeta> {
	let content_disposition = make_content_disposition(
		content.content_disposition.as_ref(),
		content.content_type.as_deref(),
		None,
	);

	self.create(
		mxc,
		user,
		Some(&content_disposition),
		content.content_type.as_deref(),
		&content.file,
	)
	.await
	.map(|()| FileMeta {
		content: Some(content.file),
		content_type: content.content_type.map(Into::into),
		content_disposition: Some(content_disposition),
	})
}

#[implement(super::Service)]
async fn handle_location(
	&self,
	mxc: &Mxc<'_>,
	user: Option<&UserId>,
	location: &str,
) -> Result<FileMeta> {
	self.location_request(location).await.map_err(|error| {
		err!(Request(NotFound(
			debug_warn!(%mxc, user = user.map(tracing::field::display), ?location, ?error, "Fetching media from location failed")
		)))
	})
}

#[implement(super::Service)]
async fn location_request(&self, location: &str) -> Result<FileMeta> {
	let response = self
		.services
		.client
		.extern_media
		.get(location)
		.send()
		.await?;

	let content_type = response
		.headers()
		.get(CONTENT_TYPE)
		.map(HeaderValue::to_str)
		.and_then(Result::ok)
		.map(str::to_owned);

	let content_disposition = response
		.headers()
		.get(CONTENT_DISPOSITION)
		.and_then(|h| h.to_str().ok())
		.and_then(|s| parse_content_disposition(s).ok());

	response
		.limit_read(
			self.services
				.server
				.config
				.max_request_size
				.try_into()
				.expect("u64 should fit in usize"),
		)
		.await
		.map(|content| FileMeta {
			content: Some(content),
			content_type: content_type.clone(),
			content_disposition: Some(make_content_disposition(
				content_disposition.as_ref(),
				content_type.as_deref(),
				None,
			)),
		})
}

#[implement(super::Service)]
async fn federation_request<Request>(
	&self,
	mxc: &Mxc<'_>,
	server: Option<&ServerName>,
	request: Request,
) -> Result<Request::IncomingResponse>
where
	Request: OutgoingRequest + Send + Debug,
{
	self.services
		.sending
		.send_federation_request(server.unwrap_or(mxc.server_name), request)
		.await
}

#[implement(super::Service)]
fn check_fetch_authorized(&self, mxc: &Mxc<'_>) -> Result<()> {
	if self
		.services
		.moderation
		.is_remote_server_media_downloads_forbidden(mxc.server_name)
	{
		// we'll lie to the client and say the blocked server's media was not found and
		// log. the client has no way of telling anyways so this is a security bonus.
		debug_warn!(%mxc, "Received request for media on blocklisted server");
		return Err!(Request(NotFound("Media not found.")));
	}

	Ok(())
}

#[implement(super::Service)]
pub fn check_legacy_freeze(&self) -> Result<()> {
	(!self.services.server.config.freeze_legacy_media)
		.then_some(())
		.ok_or(err!(Request(NotFound("Remote media is frozen."))))
}

/// Parse a Content-Disposition header into slipstream's ContentDisposition
fn parse_content_disposition(s: &str) -> Result<ContentDisposition> {
	let mut parts = s.split(';');
	let disposition = parts.next().unwrap_or("").trim();

	let disposition_type = match disposition {
		| "attachment" => ContentDispositionType::Attachment,
		| _ => ContentDispositionType::Inline,
	};

	let mut filename = None;
	for part in parts {
		let part = part.trim();
		if let Some(value) = part.strip_prefix("filename=") {
			filename = Some(value.trim_matches('"').to_owned());
			break;
		}
	}

	Ok(ContentDisposition::new(disposition_type).with_filename(filename))
}
