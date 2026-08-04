#![allow(deprecated)]

use axum::extract::State;
use axum_client_ip::ClientIp;
use conduwuit::{Err, Result, err, utils::math::ruma_from_usize};
use conduwuit_service::media::{CACHE_CONTROL_IMMUTABLE, CORP_CROSS_ORIGIN, Dim, FileMeta};
use ruma::{
	Mxc,
	api::client::media::{
		create_content, create_content_async, create_mxc_uri, get_content,
		get_content_as_filename, get_content_thumbnail, get_media_config, get_media_preview,
	},
};

use crate::{Ruma, RumaResponse, client::create_content_route};

/// # `POST /_matrix/media/v1/create`
///
/// Reserve an MXC URI for asynchronous upload.
pub(crate) async fn create_mxc_uri_route(
	State(services): State<crate::State>,
	body: Ruma<create_mxc_uri::v1::Request>,
) -> Result<create_mxc_uri::v1::Response> {
	let user = body.sender_user();
	if services.users.is_suspended(user).await? {
		return Err!(Request(UserSuspended("You cannot perform this action while suspended.")));
	}

	let ref mxc = Mxc {
		server_name: services.globals.server_name(),
		media_id: &conduwuit::utils::random_string(conduwuit_service::media::MXC_LENGTH),
	};

	services.media.create_async(mxc, Some(user))?;

	Ok(create_mxc_uri::v1::Response::new(mxc.to_string().into()))
}

/// # `PUT /_matrix/media/v3/upload/{serverName}/{mediaId}`
///
/// Upload media into a previously reserved MXC URI.
pub(crate) async fn create_content_async_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<create_content_async::v3::Request>,
) -> Result<create_content_async::v3::Response> {
	let user = body.sender_user();
	if services.users.is_suspended(user).await? {
		return Err!(Request(UserSuspended("You cannot perform this action while suspended.")));
	}

	if body.server_name != *services.globals.server_name() {
		return Err!(Request(NotFound("Cannot upload to a remote media repository.")));
	}

	let filename = body.filename.as_deref();
	let content_type = body.content_type.as_deref();
	let content_disposition = conduwuit::utils::content_disposition::make_content_disposition(
		None,
		content_type,
		filename,
	);
	let ref mxc = Mxc {
		server_name: &body.server_name,
		media_id: &body.media_id,
	};

	match services
		.media
		.create_async_upload(
			mxc,
			Some(user),
			Some(&content_disposition),
			content_type,
			&body.file,
		)
		.await
	{
		| Ok(()) => {},
		| Err(e)
			if matches!(e.kind(), ruma::api::client::error::ErrorKind::CannotOverwriteMedia) =>
			return Err(e),
		| Err(e) => {
			conduwuit::debug_error!(%client, %mxc, "Failed async media upload: {e}");
			return Err!(Request(Unknown("Failed async media upload.")));
		},
	}

	Ok(create_content_async::v3::Response {})
}

/// # `GET /_matrix/media/v3/config`
///
/// Returns max upload size.
pub(crate) async fn get_media_config_legacy_route(
	State(services): State<crate::State>,
	_body: Ruma<get_media_config::v3::Request>,
) -> Result<get_media_config::v3::Response> {
	Ok(get_media_config::v3::Response {
		upload_size: ruma_from_usize(services.server.config.max_request_size),
	})
}

/// # `GET /_matrix/media/v1/config`
///
/// This is a legacy endpoint ("/v1/") that some very old homeservers and/or
/// clients may call. conduwuit adds these for compatibility purposes.
/// See <https://spec.matrix.org/legacy/legacy/#id27>
///
/// Returns max upload size.
pub(crate) async fn get_media_config_legacy_legacy_route(
	State(services): State<crate::State>,
	body: Ruma<get_media_config::v3::Request>,
) -> Result<RumaResponse<get_media_config::v3::Response>> {
	get_media_config_legacy_route(State(services), body)
		.await
		.map(RumaResponse)
}

/// # `GET /_matrix/media/v3/preview_url`
///
/// Returns URL preview.
#[tracing::instrument(skip_all, fields(%client), name = "url_preview_legacy", level = "debug")]
pub(crate) async fn get_media_preview_legacy_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<get_media_preview::v3::Request>,
) -> Result<get_media_preview::v3::Response> {
	let sender_user = body.sender_user();

	let url = &body.url;
	let url = conduwuit_service::media::parse_preview_url(&body.url).map_err(|e| {
		err!(Request(InvalidParam(
			debug_warn!(%sender_user, %url, "Requested URL is not valid: {e}")
		)))
	})?;

	if !services.media.url_preview_allowed(&url) {
		return Err!(Request(Forbidden(
			debug_warn!(%sender_user, %url, "URL is not allowed to be previewed")
		)));
	}

	let preview = services.media.get_url_preview(&url).await.map_err(|e| {
		err!(Request(Unknown(
			debug_error!(%sender_user, %url, "Failed to fetch a URL preview: {e}")
		)))
	})?;

	serde_json::value::to_raw_value(&preview)
		.map(get_media_preview::v3::Response::from_raw_value)
		.map_err(|error| {
			err!(Request(Unknown(
				debug_error!(%sender_user, %url, "Failed to parse URL preview: {error}")
			)))
		})
}

/// # `GET /_matrix/media/v1/preview_url`
///
/// This is a legacy endpoint ("/v1/") that some very old homeservers and/or
/// clients may call. conduwuit adds these for compatibility purposes.
/// See <https://spec.matrix.org/legacy/legacy/#id27>
///
/// Returns URL preview.
pub(crate) async fn get_media_preview_legacy_legacy_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<get_media_preview::v3::Request>,
) -> Result<RumaResponse<get_media_preview::v3::Response>> {
	get_media_preview_legacy_route(State(services), ClientIp(client), body)
		.await
		.map(RumaResponse)
}

/// # `POST /_matrix/media/v1/upload`
///
/// Permanently save media in the server.
///
/// This is a legacy endpoint ("/v1/") that some very old homeservers and/or
/// clients may call. conduwuit adds these for compatibility purposes.
/// See <https://spec.matrix.org/legacy/legacy/#id27>
///
/// - Some metadata will be saved in the database
/// - Media will be saved in the media/ directory
pub(crate) async fn create_content_legacy_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<create_content::v3::Request>,
) -> Result<RumaResponse<create_content::v3::Response>> {
	create_content_route(State(services), ClientIp(client), body)
		.await
		.map(RumaResponse)
}

/// # `GET /_matrix/media/v3/download/{serverName}/{mediaId}`
///
/// Load media from our server or over federation.
///
/// - Only allows federation if `allow_remote` is true
/// - Only redirects if `allow_redirect` is true
/// - Uses client-provided `timeout_ms` if available, else defaults to 20
///   seconds
#[tracing::instrument(skip_all, fields(%client), name = "media_get_legacy", level = "debug")]
pub(crate) async fn get_content_legacy_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<get_content::v3::Request>,
) -> Result<get_content::v3::Response> {
	let mxc = Mxc {
		server_name: &body.server_name,
		media_id: &body.media_id,
	};

	let FileMeta {
		content,
		content_type,
		content_disposition,
	} = match super::media::fetch_file(
		&services,
		&mxc,
		None,
		body.timeout_ms,
		None,
		body.allow_remote,
	)
	.await
	{
		| Ok(meta) => meta,
		| Err(conduwuit::Error::Io(e)) => match e.kind() {
			| std::io::ErrorKind::NotFound => return Err!(Request(NotFound("Media not found."))),
			| std::io::ErrorKind::PermissionDenied => {
				conduwuit::error!("Permission denied when trying to read file: {e:?}");
				return Err!(Request(Unknown("Unknown error when fetching file.")));
			},
			| _ => return Err!(Request(Unknown("Unknown error when fetching file."))),
		},
		| Err(e) if matches!(e.kind(), ruma::api::client::error::ErrorKind::NotYetUploaded) =>
			return Err(e),
		| Err(e) => {
			conduwuit::debug_warn!(%mxc, "Fetching media failed: {e:?}");
			return Err!(Request(NotFound("Media not found.")));
		},
	};

	let Some(file) = content else {
		return Err!(Request(NotFound("Media not found.")));
	};

	Ok(get_content::v3::Response {
		file,
		content_type: content_type.map(Into::into),
		content_disposition,
		cross_origin_resource_policy: Some(CORP_CROSS_ORIGIN.into()),
		cache_control: Some(CACHE_CONTROL_IMMUTABLE.into()),
	})
}

/// # `GET /_matrix/media/v1/download/{serverName}/{mediaId}`
///
/// Load media from our server or over federation.
///
/// This is a legacy endpoint ("/v1/") that some very old homeservers and/or
/// clients may call. conduwuit adds these for compatibility purposes.
/// See <https://spec.matrix.org/legacy/legacy/#id27>
///
/// - Only allows federation if `allow_remote` is true
/// - Only redirects if `allow_redirect` is true
/// - Uses client-provided `timeout_ms` if available, else defaults to 20
///   seconds
#[tracing::instrument(skip_all, fields(%client), name = "media_get_legacy", level = "debug")]
pub(crate) async fn get_content_legacy_legacy_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<get_content::v3::Request>,
) -> Result<RumaResponse<get_content::v3::Response>> {
	get_content_legacy_route(State(services), ClientIp(client), body)
		.await
		.map(RumaResponse)
}

/// # `GET /_matrix/media/v3/download/{serverName}/{mediaId}/{fileName}`
///
/// Load media from our server or over federation, permitting desired filename.
///
/// - Only allows federation if `allow_remote` is true
/// - Only redirects if `allow_redirect` is true
/// - Uses client-provided `timeout_ms` if available, else defaults to 20
///   seconds
#[tracing::instrument(skip_all, fields(%client), name = "media_get_legacy", level = "debug")]
pub(crate) async fn get_content_as_filename_legacy_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<get_content_as_filename::v3::Request>,
) -> Result<get_content_as_filename::v3::Response> {
	let mxc = Mxc {
		server_name: &body.server_name,
		media_id: &body.media_id,
	};

	let filename = (!body.filename.is_empty()).then_some(body.filename.as_str());

	let FileMeta {
		content,
		content_type,
		content_disposition,
	} = match super::media::fetch_file(
		&services,
		&mxc,
		None,
		body.timeout_ms,
		filename,
		body.allow_remote,
	)
	.await
	{
		| Ok(meta) => meta,
		| Err(conduwuit::Error::Io(e)) => match e.kind() {
			| std::io::ErrorKind::NotFound => return Err!(Request(NotFound("Media not found."))),
			| std::io::ErrorKind::PermissionDenied => {
				conduwuit::error!("Permission denied when trying to read file: {e:?}");
				return Err!(Request(Unknown("Unknown error when fetching file.")));
			},
			| _ => return Err!(Request(Unknown("Unknown error when fetching file."))),
		},
		| Err(e) if matches!(e.kind(), ruma::api::client::error::ErrorKind::NotYetUploaded) =>
			return Err(e),
		| Err(e) => {
			conduwuit::debug_warn!(%mxc, "Fetching media failed: {e:?}");
			return Err!(Request(NotFound("Media not found.")));
		},
	};

	let Some(file) = content else {
		return Err!(Request(NotFound("Media not found.")));
	};

	Ok(get_content_as_filename::v3::Response {
		file,
		content_type: content_type.map(Into::into),
		content_disposition,
		cross_origin_resource_policy: Some(CORP_CROSS_ORIGIN.into()),
		cache_control: Some(CACHE_CONTROL_IMMUTABLE.into()),
	})
}

/// # `GET /_matrix/media/v1/download/{serverName}/{mediaId}/{fileName}`
///
/// Load media from our server or over federation, permitting desired filename.
///
/// This is a legacy endpoint ("/v1/") that some very old homeservers and/or
/// clients may call. conduwuit adds these for compatibility purposes.
/// See <https://spec.matrix.org/legacy/legacy/#id27>
///
/// - Only allows federation if `allow_remote` is true
/// - Only redirects if `allow_redirect` is true
/// - Uses client-provided `timeout_ms` if available, else defaults to 20
///   seconds
pub(crate) async fn get_content_as_filename_legacy_legacy_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<get_content_as_filename::v3::Request>,
) -> Result<RumaResponse<get_content_as_filename::v3::Response>> {
	get_content_as_filename_legacy_route(State(services), ClientIp(client), body)
		.await
		.map(RumaResponse)
}

/// # `GET /_matrix/media/v3/thumbnail/{serverName}/{mediaId}`
///
/// Load media thumbnail from our server or over federation.
///
/// - Only allows federation if `allow_remote` is true
/// - Only redirects if `allow_redirect` is true
/// - Uses client-provided `timeout_ms` if available, else defaults to 20
///   seconds
#[tracing::instrument(skip_all, fields(%client), name = "media_thumbnail_get_legacy", level = "debug")]
pub(crate) async fn get_content_thumbnail_legacy_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<get_content_thumbnail::v3::Request>,
) -> Result<get_content_thumbnail::v3::Response> {
	let mxc = Mxc {
		server_name: &body.server_name,
		media_id: &body.media_id,
	};

	let dim = Dim::from_ruma(body.width, body.height, body.method.clone())?;

	let FileMeta {
		content,
		content_type,
		content_disposition,
	} = match super::media::fetch_thumbnail(
		&services,
		&mxc,
		None,
		body.timeout_ms,
		&dim,
		body.allow_remote,
	)
	.await
	{
		| Ok(meta) => meta,
		| Err(conduwuit::Error::Io(e)) => match e.kind() {
			| std::io::ErrorKind::NotFound => return Err!(Request(NotFound("Media not found."))),
			| std::io::ErrorKind::PermissionDenied => {
				conduwuit::error!("Permission denied when trying to read file: {e:?}");
				return Err!(Request(Unknown("Unknown error when fetching file.")));
			},
			| _ => return Err!(Request(Unknown("Unknown error when fetching file."))),
		},
		| Err(e) => {
			conduwuit::info!(target: "media:thumbnail", %mxc, "Fetching thumbnail failed: {e:?}");
			return Err!(Request(NotFound("Media not found.")));
		},
	};

	let Some(file) = content else {
		return Err!(Request(NotFound("Media not found.")));
	};

	Ok(get_content_thumbnail::v3::Response {
		file,
		content_type: content_type.map(Into::into),
		cross_origin_resource_policy: Some(CORP_CROSS_ORIGIN.into()),
		cache_control: Some(CACHE_CONTROL_IMMUTABLE.into()),
		content_disposition,
	})
}

/// # `GET /_matrix/media/v1/thumbnail/{serverName}/{mediaId}`
///
/// Load media thumbnail from our server or over federation.
///
/// This is a legacy endpoint ("/v1/") that some very old homeservers and/or
/// clients may call. conduwuit adds these for compatibility purposes.
/// See <https://spec.matrix.org/legacy/legacy/#id27>
///
/// - Only allows federation if `allow_remote` is true
/// - Only redirects if `allow_redirect` is true
/// - Uses client-provided `timeout_ms` if available, else defaults to 20
///   seconds
pub(crate) async fn get_content_thumbnail_legacy_legacy_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<get_content_thumbnail::v3::Request>,
) -> Result<RumaResponse<get_content_thumbnail::v3::Response>> {
	get_content_thumbnail_legacy_route(State(services), ClientIp(client), body)
		.await
		.map(RumaResponse)
}
