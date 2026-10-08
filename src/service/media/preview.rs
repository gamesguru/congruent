//! URL Previews
//!
//! This functionality is gated by 'url_preview', but not at the unit level for
//! historical and simplicity reasons. Instead the feature gates the inclusion
//! of dependencies and nulls out results through the existing interface when
//! not featured.

use std::{net::IpAddr, time::SystemTime};

use conduwuit::{Err, Result, debug, err, info};
use conduwuit_core::implement;
#[cfg(feature = "url_preview")]
use conduwuit_core::utils::response::LimitReadExt;
use serde::Serialize;
#[cfg(feature = "url_preview")]
use slipstream::OwnedMxcUri;
use url::Url;

use super::Service;

#[derive(Serialize, Default, Clone)]
pub struct UrlPreviewData {
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:title"))]
	pub title: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:description"))]
	pub description: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:type"))]
	pub og_type: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:url"))]
	pub og_url: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:image"))]
	pub image: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "matrix:image:size"))]
	pub image_size: Option<usize>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:image:width"))]
	pub image_width: Option<u32>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:image:height"))]
	pub image_height: Option<u32>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:video"))]
	pub video: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "matrix:video:size"))]
	pub video_size: Option<usize>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:video:width"))]
	pub video_width: Option<u32>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:video:height"))]
	pub video_height: Option<u32>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:audio"))]
	pub audio: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "matrix:audio:size"))]
	pub audio_size: Option<usize>,
}

impl slipstream::codec::Serialize for UrlPreviewData {
	fn to_json(&self) -> slipstream::json::Value {
		let mut object = slipstream::ObjectBuilder::new();
		if let Some(value) = &self.title {
			object.field("og:title", value);
		}
		if let Some(value) = &self.description {
			object.field("og:description", value);
		}
		if let Some(value) = &self.og_type {
			object.field("og:type", value);
		}
		if let Some(value) = &self.og_url {
			object.field("og:url", value);
		}
		if let Some(value) = &self.image {
			object.field("og:image", value);
		}
		if let Some(value) = &self.image_size {
			object.field("matrix:image:size", value);
		}
		if let Some(value) = &self.image_width {
			object.field("og:image:width", value);
		}
		if let Some(value) = &self.image_height {
			object.field("og:image:height", value);
		}
		if let Some(value) = &self.video {
			object.field("og:video", value);
		}
		if let Some(value) = &self.video_size {
			object.field("matrix:video:size", value);
		}
		if let Some(value) = &self.video_width {
			object.field("og:video:width", value);
		}
		if let Some(value) = &self.video_height {
			object.field("og:video:height", value);
		}
		if let Some(value) = &self.audio {
			object.field("og:audio", value);
		}
		if let Some(value) = &self.audio_size {
			object.field("matrix:audio:size", value);
		}
		object.finish()
	}
}

#[implement(Service)]
pub fn remove_url_preview(&self, url: &str) -> Result<()> {
	// TODO: also remove the downloaded image
	self.db.remove_url_preview(url)
}

#[implement(Service)]
pub async fn clear_url_previews(&self) { self.db.clear_url_previews().await; }

#[implement(Service)]
pub fn set_url_preview(&self, url: &str, data: &UrlPreviewData) -> Result<()> {
	let now = SystemTime::now()
		.duration_since(SystemTime::UNIX_EPOCH)
		.expect("valid system time");
	info!(
		%url,
		title = ?data.title,
		description = ?data.description.as_ref().map(String::len),
		image_dimensions = ?data.image_width.zip(data.image_height),
		"URL preview successfully generated",
	);
	self.db.set_url_preview(url, data, now)
}

#[implement(Service)]
pub async fn get_url_preview(&self, url: &Url) -> Result<UrlPreviewData> {
	if let Ok(preview) = self.db.get_url_preview(url.as_str()).await {
		return Ok(preview);
	}

	// ensure that only one request is made per URL
	let _request_lock = self.url_preview_mutex.lock(url.as_str()).await;

	match self.db.get_url_preview(url.as_str()).await {
		| Ok(preview) => Ok(preview),
		| Err(_) => self.request_url_preview(url).await,
	}
}

#[implement(Service)]
async fn request_url_preview(&self, url: &Url) -> Result<UrlPreviewData> {
	if let Ok(ip) = url
		.host_str()
		.expect("URL previously validated")
		.parse::<IpAddr>()
	{
		if !self.services.client.valid_cidr_range(&ip) {
			return Err!(Request(Forbidden("Requesting from this address is forbidden")));
		}
	}

	let client = &self.services.client.url_preview;
	let mut response = client.head(url.as_str()).send().await?;

	let mut status = response.status();
	if status == http::StatusCode::METHOD_NOT_ALLOWED
		|| status == http::StatusCode::FORBIDDEN
		|| status == http::StatusCode::NOT_IMPLEMENTED
	{
		debug!(%url, "URL preview HEAD probe returned {status}, falling back to GET");
		let mut req = client.get(url.as_str());
		if status == http::StatusCode::FORBIDDEN {
			req = req.header(
				http::header::USER_AGENT,
				self.services
					.server
					.config
					.url_preview_user_agent
					.as_deref()
					.unwrap_or(&self.services.server.config.user_agent),
			);
		}
		response = req.send().await?;
		status = response.status();
	}

	if !status.is_success() {
		return Err!(Request(Unknown(warn!("HTTP {status} fetching URL preview probe"))));
	}

	debug!(%url, "URL preview response headers: {:?}", response.headers());

	let Some(content_type) = response.headers().get(http::header::CONTENT_TYPE) else {
		return Err!(Request(Unknown("Unknown or invalid Content-Type header")));
	};

	let content_type = content_type
		.to_str()
		.map_err(|e| err!(Request(Unknown("Unknown or invalid Content-Type header: {e}"))))?;

	let data = match classify_content_type(content_type) {
		| Some(MediaType::Html) => self.download_html(url.as_str()).await?,
		| Some(MediaType::Image) => self.download_image(url.as_str(), None).await?,
		| Some(MediaType::Video) => self.download_video(url.as_str(), None).await?,
		| Some(MediaType::Audio) => self.download_audio(url.as_str(), None).await?,
		| None => {
			return Err!(Request(Unknown(error!("Unsupported Content-Type: {content_type}"))));
		},
	};

	self.set_url_preview(url.as_str(), &data)?;

	Ok(data)
}

#[cfg(feature = "url_preview")]
#[implement(Service)]
pub async fn download_image(
	&self,
	url: &str,
	preview_data: Option<UrlPreviewData>,
) -> Result<UrlPreviewData> {
	use conduwuit::utils::random_string;
	use image::{ImageFormat, ImageReader, imageops::FilterType};
	use slipstream::Mxc;

	let mut preview_data = preview_data.unwrap_or_default();

	let mut response = self.services.client.url_preview.get(url).send().await?;

	if response.status() == http::StatusCode::FORBIDDEN {
		response = self
			.services
			.client
			.url_preview
			.get(url)
			.header(
				http::header::USER_AGENT,
				self.services
					.server
					.config
					.url_preview_user_agent
					.as_deref()
					.unwrap_or(&self.services.server.config.user_agent),
			)
			.send()
			.await?;
	}

	if let Err(e) = response.error_for_status_ref() {
		return Err!(Request(Unknown(error!("HTTP {e} fetching image"))));
	}

	let mut image = response
		.limit_read(
			self.services
				.server
				.config
				.max_request_size
				.try_into()
				.expect("u64 should fit in usize"),
		)
		.await?;

	let (width, height);

	// Metadata (size/dimensions) reported to clients always describes the
	// original fetched image, even though a downscaled copy may be what's
	// actually stored below.
	preview_data.image_size = Some(image.len());

	let cursor = std::io::Cursor::new(&image);
	if let Ok(reader) = ImageReader::new(cursor).with_guessed_format() {
		if let Ok(dim) = reader.into_dimensions() {
			width = Some(dim.0);
			height = Some(dim.1);

			// Dynamically scale down massive URL preview images to 250x250 limits
			// to avoid gigabytes of raw 4K database hoarding.
			if dim.0 > 250 || dim.1 > 250 {
				if let Ok(img) = image::load_from_memory(&image) {
					let resized = img.resize(250, 250, FilterType::CatmullRom);
					let mut cursor = std::io::Cursor::new(Vec::new());

					if resized.write_to(&mut cursor, ImageFormat::Jpeg).is_ok() {
						image = cursor.into_inner();
					}
				}
			}
		} else {
			return Err!(Request(Unknown(
				"URL preview image metadata invalid or inherently unparsable"
			)));
		}
	} else {
		return Err!(Request(Unknown(
			"URL preview image buffer failed to guess its own target format"
		)));
	}

	let mxc = Mxc {
		server_name: self.services.globals.server_name(),
		media_id: &random_string(super::MXC_LENGTH),
	};

	self.create(&mxc, None, None, None, &image).await?;

	preview_data.image = Some(mxc.to_string());
	preview_data.image_width = width.or(preview_data.image_width);
	preview_data.image_height = height.or(preview_data.image_height);

	Ok(preview_data)
}

#[cfg(feature = "url_preview")]
#[implement(Service)]
pub async fn download_video(
	&self,
	url: &str,
	preview_data: Option<UrlPreviewData>,
) -> Result<UrlPreviewData> {
	let mut preview_data = preview_data.unwrap_or_default();

	if self.services.globals.url_preview_allow_audio_video() {
		let (url, size) = self.download_media(url).await?;
		preview_data.video = Some(url.to_string());
		preview_data.video_size = Some(size);
	}

	Ok(preview_data)
}

#[cfg(feature = "url_preview")]
#[implement(Service)]
pub async fn download_audio(
	&self,
	url: &str,
	preview_data: Option<UrlPreviewData>,
) -> Result<UrlPreviewData> {
	let mut preview_data = preview_data.unwrap_or_default();

	if self.services.globals.url_preview_allow_audio_video() {
		let (url, size) = self.download_media(url).await?;
		preview_data.audio = Some(url.to_string());
		preview_data.audio_size = Some(size);
	}

	Ok(preview_data)
}

#[cfg(feature = "url_preview")]
#[implement(Service)]
pub async fn download_media(&self, url: &str) -> Result<(OwnedMxcUri, usize)> {
	use conduwuit::utils::random_string;
	use http::header::CONTENT_TYPE;
	use slipstream::Mxc;

	let mut response = self.services.client.url_preview.get(url).send().await?;

	if response.status() == http::StatusCode::FORBIDDEN {
		response = self
			.services
			.client
			.url_preview
			.get(url)
			.header(
				http::header::USER_AGENT,
				self.services
					.server
					.config
					.url_preview_user_agent
					.as_deref()
					.unwrap_or(&self.services.server.config.user_agent),
			)
			.send()
			.await?;
	}

	if let Err(e) = response.error_for_status_ref() {
		return Err!(Request(Unknown(error!("HTTP {e} fetching media blob"))));
	}
	let content_type = response.headers().get(CONTENT_TYPE).cloned();
	let media = response
		.limit_read(
			self.services
				.server
				.config
				.max_request_size
				.try_into()
				.expect("u64 should fit in usize"),
		)
		.await?;

	let mxc = Mxc {
		server_name: self.services.globals.server_name(),
		media_id: &random_string(super::MXC_LENGTH),
	};

	let content_type = content_type.and_then(|v| v.to_str().map(ToOwned::to_owned).ok());
	self.create(&mxc, None, None, content_type.as_deref(), &media)
		.await?;

	Ok((OwnedMxcUri::parse(mxc.to_string())?, media.len()))
}

#[cfg(not(feature = "url_preview"))]
#[implement(Service)]
pub async fn download_image(
	&self,
	_url: &str,
	_preview_data: Option<UrlPreviewData>,
) -> Result<UrlPreviewData> {
	std::future::ready(()).await;
	Err!(FeatureDisabled("url_preview"))
}

#[cfg(not(feature = "url_preview"))]
#[implement(Service)]
pub async fn download_video(
	&self,
	_url: &str,
	_preview_data: Option<UrlPreviewData>,
) -> Result<UrlPreviewData> {
	std::future::ready(()).await;
	Err!(FeatureDisabled("url_preview"))
}

#[cfg(not(feature = "url_preview"))]
#[implement(Service)]
pub async fn download_audio(
	&self,
	_url: &str,
	_preview_data: Option<UrlPreviewData>,
) -> Result<UrlPreviewData> {
	std::future::ready(()).await;
	Err!(FeatureDisabled("url_preview"))
}

#[cfg(not(feature = "url_preview"))]
#[implement(Service)]
pub async fn download_media(&self, _url: &str) -> Result<UrlPreviewData> {
	std::future::ready(()).await;
	Err!(FeatureDisabled("url_preview"))
}

#[cfg(feature = "url_preview")]
#[implement(Service)]
async fn download_html(&self, url: &str) -> Result<UrlPreviewData> {
	let client = &self.services.client.url_preview;
	let mut response = client.get(url).send().await?;

	if response.status() == http::StatusCode::FORBIDDEN {
		response = client
			.get(url)
			.header(
				http::header::USER_AGENT,
				self.services
					.server
					.config
					.url_preview_user_agent
					.as_deref()
					.unwrap_or(&self.services.server.config.user_agent),
			)
			.send()
			.await?;
	}

	if let Err(e) = response.error_for_status_ref() {
		return Err!(Request(Unknown(error!("HTTP {e} fetching HTML text"))));
	}

	let body = response
		.limit_read_text(
			self.services
				.server
				.config
				.max_request_size
				.try_into()
				.expect("u64 should fit in usize"),
		)
		.await?;
	let html = parse_html_metadata(&body);

	let mut preview_data = UrlPreviewData::default();

	let base_url = Url::parse(url).ok();
	let resolve = |raw: &str| -> String {
		base_url
			.as_ref()
			.and_then(|base| base.join(raw).ok())
			.map_or_else(|| raw.to_owned(), |joined| joined.to_string())
	};

	if let Some(image) = html.image.as_ref() {
		let image_url = resolve(image);
		if let Ok(data_with_img) = self
			.download_image(&image_url, Some(preview_data.clone()))
			.await
		{
			preview_data = data_with_img;
			preview_data.image_width = preview_data.image_width.or(html.image_width);
			preview_data.image_height = preview_data.image_height.or(html.image_height);
		}
	}

	if let Some(video) = html.video.as_ref() {
		let video_url = resolve(video);
		preview_data = self.download_video(&video_url, Some(preview_data)).await?;
		preview_data.video_width = html.video_width;
		preview_data.video_height = html.video_height;
	}

	if let Some(audio) = html.audio.as_ref() {
		let audio_url = resolve(audio);
		preview_data = self.download_audio(&audio_url, Some(preview_data)).await?;
	}

	/* use OpenGraph title/description, but fall back to HTML if not available */
	preview_data.title = html.og_title.or(html.title);
	preview_data.description = html.og_description.or(html.description);
	preview_data.og_type = html.og_type;
	preview_data.og_url = html.og_url;

	Ok(preview_data)
}

#[cfg(not(feature = "url_preview"))]
#[implement(Service)]
async fn download_html(&self, _url: &str) -> Result<UrlPreviewData> {
	std::future::ready(()).await;
	Err!(FeatureDisabled("url_preview"))
}

#[implement(Service)]
pub fn url_preview_allowed(&self, url: &Url) -> bool {
	if ["http", "https"]
		.iter()
		.all(|&scheme| scheme != url.scheme().to_lowercase())
	{
		debug!("Ignoring non-HTTP/HTTPS URL to preview: {}", url);
		return false;
	}

	let host = match url.host_str() {
		| None => {
			debug!("Ignoring URL preview for a URL that does not have a host (?): {}", url);
			return false;
		},
		| Some(h) => h.to_owned(),
	};

	let allowlist_domain_contains = self
		.services
		.globals
		.url_preview_domain_contains_allowlist();
	let allowlist_domain_explicit = self
		.services
		.globals
		.url_preview_domain_explicit_allowlist();
	let denylist_domain_explicit = self.services.globals.url_preview_domain_explicit_denylist();
	let allowlist_url_contains = self.services.globals.url_preview_url_contains_allowlist();

	if allowlist_domain_contains.contains(&"*".to_owned())
		|| allowlist_domain_explicit.contains(&"*".to_owned())
		|| allowlist_url_contains.contains(&"*".to_owned())
	{
		debug!("Config key contains * which is allowing all URL previews. Allowing URL {}", url);
		return true;
	}

	if !host.is_empty() {
		if denylist_domain_explicit.contains(&host) {
			debug!(
				"Host {} is not allowed by url_preview_domain_explicit_denylist (check 1/4)",
				&host
			);
			return false;
		}

		if allowlist_domain_explicit.contains(&host) {
			debug!(
				"Host {} is allowed by url_preview_domain_explicit_allowlist (check 2/4)",
				&host
			);
			return true;
		}

		if allowlist_domain_contains
			.iter()
			.any(|domain_s| domain_s.contains(&host.clone()))
		{
			debug!(
				"Host {} is allowed by url_preview_domain_contains_allowlist (check 3/4)",
				&host
			);
			return true;
		}

		if allowlist_url_contains
			.iter()
			.any(|url_s| url.to_string().contains(url_s))
		{
			debug!("URL {} is allowed by url_preview_url_contains_allowlist (check 4/4)", &host);
			return true;
		}

		// check root domain if available and if user has root domain checks
		if self.services.globals.url_preview_check_root_domain() {
			debug!("Checking root domain");
			match host.split_once('.') {
				| None => return false,
				| Some((_, root_domain)) => {
					if denylist_domain_explicit.contains(&root_domain.to_owned()) {
						debug!(
							"Root domain {} is not allowed by \n\t\t\t\t\t\t \
							 url_preview_domain_explicit_denylist (check 1/3)",
							&root_domain
						);
						return false;
					}

					if allowlist_domain_explicit.contains(&root_domain.to_owned()) {
						debug!(
							"Root domain {} is allowed by url_preview_domain_explicit_allowlist \
							 \n\t\t\t\t\t (check 2/3)",
							&root_domain
						);
						return true;
					}

					if allowlist_domain_contains
						.iter()
						.any(|domain_s| domain_s.contains(&root_domain.to_owned()))
					{
						debug!(
							"Root domain {} is allowed by url_preview_domain_contains_allowlist \
							 \n\t\t\t\t\t (check 3/3)",
							&root_domain
						);
						return true;
					}
				},
			}
		}
	}

	false
}

pub fn parse_preview_url(url_str: &str) -> std::result::Result<Url, url::ParseError> {
	let finder = linkify::LinkFinder::new();
	let clean_url = finder.links(url_str).next().map_or(url_str, |l| l.as_str());

	match Url::parse(clean_url) {
		| Ok(url) => Ok(url),
		| Err(url::ParseError::RelativeUrlWithoutBase) => {
			let mut with_schema = String::with_capacity(clean_url.len().saturating_add(8));
			with_schema.push_str("https://");
			with_schema.push_str(clean_url);

			let final_url = finder
				.links(&with_schema)
				.next()
				.map_or(with_schema.as_str(), |l| l.as_str());

			Url::parse(final_url)
		},
		| Err(e) => Err(e),
	}
}
#[cfg(feature = "url_preview")]
#[derive(Default)]
pub(crate) struct HtmlMetadata {
	pub(crate) title: Option<String>,
	pub(crate) description: Option<String>,
	pub(crate) og_title: Option<String>,
	pub(crate) og_description: Option<String>,
	pub(crate) og_type: Option<String>,
	pub(crate) og_url: Option<String>,
	pub(crate) image: Option<String>,
	pub(crate) image_width: Option<u32>,
	pub(crate) image_height: Option<u32>,
	pub(crate) video: Option<String>,
	pub(crate) video_width: Option<u32>,
	pub(crate) video_height: Option<u32>,
	pub(crate) audio: Option<String>,
}

#[cfg(feature = "url_preview")]
pub(crate) fn parse_html_metadata(body: &str) -> HtmlMetadata {
	let body = body.as_bytes();
	let lower: Vec<_> = body.iter().map(u8::to_ascii_lowercase).collect();
	let mut metadata = HtmlMetadata::default();
	let mut offset = 0;

	while let Some(start) = lower
		.get(offset..)
		.and_then(|remaining| remaining.iter().position(|&byte| byte == b'<'))
		.map(|index| offset.saturating_add(index))
	{
		let Some(end) = lower
			.get(start..)
			.and_then(|remaining| remaining.iter().position(|&byte| byte == b'>'))
			.map(|index| start.saturating_add(index))
		else {
			break;
		};

		let tag_start = start.saturating_add(1);
		let Some(tag) = body.get(tag_start..end) else { break };
		let mut name_start = 0;
		while tag
			.get(name_start)
			.is_some_and(|&byte| byte.is_ascii_whitespace() || byte == b'/')
		{
			name_start = name_start.saturating_add(1);
		}
		let mut name_end = name_start;
		while tag
			.get(name_end)
			.is_some_and(|&byte| !byte.is_ascii_whitespace() && byte != b'/' && byte != b'>')
		{
			name_end = name_end.saturating_add(1);
		}
		let tag_name = tag.get(name_start..name_end).unwrap_or_default();

		if tag_name.eq_ignore_ascii_case(b"meta") {
			let key = html_attribute(tag, "property")
				.or_else(|| html_attribute(tag, "name"))
				.map(|value| value.to_ascii_lowercase());
			let value = html_attribute(tag, "content").map(|value| decode_html_entities(&value));

			if let (Some(key), Some(value)) = (key, value) {
				match key.as_str() {
					| "og:title" => set_once(&mut metadata.og_title, value),
					| "og:description" => set_once(&mut metadata.og_description, value),
					| "og:type" => set_once(&mut metadata.og_type, value),
					| "og:url" => set_once(&mut metadata.og_url, value),
					| "og:image" => set_once(&mut metadata.image, value),
					| "og:image:width" => metadata.image_width = value.parse().ok(),
					| "og:image:height" => metadata.image_height = value.parse().ok(),
					| "og:video" => set_once(&mut metadata.video, value),
					| "og:video:width" => metadata.video_width = value.parse().ok(),
					| "og:video:height" => metadata.video_height = value.parse().ok(),
					| "og:audio" => set_once(&mut metadata.audio, value),
					| "description" => set_once(&mut metadata.description, value),
					| _ => {},
				}
			}
		} else if tag_name.eq_ignore_ascii_case(b"title") {
			let content_start = end.saturating_add(1);
			if let Some(close) = lower.get(content_start..).and_then(|remaining| {
				remaining
					.windows(8)
					.position(|window| window == b"</title>")
			}) {
				let content_end = content_start.saturating_add(close);
				set_once(
					&mut metadata.title,
					decode_html_entities(
						String::from_utf8_lossy(
							body.get(content_start..content_end).unwrap_or_default(),
						)
						.trim(),
					),
				);
			}
		}

		offset = end.saturating_add(1);
	}

	metadata
}

#[cfg(feature = "url_preview")]
fn html_attribute(tag: &[u8], wanted: &str) -> Option<String> {
	let mut offset = 0;

	while offset < tag.len() {
		while tag
			.get(offset)
			.is_some_and(|&byte| byte.is_ascii_whitespace() || byte == b'/')
		{
			offset = offset.saturating_add(1);
		}

		let name_start = offset;
		while offset < tag.len()
			&& !tag[offset].is_ascii_whitespace()
			&& tag[offset] != b'='
			&& tag[offset] != b'/'
		{
			offset = offset.saturating_add(1);
		}
		if name_start == offset {
			break;
		}

		let name = tag.get(name_start..offset).unwrap_or_default();
		while tag.get(offset).is_some_and(u8::is_ascii_whitespace) {
			offset = offset.saturating_add(1);
		}
		if tag.get(offset) != Some(&b'=') {
			continue;
		}
		offset = offset.saturating_add(1);
		while tag.get(offset).is_some_and(u8::is_ascii_whitespace) {
			offset = offset.saturating_add(1);
		}

		let value = if matches!(tag.get(offset), Some(b'\'' | b'"')) {
			let quote = tag[offset];
			offset = offset.saturating_add(1);
			let value_start = offset;
			while offset < tag.len() && tag[offset] != quote {
				offset = offset.saturating_add(1);
			}
			let value = String::from_utf8_lossy(tag.get(value_start..offset).unwrap_or_default())
				.into_owned();
			offset = offset.saturating_add(usize::from(offset < tag.len()));
			value
		} else {
			let value_start = offset;
			while offset < tag.len() && !tag[offset].is_ascii_whitespace() && tag[offset] != b'/'
			{
				offset = offset.saturating_add(1);
			}
			String::from_utf8_lossy(tag.get(value_start..offset).unwrap_or_default()).into_owned()
		};

		if name.eq_ignore_ascii_case(wanted.as_bytes()) {
			return Some(value);
		}
	}

	None
}

#[cfg(feature = "url_preview")]
fn decode_html_entities(value: &str) -> String {
	value
		.replace("&amp;", "&")
		.replace("&quot;", "\"")
		.replace("&#39;", "'")
		.replace("&lt;", "<")
		.replace("&gt;", ">")
}

#[cfg(feature = "url_preview")]
fn set_once<T>(slot: &mut Option<T>, value: T) {
	if slot.is_none() {
		*slot = Some(value);
	}
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum MediaType {
	Html,
	Image,
	Video,
	Audio,
}

pub(super) fn classify_content_type(content_type: &str) -> Option<MediaType> {
	let lower = content_type.to_lowercase();
	if lower.starts_with("text/html") || lower.starts_with("application/xhtml+xml") {
		Some(MediaType::Html)
	} else if lower.starts_with("image/") {
		Some(MediaType::Image)
	} else if lower.starts_with("video/") {
		Some(MediaType::Video)
	} else if lower.starts_with("audio/") {
		Some(MediaType::Audio)
	} else {
		None
	}
}
