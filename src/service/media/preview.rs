//! URL Previews
//!
//! This functionality is gated by 'url_preview', but not at the unit level for
//! historical and simplicity reasons. Instead the feature gates the inclusion
//! of dependencies and nulls out results through the existing interface when
//! not featured.

use std::time::SystemTime;

use conduwuit::{Err, Result, debug, err, info};
use conduwuit_core::implement;
#[cfg(feature = "url_preview")]
use conduwuit_core::utils::response::LimitReadExt;
use ipaddress::IPAddress;
#[cfg(feature = "url_preview")]
use ruma::OwnedMxcUri;
use serde::Serialize;
use url::Url;

use super::Service;

#[derive(Serialize, Default, Clone)]
pub struct UrlPreviewData {
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:title"))]
	pub title: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:description"))]
	pub description: Option<String>,
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

#[implement(Service)]
pub async fn remove_url_preview(&self, url: &str) -> Result<()> {
	// TODO: also remove the downloaded image
	self.db.remove_url_preview(url)
}

#[implement(Service)]
pub async fn clear_url_previews(&self) { self.db.clear_url_previews().await; }

#[implement(Service)]
pub async fn set_url_preview(&self, url: &str, data: &UrlPreviewData) -> Result<()> {
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
	if let Ok(ip) = IPAddress::parse(url.host_str().expect("URL previously validated")) {
		if !self.services.client.valid_cidr_range(&ip) {
			return Err!(Request(Forbidden("Requesting from this address is forbidden")));
		}
	}

	let client = &self.services.client.url_preview;
	let mut response = client.head(url.as_str()).send().await?;

	if let Err(e) = response.error_for_status_ref() {
		if let Some(status) = e.status() {
			if status == reqwest::StatusCode::METHOD_NOT_ALLOWED
				|| status == reqwest::StatusCode::FORBIDDEN
				|| status == reqwest::StatusCode::NOT_IMPLEMENTED
			{
				debug!(%url, "URL preview HEAD probe returned {status}, falling back to GET");
				let mut req = client.get(url.as_str());
				if status == reqwest::StatusCode::FORBIDDEN {
					req =
						req.header(reqwest::header::USER_AGENT, conduwuit::version::user_agent());
				}
				response = req.send().await?;
			}
		}
	}

	if let Err(e) = response.error_for_status_ref() {
		return Err!(Request(Unknown(error!("HTTP {e} fetching URL preview probe"))));
	}

	debug!(%url, "URL preview response headers: {:?}", response.headers());

	if let Some(remote_addr) = response.remote_addr() {
		debug!(%url, "URL preview response remote address: {:?}", remote_addr);

		if let Ok(ip) = IPAddress::parse(remote_addr.ip().to_string()) {
			if !self.services.client.valid_cidr_range(&ip) {
				return Err!(Request(Forbidden("Requesting from this address is forbidden")));
			}
		}
	}

	let Some(content_type) = response.headers().get(reqwest::header::CONTENT_TYPE) else {
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

	self.set_url_preview(url.as_str(), &data).await?;

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
	use image::ImageReader;
	use ruma::Mxc;

	let mut preview_data = preview_data.unwrap_or_default();

	let mut response = self.services.client.url_preview.get(url).send().await?;

	if response.status() == reqwest::StatusCode::FORBIDDEN {
		response = self
			.services
			.client
			.url_preview
			.get(url)
			.header(reqwest::header::USER_AGENT, conduwuit::version::user_agent())
			.send()
			.await?;
	}

	if let Err(e) = response.error_for_status_ref() {
		return Err!(Request(Unknown(error!("HTTP {e} fetching image"))));
	}

	let image = response
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

	let mut final_image = image;
	let mut final_width;
	let mut final_height;

	let cursor = std::io::Cursor::new(&final_image);
	if let Ok(reader) = ImageReader::new(cursor).with_guessed_format() {
		if let Ok(dim) = reader.into_dimensions() {
			final_width = Some(dim.0);
			final_height = Some(dim.1);

			// Dynamically scale down massive URL preview images to 250x250 limits
			// to avoid gigabytes of raw 4K database hoarding.
			if dim.0 > 250 || dim.1 > 250 {
				if let Ok(img) = image::load_from_memory(&final_image) {
					use image::imageops::FilterType;
					let resized = img.resize(250, 250, FilterType::CatmullRom);
					let mut cursor = std::io::Cursor::new(Vec::new());

					if resized
						.write_to(&mut cursor, image::ImageFormat::Jpeg)
						.is_ok()
					{
						final_image = cursor.into_inner();
						final_width = Some(resized.width());
						final_height = Some(resized.height());
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

	preview_data.image_width = final_width.or(preview_data.image_width);
	preview_data.image_height = final_height.or(preview_data.image_height);

	self.create(&mxc, None, None, None, &final_image).await?;

	preview_data.image = Some(mxc.to_string());

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
	use ruma::Mxc;

	let mut response = self.services.client.url_preview.get(url).send().await?;

	if response.status() == reqwest::StatusCode::FORBIDDEN {
		response = self
			.services
			.client
			.url_preview
			.get(url)
			.header(reqwest::header::USER_AGENT, conduwuit::version::user_agent())
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

	Ok((OwnedMxcUri::from(mxc.to_string()), media.len()))
}

#[cfg(not(feature = "url_preview"))]
#[implement(Service)]
pub async fn download_image(
	&self,
	_url: &str,
	_preview_data: Option<UrlPreviewData>,
) -> Result<UrlPreviewData> {
	Err!(FeatureDisabled("url_preview"))
}

#[cfg(not(feature = "url_preview"))]
#[implement(Service)]
pub async fn download_video(
	&self,
	_url: &str,
	_preview_data: Option<UrlPreviewData>,
) -> Result<UrlPreviewData> {
	Err!(FeatureDisabled("url_preview"))
}

#[cfg(not(feature = "url_preview"))]
#[implement(Service)]
pub async fn download_audio(
	&self,
	_url: &str,
	_preview_data: Option<UrlPreviewData>,
) -> Result<UrlPreviewData> {
	Err!(FeatureDisabled("url_preview"))
}

#[cfg(not(feature = "url_preview"))]
#[implement(Service)]
pub async fn download_media(&self, _url: &str) -> Result<UrlPreviewData> {
	Err!(FeatureDisabled("url_preview"))
}

#[cfg(feature = "url_preview")]
#[implement(Service)]
async fn download_html(&self, url: &str) -> Result<UrlPreviewData> {
	use webpage::HTML;

	let client = &self.services.client.url_preview;
	let mut response = client.get(url).send().await?;

	if response.status() == reqwest::StatusCode::FORBIDDEN {
		response = client
			.get(url)
			.header(reqwest::header::USER_AGENT, conduwuit::version::user_agent())
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
	let Ok(html) = HTML::from_string(body.clone(), Some(url.to_owned())) else {
		return Err!(Request(Unknown("Failed to parse HTML")));
	};

	let mut preview_data = UrlPreviewData::default();

	if let Some(obj) = html.opengraph.images.first() {
		if let Ok(data_with_img) = self
			.download_image(&obj.url, Some(preview_data.clone()))
			.await
		{
			preview_data = data_with_img;
			preview_data = apply_opengraph_dimensions(preview_data, obj);
		}
	}

	if let Some(obj) = html.opengraph.videos.first() {
		preview_data = self.download_video(&obj.url, Some(preview_data)).await?;
		preview_data.video_width = obj.properties.get("width").and_then(|v| v.parse().ok());
		preview_data.video_height = obj.properties.get("height").and_then(|v| v.parse().ok());
	}

	if let Some(obj) = html.opengraph.audios.first() {
		preview_data = self.download_audio(&obj.url, Some(preview_data)).await?;
	}

	let props = html.opengraph.properties;

	/* use OpenGraph title/description, but fall back to HTML if not available */
	preview_data.title = props.get("title").cloned().or(html.title);
	preview_data.description = props.get("description").cloned().or(html.description);

	Ok(preview_data)
}

#[cfg(not(feature = "url_preview"))]
#[implement(Service)]
async fn download_html(&self, _url: &str) -> Result<UrlPreviewData> {
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
							"Root domain {} is not allowed by \
							 url_preview_domain_explicit_denylist (check 1/3)",
							&root_domain
						);
						return false;
					}

					if allowlist_domain_explicit.contains(&root_domain.to_owned()) {
						debug!(
							"Root domain {} is allowed by url_preview_domain_explicit_allowlist \
							 (check 2/3)",
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
							 (check 3/3)",
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
pub(super) fn apply_opengraph_dimensions(
	mut preview_data: UrlPreviewData,
	obj: &webpage::OpengraphObject,
) -> UrlPreviewData {
	preview_data.image_width = preview_data
		.image_width
		.or_else(|| obj.properties.get("width").and_then(|v| v.parse().ok()));
	preview_data.image_height = preview_data
		.image_height
		.or_else(|| obj.properties.get("height").and_then(|v| v.parse().ok()));
	preview_data
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
