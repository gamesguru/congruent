#![cfg(test)]

#[tokio::test]
#[cfg(disable)] //TODO: fixme
async fn long_file_names_works() {
	use std::path::PathBuf;

	use base64::{Engine as _, engine::general_purpose};

	use super::*;

	struct MockedKVDatabase;

	impl Data for MockedKVDatabase {
		fn create_file_metadata(
			&self,
			_sender_user: Option<&str>,
			mxc: String,
			width: u32,
			height: u32,
			content_disposition: Option<&str>,
			content_type: Option<&str>,
		) -> Result<Vec<u8>> {
			// copied from src/database/key_value/media.rs
			let mut key = mxc.as_bytes().to_vec();
			key.push(0xFF);
			key.extend_from_slice(&width.to_be_bytes());
			key.extend_from_slice(&height.to_be_bytes());
			key.push(0xFF);
			key.extend_from_slice(
				content_disposition
					.as_ref()
					.map(|f| f.as_bytes())
					.unwrap_or_default(),
			);
			key.push(0xFF);
			key.extend_from_slice(
				content_type
					.as_ref()
					.map(|c| c.as_bytes())
					.unwrap_or_default(),
			);

			Ok(key)
		}

		fn delete_file_mxc(&self, _mxc: String) -> Result<()> { todo!() }

		fn search_mxc_metadata_prefix(&self, _mxc: String) -> Result<Vec<Vec<u8>>> { todo!() }

		fn get_all_media_keys(&self) -> Vec<Vec<u8>> { todo!() }

		fn search_file_metadata(
			&self,
			_mxc: String,
			_width: u32,
			_height: u32,
		) -> Result<(Option<String>, Option<String>, Vec<u8>)> {
			todo!()
		}

		fn remove_url_preview(&self, _url: &str) -> Result<()> { todo!() }

		fn set_url_preview(
			&self,
			_url: &str,
			_data: &UrlPreviewData,
			_timestamp: std::time::Duration,
		) -> Result<()> {
			todo!()
		}

		fn get_url_preview(&self, _url: &str) -> Option<UrlPreviewData> { todo!() }
	}

	let db: Arc<MockedKVDatabase> = Arc::new(MockedKVDatabase);
	let mxc = "mxc://example.com/ascERGshawAWawugaAcauga".to_owned();
	let width = 100;
	let height = 100;
	let content_disposition = "attachment; filename=\"this is a very long file name with spaces \
	                           and special characters like äöüß and even emoji like 🦀.png\"";
	let content_type = "image/png";
	let key = db
		.create_file_metadata(
			None,
			mxc,
			width,
			height,
			Some(content_disposition),
			Some(content_type),
		)
		.unwrap();
	let mut r = PathBuf::from("/tmp/media");
	// r.push(base64::encode_config(key, base64::URL_SAFE_NO_PAD));
	// use the sha256 hash of the key as the file name instead of the key itself
	// this is because the base64 encoded key can be longer than 255 characters.
	r.push(general_purpose::URL_SAFE_NO_PAD.encode(<sha2::Sha256 as sha2::Digest>::digest(key)));
	// Check that the file path is not longer than 255 characters
	// (255 is the maximum length of a file path on most file systems)
	assert!(
		r.to_str().unwrap().len() <= 255,
		"File path is too long: {}",
		r.to_str().unwrap().len()
	);
}

#[cfg(all(test, feature = "url_preview"))]
mod url_and_opengraph_parsing_tests {
	use webpage::HTML;

	#[test]
	fn test_valid_urls_parse_correctly() {
		use super::super::preview::parse_preview_url;

		// Standard parsing should pass through
		assert!(parse_preview_url("https://upload.wikimedia.org/wikipedia/commons/thumb/7/71/Bertrand_Russell_smoking_in_1936.jpg").is_ok());
		assert!(parse_preview_url("http://example.com").is_ok());

		// Missing schemas should automatically fallback to https
		let appended = parse_preview_url("wikipedia.org").expect("failed to parse schemeless");
		assert_eq!(appended.scheme(), "https");
		assert_eq!(appended.host_str(), Some("wikipedia.org"));
		assert_eq!(appended.as_str(), "https://wikipedia.org/");
	}

	#[test]
	fn test_url_formatting_artifacts_stripped() {
		use super::super::preview::parse_preview_url;

		// Single quotes parsed by clients shouldn't break the URL
		let stripped_quote =
			parse_preview_url("https://cinny.nutra.tk/'").expect("linkify single quote");
		assert_eq!(stripped_quote.as_str(), "https://cinny.nutra.tk/");

		// Common punctuation shouldn't break the URL
		let stripped_comma =
			parse_preview_url("https://wikipedia.org/wiki/Rust,").expect("linkify comma");
		assert_eq!(stripped_comma.as_str(), "https://wikipedia.org/wiki/Rust");

		let stripped_parens =
			parse_preview_url("(https://wikipedia.org/wiki/Rust)").expect("linkify parens");
		assert_eq!(stripped_parens.as_str(), "https://wikipedia.org/wiki/Rust");

		// Even completely raw schema-less ones with padding should cleanly extract
		let schemeless_artifact =
			parse_preview_url("wikipedia.org/").expect("linkify schemeless");
		assert_eq!(schemeless_artifact.as_str(), "https://wikipedia.org/");
	}

	#[test]
	#[cfg(feature = "url_preview")]
	fn test_content_type_parsing() {
		use super::super::preview::{MediaType, classify_content_type};

		// standard parsing
		assert_eq!(classify_content_type("text/html"), Some(MediaType::Html));
		assert_eq!(classify_content_type("text/html; charset=UTF-8"), Some(MediaType::Html));

		// uppercase edgecases (Wikimedia)
		assert_eq!(classify_content_type("Text/HTML; charset=UTF-8"), Some(MediaType::Html));

		// xhtml support
		assert_eq!(classify_content_type("application/xhtml+xml"), Some(MediaType::Html));

		// media support
		assert_eq!(classify_content_type("image/jpeg"), Some(MediaType::Image));
		assert_eq!(classify_content_type("video/mp4"), Some(MediaType::Video));
		assert_eq!(classify_content_type("audio/ogg"), Some(MediaType::Audio));

		// invalid fallback
		assert_eq!(classify_content_type("application/json"), None);
		assert_eq!(classify_content_type("text/plain"), None);
		assert_eq!(classify_content_type(""), None);
	}

	#[test]
	fn test_wikipedia_html_snippet() {
		let wikipedia = r#"<meta property="og:image" content="https://upload.wikimedia.org/wikipedia/commons/thumb/7/71/Bertrand_Russell_smoking_in_1936.jpg/960px-Bertrand_Russell_smoking_in_1936.jpg">
            <meta property="og:image:width" content="955">
            <meta property="og:image:height" content="1200">
            <meta property="og:title" content="Bertrand Russell - Wikipedia">
            <meta property="og:type" content="website">
        "#;
		let html = HTML::from_string(wikipedia.to_string(), None).expect("failed to parse HTML");

		let img = html.opengraph.images.first().expect("no og:image found");
		assert_eq!(
			img.url,
			"https://upload.wikimedia.org/wikipedia/commons/thumb/7/71/Bertrand_Russell_smoking_in_1936.jpg/960px-Bertrand_Russell_smoking_in_1936.jpg"
		);
		assert_eq!(img.properties.get("width").map(|s| s.as_str()), Some("955"));
		assert_eq!(img.properties.get("height").map(|s| s.as_str()), Some("1200"));
	}

	#[test]
	fn test_youtube_music_html_snippet() {
		let youtube = r#"<meta property="og:site_name" content="YouTube Music">
            <meta property="og:url" content="https://music.youtube.com/watch?v=Sg8sw-OvcGk&amp;list=RDAMVMoZ_wMBHxYac&amp;index=0">
            <meta property="og:title" content="Hands On Transparent">
            <meta property="og:description" content="Nicone">
            <meta property="og:image" content="https://lh3.googleusercontent.com/B260PhEADGfdW2KWv9fSOSEyQ2AXPMOwaZcNOYN4wDOiVC6fHSr-Un9SonuWQyuFoQip64Gnyuuwggo">
            <meta property="og:image:width" content="1000">
            <meta property="og:image:height" content="1000">
            <meta property="og:type" content="video.other">
            <meta property="og:video:tag" content="Nicone">
        "#;
		let html = HTML::from_string(youtube.to_string(), None).expect("failed to parse HTML");

		let img = html.opengraph.images.first().expect("no og:image found");
		assert_eq!(
			img.url,
			"https://lh3.googleusercontent.com/B260PhEADGfdW2KWv9fSOSEyQ2AXPMOwaZcNOYN4wDOiVC6fHSr-Un9SonuWQyuFoQip64Gnyuuwggo"
		);
		assert_eq!(img.properties.get("width").map(|s| s.as_str()), Some("1000"));
		assert_eq!(img.properties.get("height").map(|s| s.as_str()), Some("1000"));

		// Assert no og:video element since it wasn't defined
		assert!(html.opengraph.videos.first().is_none());
	}

	#[test]
	fn test_apply_opengraph_dimensions_propagation() {
		use super::super::preview::{UrlPreviewData, apply_opengraph_dimensions};

		let html_snippet = r#"<meta property="og:image" content="https://example.com/image.jpg">
            <meta property="og:image:width" content="1920">
            <meta property="og:image:height" content="1080">
        "#;
		let html =
			HTML::from_string(html_snippet.to_string(), None).expect("failed to parse HTML");
		let obj = html.opengraph.images.first().expect("no og:image found");

		let preview_data = UrlPreviewData::default();
		let result = apply_opengraph_dimensions(preview_data, obj);

		assert_eq!(result.image_width, Some(1920));
		assert_eq!(result.image_height, Some(1080));
	}
}
