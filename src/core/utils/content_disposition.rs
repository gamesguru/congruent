use std::borrow::Cow;

use slipstream::http_headers::{ContentDisposition, ContentDispositionType};

use crate::debug_info;

const ALLOWED_INLINE_CONTENT_TYPES: [&str; 26] = [
	"application/json",
	"application/ld+json",
	"audio/aac",
	"audio/flac",
	"audio/mp4",
	"audio/mpeg",
	"audio/ogg",
	"audio/wav",
	"audio/wave",
	"audio/webm",
	"audio/x-flac",
	"audio/x-pn-wav",
	"audio/x-wav",
	"image/apng",
	"image/avif",
	"image/gif",
	"image/jpeg",
	"image/png",
	"image/webp",
	"text/css",
	"text/csv",
	"text/plain",
	"video/mp4",
	"video/ogg",
	"video/quicktime",
	"video/webm",
];

#[must_use]
pub fn content_disposition_type(content_type: Option<&str>) -> ContentDispositionType {
	let Some(content_type) = content_type else {
		debug_info!("No Content-Type was given, assuming attachment for Content-Disposition");
		return ContentDispositionType::Attachment;
	};

	debug_assert!(
		ALLOWED_INLINE_CONTENT_TYPES.is_sorted(),
		"inline content types must remain sorted for binary_search"
	);
	let content_type: Cow<'_, str> = content_type
		.split(';')
		.next()
		.unwrap_or(content_type)
		.to_ascii_lowercase()
		.into();

	if ALLOWED_INLINE_CONTENT_TYPES
		.binary_search(&content_type.as_ref())
		.is_ok()
	{
		ContentDispositionType::Inline
	} else {
		ContentDispositionType::Attachment
	}
}

/// Sanitises a file name for use in Content-Disposition.
#[must_use]
pub fn sanitise_filename(filename: &str) -> String {
	filename
		.chars()
		.map(|character| {
			if character.is_control() || r#"/\?%*:|"$<>"#.contains(character) {
				'_'
			} else {
				character
			}
		})
		.collect()
}

pub fn make_content_disposition(
	content_disposition: Option<&ContentDisposition>,
	content_type: Option<&str>,
	filename: Option<&str>,
) -> ContentDisposition {
	ContentDisposition::new(content_disposition_type(content_type)).with_filename(
		filename
			.or_else(|| content_disposition.and_then(|value| value.filename.as_deref()))
			.map(sanitise_filename),
	)
}

#[cfg(test)]
mod tests {
	#[test]
	fn replaces_unsafe_characters() {
		assert_eq!("a_b_c_d_e_f_g", super::sanitise_filename("a/b\\c?d:e*f|g"));
		assert_eq!("line_one", super::sanitise_filename("line\none"));
	}

	#[test]
	fn preserves_empty_names() {
		assert_eq!("", super::sanitise_filename(""));
	}
}
