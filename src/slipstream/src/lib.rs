//! `slipstream`: facade over `ruma`.
//!
//! By default every item is real `ruma`. With the `slipstream` feature,
//! items listed in the override modules below are served by `mtx-slipstream`
//! instead. Explicit items shadow the glob re-export, so an override is just
//! a same-named item here; everything not overridden stays real ruma.

pub use ruma::*;

/// Slipstream's serde-free value codec.
pub mod codec {
	pub use mtx_slipstream::codec::*;
}

/// Canonical JSON entry points used at the remaining serde boundary.
pub mod canonical_json {
	pub use ruma::canonical_json::*;

	pub fn into_object(value: ruma::CanonicalJsonValue) -> Option<ruma::CanonicalJsonObject> {
		match value {
			| ruma::CanonicalJsonValue::Object(object) => Some(object),
			| _ => None,
		}
	}

	pub fn from_json_str<T>(input: &str) -> Result<T, serde_json::Error>
	where
		T: serde::de::DeserializeOwned,
	{
		serde_json::from_str(input)
	}
}
