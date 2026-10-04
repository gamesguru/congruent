//! The local Matrix compatibility surface.

pub use mtx_slipstream::*;

/// Slipstream's serde-free value codec.
pub mod codec {
	pub use mtx_slipstream::codec::*;
}

/// Canonical JSON entry points used at the remaining serde boundary.
pub mod canonical_json {
	pub use mtx_slipstream::canonical_json::*;
}
