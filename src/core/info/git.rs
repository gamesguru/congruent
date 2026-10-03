//! Git/commit info for the running binary.
//!
//! This is injected at startup by the binary (see `main`), rather than being
//! read from a build script here, so a new commit doesn't invalidate this
//! crate or anything downstream of it.

use std::sync::OnceLock;

#[derive(Clone, Copy, Debug, Default)]
pub struct GitInfo {
	pub commit_hash: Option<&'static str>,
	pub commit_hash_short: Option<&'static str>,
	pub version_extra: Option<&'static str>,
	pub branch: Option<&'static str>,
	pub remote_url: Option<&'static str>,
	pub remote_web_url: Option<&'static str>,
	pub remote_commit_url: Option<&'static str>,
}

static GIT_INFO: OnceLock<GitInfo> = OnceLock::new();

/// Set once at startup, before `version()` is first called. Later calls are
/// ignored.
pub fn set(info: GitInfo) { let _ = GIT_INFO.set(info); }

#[must_use]
pub fn get() -> GitInfo { GIT_INFO.get().copied().unwrap_or_default() }

#[must_use]
pub fn version_tag() -> Option<&'static str> {
	let info = GIT_INFO.get()?;
	info.version_extra
		.filter(|s| !s.is_empty())
		.or(info.commit_hash_short)
}

#[must_use]
pub fn commit_url() -> Option<&'static str> {
	let info = GIT_INFO.get()?;
	info.remote_commit_url.or(info.remote_web_url)
}
