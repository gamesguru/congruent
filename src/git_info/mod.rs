//! Git/version info. Kept out of every crate except the binary so that a new
//! commit only rebuilds this crate and `main`; the values are handed to the
//! rest of the program at runtime via `conduwuit_core::info::git`.

pub static GIT_COMMIT_HASH: Option<&str> = option_env!("GIT_COMMIT_HASH");

pub static GIT_COMMIT_HASH_SHORT: Option<&str> = option_env!("GIT_COMMIT_HASH_SHORT");

// this would be a lot better if Option::or was const.
pub static VERSION_EXTRA: Option<&str> =
	if let v @ Some(_) = option_env!("CONTINUWUITY_VERSION_EXTRA") {
		v
	} else if let v @ Some(_) = option_env!("CONDUWUIT_VERSION_EXTRA") {
		v
	} else {
		option_env!("CONDUIT_VERSION_EXTRA")
	};

#[must_use]
pub fn version_tag() -> Option<&'static str> {
	VERSION_EXTRA
		.filter(|s| !s.is_empty())
		.or(GIT_COMMIT_HASH_SHORT)
}

pub static GIT_REMOTE_WEB_URL: Option<&str> = option_env!("GIT_REMOTE_WEB_URL");
pub static GIT_REMOTE_COMMIT_URL: Option<&str> = option_env!("GIT_REMOTE_COMMIT_URL");
pub static GIT_REMOTE_URL: Option<&str> = option_env!("GIT_REMOTE_URL");
pub static GIT_BRANCH: Option<&str> = option_env!("GIT_BRANCH");

#[must_use]
pub fn verbose_version() -> String {
	let semver = env!("CARGO_PKG_VERSION");
	let fields = [
		("version", Some(semver)),
		("version_extra", VERSION_EXTRA),
		("commit", GIT_COMMIT_HASH),
		("commit_short", GIT_COMMIT_HASH_SHORT),
		("branch", GIT_BRANCH),
		("remote_url", GIT_REMOTE_URL),
		("remote_web_url", GIT_REMOTE_WEB_URL),
		("remote_commit_url", GIT_REMOTE_COMMIT_URL),
	];

	fields
		.iter()
		.map(|(k, v)| format!("{k}: {}", v.unwrap_or("(unknown)")))
		.collect::<Vec<_>>()
		.join("\n")
}
