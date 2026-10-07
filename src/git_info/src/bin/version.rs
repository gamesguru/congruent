use conduwuit_git_info as metadata;

fn main() {
	let semver = env!("CARGO_PKG_VERSION");
	if let Some(extra) = metadata::version_tag() {
		if extra.starts_with('+') {
			println!("{semver}{extra}");
		} else {
			println!("{semver} ({extra})");
		}
	} else {
		println!("{semver}");
	}
}
