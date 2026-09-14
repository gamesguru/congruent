use std::path::Path;

fn main() {
	println!("cargo:rustc-check-cfg=cfg(compare_has_ruma_upstream)");

	let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
	let fixtures_dir = Path::new(&manifest_dir)
		.join("../../ruma-upstream/crates/ruma-state-res/tests/it/resolve/fixtures");
	let snapshots_dir = Path::new(&manifest_dir)
		.join("../../ruma-upstream/crates/ruma-state-res/tests/it/resolve/snapshots");

	if fixtures_dir.exists() && snapshots_dir.exists() {
		println!("cargo:rustc-cfg=compare_has_ruma_upstream");
	}
}
