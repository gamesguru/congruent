use std::{collections::BTreeMap, env, fmt::Write as FmtWrite, fs, io::Write, path::Path};

use cargo_metadata::MetadataCommand;

fn main() {
	println!("cargo:rerun-if-changed=Cargo.toml");

	let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap(); // Cargo.toml path
	let manifest_path = Path::new(&manifest_dir).join("Cargo.toml");

	let metadata = MetadataCommand::new()
		.manifest_path(&manifest_path)
		.no_deps()
		.exec()
		.expect("failed to parse `cargo metadata`");

	let workspace_packages = metadata
		.workspace_members
		.iter()
		.map(|package| {
			let package = metadata.packages.iter().find(|p| p.id == *package).unwrap();
			println!("cargo:rerun-if-changed={}", package.manifest_path.as_str());
			package
		})
		.collect::<Vec<_>>();

	// Extract available features from workspace packages
	let mut available_features: BTreeMap<String, Vec<String>> = BTreeMap::new();
	for package in &workspace_packages {
		let crate_name = package
			.name
			.trim_start_matches("conduwuit-")
			.replace('-', "_");
		let features: Vec<String> = package.features.keys().cloned().collect();
		if !features.is_empty() {
			available_features.insert(crate_name, features);
		}
	}

	// Generate Rust code for available features
	let features_code = generate_features_code(&available_features);
	let features_dst =
		Path::new(&env::var("OUT_DIR").expect("OUT_DIR not set")).join("available_features.rs");
	let mut features_file = fs::File::create(features_dst).unwrap();
	features_file.write_all(features_code.as_bytes()).unwrap();

	// Host info
	println!("cargo:rustc-env=HOST_OS={}", env::consts::OS);
	println!("cargo:rustc-env=HOST_ARCH={}", env::consts::ARCH);

	// Build profile and environment variables passed by Cargo to the build script
	if let Ok(profile) = env::var("PROFILE") {
		println!("cargo:rustc-env=PROFILE={profile}");
	}
	if let Ok(opt_level) = env::var("OPT_LEVEL") {
		println!("cargo:rustc-env=OPT_LEVEL={opt_level}");
	}
	if let Ok(debug) = env::var("DEBUG") {
		println!("cargo:rustc-env=DEBUG={debug}");
	}
	if let Ok(target) = env::var("TARGET") {
		println!("cargo:rustc-env=TARGET={target}");
	}
	if let Ok(host) = env::var("HOST") {
		println!("cargo:rustc-env=HOST={host}");
	}

	// Target Configuration Variables
	if let Ok(endian) = env::var("CARGO_CFG_TARGET_ENDIAN") {
		println!("cargo:rustc-env=CFG_ENDIAN={endian}");
	}
	if let Ok(ptr_width) = env::var("CARGO_CFG_TARGET_POINTER_WIDTH") {
		println!("cargo:rustc-env=CFG_POINTER_WIDTH={ptr_width}");
	}
	if let Ok(env) = env::var("CARGO_CFG_TARGET_ENV") {
		println!("cargo:rustc-env=CFG_ENV={env}");
	}

	// Rustc Version
	if let Ok(rustc) = std::process::Command::new("rustc")
		.arg("--version")
		.output()
	{
		println!(
			"cargo:rustc-env=RUSTC_VERSION={}",
			String::from_utf8_lossy(&rustc.stdout).trim()
		);
	}
}

fn generate_features_code(features: &BTreeMap<String, Vec<String>>) -> String {
	let mut code = String::from(
		"/// All available features for workspace crates\npub const WORKSPACE_FEATURES: \
		 &[(&str, &[&str])] = &[\n",
	);

	for (crate_name, feature_list) in features {
		write!(code, "    (\"{crate_name}\", &[").unwrap();
		for (i, feature) in feature_list.iter().enumerate() {
			if i > 0 {
				code.push_str(", ");
			}
			write!(code, "\"{feature}\"").unwrap();
		}
		code.push_str("]),\n");
	}

	code.push_str("];\n");

	code
}
