use std::{
	collections::BTreeMap,
	env,
	fmt::Write as FmtWrite,
	fs,
	io::Write,
	path::{Path, PathBuf},
};

fn main() {
	println!("cargo:rerun-if-changed=Cargo.toml");

	let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
	let workspace_root = manifest_dir
		.parent()
		.and_then(Path::parent)
		.expect("build metadata crate must be inside the workspace");
	let workspace_manifest = workspace_root.join("Cargo.toml");
	let workspace = read_table(&workspace_manifest);

	println!("cargo:rerun-if-changed={}", workspace_manifest.display());
	let workspace_packages = workspace_members(workspace_root, &workspace)
		.into_iter()
		.map(|manifest_path| {
			println!("cargo:rerun-if-changed={}", manifest_path.display());
			let manifest = read_table(&manifest_path);
			let package = manifest
				.get("package")
				.and_then(toml::Value::as_table)
				.cloned()
				.expect("workspace member is missing [package]");
			(package, manifest_path)
		})
		.collect::<Vec<_>>();

	// Extract available features from workspace packages
	let mut available_features: BTreeMap<String, Vec<String>> = BTreeMap::new();
	for (package, _) in &workspace_packages {
		let crate_name = package
			.get("name")
			.and_then(toml::Value::as_str)
			.expect("workspace package is missing a name")
			.trim_start_matches("conduwuit-")
			.replace('-', "_");
		let features = package
			.get("features")
			.and_then(toml::Value::as_table)
			.map_or_else(Vec::new, |features| features.keys().cloned().collect());
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

fn read_table(path: &Path) -> toml::Table {
	fs::read_to_string(path)
		.unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
		.parse()
		.unwrap_or_else(|error| panic!("failed to parse {}: {error}", path.display()))
}

fn workspace_members(root: &Path, workspace: &toml::Table) -> Vec<PathBuf> {
	let members = workspace
		.get("workspace")
		.and_then(toml::Value::as_table)
		.and_then(|workspace| workspace.get("members"))
		.and_then(toml::Value::as_array)
		.expect("workspace is missing members");

	members
		.iter()
		.flat_map(|member| {
			let pattern = member.as_str().expect("workspace member must be a string");
			let path = Path::new(pattern);
			if path.file_name() == Some(std::ffi::OsStr::new("*")) {
				let parent = root.join(path.parent().unwrap_or_else(|| Path::new(".")));
				fs::read_dir(parent)
					.expect("failed to read workspace member directory")
					.filter_map(Result::ok)
					.map(|entry| entry.path().join("Cargo.toml"))
					.filter(|manifest| manifest.is_file())
					.collect::<Vec<_>>()
			} else {
				vec![root.join(path).join("Cargo.toml")]
			}
		})
		.collect()
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
