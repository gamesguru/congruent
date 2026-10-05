use std::env;

fn main() {
	println!("cargo:rerun-if-env-changed=ROCKSDB_LIB_DIR");

	if let Ok(lib_dir) = env::var("ROCKSDB_LIB_DIR") {
		println!("cargo:rustc-link-search=native={lib_dir}");
	}

	let libs = ["z", "snappy", "zstd", "uring", "stdc++"];
	for lib in libs {
		println!("cargo:rustc-link-lib={lib}");
	}
}
