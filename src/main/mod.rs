#![type_length_limit = "49152"] //TODO: reduce me
#![deny(unused_must_use)]
#![allow(clippy::disallowed_macros)]

use std::sync::{Arc, atomic::Ordering};

use conduwuit_core::{debug_info, error};

conduwuit_macros::introspect_crate! {}

mod clap;
mod logging;
mod mods;
mod panic;
mod restart;
mod runtime;
mod server;
mod signal;

use conduwuit_core::config::Config;

use crate::clap::{Command, command, generate_completions, update};

#[cfg(feature = "console")]
mod attach;

pub mod build_features {
	include!(concat!(env!("OUT_DIR"), "/features.rs"));
}

pub use conduwuit_core::{Error, Result};
use server::Server;

pub use crate::clap::Args;

pub fn run() -> Result<()> {
	panic::init();

	let args = clap::parse();

	if let Some(Command::Completions { shell }) = args.command {
		let mut command = command();
		generate_completions(&mut command, shell);
		return Ok(());
	}

	if args.version_verbose {
		let mut output = conduwuit_git_info::verbose_version();

		let enabled = build_features::ENABLED_FEATURES;
		output.push_str("\nenabled_features: ");
		output.push_str(&enabled.join(", "));

		let disabled = build_features::DISABLED_FEATURES;
		output.push_str("\ndisabled_features: ");
		output.push_str(&disabled.join(", "));

		println!("{output}");
		return Ok(());
	}

	#[cfg(feature = "console")]
	if args.attach {
		return attach::run(&args);
	}

	run_with_args(&args)
}

pub fn run_with_args(args: &Args) -> Result<()> {
	init_git_info();

	// Because we're not using rustls default-tls, we have to initialise a TLS
	// provider
	#[cfg(feature = "ring")]
	rustls::crypto::ring::default_provider()
		.install_default()
		.expect("failed to initialise ring rustls crypto provider");

	let config_paths = args.config.clone().unwrap_or_default();
	let config = Config::load(&config_paths)
		.and_then(|raw| update(raw, args))
		.and_then(|raw| Config::new(&raw))?;

	let runtime = runtime::new(args, &config)?;
	let server = Server::new(config, Some(runtime.handle()))?;

	runtime.spawn(signal::signal(server.clone()));
	runtime.block_on(async_main(&server, args.drop_sync_tokens))?;
	runtime::shutdown(&server, runtime);

	#[cfg(unix)]
	if server.server.restarting.load(Ordering::Acquire) {
		restart::restart();
	}

	debug_info!("Exit");
	Ok(())
}

async fn drop_sync_tokens(db: &conduwuit_database::Database) {
	conduwuit_core::info!("Dropping all sync tokens as requested by CLI flag...");
	if let Err(e) = db.db.drop_cf("roomsynctoken_shortstatehash") {
		conduwuit_core::warn!("Failed to drop sync tokens column family: {e}");
	}
	conduwuit_core::info!(
		"Finished dropping all sync tokens (requires restart to recreate the empty table)."
	);
}

/// Operate the server normally in release-mode static builds. This will start,
/// run and stop the server within the asynchronous runtime.
#[cfg(any(not(conduwuit_mods), not(feature = "conduwuit_mods")))]
async fn async_main(server: &Arc<Server>, drop_sync_tokens_flag: bool) -> Result<(), Error> {
	extern crate conduwuit_router as router;

	match router::start(&server.server).await {
		| Ok(services) => {
			if drop_sync_tokens_flag {
				drop_sync_tokens(&services.db).await;
			}
			let _ = server.services.lock().await.insert(services);
		},
		| Err(error) => {
			error!("Critical error starting server: {error}");
			return Err(error);
		},
	}

	if let Err(error) = router::run(
		server
			.services
			.lock()
			.await
			.as_ref()
			.expect("services initialized"),
	)
	.await
	{
		error!("Critical error running server: {error}");
		return Err(error);
	}

	if let Err(error) = router::stop(
		server
			.services
			.lock()
			.await
			.take()
			.expect("services initialized"),
	)
	.await
	{
		error!("Critical error stopping server: {error}");
		return Err(error);
	}

	debug_info!("Exit runtime");
	Ok(())
}

/// Operate the server in developer-mode dynamic builds. This will start, run,
/// and hot-reload portions of the server as-needed before returning for an
/// actual shutdown. This is not available in release-mode or static builds.
#[cfg(all(conduwuit_mods, feature = "conduwuit_mods"))]
async fn async_main(server: &Arc<Server>, drop_sync_tokens_flag: bool) -> Result<(), Error> {
	let mut starts = true;
	let mut reloads = true;
	while reloads {
		if let Err(error) = mods::open(server).await {
			error!("Loading router: {error}");
			return Err(error);
		}

		if starts && drop_sync_tokens_flag {
			drop_sync_tokens(&server.server.db).await;
		}

		let result = mods::run(server, starts).await;
		if let Ok(result) = result {
			(starts, reloads) = result;
		}

		let force = !reloads || result.is_err();
		if let Err(error) = mods::close(server, force).await {
			error!("Unloading router: {error}");
			return Err(error);
		}

		if let Err(error) = result {
			error!("{error}");
			return Err(error);
		}
	}

	debug_info!("Exit runtime");
	Ok(())
}

/// Hand the compile-time git info to core so the rest of the program can read
/// it without depending on the crate that changes on every commit.
fn init_git_info() {
	use conduwuit_git_info as git;

	conduwuit_core::info::git::set(conduwuit_core::info::git::GitInfo {
		commit_hash: git::GIT_COMMIT_HASH,
		commit_hash_short: git::GIT_COMMIT_HASH_SHORT,
		version_extra: git::VERSION_EXTRA,
		branch: git::GIT_BRANCH,
		remote_url: git::GIT_REMOTE_URL,
		remote_web_url: git::GIT_REMOTE_WEB_URL,
		remote_commit_url: git::GIT_REMOTE_COMMIT_URL,
	});
}
