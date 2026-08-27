use std::{fmt::Write, path::PathBuf, sync::Arc};

use conduwuit::{
	Err, Result,
	utils::{stream::IterStream, time},
	warn,
};
use futures::{StreamExt, TryStreamExt};

use crate::admin_command;

#[admin_command]
pub(super) async fn uptime(&self) -> Result {
	let elapsed = self
		.services
		.server
		.started
		.elapsed()
		.expect("standard duration");

	let result = time::pretty(elapsed);
	self.write_str(&format!("{result}.")).await
}

#[admin_command]
pub(super) async fn show_config(&self) -> Result {
	self.bail_restricted()?;

	self.write_str(&format!("{}", *self.services.server.config))
		.await
}

#[admin_command]
pub(super) async fn reload_config(&self, path: Option<PathBuf>) -> Result {
	// The path argument is only what's optionally passed via the admin command,
	// so we need to merge it with the existing paths if any were given at startup.
	let mut paths = Vec::new();

	// Add previously saved paths to the argument list
	self.services
		.config
		.config_paths
		.clone()
		.unwrap_or_default()
		.iter()
		.for_each(|p| paths.push(p.to_owned()));

	// If a path is given, and it's not already in the list,
	// add it last, so that it overrides earlier files
	if let Some(p) = path {
		if !paths.contains(&p) {
			paths.push(p);
		}
	}

	self.services.config.reload(&paths)?;

	self.write_str(&format!("Successfully reconfigured from paths: {paths:?}"))
		.await
}

#[admin_command]
pub(super) async fn memory_usage(&self) -> Result {
	let services_usage = self.services.memory_usage().await?;
	let database_usage = self.services.db.db.memory_usage()?;
	let allocator_usage =
		conduwuit::alloc::memory_usage().map_or(String::new(), |s| format!("\nAllocator:\n{s}"));

	self.write_str(&format!(
		"Services:\n{services_usage}\nDatabase:\n{database_usage}{allocator_usage}",
	))
	.await
}

#[admin_command]
pub(super) async fn clear_caches(&self) -> Result {
	self.services.clear_cache().await;

	self.write_str("Done.").await
}

#[admin_command]
pub(super) async fn list_backups(&self) -> Result {
	let db = Arc::clone(&self.services.db);
	self.services
		.server
		.runtime()
		.spawn_blocking(move || db.db.backup_list().map(Iterator::collect::<Vec<_>>))
		.await??
		.into_iter()
		.stream()
		.map(Ok)
		.try_for_each(|result| writeln!(self, "{result}"))
		.await
}

#[admin_command]
pub(super) async fn backup_database(&self) -> Result {
	self.bail_restricted()?;

	let db = Arc::clone(&self.services.db);
	let result = self
		.services
		.server
		.runtime()
		.spawn_blocking(move || match db.db.backup() {
			| Ok(()) => "Done".to_owned(),
			| Err(e) => format!("Failed: {e}"),
		})
		.await?;

	let db = Arc::clone(&self.services.db);
	let count = self
		.services
		.server
		.runtime()
		.spawn_blocking(move || db.db.backup_count())
		.await??;

	self.write_str(&format!("{result}. Currently have {count} backups."))
		.await
}

#[admin_command]
pub(super) async fn admin_notice(&self, message: Vec<String>) -> Result {
	let message = message.join(" ");
	self.services.admin.send_text(&message).await;

	self.write_str("Notice was sent to #admins").await
}

#[admin_command]
pub(super) async fn reload_mods(&self) -> Result {
	self.bail_restricted()?;

	self.services.server.reload()?;

	self.write_str("Reloading server...").await
}

#[admin_command]
#[cfg(unix)]
pub(super) async fn restart(&self, force: bool) -> Result {
	use conduwuit::utils::sys::current_exe_deleted;

	if !force && current_exe_deleted() {
		return Err!(
			"The server cannot be restarted because the executable changed. If this is expected
			 use --force to override."
		);
	}

	self.services.server.restart()?;

	self.write_str("Restarting server...").await
}

#[admin_command]
pub(super) async fn shutdown(&self) -> Result {
	self.bail_restricted()?;

	warn!("shutdown command");
	self.services.server.shutdown()?;

	self.write_str("Shutting down server...").await
}

#[admin_command]
pub(super) async fn list_features(&self) -> Result {
	let mut enabled_features = conduwuit::info::introspection::ENABLED_FEATURES
		.lock()
		.expect("locked")
		.values()
		.flat_map(|f| f.iter().map(|s| s.replace('_', "-")))
		.collect::<Vec<_>>();

	enabled_features.sort_unstable();
	enabled_features.dedup();

	let mut available_features = conduwuit::build_metadata::WORKSPACE_FEATURES
		.iter()
		.flat_map(|(_, f)| f.iter().map(|s| s.replace('_', "-")))
		.collect::<Vec<_>>();

	available_features.sort_unstable();
	available_features.dedup();

	let disabled_features: Vec<_> = available_features
		.into_iter()
		.filter(|f| !enabled_features.contains(f))
		.collect();

	let mut features = String::new();

	for feature in enabled_features {
		writeln!(features, "✓ {feature} [enabled]")?;
	}

	for feature in disabled_features {
		writeln!(features, "x {feature}")?;
	}

	self.write_str(&features).await
}

#[admin_command]
pub(super) async fn build_info(&self) -> Result {
	let mut info = String::new();

	// Version information
	writeln!(info, "# Build Information\n")?;
	writeln!(info, "**Version:** {}", env!("CARGO_PKG_VERSION"))?;
	writeln!(info, "**Package:** {}", env!("CARGO_PKG_NAME"))?;
	writeln!(info, "**Description:** {}", env!("CARGO_PKG_DESCRIPTION"))?;

	// Git information
	writeln!(info, "\n## Git Information\n")?;
	if let Some(hash) = conduwuit::build_metadata::GIT_COMMIT_HASH {
		writeln!(info, "**Commit Hash:** {hash}")?;
	}
	if let Some(hash) = conduwuit::build_metadata::GIT_COMMIT_HASH_SHORT {
		writeln!(info, "**Commit Hash (short):** {hash}")?;
	}
	if let Some(url) = conduwuit::build_metadata::GIT_REMOTE_WEB_URL {
		writeln!(info, "**Repository:** {url}")?;
	}
	if let Some(url) = conduwuit::build_metadata::GIT_REMOTE_COMMIT_URL {
		writeln!(info, "**Commit URL:** {url}")?;
	}

	// Build environment
	writeln!(info, "\n## Build Environment\n")?;
	if let Some(profile) = conduwuit::build_metadata::PROFILE {
		writeln!(info, "**Profile:** {profile}")?;
	}
	if let Some(opt) = conduwuit::build_metadata::OPT_LEVEL {
		writeln!(info, "**Optimization Level:** {opt}")?;
	}
	if let Some(debug) = conduwuit::build_metadata::DEBUG {
		writeln!(info, "**Debug:** {debug}")?;
	}
	if let Some(target) = conduwuit::build_metadata::TARGET {
		writeln!(info, "**Target:** {target}")?;
	}
	if let Some(host) = conduwuit::build_metadata::HOST {
		writeln!(info, "**Host:** {host}")?;
	}

	// Rust compiler information
	writeln!(info, "\n## Compiler Information\n")?;
	if let Some(rustc) = conduwuit::build_metadata::RUSTC_VERSION {
		writeln!(info, "**Rustc Version:** {rustc}")?;
	}

	// Target configuration
	writeln!(info, "\n## Target Configuration\n")?;
	writeln!(info, "**Architecture:** {}", std::env::consts::ARCH)?;
	writeln!(info, "**OS:** {}", std::env::consts::OS)?;
	writeln!(info, "**Family:** {}", std::env::consts::FAMILY)?;
	if let Some(endian) = conduwuit::build_metadata::CFG_ENDIAN {
		writeln!(info, "**Endianness:** {endian}")?;
	}
	if let Some(ptr_width) = conduwuit::build_metadata::CFG_POINTER_WIDTH {
		writeln!(info, "**Pointer Width:** {ptr_width} bits")?;
	}
	if let Some(env) = conduwuit::build_metadata::CFG_ENV {
		if !env.is_empty() {
			writeln!(info, "**Environment:** {env}")?;
		}
	}

	self.write_str(&info).await
}
