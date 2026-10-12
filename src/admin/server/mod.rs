mod commands;

use std::path::PathBuf;

use clap::Subcommand;
use conduwuit::Result;

use crate::admin_command_dispatch;

#[admin_command_dispatch]
#[derive(Debug, Subcommand)]
pub enum ServerCommand {
	/// Time elapsed since startup
	Uptime,

	/// Show configuration values
	ShowConfig,

	/// Show non-default config values and keys missing from your config
	/// file (useful for detecting drift after upgrades)
	ConfigDiff,

	/// Reload configuration values
	ReloadConfig {
		path: Option<PathBuf>,
	},

	/// Set a configuration key dynamically
	SetConfig {
		key: String,
		value: String,
	},

	/// Unset a dynamically configured key
	UnsetConfig {
		key: String,
	},

	/// Print database memory usage statistics
	MemoryUsage,

	/// Clears all of Continuwuity's caches
	ClearCaches,

	/// Performs an online backup of the database (only available for RocksDB
	///   at the moment)
	BackupDatabase,

	/// List database backups
	ListBackups,

	/// Send a message to the admin room.
	AdminNotice {
		message: Vec<String>,
	},

	/// Hot-reload the server
	#[clap(alias = "reload")]
	ReloadMods,

	#[cfg(unix)]
	/// Restart the server
	Restart {
		#[arg(short, long)]
		force: bool,
	},

	/// Shutdown the server
	Shutdown,

	/// List features built into the server
	ListFeatures,

	/// Build information
	BuildInfo,

	/// Print server diagnostics and metrics
	Diagnostics,
}
