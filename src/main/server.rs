use std::sync::Arc;

use conduwuit_core::{
	Error, Result,
	config::Config,
	info,
	log::Log,
	utils::{stream, sys},
};
use tokio::{runtime, sync::Mutex};

/// Server runtime state; complete
pub(crate) struct Server {
	/// Server runtime state; public portion
	pub(crate) server: Arc<conduwuit_core::Server>,

	pub(crate) services: Mutex<Option<Arc<conduwuit_service::Services>>>,

	#[cfg(all(conduwuit_mods, feature = "conduwuit_mods"))]
	// Module instances; TODO: move to mods::loaded mgmt vector
	pub(crate) mods: tokio::sync::RwLock<Vec<conduwuit_core::mods::Module>>,
}

impl Server {
	pub(crate) fn new(
		config: Config,
		runtime: Option<&runtime::Handle>,
	) -> Result<Arc<Self>, Error> {
		let _runtime_guard = runtime.map(runtime::Handle::enter);

		let (tracing_reload_handle, capture) = crate::logging::init(&config)?;

		config.check()?;

		#[cfg(unix)]
		sys::maximize_fd_limit()
			.expect("Unable to increase maximum soft and hard file descriptor limit");

		let (_old_width, _new_width) = stream::set_width(config.stream_width_default);
		let (_old_amp, _new_amp) = stream::set_amplification(config.stream_amplification);

		info!(
			server_name = %config.server_name,
			database_path = ?config.database_path,
			log_levels = %config.log,
			"{}",
			conduwuit_core::version(),
		);

		Ok(Arc::new(Self {
			server: Arc::new(conduwuit_core::Server::new(config, runtime, Log {
				reload: tracing_reload_handle,
				capture,
			})),

			services: None.into(),

			#[cfg(all(conduwuit_mods, feature = "conduwuit_mods"))]
			mods: tokio::sync::RwLock::new(Vec::new()),
		}))
	}
}
