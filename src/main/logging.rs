use conduwuit_core::{
	Result,
	config::Config,
	log::{LogLevelReloadHandles, capture},
};

struct Logger;

impl log::Log for Logger {
	fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
		metadata.level() <= log::Level::Trace
	}

	fn log(&self, record: &log::Record<'_>) {
		if self.enabled(record.metadata()) {
			eprintln!("{} [{}] {}", record.level(), record.target(), record.args());
		}
	}

	fn flush(&self) {}
}

static LOGGER: Logger = Logger;

pub(crate) fn init(
	_config: &Config,
) -> Result<(LogLevelReloadHandles, std::sync::Arc<capture::State>)> {
	let _ = log::set_logger(&LOGGER);
	log::set_max_level(log::LevelFilter::Trace);
	Ok((LogLevelReloadHandles, std::sync::Arc::new(capture::State::new())))
}
