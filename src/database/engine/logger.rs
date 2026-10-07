use conduwuit::{debug, error, warn};
use rocksdb::LogLevel;

pub(crate) fn handle(level: LogLevel, msg: &str) {
	let msg = msg.trim();
	if msg.starts_with("Options") {
		return;
	}

	match level {
		| LogLevel::Header | LogLevel::Debug | LogLevel::Info => debug!("{msg}"),
		| LogLevel::Error | LogLevel::Fatal => error!("{msg}"),
		| LogLevel::Warn => warn!("{msg}"),
	}
}
