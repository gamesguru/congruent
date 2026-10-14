use std::{str::FromStr, sync::OnceLock};

use conduwuit_core::{
	Result,
	config::Config,
	log::{LogLevelReloadHandles, capture},
};
use log::LevelFilter;

/// Console log filter parsed from the `log` config option.
///
/// Understands the comma-separated `[default,]target=level,...` subset of the
/// old `EnvFilter` syntax. A target directive matches that module path and
/// every module below it; the longest matching directive wins.
struct Filter {
	default: LevelFilter,
	directives: Vec<(String, LevelFilter)>,
}

impl Filter {
	fn parse(spec: &str) -> Self {
		let mut default = LevelFilter::Error;
		let mut directives = Vec::new();
		for part in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
			match part.split_once('=') {
				| Some((target, level)) => {
					// span/field directives (`target[span]=level`) have no meaning here
					if target.contains('[') {
						continue;
					}
					if let Ok(level) = LevelFilter::from_str(level.trim()) {
						directives.push((target.trim().to_owned(), level));
					}
				},
				| None =>
					if let Ok(level) = LevelFilter::from_str(part) {
						default = level;
					},
			}
		}

		Self { default, directives }
	}

	fn max_level(&self) -> LevelFilter {
		self.directives
			.iter()
			.map(|(_, level)| *level)
			.fold(self.default, Ord::max)
	}

	fn level_for(&self, target: &str) -> LevelFilter {
		self.directives
			.iter()
			.filter(|(prefix, _)| {
				target == prefix
					|| target
						.strip_prefix(prefix.as_str())
						.is_some_and(|rest| rest.starts_with("::"))
			})
			.max_by_key(|(prefix, _)| prefix.len())
			.map_or(self.default, |(_, level)| *level)
	}
}

static FILTER: OnceLock<Filter> = OnceLock::new();

struct Logger;

impl log::Log for Logger {
	fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
		FILTER
			.get()
			.is_none_or(|filter| metadata.level() <= filter.level_for(metadata.target()))
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
	config: &Config,
) -> Result<(LogLevelReloadHandles, std::sync::Arc<capture::State>)> {
	let filter = Filter::parse(&config.log);
	let max_level = filter.max_level();
	let _ = FILTER.set(filter);
	let _ = log::set_logger(&LOGGER);
	log::set_max_level(max_level);
	Ok((LogLevelReloadHandles, std::sync::Arc::new(capture::State::new())))
}
