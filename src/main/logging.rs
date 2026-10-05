use std::sync::Arc;

use conduwuit_core::{
	Result,
	config::Config,
	err,
	log::{ConsoleFormat, ConsoleWriter, LogLevelReloadHandles, capture, fmt_span},
	result::UnwrapOrErr,
};
use tracing_subscriber::{EnvFilter, Layer, Registry, fmt, layer::SubscriberExt, reload};

#[allow(clippy::redundant_clone)]
pub(crate) fn init(config: &Config) -> Result<(LogLevelReloadHandles, Arc<capture::State>)> {
	let reload_handles = LogLevelReloadHandles::default();

	let console_span_events = fmt_span::from_str(&config.log_span_events).unwrap_or_err();

	let console_filter = EnvFilter::builder()
		.with_regex(config.log_filter_regex)
		.parse(&config.log)
		.map_err(|e| err!(Config("log", "{e}.")))?;

	let console_layer = fmt::Layer::new()
		.with_span_events(console_span_events)
		.event_format(ConsoleFormat::new(config))
		.fmt_fields(ConsoleFormat::new(config))
		.with_writer(ConsoleWriter::new(config));

	let (console_reload_filter, console_reload_handle) =
		reload::Layer::new(console_filter.clone());

	reload_handles.add("console", Box::new(console_reload_handle));

	let cap_state = Arc::new(capture::State::new());
	let cap_layer = capture::Layer::new(&cap_state);

	let subscriber = Registry::default()
		.with(console_layer.with_filter(console_reload_filter))
		.with(cap_layer);

	let ret = (reload_handles, cap_state);

	set_global_default(subscriber);

	Ok(ret)
}

fn set_global_default<S>(subscriber: S)
where
	S: tracing::Subscriber + Send + Sync + 'static,
{
	tracing::subscriber::set_global_default(subscriber)
		.expect("the global default tracing subscriber failed to be initialized");
}
