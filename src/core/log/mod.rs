#![allow(clippy::disallowed_macros)]

use std::sync::Arc;

pub mod capture;
pub mod color;
mod suppress;

pub use capture::Capture;
pub use suppress::Suppress;

#[must_use]
pub const fn is_systemd_mode() -> bool { false }

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Level {
	ERROR,
	WARN,
	INFO,
	DEBUG,
	TRACE,
}

#[derive(Clone, Debug, Default)]
pub struct EnvFilter(String);

impl EnvFilter {
	pub fn try_new(value: impl AsRef<str>) -> Result<Self, String> {
		Ok(Self(value.as_ref().to_owned()))
	}
}

#[derive(Default)]
pub struct LogLevelReloadHandles;

impl LogLevelReloadHandles {
	pub fn add(&self, _name: &str, _handle: Box<dyn Send + Sync>) {}

	pub fn reload(&self, _filter: &EnvFilter, _names: Option<&[&str]>) -> crate::Result<()> {
		Ok(())
	}

	pub fn current(&self, _name: &str) -> Option<EnvFilter> { None }
}

pub struct Log {
	pub reload: LogLevelReloadHandles,
	pub capture: Arc<capture::State>,
}

#[macro_export]
macro_rules! error {
	($fmt:literal $(, $args:expr)* $(,)?) => { ::log::error!($fmt $(, $args)*) };
	($($x:tt)+) => { ::log::error!("{}", stringify!($($x)+)) };
}
#[macro_export]
macro_rules! warn {
	($fmt:literal $(, $args:expr)* $(,)?) => { ::log::warn!($fmt $(, $args)*) };
	($($x:tt)+) => { ::log::warn!("{}", stringify!($($x)+)) };
}
#[macro_export]
macro_rules! info {
	($fmt:literal $(, $args:expr)* $(,)?) => { ::log::info!($fmt $(, $args)*) };
	($($x:tt)+) => { ::log::info!("{}", stringify!($($x)+)) };
}
#[macro_export]
macro_rules! debug {
	($fmt:literal $(, $args:expr)* $(,)?) => { ::log::debug!($fmt $(, $args)*) };
	($($x:tt)+) => { ::log::debug!("{}", stringify!($($x)+)) };
}
#[macro_export]
macro_rules! trace {
	($fmt:literal $(, $args:expr)* $(,)?) => { ::log::trace!($fmt $(, $args)*) };
	($($x:tt)+) => { ::log::trace!("{}", stringify!($($x)+)) };
}

#[macro_export]
macro_rules! event {
	($level:expr_2021, $($x:tt)+) => { $crate::debug!($($x)+) };
}
