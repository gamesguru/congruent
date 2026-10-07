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

#[derive(Clone, Copy, Debug, Default)]
pub struct EnvFilter;

impl EnvFilter {
	pub fn try_new(value: impl AsRef<str>) -> Result<Self, String> {
		let _ = value;
		Ok(Self)
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
	($($x:tt)+) => { $crate::__conduwuit_log!(error, $($x)+) };
}
#[macro_export]
macro_rules! warn {
	($($x:tt)+) => { $crate::__conduwuit_log!(warn, $($x)+) };
}
#[macro_export]
macro_rules! info {
	($($x:tt)+) => { $crate::__conduwuit_log!(info, $($x)+) };
}
#[macro_export]
macro_rules! debug {
	($($x:tt)+) => { $crate::__conduwuit_log!(debug, $($x)+) };
}
#[macro_export]
macro_rules! trace {
	($($x:tt)+) => { $crate::__conduwuit_log!(trace, $($x)+) };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __conduwuit_log {
	($level:ident, target: $target:literal, $($rest:tt)+) => {
		$crate::__conduwuit_log!(@parse $level, ($target), String::new(), $($rest)+)
	};
	($level:ident, $($rest:tt)+) => {
		$crate::__conduwuit_log!(@parse $level, default, String::new(), $($rest)+)
	};

	(@parse $level:ident, $target:tt, $prefix:expr, %$value:expr, $($rest:tt)+) => {{
		let prefix = format!("{}{}={}", $prefix, stringify!($value), &$value);
		$crate::__conduwuit_log!(@parse $level, $target, prefix, $($rest)+)
	}};
	(@parse $level:ident, $target:tt, $prefix:expr, ?$value:expr, $($rest:tt)+) => {{
		let prefix = format!("{}{}={:?} ", $prefix, stringify!($value), &$value);
		$crate::__conduwuit_log!(@parse $level, $target, prefix, $($rest)+)
	}};
	(@parse $level:ident, $target:tt, $prefix:expr, $name:ident, $($rest:tt)+) => {{
		let prefix = format!("{}{}={} ", $prefix, stringify!($name), &$name);
		$crate::__conduwuit_log!(@parse $level, $target, prefix, $($rest)+)
	}};
	(@parse $level:ident, $target:tt, $prefix:expr, $name:ident = %$value:expr, $($rest:tt)+) => {{
		let prefix = format!("{}{}={}", $prefix, stringify!($name), &$value);
		$crate::__conduwuit_log!(@parse $level, $target, prefix, $($rest)+)
	}};
	(@parse $level:ident, $target:tt, $prefix:expr, $name:ident = ?$value:expr, $($rest:tt)+) => {{
		let prefix = format!("{}{}={:?} ", $prefix, stringify!($name), &$value);
		$crate::__conduwuit_log!(@parse $level, $target, prefix, $($rest)+)
	}};
	(@parse $level:ident, $target:tt, $prefix:expr, $name:ident = $value:expr, $($rest:tt)+) => {{
		let prefix = format!("{}{}={} ", $prefix, stringify!($name), &$value);
		$crate::__conduwuit_log!(@parse $level, $target, prefix, $($rest)+)
	}};

	(@parse $level:ident, default, $prefix:expr, $fmt:literal $(, $args:expr)* $(,)?) => {
		::log::$level!("{}{}", $prefix, format_args!($fmt $(, $args)*))
	};
	(@parse $level:ident, ($target:literal), $prefix:expr, $fmt:literal $(, $args:expr)* $(,)?) => {
		::log::$level!(target: $target, "{}{}", $prefix, format_args!($fmt $(, $args)*))
	};
}

#[macro_export]
macro_rules! event {
	($level:expr_2021, $($x:tt)+) => { $crate::debug!($($x)+) };
}
