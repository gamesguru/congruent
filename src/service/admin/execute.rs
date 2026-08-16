use conduwuit::{Err, Result, debug, debug_info, error, implement, info};
use ruma::events::room::message::RoomMessageEventContent;
use tokio::time::{Duration, sleep};

use crate::admin::InvocationSource;

pub(super) const SIGNAL: &str = "SIGUSR2";

/// Possibly spawn the terminal console at startup if configured.
#[implement(super::Service)]
pub(super) async fn console_auto_start(&self) {
	#[cfg(feature = "console")]
	{
		self.console.start_listener().await;
		if self.services.server.config.admin_console_automatic {
			// Allow more of the startup sequence to execute before spawning
			tokio::task::yield_now().await;
			self.console.start();
		}
	}
}

/// Shutdown the console when the admin worker terminates.
#[implement(super::Service)]
pub(super) async fn console_auto_stop(&self) {
	#[cfg(feature = "console")]
	self.console.close().await;
}

/// Execute admin commands after startup
#[implement(super::Service)]
pub async fn startup_execute(&self) -> Result {
	// List of commands to execute
	let commands = &self.services.server.config.admin_execute;

	// Determine if we're running in smoketest-mode which will change some behaviors
	let smoketest = self.services.server.config.test.contains("smoke");

	// When true, errors are ignored and startup continues.
	let errors = !smoketest && self.services.server.config.admin_execute_errors_ignore;

	//TODO: remove this after run-states are broadcast
	sleep(Duration::from_millis(500)).await;

	for (i, command) in commands.iter().enumerate() {
		if let Err(e) = self.execute_command(i, command.clone()).await {
			if !errors {
				return Err(e);
			}
		}

		tokio::task::yield_now().await;
	}

	if should_shutdown_after_startup_execute(
		self.services.server.config.admin_console_automatic,
		commands.len(),
		smoketest,
	) {
		debug_info!("Startup console commands complete. Shutting down now...");
		self.services
			.server
			.shutdown()
			.inspect_err(error::inspect_log)
			.expect("Error shutting down after startup console commands");
	}

	// The smoketest functionality is placed here for now and simply initiates
	// shutdown after all commands have executed.
	if smoketest {
		debug_info!("Smoketest mode. All commands complete. Shutting down now...");
		self.services
			.server
			.shutdown()
			.inspect_err(error::inspect_log)
			.expect("Error shutting down from smoketest");
	}

	Ok(())
}

/// Execute admin commands after signal
#[implement(super::Service)]
pub(super) async fn signal_execute(&self) -> Result {
	// List of commands to execute
	let commands = self.services.server.config.admin_signal_execute.clone();

	// When true, errors are ignored and execution continues.
	let ignore_errors = self.services.server.config.admin_execute_errors_ignore;

	for (i, command) in commands.iter().enumerate() {
		if let Err(e) = self.execute_command(i, command.clone()).await {
			if !ignore_errors {
				return Err(e);
			}
		}

		tokio::task::yield_now().await;
	}

	Ok(())
}

/// Execute one admin command after startup or signal
#[implement(super::Service)]
async fn execute_command(&self, i: usize, command: String) -> Result {
	debug!("Execute command #{i}: executing {command:?}");

	match self
		.command_in_place(command, None, InvocationSource::Console)
		.await
	{
		| Ok(Some(output)) => Self::execute_command_output(i, &output),
		| Err(output) => Self::execute_command_error(i, &output),
		| Ok(None) => {
			info!("Execute command #{i} completed (no output).");
			Ok(())
		},
	}
}

#[cfg(feature = "console")]
#[implement(super::Service)]
fn execute_command_output(i: usize, content: &RoomMessageEventContent) -> Result {
	debug_info!("Execute command #{i} completed:");
	super::console::print(content.body());
	Ok(())
}

#[cfg(feature = "console")]
#[implement(super::Service)]
fn execute_command_error(i: usize, content: &RoomMessageEventContent) -> Result {
	super::console::print_err(content.body());
	Err!(debug_error!("Execute command #{i} failed."))
}

#[cfg(not(feature = "console"))]
#[implement(super::Service)]
fn execute_command_output(i: usize, content: &RoomMessageEventContent) -> Result {
	info!("Execute command #{i} completed:\n{:#}", content.body());
	Ok(())
}

#[cfg(not(feature = "console"))]
#[implement(super::Service)]
fn execute_command_error(i: usize, content: &RoomMessageEventContent) -> Result {
	Err!(error!("Execute command #{i} failed:\n{:#}", content.body()))
}

#[must_use]
fn should_shutdown_after_startup_execute(
	admin_console_automatic: bool,
	command_count: usize,
	smoketest: bool,
) -> bool {
	smoketest || (admin_console_automatic && command_count > 0)
}

#[cfg(test)]
mod tests {
	use super::should_shutdown_after_startup_execute;

	#[test]
	fn startup_console_shuts_down_after_execute_commands() {
		assert!(should_shutdown_after_startup_execute(true, 1, false));
		assert!(should_shutdown_after_startup_execute(true, 2, false));
	}

	#[test]
	fn startup_console_stays_alive_without_execute_commands() {
		assert!(!should_shutdown_after_startup_execute(true, 0, false));
		assert!(!should_shutdown_after_startup_execute(false, 0, false));
		assert!(!should_shutdown_after_startup_execute(false, 3, false));
	}

	#[test]
	fn smoketest_always_shuts_down() {
		assert!(should_shutdown_after_startup_execute(false, 0, true));
		assert!(should_shutdown_after_startup_execute(true, 0, true));
		assert!(should_shutdown_after_startup_execute(false, 3, true));
	}
}
