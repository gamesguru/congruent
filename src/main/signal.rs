use std::sync::Arc;

use conduwuit_core::{debug_error, trace, warn};
use futures::{FutureExt, pin_mut};
use tokio::signal;

use super::server::Server;

#[cfg(unix)]
pub(super) async fn signal(server: Arc<Server>) {
	use signal::unix;
	use unix::SignalKind;

	const CONSOLE: bool = cfg!(feature = "console");
	const RELOADING: bool = cfg!(all(conduwuit_mods, feature = "conduwuit_mods", not(CONSOLE)));

	let mut quit = unix::signal(SignalKind::quit()).expect("SIGQUIT handler");
	let mut term = unix::signal(SignalKind::terminate()).expect("SIGTERM handler");
	let mut usr1 = unix::signal(SignalKind::user_defined1()).expect("SIGUSR1 handler");
	let mut usr2 = unix::signal(SignalKind::user_defined2()).expect("SIGUSR2 handler");
	loop {
		trace!("Installed signal handlers");
		let sig: &'static str;
		let ctrl_c = signal::ctrl_c().fuse();
		let quit_signal = quit.recv().fuse();
		let term_signal = term.recv().fuse();
		let usr1_signal = usr1.recv().fuse();
		let usr2_signal = usr2.recv().fuse();
		pin_mut!(ctrl_c, quit_signal, term_signal, usr1_signal, usr2_signal);
		futures::select! {
			_ = ctrl_c => { sig = "SIGINT"; },
			_ = quit_signal => { sig = "SIGQUIT"; },
			_ = term_signal => { sig = "SIGTERM"; },
			_ = usr1_signal => { sig = "SIGUSR1"; },
			_ = usr2_signal => { sig = "SIGUSR2"; },
		}

		warn!("Received {sig}");
		let result = if RELOADING && sig == "SIGINT" {
			server.server.reload()
		} else if matches!(sig, "SIGQUIT" | "SIGTERM") || (!CONSOLE && sig == "SIGINT") {
			server.server.shutdown()
		} else {
			server.server.signal(sig)
		};

		if let Err(e) = result {
			debug_error!(%sig, "signal: {e}");
		}
	}
}

#[cfg(not(unix))]
pub(super) async fn signal(server: Arc<Server>) {
	loop {
		signal::ctrl_c().await.expect("Ctrl+C handler");
		warn!("Received Ctrl+C");
		if let Err(e) = server.server.signal.send("SIGINT") {
			debug_error!("signal channel: {e}");
		}
	}
}
