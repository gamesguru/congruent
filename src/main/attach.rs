use conduwuit_core::{Config, Result, console_history::ConsoleHistory, error::Error};
use rustyline_async::{Readline, ReadlineEvent};
use tokio::{
	io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
	net::UnixStream,
};

use crate::clap::{Args, update};

pub(crate) fn run(args: &Args) -> Result<()> {
	let mut config_paths = args.config.clone().unwrap_or_default();
	if config_paths.is_empty() {
		let env_set = std::env::var("CONDUIT_CONFIG").is_ok()
			|| std::env::var("CONDUWUIT_CONFIG").is_ok()
			|| std::env::var("CONTINUWUITY_CONFIG").is_ok();

		if std::path::Path::new("conduwuit.toml").exists() {
			config_paths.push("conduwuit.toml".into());
		} else if !env_set {
			return Err(Error::Err(
				"No config file found. Please specify a config path using the --config flag, \
				 set CONDUWUIT_CONFIG, or run this command in a directory with a conduwuit.toml \
				 file."
					.into(),
			));
		}
	}

	let config = Config::load(&config_paths)
		.and_then(|raw| update(raw, args))
		.and_then(|raw| Config::new(&raw))?;

	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|e| {
			eprintln!("Failed to initialize tokio runtime: {e}");
			Error::Err(format!("Failed to initialize tokio runtime: {e}").into())
		})?;

	runtime.block_on(async_run(&config, &args.execute))
}

async fn async_run(config: &Config, execute: &[String]) -> Result<()> {
	let socket_path = config.database_path.join("console.sock");

	let stream = match UnixStream::connect(&socket_path).await {
		| Ok(s) => s,
		| Err(e) => {
			eprintln!("Failed to connect to console socket at {}: {e}", socket_path.display());
			eprintln!("Is the conduwuit server currently running?");
			return Err(Error::bad_database("Failed to connect to server"));
		},
	};

	// We don't have a conduwuit instance here, so we can't use
	// `conduwuit_core::Log`, we don't have any logs anyway!

	// Headless mode: skip readline and send the supplied commands directly
	// over the console socket.
	if !execute.is_empty() {
		return run_execute_mode(stream, execute).await;
	}

	println!("Connected to conduwuit admin console at {}", socket_path.display());
	println!("Type \"help\" for help, ^D or `Quit` to exit.");

	run_interactive_mode(socket_path, stream).await
}

async fn run_execute_mode(mut stream: UnixStream, execute: &[String]) -> Result<()> {
	let mut stream_reader = BufReader::new(&mut stream);
	let mut response_buf = Vec::new();

	for command in execute {
		let trimmed = command.trim();
		if trimmed.is_empty() {
			continue;
		}

		if trimmed.eq_ignore_ascii_case("quit") {
			break;
		}

		if let Err(_e) = stream_reader.get_mut().write_all(command.as_bytes()).await {
			println!("Failed to write to socket");
			break;
		}
		if let Err(_e) = stream_reader.get_mut().write_all(b"\n").await {
			println!("Failed to write to socket");
			break;
		}

		response_buf.clear();
		match stream_reader.read_until(b'\0', &mut response_buf).await {
			| Ok(0) => {
				println!("Server disconnected.");
				break;
			},
			| Ok(_) => {
				if response_buf.ends_with(b"\0") {
					response_buf.pop();
				}
				let response_str = String::from_utf8_lossy(&response_buf);
				if !response_str.is_empty() {
					let formatted = conduwuit_service::admin::console::format(&response_str);
					print!("{formatted}");
				}
			},
			| Err(_e) => {
				println!("Failed to read from socket");
				break;
			},
		}
	}

	Ok(())
}

async fn run_interactive_mode(
	socket_path: std::path::PathBuf,
	mut stream: UnixStream,
) -> Result<()> {
	let mut stream_reader = BufReader::new(&mut stream);
	let mut response_buf = Vec::new();
	let mut history = ConsoleHistory::new();

	loop {
		let (mut readline, writer) = Readline::new("uwu> ".to_owned()).map_err(|e| {
			eprintln!("Failed to initialize readline: {e:?}");
			Error::bad_database("Failed to initialize readline")
		})?;

		readline.set_tab_completer(conduwuit_admin::complete);
		for line in history.iter() {
			_ = readline.add_history_entry(line.clone());
		}

		let event = readline.readline().await;

		// Drop readline immediately to restore terminal control
		// This ensures standard SIGINT handling works and the prompt is hidden.
		_ = readline.flush();
		drop(readline);
		drop(writer);

		match event {
			| Ok(ReadlineEvent::Line(line)) => {
				let trimmed = line.trim();
				if trimmed.is_empty() {
					continue;
				}

				// Local client-side exit just drops the socket
				if trimmed.eq_ignore_ascii_case("quit") {
					break;
				}

				history.add(&line);

				// Send line to server
				if let Err(_e) = stream_reader.get_mut().write_all(line.as_bytes()).await {
					println!("Failed to write to socket");
					break;
				}
				if let Err(_e) = stream_reader.get_mut().write_all(b"\n").await {
					println!("Failed to write to socket");
					break;
				}

				// Await response from server OR Ctrl+C
				// Since readline is dropped, tokio::signal::ctrl_c() will catch SIGINT
				// correctly.
				response_buf.clear();
				tokio::select! {
					res = stream_reader.read_until(b'\0', &mut response_buf) => {
						match res {
							| Ok(0) => {
								println!("Server disconnected.");
								break;
							},
							| Ok(_) => {
								if response_buf.ends_with(b"\0") {
									response_buf.pop();
								}
								let response_str = String::from_utf8_lossy(&response_buf);
								if !response_str.is_empty() {
									let formatted = conduwuit_service::admin::console::format(&response_str);
									print!("{formatted}");
								}
							},
							| Err(_e) => {
								println!("Failed to read from socket");
								break;
							}
						}
					},
					_ = tokio::signal::ctrl_c() => {
						println!("Interrupted.");
						// Drop stream and reconnect to cancel server job
						let new_stream = match UnixStream::connect(&socket_path).await {
							| Ok(s) => s,
							| Err(_e) => {
								eprintln!("Failed to reconnect to console socket");
								break;
							}
						};
						// Ensure the existing BufReader<&mut UnixStream> is dropped
						// before moving a new UnixStream into `stream`.
						drop(stream_reader);
						stream = new_stream;
						stream_reader = BufReader::new(&mut stream);
					}
				}
			},
			| Ok(ReadlineEvent::Interrupted) => continue,
			| Ok(ReadlineEvent::Eof | ReadlineEvent::Quit) => break,
			| Err(e) => {
				println!("Console read error: {e}");
				break;
			},
		}

		// Small yield to let terminal state settle
		tokio::task::yield_now().await;
	}

	Ok(())
}
