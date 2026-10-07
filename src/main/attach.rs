use conduwuit_core::{Config, Result, error::Error};
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

	run_interactive_mode(stream).await
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

async fn run_interactive_mode(mut stream: UnixStream) -> Result<()> {
	let mut stream_reader = BufReader::new(&mut stream);
	let mut input_reader = BufReader::new(tokio::io::stdin());
	let mut response_buf = Vec::new();
	loop {
		print!("uwu> ");
		let mut input = String::new();
		if input_reader.read_line(&mut input).await? == 0 {
			break;
		}
		let trimmed = input.trim();
		if trimmed.is_empty() {
			continue;
		}
		if trimmed.eq_ignore_ascii_case("quit") {
			break;
		}
		stream_reader
			.get_mut()
			.write_all(trimmed.as_bytes())
			.await?;
		stream_reader.get_mut().write_all(b"\n").await?;
		response_buf.clear();
		match stream_reader.read_until(b'\0', &mut response_buf).await? {
			| 0 => break,
			| _ => {
				if response_buf.ends_with(b"\0") {
					response_buf.pop();
				}
				let response_str = String::from_utf8_lossy(&response_buf);
				if !response_str.is_empty() {
					print!("{}", conduwuit_service::admin::console::format(&response_str));
				}
			},
		}
	}

	Ok(())
}
