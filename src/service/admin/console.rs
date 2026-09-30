#![cfg(feature = "console")]

use std::sync::Arc;

use conduwuit::{
	Server, SyncMutex, console_history::ConsoleHistory, debug, defer, error, log,
	log::is_systemd_mode,
};
use futures::future::{AbortHandle, Abortable};
use ruma::events::room::message::RoomMessageEventContent;
use rustyline_async::{Readline, ReadlineError, ReadlineEvent};
use termimad::MadSkin;
use tokio::task::JoinHandle;

use crate::{
	Dep,
	admin::{self, InvocationSource},
};

pub struct Console {
	server: Arc<Server>,
	admin: Dep<admin::Service>,
	worker_join: SyncMutex<Option<JoinHandle<()>>>,
	input_abort: SyncMutex<Option<AbortHandle>>,
	command_abort: SyncMutex<Option<AbortHandle>>,
	history: SyncMutex<ConsoleHistory>,
	output: MadSkin,
}

const PROMPT: &str = "uwu> ";

impl Console {
	pub(super) fn new(args: &crate::Args<'_>) -> Arc<Self> {
		Arc::new(Self {
			server: args.server.clone(),
			admin: args.depend::<admin::Service>("admin"),
			worker_join: None.into(),
			input_abort: None.into(),
			command_abort: None.into(),
			history: ConsoleHistory::new().into(),
			output: configure_output(MadSkin::default_dark()),
		})
	}

	pub(super) fn handle_signal(self: &Arc<Self>, sig: &'static str) {
		use std::io::IsTerminal;

		if !self.server.running() {
			self.interrupt();
		} else if sig == "SIGINT" {
			let running = self.worker_join.lock().is_some();
			if running {
				self.interrupt_command();
			} else if std::io::stdout().is_terminal() {
				self.start();
			} else {
				self.server.shutdown().unwrap_or_else(error::default_log);
			}
		}
	}

	pub fn start(self: &Arc<Self>) {
		let mut worker_join = self.worker_join.lock();
		if worker_join.is_none() {
			let self_ = Arc::clone(self);
			_ = worker_join.insert(self.server.runtime().spawn(self_.worker()));
		}
	}

	pub async fn start_listener(self: &Arc<Self>) {
		let self_ = Arc::clone(self);
		self.server.runtime().spawn(self_.socket_worker());
	}

	pub async fn close(self: &Arc<Self>) {
		self.interrupt();

		let Some(worker_join) = self.worker_join.lock().take() else {
			return;
		};

		_ = worker_join.await;
	}

	pub fn interrupt(self: &Arc<Self>) {
		self.interrupt_command();
		self.interrupt_readline();
		self.worker_join.lock().as_ref().map(JoinHandle::abort);
	}

	pub fn interrupt_readline(self: &Arc<Self>) {
		if let Some(input_abort) = self.input_abort.lock().take() {
			debug!("Interrupting console readline...");
			input_abort.abort();
		}
	}

	pub fn interrupt_command(self: &Arc<Self>) {
		if let Some(command_abort) = self.command_abort.lock().take() {
			debug!("Interrupting console command...");
			command_abort.abort();
		}
	}

	#[tracing::instrument(skip_all, name = "console", level = "trace")]
	async fn worker(self: Arc<Self>) {
		debug!("session starting");

		self.output
			.write_inline_on(
				&mut std::io::stdout(),
				&format!("**conduwuit {}** admin console\n", conduwuit::version()),
			)
			.ok();
		self.output
			.write_text_on(
				&mut std::io::stdout(),
				concat!(
					"\"help\" for help, ^D to exit the console, ^\\ to stop the server\n",
					"^W to clear word, ctrl-left/right to skip words\n"
				),
			)
			.ok();

		while self.server.running() {
			match self.readline().await {
				| Ok(event) => match event {
					| ReadlineEvent::Line(string) => self.clone().handle(string).await,
					| ReadlineEvent::Interrupted => continue,
					| ReadlineEvent::Eof => break,
					| ReadlineEvent::Quit =>
						self.server.shutdown().unwrap_or_else(error::default_log),
				},
				| Err(error) => match error {
					| ReadlineError::Closed => break,
					| ReadlineError::IO(error) => {
						error!("console I/O: {error:?}");
						break;
					},
				},
			}
		}

		debug!("session ending");
		self.worker_join.lock().take();
	}

	#[tracing::instrument(skip_all, name = "console_socket", level = "trace")]
	async fn socket_worker(self: Arc<Self>) {
		let socket_path = self.server.config.database_path.join("console.sock");
		_ = tokio::fs::remove_file(&socket_path).await;

		let listener = match tokio::net::UnixListener::bind(&socket_path) {
			| Ok(l) => l,
			| Err(e) => {
				error!("Failed to bind console socket at {socket_path:?}: {e}");
				return;
			},
		};

		use std::os::unix::fs::PermissionsExt;
		if let Ok(meta) = tokio::fs::metadata(&socket_path).await {
			let mut perms = meta.permissions();
			// e.g. self.server.config.unix_socket_perms
			perms.set_mode(self.server.config.unix_socket_perms);
			_ = tokio::fs::set_permissions(&socket_path, perms).await;
		}

		while self.server.running() {
			match listener.accept().await {
				| Ok((mut stream, _)) => {
					let self_ = Arc::clone(&self);
					self.server.runtime().spawn(async move {
						self_.handle_connection(&mut stream).await;
					});
				},
				| Err(e) => {
					error!("Console socket accept error: {e}");
					break;
				},
			}
		}
	}

	async fn handle_connection(self: Arc<Self>, stream: &mut tokio::net::UnixStream) {
		use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
		let (reader, mut writer) = stream.split();
		let mut reader = BufReader::new(reader);
		let mut line = String::new();

		while self.server.running() {
			line.clear();
			match reader.read_line(&mut line).await {
				| Ok(0) | Err(_) => break, // EOF or Error
				| Ok(_) => {
					let input = line.trim().to_owned();
					if input.is_empty() {
						continue;
					}

					let admin_future =
						self.admin
							.command_in_place(input, None, InvocationSource::Console);

					tokio::pin!(admin_future);

					let result = tokio::select! {
						res = &mut admin_future => res,
						res = reader.fill_buf() => {
							match res {
								| Ok([]) | Err(_) => return, // EOF or Error
								| Ok(_) => admin_future.await,
							}
						}
					};

					let output_body = match &result {
						| Ok(Some(content)) => content.body(),
						| Err(content) => content.body(),
						| Ok(None) => "",
					};

					if writer.write_all(output_body.as_bytes()).await.is_err() {
						break;
					}
					if writer.write_all(b"\0").await.is_err() {
						break;
					}
				},
			}
		}
	}

	async fn readline(self: &Arc<Self>) -> Result<ReadlineEvent, ReadlineError> {
		let _suppression = (!is_systemd_mode()).then(|| log::Suppress::new(&self.server));

		let (mut readline, _writer) = Readline::new(PROMPT.to_owned())?;
		let self_ = Arc::clone(self);
		readline.set_tab_completer(move |line| self_.tab_complete(line));
		self.set_history(&mut readline);

		std::io::Write::flush(&mut std::io::stdout()).ok();

		let future = readline.readline();

		let (abort, abort_reg) = AbortHandle::new_pair();
		let future = Abortable::new(future, abort_reg);
		_ = self.input_abort.lock().insert(abort);
		defer! {{
			_ = self.input_abort.lock().take();
		}}

		let Ok(result) = future.await else {
			return Ok(ReadlineEvent::Eof);
		};

		readline.flush()?;
		result
	}

	async fn handle(self: Arc<Self>, line: String) {
		if line.trim().is_empty() {
			return;
		}

		self.add_history(&line);
		let future = self.clone().process(line);

		let (abort, abort_reg) = AbortHandle::new_pair();
		let future = Abortable::new(future, abort_reg);
		_ = self.command_abort.lock().insert(abort);
		defer! {{
			_ = self.command_abort.lock().take();
		}}

		_ = future.await;
	}

	async fn process(self: Arc<Self>, line: String) {
		match self
			.admin
			.command_in_place(line, None, InvocationSource::Console)
			.await
		{
			| Ok(Some(ref content)) => self.output(content),
			| Err(ref content) => self.output_err(content),
			| _ => unreachable!(),
		}
	}

	fn output_err(self: Arc<Self>, output_content: &RoomMessageEventContent) {
		let output = configure_output_err(self.output.clone());
		output
			.write_text_on(&mut std::io::stdout(), output_content.body())
			.ok();
	}

	fn output(self: Arc<Self>, output_content: &RoomMessageEventContent) {
		self.output
			.write_text_on(&mut std::io::stdout(), output_content.body())
			.ok();
	}

	fn set_history(&self, readline: &mut Readline) {
		self.history.lock().iter().for_each(|entry| {
			readline
				.add_history_entry(entry.clone())
				.expect("added history entry");
		});
	}

	fn add_history(&self, line: &str) { self.history.lock().add(line); }

	fn tab_complete(&self, line: &str) -> String {
		self.admin
			.complete_command(line)
			.unwrap_or_else(|| line.to_owned())
	}
}

/// Standalone/static markdown printer for errors.
pub fn print_err(markdown: &str) {
	let output = configure_output_err(MadSkin::default_dark());
	output.write_text_on(&mut std::io::stdout(), markdown).ok();
}
/// Standalone/static markdown printer.
pub fn print(markdown: &str) {
	let output = configure_output(MadSkin::default_dark());
	output.write_text_on(&mut std::io::stdout(), markdown).ok();
}

/// Standalone markdown string formatter for attach.
#[must_use]
pub fn format(markdown: &str) -> String {
	let output = configure_output(MadSkin::default_dark());
	format!("{}", output.text(markdown, None))
}

fn configure_output_err(mut output: MadSkin) -> MadSkin {
	use termimad::{Alignment, CompoundStyle, LineStyle, crossterm::style::Color};

	let code_style = CompoundStyle::with_fgbg(Color::AnsiValue(196), Color::AnsiValue(234));
	output.inline_code = code_style.clone();
	output.code_block = LineStyle {
		left_margin: 0,
		right_margin: 0,
		align: Alignment::Left,
		compound_style: code_style,
	};

	output
}

fn configure_output(mut output: MadSkin) -> MadSkin {
	use termimad::{Alignment, CompoundStyle, LineStyle, crossterm::style::Color};

	let code_style = CompoundStyle::with_fgbg(Color::AnsiValue(40), Color::AnsiValue(234));
	output.inline_code = code_style.clone();
	output.code_block = LineStyle {
		left_margin: 0,
		right_margin: 0,
		align: Alignment::Left,
		compound_style: code_style,
	};

	let table_style = CompoundStyle::default();
	output.table = LineStyle {
		left_margin: 1,
		right_margin: 1,
		align: Alignment::Left,
		compound_style: table_style,
	};

	output
}
