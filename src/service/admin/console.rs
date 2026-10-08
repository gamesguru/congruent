#![cfg(feature = "console")]

use std::{
	io::BufRead,
	os::unix::{fs::PermissionsExt, net::{UnixListener, UnixStream}},
	sync::Arc,
	thread::JoinHandle,
};

use async_fs as fs;
use async_io::Async;
use conduwuit::{RuntimeHandle, Server, SyncMutex, debug, error};
use futures::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use crate::{
	Dep,
	admin::{self, InvocationSource},
};

pub struct Console {
	server: Arc<Server>,
	admin: Dep<admin::Service>,
	worker_join: SyncMutex<Option<JoinHandle<()>>>,
}

impl Console {
	pub(super) fn new(args: &crate::Args<'_>) -> Arc<Self> {
		Arc::new(Self {
			server: args.server.clone(),
			admin: args.depend::<admin::Service>("admin"),
			worker_join: None.into(),
		})
	}

	pub(super) fn handle_signal(self: &Arc<Self>, sig: &'static str) {
		if sig == "SIGINT" {
			self.server.shutdown().unwrap_or_else(error::default_log);
		}
	}

	pub fn start(self: &Arc<Self>) {
		let mut worker_join = self.worker_join.lock();
		if worker_join.is_none() {
			let self_ = Arc::clone(self);
			let runtime = self.server.runtime().clone();
			_ = worker_join.insert(std::thread::spawn(move || self_.worker(&runtime)));
		}
	}

	pub async fn start_listener(self: &Arc<Self>) {
		let self_ = Arc::clone(self);
		drop(self.server.runtime().spawn(self_.socket_worker()));
	}

	pub async fn close(self: &Arc<Self>) {
		self.interrupt();
		// A blocking stdin read cannot be interrupted portably. Dropping the
		// handle detaches the thread; it exits when stdin reaches EOF.
		self.worker_join.lock().take();
	}

	pub fn interrupt(self: &Arc<Self>) { self.worker_join.lock().take(); }

	fn worker(self: Arc<Self>, _runtime: &RuntimeHandle) {
		debug!("admin console session starting");
		println!("conduwuit admin console; type commands and press Enter");

		for line in std::io::stdin().lock().lines() {
			if !self.server.running() {
				break;
			}
			match line {
				| Ok(line) => smol::block_on(self.handle(line)),
				| Err(e) => {
					error!("console I/O: {e}");
					break;
				},
			}
		}

		debug!("admin console session ending");
		self.worker_join.lock().take();
	}

	async fn socket_worker(self: Arc<Self>) {
		let socket_path = self.server.config.database_path.join("console.sock");
		_ = fs::remove_file(&socket_path).await;

		let listener = match UnixListener::bind(&socket_path)
			.and_then(|listener| {
				listener.set_nonblocking(true)?;
				Async::new(listener)
			})
		{
			| Ok(listener) => listener,
			| Err(e) => {
				error!("Failed to bind console socket at {socket_path:?}: {e}");
				return;
			},
		};

		if let Ok(meta) = fs::metadata(&socket_path).await {
			let mut perms = meta.permissions();
			perms.set_mode(self.server.config.unix_socket_perms);
			_ = fs::set_permissions(&socket_path, perms).await;
		}

		while self.server.running() {
			match listener.accept().await {
				| Ok((stream, _)) => {
					let self_ = Arc::clone(&self);
					drop(self.server.runtime().spawn(async move {
						self_.handle_connection(stream).await;
					}));
				},
				| Err(e) => {
					error!("Console socket accept error: {e}");
					break;
				},
			}
		}
	}

	async fn handle_connection(self: Arc<Self>, stream: Async<UnixStream>) {
		let (reader, mut writer) = stream.split();
		let mut reader = BufReader::new(reader);
		let mut line = String::new();

		while self.server.running() {
			line.clear();
			match reader.read_line(&mut line).await {
				| Ok(0) | Err(_) => break,
				| Ok(_) => {
					let input = line.trim();
					if input.is_empty() {
						continue;
					}

					let result = self
						.admin
						.command_in_place(input.to_owned(), None, InvocationSource::Console)
						.await;
					let output = match result {
						| Ok(Some(content)) => content.body().to_owned(),
						| Err(content) => content.body().to_owned(),
						| Ok(None) => String::new(),
					};

					if writer.write_all(output.as_bytes()).await.is_err()
						|| writer.write_all(b"\0").await.is_err()
					{
						break;
					}
				},
			}
		}
	}

	async fn handle(&self, line: String) {
		let input = line.trim();
		if input.is_empty() {
			return;
		}
		if input.eq_ignore_ascii_case("quit") {
			self.server.shutdown().unwrap_or_else(error::default_log);
			return;
		}

		match self
			.admin
			.command_in_place(input.to_owned(), None, InvocationSource::Console)
			.await
		{
			| Ok(Some(content)) => print(content.body()),
			| Err(content) => print(content.body()),
			| Ok(None) => {},
		}
	}
}

pub fn print_err(markdown: &str) {
	println!("{markdown}");
}

pub fn print(markdown: &str) {
	println!("{markdown}");
}

#[must_use]
pub fn format(markdown: &str) -> String {
	let mut output = markdown.to_owned();
	if !output.ends_with('\n') {
		output.push('\n');
	}
	output
}
