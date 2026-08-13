use std::{fmt::Write, panic::AssertUnwindSafe, sync::Arc, time::SystemTime};

use clap::{CommandFactory, Parser};
use conduwuit::{
	Error, Result, SyncMutex, debug, error,
	log::{
		capture,
		capture::Capture,
		fmt::{markdown_table, markdown_table_head},
	},
	trace,
	utils::string::{collect_stream, common_prefix},
	warn,
};
use futures::{AsyncWriteExt, future::FutureExt, io::BufWriter};
use regex::Regex;
use ruma::{
	EventId,
	events::{
		relation::InReplyTo,
		room::message::{Relation::Reply, RoomMessageEventContent},
	},
};
use service::{
	Services,
	admin::{CommandInput, CommandOutput, ProcessorFuture, ProcessorResult},
};
use tracing::Level;
use tracing_subscriber::{EnvFilter, filter::LevelFilter};

use crate::{admin, admin::AdminCommand, context::Context};

type ParsedCommand<'a> = (AdminCommand, Vec<String>, Vec<&'a str>);

#[must_use]
pub fn complete(line: &str) -> String { complete_command(AdminCommand::command(), line) }

pub(super) fn dispatch(services: Arc<Services>, command: CommandInput) -> ProcessorFuture {
	Box::pin(async move { handle_command(services, command).await })
}

#[tracing::instrument(skip_all, name = "admin", level = "info")]
async fn handle_command(services: Arc<Services>, command: CommandInput) -> ProcessorResult {
	let reply_id = command.reply_id.clone();
	AssertUnwindSafe(Box::pin(process_command(services, command)))
		.catch_unwind()
		.await
		.map_err(Error::from_panic)
		.unwrap_or_else(|error| handle_panic(&error, reply_id.as_deref()))
}

async fn process_command(services: Arc<Services>, input: CommandInput) -> ProcessorResult {
	let (command, args, body) = match parse(&services, &input) {
		| Err(error) => return Err(error),
		| Ok(parsed) => parsed,
	};

	let context = Context {
		services: &services,
		body: &body,
		timer: SystemTime::now(),
		_reply_id: input.reply_id.as_deref(),
		sender: input.sender.as_deref(),
		output: BufWriter::new(Vec::new()).into(),
		source: input.source,
	};

	let (result, mut logs) = process(&context, command, &args).await;

	let mut output = context.output.into_inner();
	output.flush().await.expect("final flush of output stream");

	let output =
		String::from_utf8(output.into_inner()).expect("invalid utf8 in command output stream");

	// Wrap command output in code blocks if it's not already markdown
	let output = if !output.is_empty() && !looks_like_markdown(&output) {
		format!("```\n{output}\n```")
	} else {
		output
	};

	match result {
		| Ok(()) if logs.is_empty() => Ok(Some(reply(
			RoomMessageEventContent::notice_markdown(output),
			input.reply_id.as_deref(),
		))),

		| Ok(()) => {
			logs.write_str(output.as_str()).expect("output buffer");
			Ok(Some(reply(
				RoomMessageEventContent::notice_markdown(logs),
				input.reply_id.as_deref(),
			)))
		},
		| Err(error) => {
			write!(&mut logs, "Command failed with error:\n```\n{error:#?}\n```")
				.expect("output buffer");

			Err(Box::new(reply(
				RoomMessageEventContent::notice_markdown(logs),
				input.reply_id.as_deref(),
			)))
		},
	}
}

fn handle_panic(error: &Error, reply_id: Option<&EventId>) -> ProcessorResult {
	let link =
		"Please submit a [bug report](https://github.com/gamesguru/continuwuity/issues/new). 🥺";
	let msg = format!("Panic occurred while processing command:\n```\n{error:#?}\n```\n{link}");
	let content = RoomMessageEventContent::notice_markdown(msg);
	error!("Panic while processing command: {error:?}");
	Err(Box::new(reply(content, reply_id)))
}

/// Parse and process a message from the admin room
async fn process(
	context: &Context<'_>,
	command: AdminCommand,
	args: &[String],
) -> (Result, String) {
	let (capture, logs) = capture_create(context);

	let capture_scope = capture.start();
	let result = Box::pin(admin::process(command, context)).await;
	drop(capture_scope);

	debug!(
		ok = result.is_ok(),
		elapsed = ?context.timer.elapsed(),
		command = ?args,
		"command processed"
	);

	let mut output = String::new();

	// Prepend the logs only if any were captured
	let logs = logs.lock();
	if logs.lines().count() > 2 {
		writeln!(&mut output, "```\n{logs}\n```")
			.expect("failed to format logs to command output");
	}
	drop(logs);

	(result, output)
}

fn capture_create(context: &Context<'_>) -> (Arc<Capture>, Arc<SyncMutex<String>>) {
	let env_config = &context.services.server.config.admin_log_capture;
	let env_filter = EnvFilter::try_new(env_config).unwrap_or_else(|e| {
		warn!("admin_log_capture filter invalid: {e:?}");
		cfg!(debug_assertions)
			.then_some("debug")
			.or(Some("info"))
			.map(Into::into)
			.expect("default capture EnvFilter")
	});

	let log_level = env_filter
		.max_level_hint()
		.and_then(LevelFilter::into_level)
		.unwrap_or(Level::DEBUG);

	let filter = move |data: capture::Data<'_>| {
		data.level() <= log_level && data.our_modules() && data.scope.contains(&"admin")
	};

	let logs = Arc::new(SyncMutex::new(
		collect_stream(|s| markdown_table_head(s)).expect("markdown table header"),
	));

	let capture = Capture::new(
		&context.services.server.log.capture,
		Some(filter),
		capture::fmt(markdown_table, logs.clone()),
	);

	(capture, logs)
}

/// Parse chat messages from the admin room into an AdminCommand object
fn parse<'a>(
	services: &Arc<Services>,
	input: &'a CommandInput,
) -> Result<ParsedCommand<'a>, Box<CommandOutput>> {
	let lines = input.command.lines().filter(|line| !line.trim().is_empty());
	let command_line = lines.clone().next().expect("command missing first line");
	let body = lines.skip(1).collect();
	match parse_command(command_line) {
		| Ok((command, args)) => Ok((command, args, body)),
		| Err(error) => {
			let message = error
				.to_string()
				.replace("server.name", services.globals.server_name().as_str());
			Err(Box::new(reply(
				RoomMessageEventContent::notice_plain(message),
				input.reply_id.as_deref(),
			)))
		},
	}
}

fn parse_command(line: &str) -> Result<(AdminCommand, Vec<String>)> {
	let argv = parse_line(line);
	let command = AdminCommand::try_parse_from(&argv)?;
	Ok((command, argv))
}

fn complete_command(mut cmd: clap::Command, line: &str) -> String {
	let argv = parse_line(line);
	let mut ret = Vec::<String>::with_capacity(argv.len().saturating_add(1));

	'token: for token in argv.into_iter().skip(1) {
		let cmd_ = cmd.clone();
		let mut choice = Vec::new();

		for sub in cmd_.get_subcommands() {
			let name = sub.get_name();
			if *name == token {
				// token already complete; recurse to subcommand
				ret.push(token);
				cmd.clone_from(sub);
				continue 'token;
			} else if name.starts_with(&token) {
				// partial match; add to choices
				choice.push(name.to_owned());
			}
		}

		// Tab completion for dashed flags
		for arg in cmd_.get_arguments() {
			if let Some(long) = arg.get_long() {
				let name = format!("--{long}");
				if name == token {
					ret.push(token);
					continue 'token;
				} else if name.starts_with(&token) {
					choice.push(name);
				}
			}
			if let Some(short) = arg.get_short() {
				let name = format!("-{short}");
				if name == token {
					ret.push(token);
					continue 'token;
				} else if name.starts_with(&token) {
					choice.push(name);
				}
			}
		}

		if choice.len() == 1 {
			// One choice. Add extra space because it's complete
			let choice = choice.first().unwrap();
			ret.push(choice.to_owned());
			ret.push(String::new());
		} else if choice.is_empty() {
			// Nothing found, return original string
			ret.push(token);
		} else {
			// Find the common prefix
			let choice_refs: Vec<&str> = choice.iter().map(String::as_str).collect();
			ret.push(common_prefix(&choice_refs).into());
		}

		// Return from completion
		return ret.join(" ");
	}

	// Return from no completion. Needs a space though.
	ret.push(String::new());
	ret.join(" ")
}

/// Parse chat messages from the admin room into an AdminCommand object
fn parse_line(command_line: &str) -> Vec<String> {
	let mut argv = command_line
		.split_whitespace()
		.map(str::to_owned)
		.collect::<Vec<String>>();

	// Remove any escapes that came with a server-side escape command
	if !argv.is_empty() && argv[0].ends_with("admin") {
		argv[0] = argv[0].trim_start_matches('\\').into();
	}

	// First indice has to be "admin" but for console convenience we add it here
	if !argv.is_empty() && !argv[0].ends_with("admin") && !argv[0].starts_with('@') {
		argv.insert(0, "admin".to_owned());
	}

	// Replace `help command` with `command --help`
	// Clap has a help subcommand, but it omits the long help description.
	if argv.len() > 1 && argv[1] == "help" {
		argv.remove(1);
		argv.push("--help".to_owned());
	}

	// Backwards compatibility with `register_appservice`-style commands
	if argv.len() > 1 && argv[1].contains('_') {
		argv[1] = argv[1].replace('_', "-");
	}

	// Backwards compatibility with `register_appservice`-style commands
	if argv.len() > 2 && argv[2].contains('_') {
		argv[2] = argv[2].replace('_', "-");
	}

	// if the user is using the `query` command (argv[1]), replace the database
	// function/table calls with underscores to match the codebase
	if argv.len() > 3 && argv[1].eq("query") {
		argv[3] = argv[3].replace('_', "-");
	}

	trace!(?command_line, ?argv, "parse");
	argv
}

fn reply(
	mut content: RoomMessageEventContent,
	reply_id: Option<&EventId>,
) -> RoomMessageEventContent {
	content.relates_to = reply_id.map(|event_id| Reply {
		in_reply_to: InReplyTo { event_id: event_id.to_owned() },
	});

	content
}

/// Heuristic: output that already contains markdown formatting should not be
/// wrapped in code blocks.
fn looks_like_markdown(s: &str) -> bool {
	static BOLD_RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
	static LINK_RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();

	let trimmed = s.trim_start();
	trimmed.starts_with('#')
		|| trimmed.starts_with('>')
		|| trimmed.starts_with("- ")
		|| trimmed.starts_with("* ")
		|| s.contains("```")
		|| BOLD_RE
			.get_or_init(|| {
				Regex::new(r"(^|[^\w*])\*\*[^\s*][^*]*[^\s*]\*\*([^\w*]|$)")
					.expect("valid bold regex")
			})
			.is_match(s)
		|| LINK_RE
			.get_or_init(|| {
				Regex::new(r"\[[^\]\n]+\]\((?:https?://|/|#)[^) \n]+\)")
					.expect("valid markdown link regex")
			})
			.is_match(s)
		|| s.lines().any(|line| line.trim_start().starts_with('|'))
}
