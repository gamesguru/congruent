//! Generates documentation for the various commands that may be used in the admin room and server console.
//!
//! This generates one index page and several category pages, one for each of the direct subcommands of the top-level
//! `!admin` command. Those category pages then list all of the sub-subcommands.

use std::{fmt::Write, path::Path};

use clap::{Command, CommandFactory};
use conduwuit_admin::AdminCommand;

use crate::tasks::{TaskResult, generate_docs::FileOutput};

/// The index page, which links to all of the category pages.
struct Index {
    categories: Vec<Category>
}

/// A direct subcommand of the top-level `!admin` command.
struct Category {
    name: String,
    description: String,
    commands: Vec<Subcommand>,
}

/// A second-or-deeper level subcommand of the `!admin` command.
struct Subcommand {
    name: String,
    description: String,
    /// How deeply nested this command was in the original command tree.
    /// This determines the header size used for it in the documentation.
    depth: usize,
}

impl Index {
    fn render(&self) -> String {
        let mut output = String::from(
            "# Admin Command Reference\n\n\
             Admin commands allow server administrators to manage the server from within their Matrix client. \"Server administrators\" by default means only those users which are members of the admin room, but additional server admins may be added using the `admins_list` configuration option.\n\n\
             ## Running commands\n\n\
             * All commands listed here may be used by server administrators in the admin room by sending them as messages.\n\
             * If the `admin_escape_commands` configuration option is enabled, server administrators may run certain commands in public rooms by prefixing them with a single backslash. These commands will only run on _their_ homeserver, even if they are a member of another homeserver's admin room. Some sensitive commands cannot be used outside the admin room and will return an error.\n\
             * All commands listed here may be used in the server's console, if it is enabled. Commands entered in the console do not require the `!admin` prefix.\n\n\
             ## Categories\n\n",
        );

        for category in &self.categories {
            let _ = writeln!(
                output,
                "- [`!admin {}`]({}/): {}",
                category.name, category.name, category.description
            );
        }

        output
    }
}

impl Category {
    fn render(&self) -> String {
        let mut output = format!("# `!admin {}`\n\n{}\n\n", self.name, self.description);

        for command in &self.commands {
            let header = "#".repeat((command.depth + 1).min(3));
            let _ = writeln!(
                output,
                "{header} `!admin {}`\n\n{}",
                command.name, command.description
            );
        }

        output
    }
}


fn flatten_subcommands(command: &Command) -> Vec<Subcommand> {

    fn flatten(
        subcommands: &mut Vec<Subcommand>,
        name_stack: &mut Vec<String>,
        command: &Command
    ) {
        let depth = name_stack.len();
        name_stack.push(command.get_name().to_owned());

        // do not include the root command
        if depth > 0 {
            let name = name_stack.join(" ");

            let description = command
                .get_long_about()
                .or_else(|| command.get_about())
                .map(|d| format!("{d}"));

            if let Some(description) = description {
                subcommands.push(
                    Subcommand {
                        name,
                        description,
                        depth,
                    }
                );
            }
        }

        for command in command.get_subcommands() {
            flatten(subcommands, name_stack, command);
        }

        name_stack.pop();
    }

    let mut subcommands = Vec::new();
    let mut name_stack = Vec::new();

    flatten(&mut subcommands, &mut name_stack, command);

    subcommands
}

pub(super) fn generate(out: &mut impl FileOutput) -> TaskResult<()> {
    let admin_commands = AdminCommand::command();

    let categories: Vec<_> = admin_commands
        .get_subcommands()
        .map(|command| {
            Category {
                name: command.get_name().to_owned(),
                description: command.get_about().expect("categories should have a docstring").to_string(),
                commands: flatten_subcommands(command),
            }
        })
        .collect();

    let root = Path::new("reference/admin/");

    for category in &categories {
        out.create_file(
            root.join(&category.name).with_extension("md"),
            category.render()
        );
    }

    out.create_file(
        root.join("index.md"),
        Index { categories }.render(),
    );

    Ok(())
}
