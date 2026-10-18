mod commands;

use clap::Subcommand;
use conduwuit::Result;
use slipstream::{OwnedRoomId, OwnedServerName, OwnedUserId};

use crate::admin_command_dispatch;

#[admin_command_dispatch]
#[derive(Debug, Subcommand)]
pub enum FederationCommand {
	/// List all rooms we are currently handling an incoming pdu from
	IncomingFederation,

	/// Disables incoming federation handling for a room.
	DisableRoom {
		room_id: OwnedRoomId,
	},

	/// Enables incoming federation handling for a room again.
	EnableRoom {
		room_id: OwnedRoomId,
	},

	/// Fetch `/.well-known/matrix/support` from the specified server
	///
	/// Despite the name, this is not a federation endpoint and does not go
	/// through the federation / server resolution process as per-spec this is
	/// supposed to be served at the server_name.
	///
	/// Respecting homeservers put this file here for listing administration,
	/// moderation, and security inquiries. This command provides a way to
	/// easily fetch that information.
	FetchSupportWellKnown {
		server_name: OwnedServerName,
	},

	/// Lists all the rooms we share/track with the specified *remote* user
	RemoteUserInRooms {
		user_id: OwnedUserId,
	},

	/// Queue an MSC4521 reconciliation of a room with a remote server
	Reconcile {
		room_id: OwnedRoomId,
		server: OwnedServerName,
	},

	/// Show the outbound sending queue with per-destination event counts.
	/// Optionally filter to a single server. Use --clear to drop queued events.
	SendingQueue {
		server: Option<OwnedServerName>,

		/// Clear the queued events for the specified server
		#[arg(long, conflicts_with_all = ["abort", "resume"])]
		clear: bool,

		/// Required with --clear when no server is specified
		#[arg(long, conflicts_with_all = ["abort", "resume"])]
		all: bool,

		/// Stop sending to the specified server: clear its queue and drop its
		/// retry state, scheduled flushes and any in-flight result
		#[arg(long, conflicts_with = "resume")]
		abort: bool,

		/// Lift an earlier --abort so the server can be sent to again
		#[arg(long)]
		resume: bool,
	},
}
