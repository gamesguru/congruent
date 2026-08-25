mod commands;
pub(crate) mod tester;

use clap::Subcommand;
use conduwuit::Result;
use ruma::{OwnedEventId, OwnedRoomId, OwnedRoomOrAliasId, OwnedServerName};
use service::rooms::short::{ShortEventId, ShortRoomId};

use self::tester::TesterCommand;
use crate::admin_command_dispatch;

#[admin_command_dispatch]
#[derive(Debug, Subcommand)]
pub enum DebugCommand {
	/// Echo input of admin command
	Echo {
		message: Vec<String>,
	},

	/// Get the auth_chain of a PDU
	GetAuthChain {
		/// An event ID (the $ character followed by the base64 reference hash)
		event_id: OwnedEventId,
	},

	/// Parse and print a PDU from a JSON
	///
	/// The PDU event is only checked for validity and is not added to the
	/// database.
	///
	/// This command needs a JSON blob provided in a Markdown code block below
	/// the command.
	ParsePdu,

	/// Retrieve and print a PDU by EventID from the Continuwuity database
	GetPdu {
		/// An event ID (a $ followed by the base64 reference hash)
		event_id: OwnedEventId,

		/// Also print deserialized eventid_metadata (short IDs, flags, SSH)
		#[arg(short, long)]
		verbose: bool,
	},

	/// Retrieve and print a PDU by PduId from the Continuwuity database
	GetShortPdu {
		/// Shortroomid integer
		shortroomid: ShortRoomId,

		/// Shorteventid integer
		shorteventid: ShortEventId,
	},

	/// Attempts to retrieve a PDU from a remote server. **Does not** insert
	///   it into the database
	/// or persist it anywhere.
	GetRemotePdu {
		/// An event ID (a $ followed by the base64 reference hash)
		event_id: OwnedEventId,

		/// Argument for us to attempt to fetch the event from the
		/// specified remote server.
		server: OwnedServerName,
	},

	/// Same as `get-remote-pdu` but accepts a codeblock newline delimited
	///   list of PDUs and a single server to fetch from
	GetRemotePduList {
		/// Argument for us to attempt to fetch all the events from the
		/// specified remote server.
		server: OwnedServerName,

		/// If set, ignores errors, else stops at the first error/failure.
		#[arg(short, long)]
		force: bool,
	},

	/// Gets all the room state events for the specified room.
	///
	/// This is functionally equivalent to `GET
	/// /_matrix/client/v3/rooms/{roomid}/state`, except the admin command does
	/// *not* check if the sender user is allowed to see state events. This is
	/// done because it's implied that server admins here have database access
	/// and can see/get room info themselves anyways if they were malicious
	/// admins.
	///
	/// Of course the check is still done on the actual client API.
	GetRoomState {
		/// Room ID
		room_id: OwnedRoomOrAliasId,
	},

	/// Get and display signing keys from local cache or remote server.
	GetSigningKeys {
		server_name: Option<OwnedServerName>,

		#[arg(long)]
		notary: Option<OwnedServerName>,

		#[arg(short, long)]
		query: bool,
	},

	/// Get and display signing keys from local cache or remote server.
	GetVerifyKeys {
		server_name: Option<OwnedServerName>,
	},

	/// Sends a federation request to the remote server's
	///   `/_matrix/federation/v1/version` endpoint and measures the latency it
	///   took for the server to respond
	Ping {
		server: OwnedServerName,
	},

	/// Forces device lists for all local and remote users to be updated (as
	///   having new keys available)
	ForceDeviceListUpdates,

	/// Change tracing log level/filter on the fly
	///
	/// This accepts the same format as the `log` config option.
	ChangeLogLevel {
		/// Log level/filter
		filter: Option<String>,

		/// Resets the log level/filter to the one in your config
		#[arg(short, long)]
		reset: bool,
	},

	/// Verify JSON signatures
	///
	/// This command needs a JSON blob provided in a Markdown code block below
	/// the command.
	VerifyJson,

	/// Verify PDU
	///
	/// This re-verifies a PDU existing in the database found by ID.
	VerifyPdu {
		event_id: OwnedEventId,
	},

	/// Prints the very first PDU in the specified room (typically
	///   m.room.create)
	FirstPduInRoom {
		/// The room ID
		room_id: OwnedRoomId,
	},

	/// Prints the latest ("last") PDU in the specified room (typically a
	///   message)
	LatestPduInRoom {
		/// The room ID
		room_id: OwnedRoomId,
	},

	/// Forcefully re-resolve and set room state.
	///
	/// When called without servers, rebuilds state from the local DAG
	/// and reconciles the membership cache. When one or more servers are
	/// provided, fetches state from each via federation and merges all
	/// PDUs before running state resolution.
	#[clap(alias = "force-set-room-state-from-server")]
	ForceSetState {
		/// The impacted room ID
		room_id: OwnedRoomId,
		/// Servers to query room state from. If omitted, rebuilds from
		/// the local DAG without federation. Multiple servers will be
		/// merged before resolution.
		server_names: Vec<OwnedServerName>,
		/// The event ID of the latest known PDU in the room. Will be found
		/// automatically if not provided.
		#[arg(short, long)]
		event_id: Option<OwnedEventId>,
		/// Skip signature verification AND use absolute override (shorthand
		/// for --skip-sig-verify --absolute)
		#[arg(short, long)]
		overwrite: bool,
		/// Skip signature verification on incoming PDUs
		#[arg(long)]
		skip_sig_verify: bool,
		/// Use remote state exclusively without merging with local state
		#[arg(long)]
		absolute: bool,
		/// Dump the raw federation state response to a JSON file
		#[arg(long)]
		output: Option<String>,
		/// Load state from a previously dumped JSON file instead of federation
		#[arg(long)]
		input: Option<String>,
		/// Show what would change without modifying state
		#[arg(long)]
		dry_run: bool,
		/// Skip per-member membership cache rebuild (fast path for bulk
		/// healing)
		#[arg(long, hide = true, default_value_t = false)]
		skip_membership_rebuild: bool,
	},

	/// Runs a server name through Continuwuity's true destination resolution
	///   process
	///
	/// Useful for debugging well-known issues
	ResolveTrueDestination {
		server_name: OwnedServerName,

		#[arg(short, long)]
		no_cache: bool,
	},

	/// Print extended memory usage
	///
	/// Optional argument is a character mask (a sequence of characters in any
	/// order) which enable additional extended statistics. Known characters are
	/// "abdeglmx". For convenience, a '*' will enable everything.
	MemoryStats {
		opts: Option<String>,
	},

	/// Print general tokio runtime metric totals.
	RuntimeMetrics,

	/// Print detailed tokio runtime metrics accumulated since last command
	///   invocation.
	RuntimeInterval,

	/// Print the current time
	Time,

	/// Get database statistics
	DatabaseStats {
		property: Option<String>,

		#[arg(short, long, alias("column"))]
		map: Option<String>,

		#[arg(short, long)]
		verbose: bool,
	},

	/// Trim memory usage
	TrimMemory,

	/// List database files
	DatabaseFiles {
		map: Option<String>,

		#[arg(long)]
		level: Option<i32>,
	},

	/// Send a test email to the invoking admin's email address
	SendTestEmail,

	/// Scans timeline and outlier PDUs in the database and injects missing
	/// event_id fields.
	FixPduEventIds,

	/// Developer test stubs
	#[command(subcommand)]
	#[allow(non_snake_case)]
	#[clap(hide(true))]
	Tester(TesterCommand),
}
