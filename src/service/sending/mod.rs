pub mod antispam;
mod appservice;
mod data;
mod dest;
mod sender;
pub mod stats;

use std::{
	fmt::Debug,
	hash::{DefaultHasher, Hash, Hasher},
	iter::once,
	sync::Arc,
	time::Duration,
};

use async_trait::async_trait;
use conduwuit::{
	Result, Server, debug, debug_warn, err, error, info,
	smallvec::SmallVec,
	utils::{ReadyExt, TryReadyExt, available_parallelism, math::usize_from_u64_truncated},
	warn,
};
use futures::{FutureExt, Stream, StreamExt};
use ruma::{OwnedServerName, RoomId, ServerName, UserId, api::OutgoingRequest};
use tokio::{task, task::JoinSet};

use self::data::Data;
pub use self::{
	dest::Destination,
	sender::{EDU_LIMIT, PDU_LIMIT},
};
use crate::{
	Dep, account_data, client, federation, globals, presence, pusher, rooms,
	rooms::timeline::RawPduId, users,
};

pub struct Service {
	pub db: Data,
	pub stats: stats::FederationStats,
	server: Arc<Server>,
	services: Services,
	channels: Vec<(loole::Sender<Msg>, loole::Receiver<Msg>)>,
	pub(super) semaphore: Arc<tokio::sync::Semaphore>,
	pub(super) dead_servers: std::sync::RwLock<std::collections::HashSet<OwnedServerName>>,
	/// Monotonic counter for outgoing federation transaction IDs, seeded from
	/// the current unix-ms timestamp at startup and incremented per
	/// transaction sent (same scheme Synapse's `TransactionManager` uses).
	/// Seeding from wall-clock time rather than starting at 0 keeps IDs
	/// unique across restarts without needing to persist the counter.
	pub(super) next_txn_id: std::sync::atomic::AtomicU64,
}

struct Services {
	client: Dep<client::Service>,
	globals: Dep<globals::Service>,
	state: Dep<rooms::state::Service>,
	state_cache: Dep<rooms::state_cache::Service>,
	user: Dep<rooms::user::Service>,
	users: Dep<users::Service>,
	presence: Dep<presence::Service>,
	read_receipt: Dep<rooms::read_receipt::Service>,
	timeline: Dep<rooms::timeline::Service>,
	account_data: Dep<account_data::Service>,
	appservice: Dep<crate::appservice::Service>,
	pusher: Dep<pusher::Service>,
	federation: Dep<federation::Service>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Msg {
	dest: Destination,
	event: SendingEvent,
	queue_id: Vec<u8>,
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SendingEvent {
	Pdu(RawPduId), // pduid
	Edu(EduBuf),   // edu json
	Flush,         // none
}

pub type EduBuf = SmallVec<[u8; EDU_BUF_CAP]>;
pub type EduVec = SmallVec<[EduBuf; EDU_VEC_CAP]>;

const EDU_BUF_CAP: usize = 128;
const EDU_VEC_CAP: usize = 1;

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let num_senders = num_senders(&args);
		Ok(Arc::new(Self {
			db: Data::new(&args),
			stats: stats::FederationStats::default(),
			dead_servers: std::sync::RwLock::new(std::collections::HashSet::new()),
			next_txn_id: std::sync::atomic::AtomicU64::new(
				ruma::MilliSecondsSinceUnixEpoch::now().get().into(),
			),
			server: args.server.clone(),
			services: Services {
				client: args.depend::<client::Service>("client"),
				globals: args.depend::<globals::Service>("globals"),
				state: args.depend::<rooms::state::Service>("rooms::state"),
				state_cache: args.depend::<rooms::state_cache::Service>("rooms::state_cache"),
				user: args.depend::<rooms::user::Service>("rooms::user"),
				users: args.depend::<users::Service>("users"),
				presence: args.depend::<presence::Service>("presence"),
				read_receipt: args.depend::<rooms::read_receipt::Service>("rooms::read_receipt"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
				account_data: args.depend::<account_data::Service>("account_data"),
				appservice: args.depend::<crate::appservice::Service>("appservice"),
				pusher: args.depend::<pusher::Service>("pusher"),
				federation: args.depend::<federation::Service>("federation"),
			},
			channels: (0..num_senders).map(|_| loole::unbounded()).collect(),
			semaphore: Arc::new(tokio::sync::Semaphore::new(
				args.server.config.max_concurrent_outbound_requests,
			)),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		let mut senders =
			self.channels
				.iter()
				.enumerate()
				.fold(JoinSet::new(), |mut joinset, (id, _)| {
					let self_ = self.clone();
					let worker = self_.sender(id);
					let worker = if self.unconstrained() {
						task::unconstrained(worker).boxed()
					} else {
						worker.boxed()
					};

					let runtime = self.server.runtime();
					let _abort = joinset.spawn_on(worker, runtime);
					joinset
				});

		// Periodic federation stats reporter
		let stats_self = self.clone();
		let stats_task = self.server.runtime().spawn(async move {
			loop {
				tokio::time::sleep(Duration::from_mins(5)).await;
				stats_self.stats.report_and_reset();
				stats_self.server.metrics.sending_queue_total.store(
					stats_self
						.channels
						.iter()
						.map(|(s, _)| u64::try_from(s.len()).expect("failed conversion"))
						.sum(),
					std::sync::atomic::Ordering::Relaxed,
				);
			}
		});

		while let Some(ret) = senders.join_next_with_id().await {
			match ret {
				| Ok((id, _)) => {
					debug!(?id, "sender worker finished");
				},
				| Err(error) => {
					error!(id = ?error.id(), ?error, "sender worker finished");
				},
			}
		}

		stats_task.abort();
		Ok(())
	}

	fn interrupt(&self) {
		for (sender, _) in &self.channels {
			if !sender.is_closed() {
				sender.close();
			}
		}
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }

	fn unconstrained(&self) -> bool { true }
}

impl Service {
	#[tracing::instrument(skip(self, pdu_id, user, pushkey), level = "debug")]
	pub fn send_pdu_push(&self, pdu_id: &RawPduId, user: &UserId, pushkey: String) -> Result {
		let dest = Destination::Push(user.to_owned(), pushkey);
		let event = SendingEvent::Pdu(*pdu_id);
		let _cork = self.db.db.cork();
		let keys = self.db.queue_requests(once((&event, &dest)));
		self.dispatch(Msg {
			dest,
			event,
			queue_id: keys.into_iter().next().expect("request queue key"),
		})
	}

	#[tracing::instrument(skip(self), level = "debug")]
	pub fn send_pdu_appservice(&self, appservice_id: String, pdu_id: RawPduId) -> Result {
		let dest = Destination::Appservice(appservice_id);
		let event = SendingEvent::Pdu(pdu_id);
		let _cork = self.db.db.cork();
		let keys = self.db.queue_requests(once((&event, &dest)));
		self.dispatch(Msg {
			dest,
			event,
			queue_id: keys.into_iter().next().expect("request queue key"),
		})
	}

	#[tracing::instrument(skip(self, room_id, pdu_id), level = "debug")]
	pub async fn send_pdu_room(&self, room_id: &RoomId, pdu_id: &RawPduId) -> Result {
		let servers = self
			.services
			.state_cache
			.room_servers(room_id)
			.ready_filter(|server_name| !self.services.globals.server_is_ours(server_name));

		let num_servers = self.send_pdu_servers(servers, pdu_id).await?;

		if num_servers > 100 {
			warn!("Broadcasting PDU in {room_id} to {num_servers} servers");
		} else if num_servers > 0 {
			info!("Broadcasting PDU in {room_id} to {num_servers} servers");
		}

		Ok(())
	}

	#[tracing::instrument(skip(self, servers, pdu_id), level = "debug")]
	pub async fn send_pdu_servers<'a, S>(&self, servers: S, pdu_id: &RawPduId) -> Result<usize>
	where
		S: Stream<Item = &'a ServerName> + Send + 'a,
	{
		let requests = servers
			.map(|server| {
				(Destination::Federation(server.into()), SendingEvent::Pdu(pdu_id.to_owned()))
			})
			.collect::<Vec<_>>()
			.await;

		let num_servers = requests.len();
		let _cork = self.db.db.cork();
		let keys = self.db.queue_requests(requests.iter().map(|(o, e)| (e, o)));

		for ((dest, event), queue_id) in requests.into_iter().zip(keys) {
			self.dispatch(Msg { dest, event, queue_id })?;
		}

		Ok(num_servers)
	}

	#[tracing::instrument(skip(self, server, serialized), level = "debug")]
	pub fn send_edu_server(&self, server: &ServerName, serialized: EduBuf) -> Result {
		if self.server_is_dead(server) {
			return Ok(());
		}

		let dest = Destination::Federation(server.to_owned());
		let event = SendingEvent::Edu(serialized);
		let _cork = self.db.db.cork();
		let keys = self.db.queue_requests(once((&event, &dest)));
		self.dispatch(Msg {
			dest,
			event,
			queue_id: keys.into_iter().next().expect("request queue key"),
		})
	}

	#[tracing::instrument(skip(self, server, serialized), level = "debug")]
	pub fn send_reliable_edu_server(&self, server: &ServerName, serialized: EduBuf) -> Result {
		let dest = Destination::Federation(server.to_owned());
		let event = SendingEvent::Edu(serialized);
		let _cork = self.db.db.cork();
		let keys = self.db.queue_reliable_requests(once((&event, &dest)));
		self.dispatch(Msg {
			dest,
			event,
			queue_id: keys.into_iter().next().expect("request queue key"),
		})
	}

	#[tracing::instrument(skip(self, room_id, serialized), level = "debug")]
	pub async fn send_edu_room(&self, room_id: &RoomId, serialized: EduBuf) -> Result {
		let servers = self
			.services
			.state_cache
			.room_servers(room_id)
			.ready_filter(|server_name| !self.services.globals.server_is_ours(server_name));

		self.send_edu_servers(servers, serialized).await
	}

	#[tracing::instrument(skip(self, servers, serialized), level = "debug")]
	pub async fn send_edu_servers<'a, S>(&self, servers: S, serialized: EduBuf) -> Result
	where
		S: Stream<Item = &'a ServerName> + Send + 'a,
	{
		let requests = servers
			.map(|server| {
				(
					Destination::Federation(server.to_owned()),
					SendingEvent::Edu(serialized.clone()),
				)
			})
			.collect::<Vec<_>>()
			.await;

		let num_servers = requests.len();
		info!(
			target: "federation_debug",
			"Fanning out EDU to {num_servers} servers (edu_size={})", serialized.len()
		);

		let _cork = self.db.db.cork();
		let keys = self.db.queue_requests(requests.iter().map(|(o, e)| (e, o)));

		for ((dest, event), queue_id) in requests.into_iter().zip(keys) {
			self.dispatch(Msg { dest, event, queue_id })?;
		}

		Ok(())
	}

	#[tracing::instrument(skip(self, room_id), level = "debug")]
	pub async fn flush_room(&self, room_id: &RoomId) -> Result<()> {
		let servers = self
			.services
			.state_cache
			.room_servers(room_id)
			.ready_filter(|server_name| !self.services.globals.server_is_ours(server_name));

		self.flush_servers(servers).await
	}

	#[tracing::instrument(skip(self, servers, pdu_id), level = "debug")]
	pub async fn wait_for_pdu_servers(
		&self,
		servers: Vec<OwnedServerName>,
		pdu_id: &RawPduId,
		timeout: Duration,
	) -> Result<()> {
		if servers.is_empty() {
			return Ok(());
		}

		let keys: Vec<(Vec<u8>, bool)> = servers
			.iter()
			.map(|server| {
				let mut key = Destination::Federation(server.clone()).get_prefix();
				key.extend_from_slice(pdu_id.as_ref());
				(key, false)
			})
			.collect();

		let started_at = tokio::time::Instant::now();
		let mut keys = keys;
		loop {
			let mut pending = Vec::new();
			for (key, attempted) in &mut keys {
				let is_current = self.db.servercurrentevent_data.contains(key).await;
				*attempted |= is_current;

				if is_current || (!*attempted && self.db.servernameevent_data.contains(key).await)
				{
					pending.push(key.clone());
				}
			}

			if pending.is_empty() {
				return Ok(());
			}

			let remaining = timeout.saturating_sub(started_at.elapsed());
			if remaining.is_zero() {
				return Err(err!(Request(Unknown(
					"Timed out waiting for outbound federation to deliver join event."
				))));
			}

			let mut watchers = futures::stream::FuturesUnordered::new();
			for key in &pending {
				watchers.push(self.db.servernameevent_data.watch_prefix(key));
				watchers.push(self.db.servercurrentevent_data.watch_prefix(key));
			}

			let wait = remaining.min(Duration::from_secs(1));
			let _ = tokio::time::timeout(wait, watchers.next()).await;
		}
	}

	#[tracing::instrument(skip(self, servers), level = "debug")]
	pub async fn flush_servers<'a, S>(&self, servers: S) -> Result<()>
	where
		S: Stream<Item = &'a ServerName> + Send + 'a,
	{
		servers
			.map(ToOwned::to_owned)
			.map(Destination::Federation)
			.map(Ok)
			.ready_try_for_each(|dest| {
				self.dispatch(Msg {
					dest,
					event: SendingEvent::Flush,
					queue_id: Vec::<u8>::new(),
				})
			})
			.await
	}

	/// Sends a request to a federation server
	#[inline]
	pub async fn send_federation_request<T>(
		&self,
		dest: &ServerName,
		request: T,
	) -> Result<T::IncomingResponse>
	where
		T: OutgoingRequest + Debug + Send,
	{
		self.send_federation_request_on(&self.services.client.federation, dest, request)
			.await
	}

	/// Like send_federation_request() but with a very large timeout
	#[inline]
	pub async fn send_synapse_request<T>(
		&self,
		dest: &ServerName,
		request: T,
	) -> Result<T::IncomingResponse>
	where
		T: OutgoingRequest + Debug + Send,
	{
		self.send_federation_request_on(&self.services.client.synapse, dest, request)
			.await
	}

	/// Sends a request to a federation server on a specific client
	#[inline]
	pub async fn send_federation_request_on<T>(
		&self,
		client: &reqwest::Client,
		dest: &ServerName,
		request: T,
	) -> Result<T::IncomingResponse>
	where
		T: OutgoingRequest + Debug + Send,
	{
		let _permit = self
			.semaphore
			.clone()
			.acquire_owned()
			.await
			.expect("Semaphore should not be closed");

		self.services
			.federation
			.execute_on(client, dest, request)
			.await
	}

	/// Clean up queued sending event data
	///
	/// Used after we remove an appservice registration or a user deletes a push
	/// key
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn cleanup_events(
		&self,
		appservice_id: Option<&str>,
		user_id: Option<&UserId>,
		push_key: Option<&str>,
	) -> Result {
		match (appservice_id, user_id, push_key) {
			| (None, Some(user_id), Some(push_key)) => {
				self.db
					.delete_all_requests_for(&Destination::Push(
						user_id.to_owned(),
						push_key.to_owned(),
					))
					.await;

				Ok(())
			},
			| (Some(appservice_id), None, None) => {
				self.db
					.delete_all_requests_for(&Destination::Appservice(appservice_id.to_owned()))
					.await;

				Ok(())
			},
			| _ => {
				debug_warn!("cleanup_events called with too many or too few arguments");
				Ok(())
			},
		}
	}

	fn dispatch(&self, msg: Msg) -> Result {
		let shard = self.shard_id(&msg.dest);
		let sender = &self
			.channels
			.get(shard)
			.expect("missing sender worker channels")
			.0;

		debug_assert!(!sender.is_full(), "channel full");
		debug_assert!(!sender.is_closed(), "channel closed");
		sender.send(msg).map_err(|e| err!("{e}"))
	}

	pub(super) fn shard_id(&self, dest: &Destination) -> usize {
		if self.channels.len() <= 1 {
			return 0;
		}

		let mut hash = DefaultHasher::default();
		dest.hash(&mut hash);

		let hash: u64 = hash.finish();
		let hash = usize_from_u64_truncated(hash);

		let chans = self.channels.len().max(1);
		hash.overflowing_rem(chans).0
	}

	/// Returns a list of (destination_display, queued_count, active_count) for
	/// all destinations that have pending outbound events.
	pub async fn queued_destinations(&self) -> Vec<(String, usize, usize)> {
		use std::collections::BTreeMap;

		let mut queued: BTreeMap<String, usize> = BTreeMap::new();
		let mut active: BTreeMap<String, usize> = BTreeMap::new();

		// Count queued (pending) items
		let mut stream = self.db.servernameevent_data.raw_keys().boxed();
		while let Some(Ok(key)) = stream.next().await {
			let dest_name = key
				.split(|&b| b == 0xFF)
				.next()
				.and_then(|b| std::str::from_utf8(b).ok())
				.unwrap_or("<unknown>")
				.to_owned();
			let count = queued.entry(dest_name).or_default();
			*count = count.saturating_add(1);
		}

		// Count active (in-flight) items
		let mut stream = self.db.servercurrentevent_data.raw_keys().boxed();
		while let Some(Ok(key)) = stream.next().await {
			let dest_name = key
				.split(|&b| b == 0xFF)
				.next()
				.and_then(|b| std::str::from_utf8(b).ok())
				.unwrap_or("<unknown>")
				.to_owned();
			let count = active.entry(dest_name).or_default();
			*count = count.saturating_add(1);
		}

		// Merge keys
		let mut all_keys: std::collections::BTreeSet<String> = queued.keys().cloned().collect();
		all_keys.extend(active.keys().cloned());

		all_keys
			.into_iter()
			.map(|k| {
				let q = queued.get(&k).copied().unwrap_or(0);
				let a = active.get(&k).copied().unwrap_or(0);
				(k, q, a)
			})
			.collect()
	}

	/// Clear all queued and active requests for a specific federation server.
	pub async fn clear_destination_queue(&self, server: &ServerName) {
		let dest = Destination::Federation(server.to_owned());
		self.db.delete_all_requests_for(&dest).await;
		self.db.delete_all_active_requests_for(&dest).await;
	}

	/// Clear all queued and active requests for ALL federation destinations.
	pub async fn clear_all_federation_queues(&self) {
		// Walk the queued table and delete everything
		let keys: Vec<Vec<u8>> = self
			.db
			.servernameevent_data
			.raw_keys()
			.boxed()
			.filter_map(|r| async { r.ok().map(<[u8]>::to_vec) })
			.collect()
			.await;
		for key in &keys {
			self.db.servernameevent_data.remove(key);
		}

		// Walk the active table and delete everything
		let keys: Vec<Vec<u8>> = self
			.db
			.servercurrentevent_data
			.raw_keys()
			.boxed()
			.filter_map(|r| async { r.ok().map(<[u8]>::to_vec) })
			.collect()
			.await;
		for key in &keys {
			self.db.servercurrentevent_data.remove(key);
		}
	}

	#[inline]
	pub fn server_is_dead(&self, server: &ServerName) -> bool {
		self.dead_servers.read().unwrap().contains(server)
	}
}

fn num_senders(args: &crate::Args<'_>) -> usize {
	const MIN_SENDERS: usize = 1;
	// Limit the number of senders to the number of workers threads or number of
	// cores, conservatively.
	let mut max_senders = args.server.metrics.num_workers();

	// Work around some platforms not returning the number of cores.
	let num_cores = available_parallelism();
	if num_cores > 0 {
		max_senders = max_senders.min(num_cores);
	}

	// If the user doesn't override the default 0, this is intended to then default
	// to 1 for now as multiple senders is experimental.
	args.server
		.config
		.sender_workers
		.clamp(MIN_SENDERS, max_senders.max(MIN_SENDERS))
}
