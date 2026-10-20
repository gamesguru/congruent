pub mod antispam;
mod appservice;
mod data;
mod dest;
pub(crate) mod sender;
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
	JoinError, Result, Server, debug, debug_warn, err, error, info,
	smallvec::SmallVec,
	utils::{
		MutexMap, ReadyExt, TryReadyExt, available_parallelism, math::usize_from_u64_truncated,
	},
	warn,
};
use futures::{Stream, StreamExt, stream::FuturesUnordered};
use slipstream::{OwnedServerName, RoomId, ServerName, UserId, api::OutgoingRequest};
use tokio::sync::Semaphore;

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
	pub(super) semaphore: Arc<Semaphore>,
	pub(super) dead_servers: std::sync::RwLock<std::collections::HashSet<OwnedServerName>>,
	/// Destinations whose sending was abandoned on request (`abort_destination`).
	/// Workers drop everything addressed to them, including retries, scheduled
	/// flushes and the result of a transaction that was already in flight.
	pub(super) aborted: std::sync::RwLock<std::collections::HashSet<OwnedServerName>>,
	/// Serializes federation queue insertion and request startup against abort.
	abort_gate: Arc<tokio::sync::RwLock<()>>,
	/// Serializes whole abort and resume operations against each other, so a
	/// resume cannot slip in between an abort's flag and its cleanup. Unlike
	/// `abort_gate` it is held while waiting for the worker.
	abort_serial: MutexMap<OwnedServerName, ()>,
	/// Bumped on every abort of a destination; work built before the bump is
	/// stale even once the destination is resumed.
	abort_epochs: std::sync::RwLock<std::collections::HashMap<OwnedServerName, u64>>,
	/// Wakes federation requests so they can cancel address lookup or HTTP
	/// startup when their destination's abort epoch changes.
	abort_notifications: tokio::sync::watch::Sender<()>,
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
	state_accessor: Dep<rooms::state_accessor::Service>,
	state_cache: Dep<rooms::state_cache::Service>,
	state_partial: Dep<rooms::state_partial::Service>,
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

struct Msg {
	dest: Destination,
	event: SendingEvent,
	queue_id: Vec<u8>,
	abort_ack: Option<tokio::sync::oneshot::Sender<()>>,
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
		let (abort_notifications, _) = tokio::sync::watch::channel(());
		Ok(Arc::new(Self {
			db: Data::new(&args),
			stats: stats::FederationStats::default(),
			dead_servers: std::sync::RwLock::new(std::collections::HashSet::new()),
			aborted: std::sync::RwLock::new(std::collections::HashSet::new()),
			abort_gate: Arc::new(tokio::sync::RwLock::new(())),
			abort_serial: MutexMap::new(),
			abort_epochs: std::sync::RwLock::new(std::collections::HashMap::new()),
			abort_notifications,
			next_txn_id: std::sync::atomic::AtomicU64::new(
				slipstream::MilliSecondsSinceUnixEpoch::now().get(),
			),
			server: args.server.clone(),
			services: Services {
				client: args.depend::<client::Service>("client"),
				globals: args.depend::<globals::Service>("globals"),
				state: args.depend::<rooms::state::Service>("rooms::state"),
				state_accessor: args
					.depend::<rooms::state_accessor::Service>("rooms::state_accessor"),
				state_cache: args.depend::<rooms::state_cache::Service>("rooms::state_cache"),
				state_partial: args
					.depend::<rooms::state_partial::Service>("rooms::state_partial"),
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
			semaphore: Arc::new(Semaphore::new(
				args.server.config.max_concurrent_outbound_requests,
			)),
		}))
	}

	async fn worker(self: Arc<Self>) -> Result {
		let mut senders = FuturesUnordered::new();
		for (id, _) in self.channels.iter().enumerate() {
			let self_ = self.clone();
			// Each sender runs in its own runtime task so a panic in one
			// sender resolves to a `JoinError` for that sender only instead of
			// aborting every sender and restarting the whole service.
			let handle = self
				.server
				.runtime()
				.spawn(async move { self_.sender(id).await });
			senders.push(async move { (id, handle.await) });
		}

		// Periodic federation stats reporter
		let stats_self = self.clone();
		let mut stats_task = self.server.runtime().spawn(async move {
			loop {
				smol::Timer::after(Duration::from_mins(5)).await;
				if stats_self.server.is_maintenance() {
					continue;
				}
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

		while let Some((id, ret)) = senders.next().await {
			match ret {
				| Ok(Ok(())) => {
					debug!(?id, "sender worker finished");
				},
				| Ok(Err(error)) => {
					error!(?id, ?error, "sender worker finished");
				},
				| Err(JoinError::Panic(panic)) => {
					error!(
						?id,
						panic = %debug::panic_str(&panic),
						"sender worker panicked"
					);
				},
				| Err(error) => {
					error!(?id, ?error, "sender worker cancelled");
				},
			}
		}

		// Abort (cancel) the stats reporter so it does not outlive this
		// service worker; dropping the handle would only detach it.
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
}

impl Service {
	/// Persists a PDU for the user's push key and wakes its sender worker.
	/// Returns a worker dispatch error after persistence if dispatch fails.
	pub fn send_pdu_push(&self, pdu_id: &RawPduId, user: &UserId, pushkey: String) -> Result {
		let dest = Destination::Push(user.to_owned(), pushkey);
		let event = SendingEvent::Pdu(*pdu_id);
		let _cork = self.db.db.cork();
		let keys = self.db.queue_requests(once((&event, &dest)));
		self.dispatch(Msg {
			dest,
			event,
			queue_id: keys.into_iter().next().expect("request queue key"),
			abort_ack: None,
		})
	}

	/// Persists a PDU for an application service and wakes its sender worker.
	/// Returns a worker dispatch error after persistence if dispatch fails.
	pub fn send_pdu_appservice(&self, appservice_id: String, pdu_id: RawPduId) -> Result {
		let dest = Destination::Appservice(appservice_id);
		let event = SendingEvent::Pdu(pdu_id);
		let _cork = self.db.db.cork();
		let keys = self.db.queue_requests(once((&event, &dest)));
		self.dispatch(Msg {
			dest,
			event,
			queue_id: keys.into_iter().next().expect("request queue key"),
			abort_ack: None,
		})
	}

	pub async fn send_pdu_room(&self, room_id: &RoomId, pdu_id: &RawPduId) -> Result {
		self.send_pdu_room_except(room_id, pdu_id, None).await
	}

	/// Like `send_pdu_room`, but skips `except`: the server an event came from
	/// has it already (e.g. a joining server must not be sent its own join).
	pub async fn send_pdu_room_except(
		&self,
		room_id: &RoomId,
		pdu_id: &RawPduId,
		except: Option<&ServerName>,
	) -> Result {
		// In a partial-state room the member list is incomplete, so the server we
		// joined through may be absent from `room_servers`; always include it.
		let mut servers = self
			.services
			.state_cache
			.room_servers(room_id)
			.ready_filter(|server_name| !self.services.globals.server_is_ours(server_name))
			.collect::<Vec<_>>()
			.await;
		for partial_server in self.services.state_partial.partial_servers(room_id).await {
			if !self.services.globals.server_is_ours(&partial_server)
				&& !servers.contains(&partial_server)
			{
				servers.push(partial_server);
			}
		}

		servers.retain(|server| except.is_none_or(|except| except != &**server));

		let num_servers = self
			.send_pdu_servers(futures::stream::iter(servers), pdu_id)
			.await?;

		if num_servers > 100 {
			warn!("Broadcasting PDU in {room_id} to {num_servers} servers");
		} else if num_servers > 0 {
			info!("Broadcasting PDU in {room_id} to {num_servers} servers");
		}

		Ok(())
	}

	/// Persists a PDU for the supplied servers, skipping aborted destinations,
	/// and wakes their workers. Returns the number of retained destinations.
	/// Dispatch errors propagate after persistence and may leave some workers
	/// unwoken; success indicates queueing rather than remote delivery.
	pub async fn send_pdu_servers<'a, S>(&self, servers: S, pdu_id: &RawPduId) -> Result<usize>
	where
		S: Stream<Item = OwnedServerName> + Send + 'a,
	{
		let requests = servers
			.map(|server| (Destination::Federation(server), SendingEvent::Pdu(pdu_id.to_owned())))
			.collect::<Vec<_>>()
			.await;

		let _abort_guard = self.abort_gate.clone().read_owned().await;
		let requests: Vec<_> = requests
			.into_iter()
			.filter(|(dest, _)| !self.is_aborted(dest))
			.collect();
		let num_servers = requests.len();
		let _cork = self.db.db.cork();
		let keys = self.db.queue_requests(requests.iter().map(|(o, e)| (e, o)));

		for ((dest, event), queue_id) in requests.into_iter().zip(keys) {
			self.dispatch(Msg { dest, event, queue_id, abort_ack: None })?;
		}

		Ok(num_servers)
	}

	/// Queues a serialized EDU for a server. Silently discards it if the server
	/// is dead or aborted, or an abort/resume operation holds the sending gate.
	/// Worker dispatch errors propagate after persistence.
	pub fn send_edu_server(&self, server: &ServerName, serialized: EduBuf) -> Result {
		let Ok(_abort_guard) = self.abort_gate.try_read() else {
			return Ok(());
		};
		let dest = Destination::Federation(server.to_owned());
		if self.server_is_dead(server) || self.is_aborted(&dest) {
			return Ok(());
		}

		let event = SendingEvent::Edu(serialized);
		let _cork = self.db.db.cork();
		let keys = self.db.queue_requests(once((&event, &dest)));
		self.dispatch(Msg {
			dest,
			event,
			queue_id: keys.into_iter().next().expect("request queue key"),
			abort_ack: None,
		})
	}

	/// Queues a serialized EDU in storage that survives ordinary retry cleanup
	/// until successful delivery or an explicit clear. Aborted destinations
	/// discard it and return success.
	///
	/// # Errors
	/// Returns an error asking the caller to retry if an abort/resume operation
	/// holds the sending gate. Worker dispatch errors propagate after
	/// persistence.
	pub fn send_reliable_edu_server(&self, server: &ServerName, serialized: EduBuf) -> Result {
		let dest = Destination::Federation(server.to_owned());
		// While an abort or resume is in progress, tell the caller to retry rather
		// than lose the EDU. A destination that is already aborted discards its
		// queue by design, reliable rows included.
		let Ok(_abort_guard) = self.abort_gate.try_read() else {
			return Err(err!(Request(Unknown("Sending is being reconfigured; retry shortly."))));
		};
		if self.is_aborted(&dest) {
			return Ok(());
		}
		let event = SendingEvent::Edu(serialized);
		let _cork = self.db.db.cork();
		let keys = self.db.queue_reliable_requests(once((&event, &dest)));
		self.dispatch(Msg {
			dest,
			event,
			queue_id: keys.into_iter().next().expect("request queue key"),
			abort_ack: None,
		})
	}

	pub async fn send_edu_room(&self, room_id: &RoomId, serialized: EduBuf) -> Result {
		let mut servers = self
			.services
			.state_cache
			.room_servers(room_id)
			.ready_filter(|server_name| !self.services.globals.server_is_ours(server_name))
			.collect::<Vec<_>>()
			.await;
		for partial_server in self.services.state_partial.partial_servers(room_id).await {
			if !self.services.globals.server_is_ours(&partial_server)
				&& !servers.contains(&partial_server)
			{
				servers.push(partial_server);
			}
		}

		self.send_edu_servers(futures::stream::iter(servers), serialized)
			.await
	}

	/// Queues a serialized EDU for each supplied server except aborted ones.
	/// Worker dispatch errors propagate after persistence; success does not
	/// indicate remote delivery.
	pub async fn send_edu_servers<'a, S>(&self, servers: S, serialized: EduBuf) -> Result
	where
		S: Stream<Item = OwnedServerName> + Send + 'a,
	{
		let requests = servers
			.map(|server| {
				(Destination::Federation(server), SendingEvent::Edu(serialized.clone()))
			})
			.collect::<Vec<_>>()
			.await;
		let _abort_guard = self.abort_gate.clone().read_owned().await;
		let requests: Vec<_> = requests
			.into_iter()
			.filter(|(dest, _)| !self.is_aborted(dest))
			.collect();

		let num_servers = requests.len();
		info!(
			target: "federation_debug",
			"Fanning out EDU to {num_servers} servers (edu_size={})", serialized.len()
		);

		let _cork = self.db.db.cork();
		let keys = self.db.queue_requests(requests.iter().map(|(o, e)| (e, o)));

		for ((dest, event), queue_id) in requests.into_iter().zip(keys) {
			self.dispatch(Msg { dest, event, queue_id, abort_ack: None })?;
		}

		Ok(())
	}

	pub async fn flush_room(&self, room_id: &RoomId) -> Result<()> {
		let mut servers = self
			.services
			.state_cache
			.room_servers(room_id)
			.ready_filter(|server_name| !self.services.globals.server_is_ours(server_name))
			.collect::<Vec<_>>()
			.await;
		for partial_server in self.services.state_partial.partial_servers(room_id).await {
			if !self.services.globals.server_is_ours(&partial_server)
				&& !servers.contains(&partial_server)
			{
				servers.push(partial_server);
			}
		}

		self.flush_servers(futures::stream::iter(servers)).await
	}

	pub async fn wait_for_pdu_servers(
		&self,
		servers: Vec<OwnedServerName>,
		pdu_id: &RawPduId,
		timeout: Duration,
		timeout_message: &'static str,
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

		let started_at = std::time::Instant::now();
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
				return Err(err!(Request(Unknown("{timeout_message}"))));
			}

			let mut watchers = FuturesUnordered::new();
			for key in &pending {
				watchers.push(self.db.servernameevent_data.watch_prefix(key));
				watchers.push(self.db.servercurrentevent_data.watch_prefix(key));
			}

			let wait = remaining.min(Duration::from_secs(1));
			let _ = futures::future::select(
				Box::pin(watchers.next()),
				Box::pin(smol::Timer::after(wait)),
			)
			.await;
		}
	}

	/// Asks each supplied server's worker to send pending events without adding
	/// new content. Returns the first dispatch error; does not wait for
	/// delivery.
	pub async fn flush_servers<'a, S>(&self, servers: S) -> Result<()>
	where
		S: Stream<Item = OwnedServerName> + Send + 'a,
	{
		servers
			.map(Destination::Federation)
			.map(Ok)
			.ready_try_for_each(|dest| {
				self.dispatch(Msg {
					dest,
					event: SendingEvent::Flush,
					queue_id: Vec::<u8>::new(),
					abort_ack: None,
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
		client: &client::HttpClient,
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

	/// Checks the destination's abort flag and generation before starting a
	/// federation request, then drops the request future if an abort changes that
	/// generation. This can happen during address lookup, HTTP startup, or while
	/// awaiting the response. A request canceled in flight may already have been
	/// processed by the remote. `epoch` is the generation captured when the work
	/// was built. Returns `None` if aborted or stale, or the response when
	/// completed.
	///
	/// # Errors
	/// Propagates federation policy, address resolution, request construction,
	/// transport, and response errors from the federation service.
	pub(super) async fn send_federation_request_on_if_current<T>(
		&self,
		client: &client::HttpClient,
		dest: &ServerName,
		epoch: u64,
		request: T,
	) -> Result<Option<T::IncomingResponse>>
	where
		T: OutgoingRequest + Debug + Send,
	{
		let _permit = self
			.semaphore
			.clone()
			.acquire_owned()
			.await
			.expect("Semaphore should not be closed");
		let mut abort_notifications = self.abort_notifications.subscribe();
		let gate = self.abort_gate.clone().read_owned().await;
		if self.destination_aborted(dest) || self.abort_epoch(dest) != epoch {
			return Ok(None);
		}
		drop(gate);
		let mut request = Box::pin(self.services.federation.execute_on(client, dest, request));
		loop {
			let changed = std::pin::pin!(abort_notifications.changed());
			match futures::future::select(changed, request.as_mut()).await {
				| futures::future::Either::Left((changed, _)) => {
					if changed.is_err()
						|| self.destination_aborted(dest)
						|| self.abort_epoch(dest) != epoch
					{
						return Ok(None);
					}
				},
				| futures::future::Either::Right((result, _)) => return result.map(Some),
			}
		}
	}

	/// Clean up queued sending event data
	///
	/// Used after we remove an appservice registration or a user deletes a push
	/// key
	pub async fn cleanup_events(
		&self,
		appservice_id: Option<&str>,
		user_id: Option<&UserId>,
		push_key: Option<&str>,
	) -> Result {
		let destination = match (appservice_id, user_id, push_key) {
			| (None, Some(user_id), Some(push_key)) =>
				Destination::Push(user_id.to_owned(), push_key.to_owned()),
			| (Some(appservice_id), None, None) =>
				Destination::Appservice(appservice_id.to_owned()),
			| _ => {
				debug_warn!("cleanup_events called with too many or too few arguments");
				return Ok(());
			},
		};

		self.db.delete_all_requests_for(&destination).await;
		Ok(())
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
	/// Includes reliable EDUs; does not set the destination's abort flag.
	pub async fn clear_destination_queue(&self, server: &ServerName) {
		let dest = Destination::Federation(server.to_owned());
		self.db.delete_all_requests_for(&dest).await;
		self.db.delete_all_reliable_requests_for(&dest).await;
		self.db.delete_all_active_requests_for(&dest).await;
	}

	/// Marks a federation destination aborted, advances its generation, asks
	/// its worker to discard retry state, and clears stored requests, including
	/// reliable EDUs. Waits up to 30 seconds for worker acknowledgment before
	/// clearing storage anyway. Resume is serialized with this operation.
	/// Request futures are dropped when their epoch changes, including while
	/// awaiting a response; the remote may already have processed an in-flight
	/// request. Stale results are ignored by the worker, and delayed flush
	/// messages are discarded while the flag is set.
	pub async fn abort_destination(&self, server: &ServerName) {
		// Held for the whole operation so a resume cannot interleave; the global
		// gate below is released before waiting on the worker.
		let _serial = self.abort_serial.lock(server).await;
		let gate = self.abort_gate.clone().write_owned().await;
		self.aborted.write().unwrap().insert(server.to_owned());
		self.increment_abort_epoch(server);
		drop(gate);

		let dest = Destination::Federation(server.to_owned());
		let (ack, acknowledged) = tokio::sync::oneshot::channel();
		if let Some((sender, _)) = self.channels.get(self.shard_id(&dest)) {
			if sender
				.send(Msg {
					dest,
					event: SendingEvent::Flush,
					queue_id: Vec::new(),
					abort_ack: Some(ack),
				})
				.is_ok()
			{
				// The channel outlives its worker, so a send can succeed with no one
				// left to acknowledge; do not wait forever.
				if conduwuit::timeout(Duration::from_secs(30), acknowledged)
					.await
					.is_err()
				{
					warn!(%server, "sender worker did not acknowledge the abort in time");
				}
			}
		}
		self.clear_destination_queue(server).await;
	}

	/// Lifts an earlier abort so the server can be sent to again.
	/// Waits for any abort cleanup to finish; discarded requests are not
	/// restored.
	pub async fn resume_destination(&self, server: &ServerName) {
		let _serial = self.abort_serial.lock(server).await;
		let _gate = self.abort_gate.clone().write_owned().await;
		self.aborted.write().unwrap().remove(server);
	}

	/// Whether sending to `server` was stopped by `abort_destination`.
	pub fn destination_aborted(&self, server: &ServerName) -> bool {
		self.aborted.read().unwrap().contains(server)
	}

	/// The abort generation of `server`, initially zero; increments on abort
	/// until it saturates at `u64::MAX`.
	pub(super) fn abort_epoch(&self, server: &ServerName) -> u64 {
		self.abort_epochs
			.read()
			.unwrap()
			.get(server)
			.copied()
			.unwrap_or(0)
	}

	/// Advances the server's abort generation, saturating at `u64::MAX`.
	fn increment_abort_epoch(&self, server: &ServerName) {
		let mut epochs = self.abort_epochs.write().unwrap();
		let epoch = epochs.entry(server.to_owned()).or_insert(0);
		*epoch = epoch.saturating_add(1);
		self.abort_notifications.send_replace(());
	}

	/// Whether this federation destination is aborted. Push and application
	/// service destinations always return `false`.
	pub(super) fn is_aborted(&self, dest: &Destination) -> bool {
		match dest {
			| Destination::Federation(server) => self.aborted.read().unwrap().contains(server),
			| _ => false,
		}
	}

	/// Clears all ordinary queued and active requests, including push and
	/// application service requests, and all reliable federation EDUs.
	/// Does not set abort flags or cancel requests already in flight.
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

		let keys: Vec<Vec<u8>> = self
			.db
			.federation_outbound_to_device
			.raw_keys()
			.boxed()
			.filter_map(|r| async { r.ok().map(<[u8]>::to_vec) })
			.collect()
			.await;
		for key in &keys {
			self.db.federation_outbound_to_device.remove(key);
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
