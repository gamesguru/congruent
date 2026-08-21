use std::{
	collections::{BTreeMap, HashMap, HashSet, btree_map::Entry},
	fmt::Debug,
	sync::{
		Arc,
		atomic::{AtomicU64, AtomicUsize, Ordering},
	},
	time::{Duration, Instant, SystemTime},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use conduwuit::info;
use conduwuit_core::{
	Error, Event, Result, at, debug, err, error,
	result::LogErr,
	trace,
	utils::{
		ReadyExt, calculate_hash,
		future::TryExtExt,
		stream::{BroadbandExt, IterStream, WidebandExt},
	},
	warn,
};
use futures::{
	FutureExt, StreamExt,
	future::{BoxFuture, OptionFuture},
	join, pin_mut,
	stream::FuturesUnordered,
};
use ruma::{
	CanonicalJsonObject, MilliSecondsSinceUnixEpoch, OwnedRoomId, OwnedServerName, OwnedUserId,
	RoomId, RoomVersionId, ServerName, UInt,
	api::{
		appservice::event::push_events::v1::EphemeralData,
		client::error::{ErrorKind, RetryAfter},
		federation::transactions::{
			edu::{
				DeviceListUpdateContent, Edu, PresenceContent, PresenceUpdate, ReceiptContent,
				ReceiptData, ReceiptMap,
			},
			send_transaction_message,
		},
	},
	device_id,
	events::{
		AnySyncEphemeralRoomEvent, GlobalAccountDataEventType,
		push_rules::PushRulesEvent,
		receipt::{ReceiptThread, ReceiptType},
	},
	push,
	serde::Raw,
	uint,
};
use serde_json::value::{RawValue as RawJsonValue, to_raw_value};

use super::{Destination, EduBuf, EduVec, Msg, SendingEvent, Service, data::QueueItem};

#[derive(Debug)]
enum TransactionStatus {
	Running,
	Failed {
		tries: u32,
		retry_at: Instant,
	},
	Retrying(u32), // number of times failed
}

type SendingError = (Destination, Error);
type SendingResult = Result<Destination, SendingError>;
type SendingFuture<'a> = BoxFuture<'a, SendingResult>;
type SendingFutures<'a> = FuturesUnordered<SendingFuture<'a>>;
type CurTransactionStatus = HashMap<Destination, TransactionStatus>;

const SELECT_PRESENCE_LIMIT: usize = 256;
const SELECT_RECEIPT_LIMIT: usize = 256;
const SELECT_EDU_LIMIT: usize = EDU_LIMIT - 2;
const DEQUEUE_LIMIT: usize = 48;

pub const PDU_LIMIT: usize = 50;
pub const EDU_LIMIT: usize = 100;

static EDU_TXN_COUNTER: AtomicU64 = AtomicU64::new(0);

impl Service {
	#[tracing::instrument(skip(self), level = "debug")]
	pub(super) async fn sender(self: Arc<Self>, id: usize) -> Result {
		let mut statuses: CurTransactionStatus = CurTransactionStatus::new();
		let mut futures: SendingFutures<'_> = FuturesUnordered::new();

		self.startup_netburst(id, &mut futures, &mut statuses)
			.boxed()
			.await;

		self.work_loop(id, &mut futures, &mut statuses).await;

		if !futures.is_empty() {
			self.finish_responses(&mut futures).boxed().await;
		}

		Ok(())
	}

	#[tracing::instrument(
		name = "work",
		level = "trace"
		skip_all,
		fields(
			futures = %futures.len(),
			statuses = %statuses.len(),
		),
	)]
	async fn work_loop<'a>(
		&'a self,
		id: usize,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
	) {
		let receiver = self
			.channels
			.get(id)
			.map(|(_, receiver)| receiver.clone())
			.expect("Missing channel for sender worker");

		while !receiver.is_closed() {
			let has_space = futures.len() < 128;

			if has_space {
				tokio::select! {
					Some(response) = futures.next() => {
						self.handle_response(response, futures, statuses).await;
					},
					request = receiver.recv_async() => match request {
						Ok(request) => self.handle_request(request, futures, statuses).await,
						Err(_) => return,
					},
				}
			} else if let Some(response) = futures.next().await {
				self.handle_response(response, futures, statuses).await;
			}
			tokio::task::yield_now().await;
		}
	}

	#[tracing::instrument(name = "response", level = "debug", skip_all)]
	async fn handle_response<'a>(
		&'a self,
		response: SendingResult,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
	) {
		match response {
			| Ok(dest) => self.handle_response_ok(&dest, futures, statuses).await,
			| Err((dest, e)) => {
				if let Some(dest) = self.handle_response_err(dest, statuses, &e) {
					// The destination definitively rejected this transaction; it will
					// never accept it as constructed, so drop the PDUs it contained now
					// rather than leaving them marked active. Since `statuses` no
					// longer has an entry for `dest` after this, the next transaction
					// for it won't be treated as a retry (see
					// `select_events_current`), so a later unrelated flush/EDU send
					// succeeding would otherwise wipe these out via
					// `delete_all_active_requests_for` without ever having resent them.
					self.db.delete_all_active_requests_for(&dest).await;
				}
			},
		}
	}

	/// Returns `Some(dest)` if the destination definitively rejected the
	/// transaction and its active requests must be cleaned up by the caller.
	fn handle_response_err(
		&self,
		dest: Destination,
		statuses: &mut CurTransactionStatus,
		e: &Error,
	) -> Option<Destination> {
		debug!(dest = ?dest, "{e:?}");
		let status = e.status_code();
		if status.is_client_error() && !matches!(status.as_u16(), 401 | 403 | 404 | 429) {
			warn!(dest = ?dest, "Destination rejected transaction with {status}, dropping its active PDUs: {e:?}");
			statuses.remove(&dest);
			return Some(dest);
		}

		let mut tries: u32 = 1;
		statuses
			.entry(dest.clone())
			.and_modify(|e| {
				*e = match e {
					| TransactionStatus::Running =>
						TransactionStatus::Failed { tries: 1, retry_at: Instant::now() },
					| &mut TransactionStatus::Retrying(ref n) => {
						tries = n.saturating_add(1);
						TransactionStatus::Failed { tries, retry_at: Instant::now() }
					},
					| &mut TransactionStatus::Failed { tries: t, .. } => {
						tries = t.saturating_add(1);
						TransactionStatus::Failed { tries, retry_at: Instant::now() }
					},
				}
			})
			.or_insert_with(|| TransactionStatus::Failed { tries: 1, retry_at: Instant::now() });

		// Schedule a delayed retry so EDU-only destinations (e.g. to-device
		// messages) are retried after backoff even when no new PDUs arrive.
		// If the remote gave us an explicit M_LIMIT_EXCEEDED retry_after, honor it
		// instead of our own exponential backoff.
		let base = self.server.config.sender_retry_backoff_base;
		let max = self.server.config.sender_retry_backoff_limit;
		let delay = Self::retry_delay(tries, e, base, max);
		let now = Instant::now();
		let retry_at = now.checked_add(delay).unwrap_or(now);

		if let Some(status) = statuses.get_mut(&dest) {
			*status = TransactionStatus::Failed { tries, retry_at };
		}

		self.reschedule_flush(dest, delay);
		None
	}

	/// Schedule a Flush for `dest` after `delay`.
	/// This keeps EDU-only destinations alive through backoff periods.
	fn reschedule_flush(&self, dest: Destination, delay: Duration) {
		let sender = self
			.channels
			.get(self.shard_id(&dest))
			.expect("channel")
			.0
			.clone();

		self.server.runtime().spawn(async move {
			tokio::time::sleep(delay).await;
			sender
				.send(Msg {
					dest,
					event: SendingEvent::Flush,
					queue_id: Vec::new(),
				})
				.ok();
		});
	}

	#[allow(clippy::needless_pass_by_ref_mut)]
	async fn handle_response_ok<'a>(
		&'a self,
		dest: &Destination,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
	) {
		let _cork = self.db.db.cork();
		self.db.delete_all_active_requests_for(dest).await;

		// Find events that have been added since starting the last request
		let new_events = self
			.db
			.queued_requests(dest)
			.take(DEQUEUE_LIMIT)
			.collect::<Vec<_>>()
			.await;

		// Insert any pdus we found
		if !new_events.is_empty() {
			self.db.mark_as_active(new_events.iter());

			let new_events_vec = new_events.into_iter().map(at!(1)).collect();
			futures.push(self.send_events(dest.clone(), new_events_vec, None));
		} else {
			if let Destination::Federation(server_name) = dest {
				if let Ok(since_upper) = self.services.globals.current_count() {
					let since = self.db.get_latest_educount(server_name).await;
					if since < since_upper {
						statuses.remove(dest);
						self.reschedule_flush(dest.clone(), Duration::from_millis(0));
						return;
					}
				}
			}
			statuses.remove(dest);
		}
	}

	#[allow(clippy::needless_pass_by_ref_mut)]
	#[tracing::instrument(name = "request", level = "debug", skip_all)]
	async fn handle_request<'a>(
		&'a self,
		msg: Msg,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
	) {
		let iv = vec![(msg.queue_id, msg.event)];
		if let Ok(Some((events, edu_count))) = self.select_events(&msg.dest, iv, statuses).await {
			if !events.is_empty() {
				futures.push(self.send_events(msg.dest, events, edu_count));
			} else {
				statuses.remove(&msg.dest);
			}
		}
	}

	#[tracing::instrument(
		name = "finish",
		level = "info",
		skip_all,
		fields(futures = %futures.len()),
	)]
	async fn finish_responses<'a>(&'a self, futures: &mut SendingFutures<'a>) {
		use tokio::{
			select,
			time::{Instant, sleep_until},
		};

		let timeout = self.server.config.sender_shutdown_timeout;
		let timeout = Duration::from_secs(timeout);
		let now = Instant::now();
		let deadline = now.checked_add(timeout).unwrap_or(now);
		loop {
			trace!("Waiting for {} requests to complete...", futures.len());
			select! {
				() = sleep_until(deadline) => return,
				response = futures.next() => match response {
					Some(Ok(dest)) => self.db.delete_all_active_requests_for(&dest).await,
					Some(_) => continue,
					None => return,
				},
			}
		}
	}

	#[tracing::instrument(
		name = "netburst",
		level = "debug",
		skip_all,
		fields(futures = %futures.len()),
	)]
	#[allow(clippy::needless_pass_by_ref_mut)]
	async fn startup_netburst<'a>(
		&'a self,
		id: usize,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
	) {
		let keep =
			usize::try_from(self.server.config.startup_netburst_keep).unwrap_or(usize::MAX);
		let mut txns = HashMap::<Destination, Vec<SendingEvent>>::new();
		let mut active = self.db.active_requests().boxed();

		while let Some((key, event, dest)) = active.next().await {
			if self.shard_id(&dest) != id {
				continue;
			}

			let entry = txns.entry(dest.clone()).or_default();
			if self.server.config.startup_netburst_keep >= 0 && entry.len() >= keep {
				warn!("Dropping unsent event {dest:?} {:?}", String::from_utf8_lossy(&key));
				self.db.delete_active_request(&key);
			} else {
				entry.push(event);
			}
		}

		for (dest, events) in txns {
			if self.server.config.startup_netburst && !events.is_empty() {
				statuses.insert(dest.clone(), TransactionStatus::Running);
				futures.push(self.send_events(dest.clone(), events, None));
			}
		}
	}

	#[tracing::instrument(
		name = "select",,
		level = "debug",
		skip_all,
		fields(
			?dest,
			new_events = %new_events.len(),
		)
	)]
	async fn select_events(
		&self,
		dest: &Destination,
		new_events: Vec<QueueItem>, // Events we want to send: event and full key
		statuses: &mut CurTransactionStatus,
	) -> Result<Option<(Vec<SendingEvent>, Option<u64>)>> {
		let (allow, retry) = Self::select_events_current(dest, statuses);

		// Nothing can be done for this remote, bail out.
		if !allow {
			return Ok(None);
		}

		let _cork = self.db.db.cork();
		let mut events = Vec::new();

		// Must retry any previous transaction for this remote.
		if retry {
			self.db
				.active_requests_for(dest)
				.ready_for_each(|(_, e)| events.push(e))
				.await;

			if !events.is_empty() {
				return Ok(Some((events, None)));
			}
		}

		// Compose the next transaction
		let _cork = self.db.db.cork();
		if !new_events.is_empty() {
			self.db.mark_as_active(new_events.iter());
			for (_, e) in new_events {
				events.push(e);
			}
		}

		let mut edu_count = None;
		// Add EDU's into the transaction
		if let Destination::Federation(server_name) = dest {
			if let Ok((select_edus, last_count)) = self.select_edus(server_name).await {
				debug_assert!(select_edus.len() <= EDU_LIMIT, "exceeded edus limit");
				let select_edus = select_edus.into_iter().map(SendingEvent::Edu);

				events.extend(select_edus);
				edu_count = Some(last_count);
			}
		}

		Ok(Some((events, edu_count)))
	}

	fn select_events_current(
		dest: &Destination,
		statuses: &mut CurTransactionStatus,
	) -> (bool, bool) {
		let (mut allow, mut retry) = (true, false);
		statuses
			.entry(dest.clone()) // TODO: can we avoid cloning?
			.and_modify(|e| match e {
				TransactionStatus::Failed { tries, retry_at } => {
					if Instant::now() < *retry_at && !matches!(dest, Destination::Appservice(_)) {
						allow = false;
					} else {
						retry = true;
						*e = TransactionStatus::Retrying(*tries);
					}
				},
				TransactionStatus::Running | TransactionStatus::Retrying(_) => {
					allow = false; // already running
				},
			})
			.or_insert(TransactionStatus::Running);

		(allow, retry)
	}

	fn retry_delay(tries: u32, e: &Error, base: u64, max: u64) -> Duration {
		retry_after_delay(e).unwrap_or_else(|| {
			Duration::from_secs(
				base.saturating_mul(
					1_u64
						.checked_shl(tries.saturating_sub(1))
						.unwrap_or(u64::MAX),
				)
				.min(max),
			)
		})
	}

	#[tracing::instrument(
		name = "edus",,
		level = "debug",
		skip_all,
	)]
	async fn select_edus(&self, server_name: &ServerName) -> Result<(EduVec, u64)> {
		// selection window
		let since = self.db.get_latest_educount(server_name).await;
		let since_upper = self.services.globals.current_count()?;
		let batch = (since, since_upper);
		debug_assert!(batch.0 <= batch.1, "since range must not be negative");

		let events_len = AtomicUsize::default();
		let max_edu_count = AtomicU64::new(since);

		let device_changes =
			self.select_edus_device_changes(server_name, batch, &max_edu_count, &events_len);

		let receipts: OptionFuture<_> = self
			.server
			.config
			.allow_outgoing_read_receipts
			.then(|| self.select_edus_receipts(server_name, batch, &max_edu_count))
			.into();

		let presence: OptionFuture<_> = self
			.server
			.config
			.allow_outgoing_presence
			.then(|| self.select_edus_presence(server_name, batch, &max_edu_count))
			.into();

		let (device_changes, receipts, presence) = join!(device_changes, receipts, presence);

		// Collect them all
		let receipts = receipts.flatten();
		let presence = presence.flatten();

		if !device_changes.is_empty() {
			self.stats.outgoing_device_lists.fetch_add(
				device_changes.len().try_into().unwrap_or(u64::MAX),
				Ordering::Relaxed,
			);
		}

		if receipts.is_some() {
			self.stats.outgoing_receipts.fetch_add(1, Ordering::Relaxed);
		}

		let mut events = device_changes;
		events.extend(presence);
		events.extend(receipts);

		let last_count = if events.is_empty() {
			// We scanned the full window and found nothing relevant for this server.
			// Advance to the upper bound so empty catch-up transactions do not loop
			// forever against the global count.
			since_upper
		} else {
			max_edu_count.load(Ordering::Acquire)
		};

		Ok((events, last_count))
	}

	/// Look for device changes
	#[tracing::instrument(
		name = "device_changes",
		level = "trace",
		skip(self, server_name, max_edu_count)
	)]
	async fn select_edus_device_changes(
		&self,
		server_name: &ServerName,
		since: (u64, u64),
		max_edu_count: &AtomicU64,
		events_len: &AtomicUsize,
	) -> EduVec {
		let mut events = EduVec::new();
		let server_rooms = self.services.state_cache.server_rooms(server_name);

		pin_mut!(server_rooms);
		let mut device_list_changes = HashSet::<OwnedUserId>::new();
		while let Some(room_id) = server_rooms.next().await {
			info!(
				target: "device_list_debug",
				%room_id, "Checking room for device list changes"
			);
			let keys_changed = self
				.services
				.users
				.room_keys_changed(room_id, Some(since.0), None)
				.ready_filter(|(user_id, _)| self.services.globals.user_is_local(user_id));

			pin_mut!(keys_changed);
			while let Some((user_id, count)) = keys_changed.next().await {
				info!(%user_id, %count, %room_id, "Detected device list change");
				if count > since.1 {
					break;
				}

				max_edu_count.fetch_max(count, Ordering::Relaxed);
				if !device_list_changes.insert(user_id.into()) {
					continue;
				}

				// Empty prev id forces synapse to resync; because synapse resyncs,
				// we can just insert placeholder data
				let edu = Edu::DeviceListUpdate(DeviceListUpdateContent {
					user_id: user_id.into(),
					device_id: device_id!("placeholder").to_owned(),
					device_display_name: Some("Placeholder".to_owned()),
					stream_id: uint!(1),
					prev_id: Vec::new(),
					deleted: None,
					keys: None,
				});

				let mut buf = EduBuf::new();
				serde_json::to_writer(&mut buf, &edu)
					.expect("failed to serialize device list update to JSON");

				events.push(buf);
				if events_len.fetch_add(1, Ordering::Relaxed) >= SELECT_EDU_LIMIT - 1 {
					return events;
				}
			}
		}

		events
	}

	/// Look for read receipts in this room
	#[tracing::instrument(
		name = "receipts",
		level = "trace",
		skip(self, server_name, max_edu_count)
	)]
	async fn select_edus_receipts(
		&self,
		server_name: &ServerName,
		since: (u64, u64),
		max_edu_count: &AtomicU64,
	) -> Option<EduBuf> {
		let num = Arc::new(AtomicUsize::new(0));
		let receipts: BTreeMap<OwnedRoomId, ReceiptMap> = self
			.services
			.state_cache
			.server_rooms(server_name)
			.map(ToOwned::to_owned)
			.broad_filter_map(|room_id| {
				let num = Arc::clone(&num);
				async move {
					let receipt_map = self
						.select_edus_receipts_room(&room_id, since, max_edu_count, &num)
						.await;

					receipt_map
						.read
						.is_empty()
						.eq(&false)
						.then_some((room_id, receipt_map))
				}
			})
			.collect()
			.await;

		if receipts.is_empty() {
			return None;
		}

		let receipt_content = Edu::Receipt(ReceiptContent { receipts });

		let mut buf = EduBuf::new();
		serde_json::to_writer(&mut buf, &receipt_content)
			.expect("Failed to serialize Receipt EDU to JSON vec");

		Some(buf)
	}

	/// Look for read receipts in this room
	#[tracing::instrument(
		name = "receipts",
		level = "trace",
		skip(self, since, max_edu_count, num)
	)]
	async fn select_edus_receipts_room(
		&self,
		room_id: &RoomId,
		since: (u64, u64),
		max_edu_count: &AtomicU64,
		num: &AtomicUsize,
	) -> ReceiptMap {
		let receipts = self
			.services
			.read_receipt
			.readreceipts_since(room_id, Some(since.0));

		pin_mut!(receipts);
		let mut collected = Vec::new();
		while let Some((user_id, count, read_receipt)) = receipts.next().await {
			if num.load(Ordering::Relaxed) >= SELECT_RECEIPT_LIMIT {
				break;
			}
			if count > since.1 {
				break;
			}

			max_edu_count.fetch_max(count, Ordering::Relaxed);
			if !self.services.globals.user_is_local(&user_id) {
				continue;
			}

			collected.push((user_id, count, read_receipt.json().get().to_owned()));
		}

		build_receipt_map(collected, since, SELECT_RECEIPT_LIMIT, num)
	}

	/// Look for presence
	#[tracing::instrument(
		name = "presence",
		level = "trace",
		skip(self, server_name, max_edu_count)
	)]
	async fn select_edus_presence(
		&self,
		server_name: &ServerName,
		since: (u64, u64),
		max_edu_count: &AtomicU64,
	) -> Option<EduBuf> {
		let presence_since = self.services.presence.presence_since(since.0);

		pin_mut!(presence_since);
		let mut presence_updates =
			HashMap::<OwnedUserId, PresenceUpdate>::with_capacity(SELECT_PRESENCE_LIMIT);
		while let Some((user_id, count, presence_bytes)) = presence_since.next().await {
			if count > since.1 {
				break;
			}

			max_edu_count.fetch_max(count, Ordering::Relaxed);
			if !self.services.globals.user_is_local(user_id) {
				continue;
			}

			if !self
				.services
				.state_cache
				.server_sees_user(server_name, user_id)
				.await
			{
				continue;
			}

			let Ok(presence_event) = self
				.services
				.presence
				.from_json_bytes_to_event(presence_bytes, user_id)
				.await
				.log_err()
			else {
				continue;
			};

			let update = PresenceUpdate {
				user_id: user_id.into(),
				presence: presence_event.content.presence,
				currently_active: presence_event.content.currently_active.unwrap_or(false),
				status_msg: presence_event.content.status_msg,
				last_active_ago: presence_event
					.content
					.last_active_ago
					.unwrap_or_else(|| uint!(0)),
			};

			presence_updates.insert(user_id.into(), update);
			if presence_updates.len() >= SELECT_PRESENCE_LIMIT {
				break;
			}
		}

		if presence_updates.is_empty() {
			return None;
		}

		self.stats
			.outgoing_presence
			.fetch_add(presence_updates.len().try_into().unwrap_or(u64::MAX), Ordering::Relaxed);

		let presence_content = Edu::Presence(PresenceContent {
			push: presence_updates.into_values().collect(),
		});

		let mut buf = EduBuf::new();
		serde_json::to_writer(&mut buf, &presence_content)
			.expect("failed to serialize Presence EDU to JSON");

		Some(buf)
	}

	fn send_events(
		&self,
		dest: Destination,
		events: Vec<SendingEvent>,
		edu_count: Option<u64>,
	) -> SendingFuture<'_> {
		debug_assert!(
			!events.is_empty() || matches!(dest, Destination::Federation(_)),
			"sending empty transaction"
		);
		match dest {
			| Destination::Federation(server) => self
				.send_events_dest_federation(server, events, edu_count)
				.boxed(),
			| Destination::Appservice(id) => self.send_events_dest_appservice(id, events).boxed(),
			| Destination::Push(user_id, pushkey) =>
				self.send_events_dest_push(user_id, pushkey, events).boxed(),
		}
	}

	#[tracing::instrument(
		name = "appservice",
		level = "debug",
		skip(self, events),
		fields(
			events = %events.len(),
		),
	)]
	async fn send_events_dest_appservice(
		&self,
		id: String,
		events: Vec<SendingEvent>,
	) -> SendingResult {
		let Some(appservice) = self.services.appservice.get_registration(&id).await else {
			return Err((
				Destination::Appservice(id.clone()),
				err!(Database(warn!(?id, "Missing appservice registration"))),
			));
		};

		let mut pdu_jsons = Vec::with_capacity(
			events
				.iter()
				.filter(|event| matches!(event, SendingEvent::Pdu(_)))
				.count(),
		);
		let mut edu_jsons: Vec<EphemeralData> = Vec::with_capacity(
			events
				.iter()
				.filter(|event| matches!(event, SendingEvent::Edu(_)))
				.count(),
		);
		for event in &events {
			match event {
				| SendingEvent::Pdu(pdu_id) => {
					if let Ok(pdu) = self.services.timeline.get_pdu_from_id(pdu_id).await {
						pdu_jsons.push(pdu.to_format());
					}
				},
				| SendingEvent::Edu(edu) =>
					if appservice.receive_ephemeral {
						if let Ok(edu) = serde_json::from_slice(edu) {
							edu_jsons.push(edu);
						}
					},
				| SendingEvent::Flush => {}, // flush only; no new content
			}
		}

		let txn_hash = calculate_hash(events.iter().filter_map(|e| match e {
			| SendingEvent::Edu(b) => Some(&**b),
			| SendingEvent::Pdu(b) => Some(b.as_ref()),
			| SendingEvent::Flush => None,
		}));

		let txn_id = &*URL_SAFE_NO_PAD.encode(txn_hash);

		//debug_assert!(pdu_jsons.len() + edu_jsons.len() > 0, "sending empty
		// transaction");

		match self
			.send_appservice_request(
				appservice,
				ruma::api::appservice::event::push_events::v1::Request {
					events: pdu_jsons,
					txn_id: txn_id.into(),
					ephemeral: edu_jsons,
					to_device: Vec::new(), // TODO
				},
			)
			.await
		{
			| Ok(_) => Ok(Destination::Appservice(id)),
			| Err(e) => Err((Destination::Appservice(id), e)),
		}
	}

	#[tracing::instrument(
		name = "push",
		level = "info",
		skip(self, events),
		fields(
			events = %events.len(),
		),
	)]
	async fn send_events_dest_push(
		&self,
		user_id: OwnedUserId,
		pushkey: String,
		events: Vec<SendingEvent>,
	) -> SendingResult {
		let Ok(pusher) = self.services.pusher.get_pusher(&user_id, &pushkey).await else {
			return Err((
				Destination::Push(user_id.clone(), pushkey.clone()),
				err!(Database(error!(%user_id, ?pushkey, "Missing pusher"))),
			));
		};

		let mut pdus = Vec::with_capacity(
			events
				.iter()
				.filter(|event| matches!(event, SendingEvent::Pdu(_)))
				.count(),
		);
		for event in &events {
			match event {
				| SendingEvent::Pdu(pdu_id) => {
					if let Ok(pdu) = self.services.timeline.get_pdu_from_id(pdu_id).await {
						pdus.push(pdu);
					}
				},
				| SendingEvent::Edu(_) | SendingEvent::Flush => {
					// Push gateways don't need EDUs (?) and flush only;
					// no new content
				},
			}
		}

		for pdu in pdus {
			// Redacted events are not notification targets (we don't send push for them)
			if pdu.is_redacted() {
				continue;
			}

			let rules_for_user = self
				.services
				.account_data
				.get_global(&user_id, GlobalAccountDataEventType::PushRules)
				.await
				.map_or_else(
					|_| push::Ruleset::server_default(&user_id),
					|ev: PushRulesEvent| ev.content.global,
				);

			let unread: UInt = if let Some(room_id) = pdu.room_id_or_hash() {
				self.services
					.user
					.notification_count(&user_id, &room_id)
					.await
					.try_into()
					.expect("notification count can't go that high")
			} else {
				uint!(0)
			};

			let _response = self
				.services
				.pusher
				.send_push_notice(&user_id, unread, &pusher, rules_for_user, &pdu)
				.await
				.map_err(|e| (Destination::Push(user_id.clone(), pushkey.clone()), e));
		}

		Ok(Destination::Push(user_id, pushkey))
	}

	async fn send_events_dest_federation(
		&self,
		server: OwnedServerName,
		events: Vec<SendingEvent>,
		edu_count: Option<u64>,
	) -> SendingResult {
		let pdus: Vec<_> = events
			.iter()
			.filter_map(|pdu| match pdu {
				| SendingEvent::Pdu(pdu) => Some(pdu),
				| _ => None,
			})
			.stream()
			.wide_filter_map(|pdu_id| self.services.timeline.get_pdu_json_from_id(pdu_id).ok())
			.wide_then(|pdu| self.convert_to_outgoing_federation_event(pdu))
			.collect()
			.await;

		let edus: Vec<Raw<Edu>> = events
			.iter()
			.filter_map(|edu| match edu {
				| SendingEvent::Edu(edu) => Some(edu.as_ref()),
				| _ => None,
			})
			.map(serde_json::from_slice)
			.filter_map(Result::ok)
			.collect();

		if pdus.is_empty() && edus.is_empty() {
			if let Some(count) = edu_count {
				self.db.set_latest_educount(&server, count);
			}
			return Ok(Destination::Federation(server));
		}

		let mut typing = 0_u64;
		let mut to_device = 0_u64;
		let mut unknown = 0_u64;

		for edu in &edus {
			match edu.deserialize() {
				| Ok(Edu::Typing(_)) => typing = typing.saturating_add(1),
				| Ok(Edu::DirectToDevice(_)) => to_device = to_device.saturating_add(1),
				| Ok(Edu::Presence(_) | Edu::Receipt(_) | Edu::DeviceListUpdate(_)) => {},
				| _ => unknown = unknown.saturating_add(1),
			}
		}

		if typing > 0 {
			self.stats
				.outgoing_typing
				.fetch_add(typing, Ordering::Relaxed);
		}
		if to_device > 0 {
			self.stats
				.outgoing_to_device
				.fetch_add(to_device, Ordering::Relaxed);
		}
		if unknown > 0 {
			self.stats
				.outgoing_edus
				.fetch_add(unknown, Ordering::Relaxed);
		}

		// Track federation stats
		self.stats
			.outgoing_pdus
			.fetch_add(pdus.len().try_into().unwrap_or(u64::MAX), Ordering::Relaxed);
		self.stats.outgoing_txns.fetch_add(1, Ordering::Relaxed);

		let counter_bytes;
		let preimage: Vec<&[u8]> = if pdus.is_empty() {
			let counter = EDU_TXN_COUNTER.fetch_add(1, Ordering::Relaxed);
			counter_bytes = counter.to_be_bytes();
			pdus.iter()
				.map(|raw| raw.get().as_bytes())
				.chain(edus.iter().map(|raw| raw.json().get().as_bytes()))
				.chain(std::iter::once(&counter_bytes[..]))
				.collect()
		} else {
			pdus.iter()
				.map(|raw| raw.get().as_bytes())
				.chain(edus.iter().map(|raw| raw.json().get().as_bytes()))
				.collect()
		};

		let txn_hash = calculate_hash(preimage.into_iter());
		let txn_id = &*URL_SAFE_NO_PAD.encode(txn_hash);
		let request = send_transaction_message::v1::Request {
			transaction_id: txn_id.into(),
			origin: self.server.name.clone(),
			origin_server_ts: MilliSecondsSinceUnixEpoch::now(),
			pdus,
			edus,
		};

		let result = self
			.services
			.federation
			.execute_on(&self.services.client.sender, &server, request)
			.await;

		for (event_id, result) in result.iter().flat_map(|resp| resp.pdus.iter()) {
			if let Err(e) = result {
				info!(
					%txn_id,
					%server,
					%event_id,
					remote_error=?e,
					"remote server encountered an error while processing an event"
				);
			}
		}

		match result {
			| Err(error) => {
				self.stats.outgoing_errors.fetch_add(1, Ordering::Relaxed);
				Err((Destination::Federation(server), error))
			},
			| Ok(_) => {
				if let Some(count) = edu_count {
					self.db.set_latest_educount(&server, count);
				}
				Ok(Destination::Federation(server))
			},
		}
	}

	/// This does not return a full `Pdu` it is only to satisfy ruma's types.
	pub async fn convert_to_outgoing_federation_event(
		&self,
		mut pdu_json: CanonicalJsonObject,
	) -> Box<RawJsonValue> {
		if let Some(unsigned) = pdu_json
			.get_mut("unsigned")
			.and_then(|val| val.as_object_mut())
		{
			unsigned.remove("transaction_id");
		}

		// room v3 and above removed the "event_id" field from remote PDU format
		if let Some(room_id) = pdu_json
			.get("room_id")
			.and_then(|val| RoomId::parse(val.as_str()?).ok())
		{
			match self.services.state.get_room_version(room_id).await {
				| Ok(room_version_id) => match room_version_id {
					| RoomVersionId::V1 | RoomVersionId::V2 => {},
					| _ => _ = pdu_json.remove("event_id"),
				},
				| Err(_) => _ = pdu_json.remove("event_id"),
			}
		} else {
			pdu_json.remove("event_id");
		}

		// TODO: another option would be to convert it to a canonical string to validate
		// size and return a Result<Raw<...>>
		// serde_json::from_str::<Raw<_>>(
		//     ruma::serde::to_canonical_json_string(pdu_json).expect("CanonicalJson is
		// valid serde_json::Value"), )
		// .expect("Raw::from_value always works")

		to_raw_value(&pdu_json).expect("CanonicalJson is valid serde_json::Value")
	}
}

/// Extracts the server-provided retry delay from an M_LIMIT_EXCEEDED
/// response, if present, so we back off at least as long as the remote asked
/// rather than only our own exponential schedule.
fn retry_after_delay(e: &Error) -> Option<Duration> {
	let Error::Federation(_, fed_err) = e else {
		return None;
	};

	let ErrorKind::LimitExceeded { retry_after: Some(retry_after) } = fed_err.error_kind()?
	else {
		return None;
	};

	match retry_after {
		| RetryAfter::Delay(d) => Some(*d),
		| RetryAfter::DateTime(t) => t.duration_since(SystemTime::now()).ok(),
	}
}

/// Merges collected read receipts for a room into the map sent in a federation
/// EDU, keeping at most one `ReceiptData` per user.
///
/// A user can have both an unthreaded and a threaded receipt pending in the
/// same window; the federation wire format (`ReceiptMap.read`) only carries
/// one `ReceiptData` per user, so on a clash we prefer the unthreaded receipt
/// (matching the local `/sync` merge in `rooms::read_receipt::pack_receipts`)
/// rather than letting whichever receipt was collected last silently win.
fn build_receipt_map(
	receipts: Vec<(OwnedUserId, u64, String)>,
	since: (u64, u64),
	limit: usize,
	num: &AtomicUsize,
) -> ReceiptMap {
	let mut read = BTreeMap::<OwnedUserId, ReceiptData>::new();

	for (user_id, count, read_receipt_json) in receipts {
		if count > since.1 {
			break;
		}

		let Ok(event) = serde_json::from_str(&read_receipt_json) else {
			error!(%user_id, %count, %read_receipt_json, "Invalid edu event in read_receipts.");
			continue;
		};

		let AnySyncEphemeralRoomEvent::Receipt(r) = event else {
			error!(%user_id, %count, ?event, "Invalid event type in read_receipts");
			continue;
		};

		let (event_id, mut receipt) = r
			.content
			.0
			.into_iter()
			.next()
			.expect("we only use one event per read receipt");

		let Some(mut users) = receipt.remove(&ReceiptType::Read) else {
			continue;
		};
		let Some(receipt) = users.remove(&user_id) else {
			continue;
		};

		let is_unthreaded = matches!(receipt.thread, ReceiptThread::Unthreaded);
		let receipt_data = ReceiptData {
			data: receipt,
			event_ids: vec![event_id.clone()],
		};

		match read.entry(user_id) {
			| Entry::Vacant(e) => {
				if num
					.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
						(n < limit).then_some(n.saturating_add(1))
					})
					.is_err()
				{
					break;
				}
				e.insert(receipt_data);
			},
			| Entry::Occupied(mut e) =>
				if is_unthreaded {
					e.insert(receipt_data);
				},
		}
	}

	ReceiptMap { read }
}

#[cfg(test)]
mod tests {
	use std::sync::atomic::AtomicUsize;

	use ruma::user_id;

	use super::*;

	#[test]
	fn test_build_receipt_map_under_limit() {
		let mut receipts = Vec::new();
		let user_id = user_id!("@alice:example.com").to_owned();
		let json = serde_json::json!({
			"type": "m.receipt",
			"content": {
				"$event1": {
					"m.read": {
						"@alice:example.com": {
							"ts": 12345
						}
					}
				}
			}
		});
		receipts.push((user_id, 15, json.to_string()));

		let since = (10, 20);
		let num = AtomicUsize::new(0);
		let map = build_receipt_map(receipts, since, 100, &num);

		assert_eq!(map.read.len(), 1);
		assert_eq!(num.load(Ordering::Relaxed), 1);
	}

	#[test]
	fn test_build_receipt_map_over_limit() {
		let mut receipts = Vec::new();
		for i in 1..=5 {
			let user_id_str = format!("@user{i}:example.com");
			let user_id = <OwnedUserId as TryFrom<&str>>::try_from(user_id_str.as_str()).unwrap();
			let json = serde_json::json!({
				"type": "m.receipt",
				"content": {
					"$event1": {
						"m.read": {
							&user_id_str: {
								"ts": 12345
							}
						}
					}
				}
			});
			receipts.push((user_id, 10 + u64::try_from(i).unwrap(), json.to_string()));
		}

		let since = (10, 20);
		let num = AtomicUsize::new(0);
		// Limit to 3!
		let map = build_receipt_map(receipts, since, 3, &num);

		assert_eq!(map.read.len(), 3);
		assert_eq!(num.load(Ordering::Relaxed), 3);
	}

	#[test]
	fn test_build_receipt_map_unthreaded_precedence() {
		let mut receipts = Vec::new();
		let user_id = user_id!("@alice:example.com").to_owned();

		// 1. Threaded receipt
		let json_threaded = serde_json::json!({
			"type": "m.receipt",
			"content": {
				"$event1": {
					"m.read": {
						"@alice:example.com": {
							"thread_id": "$thread1",
							"ts": 10000
						}
					}
				}
			}
		});
		receipts.push((user_id.clone(), 11, json_threaded.to_string()));

		// 2. Unthreaded receipt
		let json_unthreaded = serde_json::json!({
			"type": "m.receipt",
			"content": {
				"$event1": {
					"m.read": {
						"@alice:example.com": {
							"ts": 12345
						}
					}
				}
			}
		});
		receipts.push((user_id.clone(), 12, json_unthreaded.to_string()));

		let since = (10, 20);
		let num = AtomicUsize::new(0);
		let map = build_receipt_map(receipts, since, 100, &num);

		assert_eq!(map.read.len(), 1);
		assert_eq!(num.load(Ordering::Relaxed), 1);

		let data = &map.read[&user_id];
		assert!(matches!(data.data.thread, ReceiptThread::Unthreaded));
		assert_eq!(data.data.ts.map(|t| t.0.into()), Some(12345_u64));
	}

	#[test]
	fn test_build_receipt_map_threaded_does_not_overwrite() {
		let mut receipts = Vec::new();
		let user_id = user_id!("@alice:example.com").to_owned();

		// 1. Unthreaded receipt
		let json_unthreaded = serde_json::json!({
			"type": "m.receipt",
			"content": {
				"$event1": {
					"m.read": {
						"@alice:example.com": {
							"ts": 12345
						}
					}
				}
			}
		});
		receipts.push((user_id.clone(), 11, json_unthreaded.to_string()));

		// 2. Threaded receipt
		let json_threaded = serde_json::json!({
			"type": "m.receipt",
			"content": {
				"$event1": {
					"m.read": {
						"@alice:example.com": {
							"thread_id": "$thread1",
							"ts": 10000
						}
					}
				}
			}
		});
		receipts.push((user_id.clone(), 12, json_threaded.to_string()));

		let since = (10, 20);
		let num = AtomicUsize::new(0);
		let map = build_receipt_map(receipts, since, 100, &num);

		assert_eq!(map.read.len(), 1);
		assert_eq!(num.load(Ordering::Relaxed), 1);

		let data = &map.read[&user_id];
		assert!(matches!(data.data.thread, ReceiptThread::Unthreaded));
		assert_eq!(data.data.ts.map(|t| t.0.into()), Some(12345_u64));
	}
}
