use std::{
	collections::{BTreeMap, HashMap, HashSet},
	fmt::Debug,
	sync::{Arc, atomic::Ordering},
	time::{Duration, Instant},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use conduwuit::info;
use conduwuit_core::{
	Error, Event, Result, debug_info, err,
	result::LogErr,
	trace,
	utils::{
		ReadyExt, calculate_hash, continue_exponential_backoff_secs,
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
	CanonicalJsonObject, MilliSecondsSinceUnixEpoch, OwnedDeviceId, OwnedRoomId, OwnedServerName,
	OwnedUserId, RoomId, RoomVersionId, ServerName, UInt,
	api::{
		appservice::event::push_events::v1::EphemeralData,
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
		AnySyncEphemeralRoomEvent, GlobalAccountDataEventType, push_rules::PushRulesEvent,
		receipt::ReceiptType,
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
	Failed(u32, Instant), // number of times failed, time of last failure
	Retrying(u32),        // number of times failed
	Cooldown(Instant),
}

type SendingError = Box<(Destination, Error)>;
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

impl Service {
	#[tracing::instrument(skip(self), level = "debug")]
	pub(super) async fn sender(self: Arc<Self>, id: usize) -> Result {
		// In maintenance mode (listening=false), skip all outbound federation.
		// Queued transactions are preserved and will drain on normal boot.
		if !self.server.config.listening {
			info!("sender[{id}]: maintenance mode, skipping outbound federation");
			return Ok(());
		}

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
		level = "trace",
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
			| Err(err) => self.handle_response_err(err.0, statuses, &err.1),
		}
	}

	/// Returns `Some(dest)` if the destination definitively rejected the
	/// transaction and its active requests must be cleaned up by the caller.
	fn handle_response_err(
		&self,
		dest: Destination,
		statuses: &mut CurTransactionStatus,
		e: &Error,
	) {
		if e.status_code() == http::StatusCode::TOO_MANY_REQUESTS {
			tracing::info!(dest = ?dest, "{e:?}");
		} else {
			tracing::info!(target: "federation_debug", dest = ?dest, "{e:?}");
		}

		let mut tries = 1_u32;
		statuses.entry(dest.clone()).and_modify(|e| {
			*e = match e {
				| TransactionStatus::Running => TransactionStatus::Failed(1, Instant::now()),
				| &mut TransactionStatus::Retrying(ref n) => {
					tries = n.saturating_add(1);
					TransactionStatus::Failed(tries, Instant::now())
				},
				| TransactionStatus::Failed(n, _) => {
					tries = n.saturating_add(1);
					tracing::info!(dest = ?dest, tries = tries, "Request failed while already marked as failed");
					TransactionStatus::Failed(tries, Instant::now())
				},
				| TransactionStatus::Cooldown(_) => {
					tracing::info!(dest = ?dest, "Request failed while in cooldown");
					TransactionStatus::Failed(1, Instant::now())
				},
			}
		});

		// If max retries exceeded, drop all queued events for this destination
		let max_attempts = self.server.config.sender_retry_max_attempts;
		if max_attempts > 0 && tries >= max_attempts {
			info!(
				dest = ?dest,
				tries = tries,
				"Dropping queued events after {tries} failed attempts"
			);
			if let Destination::Federation(ref server) = dest {
				self.dead_servers.write().unwrap().insert(server.clone());
			}
			let dest_clone = dest.clone();
			let db = self.db.clone();
			self.server.runtime().spawn(async move {
				db.delete_all_requests_for(&dest_clone).await;
			});
			statuses.remove(&dest);
			return;
		}

		// Schedule a delayed retry after the backoff period
		let base = self.server.config.sender_retry_backoff_base;
		let max = self.server.config.sender_retry_backoff_limit;
		let delay_secs = base
			.saturating_mul(
				1_u64
					.checked_shl(tries.saturating_sub(1))
					.unwrap_or(u64::MAX),
			)
			.min(max);

		self.reschedule_flush(dest, Duration::from_secs(delay_secs));
	}

	/// Re-schedule a Flush for the given destination after a delay.
	/// Called when a Flush arrives but is rejected (e.g. backoff still active
	/// due to timer jitter), so the retry isn't permanently lost.
	/// The delay should match the remaining backoff time to avoid hot-polling.
	fn reschedule_flush(&self, dest: Destination, delay: Duration) {
		let delay = delay.max(Duration::from_millis(100));
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
		if let Destination::Federation(server) = dest {
			self.dead_servers.write().unwrap().remove(server);
		}
		let _cork = self.db.db.cork();
		self.db.delete_all_active_requests_for(dest).await;

		let mut has_pdu = false;
		let mut new_events = Vec::new();

		let mut stream = self.db.queued_requests(dest).take(DEQUEUE_LIMIT);
		while let Some((k, e)) = stream.next().await {
			if matches!(e, SendingEvent::Pdu(_)) {
				has_pdu = true;
			}
			new_events.push((k, e));
		}

		let remaining_limit = DEQUEUE_LIMIT.saturating_sub(new_events.len());
		if remaining_limit > 0 {
			let mut reliable_stream =
				self.db.queued_reliable_requests(dest).take(remaining_limit);
			while let Some((k, e)) = reliable_stream.next().await {
				new_events.push((k, e));
			}
		}

		if !new_events.is_empty() && has_pdu {
			// Immediately send the critical PDUs without trailing cooldown!
			self.db.mark_as_active(new_events.iter());
			let new_events_vec = new_events.into_iter().map(|(_, e)| e).collect();
			statuses.insert(dest.clone(), TransactionStatus::Running);
			futures.push(self.send_events(dest.clone(), new_events_vec, None));
			return;
		}

		if new_events.is_empty() {
			if let Destination::Federation(server_name) = dest {
				let since = self.db.get_latest_educount(server_name).await;
				let since_upper = self.services.globals.current_count().unwrap_or(0);
				info!(
					target: "receipt_debug",
					%server_name, since, since_upper,
					"handle_response_ok: empty transaction, checking for pending edus"
				);
				if since < since_upper {
					info!(
						target: "receipt_debug",
						%server_name, since, since_upper,
						"handle_response_ok: since behind since_upper, rescheduling immediate flush"
					);
					statuses.remove(dest);
					self.reschedule_flush(dest.clone(), Duration::from_millis(0));
					return;
				}
				info!(
					target: "receipt_debug",
					%server_name, since, since_upper,
					"handle_response_ok: since caught up, not rescheduling"
				);
			}
			statuses.remove(dest);
			return;
		}

		statuses.insert(dest.clone(), TransactionStatus::Cooldown(Instant::now()));

		let dest_clone = dest.clone();
		self.reschedule_flush(dest_clone, Duration::from_millis(300));
	}

	#[allow(clippy::needless_pass_by_ref_mut)]
	#[tracing::instrument(name = "request", level = "debug", skip_all)]
	async fn handle_request<'a>(
		&'a self,
		msg: Msg,
		futures: &mut SendingFutures<'a>,
		statuses: &mut CurTransactionStatus,
	) {
		let _cork = self.db.db.cork();
		let is_flush = matches!(msg.event, SendingEvent::Flush);
		let iv = if !is_flush {
			vec![(msg.queue_id, msg.event)]
		} else {
			let mut reqs = self
				.db
				.queued_requests(&msg.dest)
				.take(DEQUEUE_LIMIT)
				.collect::<Vec<_>>()
				.await;

			let reliable_reqs = self
				.db
				.queued_reliable_requests(&msg.dest)
				.take(DEQUEUE_LIMIT.saturating_sub(reqs.len()))
				.collect::<Vec<_>>()
				.await;

			reqs.extend(reliable_reqs);
			reqs
		};

		if let Ok(Some((events, edu_count))) = self.select_events(&msg.dest, iv, statuses).await {
			if !events.is_empty() {
				futures.push(self.send_events(msg.dest, events, edu_count));
			} else {
				statuses.remove(&msg.dest);
			}
		} else if is_flush {
			// Flush was rejected (e.g., backoff still active). Re-schedule
			// at the remaining backoff time to avoid hot-polling the channel.
			let delay = self.remaining_backoff(&msg.dest, statuses);
			if delay > Duration::ZERO {
				self.reschedule_flush(msg.dest, delay);
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
		if !self.server.config.allow_federation {
			debug_info!("startup_netburst[{id}]: federation disabled, skipping");
			return;
		}

		let keep =
			usize::try_from(self.server.config.startup_netburst_keep).unwrap_or(usize::MAX);
		let mut active = self.db.active_requests().boxed();

		let mut current_dest: Option<Destination> = None;
		let mut current_events = Vec::new();

		macro_rules! flush_current {
			() => {
				if let Some(old_dest) = current_dest.take() {
					if self.server.config.startup_netburst && !current_events.is_empty() {
						info!(
							"startup_netburst[{id}]: resuming {} events for {:?}",
							current_events.len(),
							old_dest
						);
						statuses.insert(old_dest.clone(), TransactionStatus::Running);
						futures.push(self.send_events(
							old_dest,
							std::mem::take(&mut current_events),
							None,
						));
					}
				}
			};
		}

		while let Some((key, event, dest)) = active.next().await {
			if self.shard_id(&dest) != id {
				continue;
			}

			if current_dest.as_ref() != Some(&dest) {
				flush_current!();
				current_dest = Some(dest.clone());
				current_events.clear();
			}

			if keep == usize::MAX || current_events.len() < keep {
				current_events.push(event);
			} else {
				warn!("Dropping unsent event {dest:?} {:?}", String::from_utf8_lossy(&key));
				self.db.delete_active_request(&key);
			}
		}

		flush_current!();

		// Process orphaned queued requests
		if self.server.config.startup_netburst {
			let mut queued_dests = HashSet::new();
			for mut stream in [
				self.db.queued_request_destinations().boxed(),
				self.db.queued_reliable_request_destinations().boxed(),
			] {
				while let Some(dest) = stream.next().await {
					if self.shard_id(&dest) == id {
						queued_dests.insert(dest);
					}
				}
			}

			if !queued_dests.is_empty() {
				info!(
					"startup_netburst[{id}]: flushing {} orphaned queued destinations",
					queued_dests.len()
				);
				let sender = self.channels.get(id).expect("channel").0.clone();
				for dest in queued_dests {
					sender
						.send(Msg {
							dest,
							event: SendingEvent::Flush,
							queue_id: Vec::new(),
						})
						.ok();
				}
			} else {
				info!("startup_netburst[{id}]: no orphaned queued destinations");
			}
		}
	}

	#[tracing::instrument(
		name = "select",
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
		let has_pdu = new_events
			.iter()
			.any(|(_, e)| matches!(e, SendingEvent::Pdu(_)));
		let (allow, retry) = self.select_events_current(dest, statuses, has_pdu)?;

		if (!allow || retry) && self.server.config.allow_outgoing_presence {
			if let Destination::Federation(server_name) = dest {
				self.services.presence.pending_updates.remove(server_name);
			}
		}

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
		&self,
		dest: &Destination,
		statuses: &mut CurTransactionStatus,
		has_pdu: bool,
	) -> Result<(bool, bool)> {
		let (mut allow, mut retry) = (true, false);
		statuses
			.entry(dest.clone()) // TODO: can we avoid cloning?
			.and_modify(|e| match e {
				TransactionStatus::Failed(tries, time) => {
					// Fail if a request has failed recently (exponential backoff)
					let min = self.server.config.sender_retry_backoff_base;
					let max = self.server.config.sender_retry_backoff_limit;
					if continue_exponential_backoff_secs(min, max, time.elapsed(), tries.saturating_sub(1))
						&& !matches!(dest, Destination::Appservice(_))
					{
						allow = false;

						let min_dur = Duration::from_secs(min)
							.saturating_mul(1_u32.checked_shl(tries.saturating_sub(1)).unwrap_or(u32::MAX));
						let min_dur = std::cmp::min(min_dur, Duration::from_secs(max));
						let remaining = min_dur.saturating_sub(time.elapsed());
						self.reschedule_flush(dest.clone(), remaining);
					} else {
						retry = true;
						*e = TransactionStatus::Retrying(*tries);
					}
				},
				TransactionStatus::Running | TransactionStatus::Retrying(_) => {
					allow = false; // already running
				},
				TransactionStatus::Cooldown(time) => {
					if !has_pdu && time.elapsed() < Duration::from_millis(300) {
						allow = false;
					} else {
						*e = TransactionStatus::Running;
					}
				},
			})
			.or_insert(TransactionStatus::Running);

		Ok((allow, retry))
	}

	/// Calculate the remaining backoff duration for a destination.
	/// Used to schedule retries at the correct time instead of hot-polling.
	fn remaining_backoff(&self, dest: &Destination, statuses: &CurTransactionStatus) -> Duration {
		let Some(status) = statuses.get(dest) else {
			return Duration::ZERO;
		};

		match status {
			| TransactionStatus::Failed(tries, time) => {
				let base = self.server.config.sender_retry_backoff_base;
				let max = self.server.config.sender_retry_backoff_limit;
				let backoff = base
					.saturating_mul(
						1_u64
							.checked_shl(tries.saturating_sub(1))
							.unwrap_or(u64::MAX),
					)
					.min(max);
				let total = Duration::from_secs(backoff);
				total
					.saturating_sub(time.elapsed())
					.max(Duration::from_secs(1))
			},
			| TransactionStatus::Cooldown(time) => Duration::from_millis(300)
				.saturating_sub(time.elapsed())
				.max(Duration::from_millis(50)),
			| _ => Duration::ZERO,
		}
	}

	#[tracing::instrument(name = "edus", level = "debug", skip_all)]
	async fn select_edus(&self, server_name: &ServerName) -> Result<(EduVec, u64)> {
		// selection window
		let since = self.db.get_latest_educount(server_name).await;
		let since_upper = self.services.globals.current_count()?;
		let batch = (since, since_upper);
		debug_assert!(batch.0 <= batch.1, "since range must not be negative");
		info!(
			target: "receipt_debug",
			%server_name, since = batch.0, since_upper = batch.1,
			"select_edus: window"
		);

		let device_changes = self.select_edus_device_changes(server_name, batch);

		let receipts: OptionFuture<_> = self
			.server
			.config
			.allow_outgoing_read_receipts
			.then(|| self.select_edus_receipts(server_name, batch))
			.into();

		let presence: OptionFuture<_> = self
			.server
			.config
			.allow_outgoing_presence
			.then(|| self.select_edus_presence(server_name, batch))
			.into();

		let (device_res, receipts_res, presence_res) = join!(device_changes, receipts, presence);

		let (device_changes, device_max) = device_res;
		let (receipts, receipt_max) = receipts_res.unwrap_or((None, since_upper));
		let (presence, presence_max) = presence_res.unwrap_or((None, since_upper));

		// The safe global cursor is the minimum of all processed bounds
		let last_count = device_max.min(receipt_max).min(presence_max);
		info!(
			target: "receipt_debug",
			%server_name, device_max, receipt_max, presence_max, last_count,
			receipt_found = receipts.is_some(),
			"select_edus: result bounds"
		);

		// Collect them all
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

		if events.is_empty() {
			return Ok((events, since_upper));
		}

		Ok((events, last_count))
	}

	/// Look for device changes
	#[tracing::instrument(name = "device_changes", level = "trace", skip(self, server_name))]
	async fn select_edus_device_changes(
		&self,
		server_name: &ServerName,
		since: (u64, u64),
	) -> (EduVec, u64) {
		let last_update = self
			.services
			.users
			.last_device_key_update_count
			.load(Ordering::Relaxed);

		if since.0 == 0 && since.0 >= last_update {
			// New server with no previous sync token AND no recent device key
			// updates — skip the expensive full-history scan. They will query
			// /keys/query when needed.
			return (EduVec::new(), since.1);
		}

		if since.0 >= last_update {
			// No global device key updates since the last time we checked.
			return (EduVec::new(), since.1);
		}

		let server_rooms = self.services.state_cache.server_rooms(server_name);

		pin_mut!(server_rooms);
		let mut all_changes = BTreeMap::<u64, HashSet<OwnedUserId>>::new();

		while let Some(room_id) = server_rooms.next().await {
			let keys_changed = self
				.services
				.users
				.room_keys_changed(room_id, Some(since.0), None)
				.ready_filter(|(user_id, _)| self.services.globals.user_is_local(user_id));

			pin_mut!(keys_changed);

			while let Some((user_id, count)) = keys_changed.next().await {
				if count > since.1 {
					break;
				}
				all_changes.entry(count).or_default().insert(user_id.into());
			}
		}

		let mut user_devices = HashMap::<OwnedUserId, Vec<OwnedDeviceId>>::new();
		for users in all_changes.values() {
			for user_id in users {
				if !user_devices.contains_key(user_id) {
					let devices: Vec<OwnedDeviceId> = self
						.services
						.users
						.all_device_ids(user_id)
						.map(ToOwned::to_owned)
						.collect()
						.await;
					user_devices.insert(user_id.clone(), devices);
				}
			}
		}

		tracing::debug!(
			target: "device_list_debug",
			changes_count = all_changes.len(),
			devices_count = user_devices.len(),
			?since,
			"select_edus_device_changes result"
		);

		build_device_list_edus(all_changes, &user_devices, since, SELECT_EDU_LIMIT)
	}

	/// Look for read receipts in this room
	#[tracing::instrument(name = "receipts", level = "trace", skip(self, server_name))]
	async fn select_edus_receipts(
		&self,
		server_name: &ServerName,
		since: (u64, u64),
	) -> (Option<EduBuf>, u64) {
		let num = Arc::new(std::sync::atomic::AtomicUsize::new(0));
		let receipts: BTreeMap<OwnedRoomId, ReceiptMap> = self
			.services
			.state_cache
			.server_rooms(server_name)
			.map(ToOwned::to_owned)
			.broad_filter_map(|room_id| {
				let num = Arc::clone(&num);
				async move {
					let receipt_map = self.select_edus_receipts_room(&room_id, since, &num).await;

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
			return (None, since.1);
		}

		let receipt_content = Edu::Receipt(ReceiptContent { receipts });

		let mut buf = EduBuf::new();
		serde_json::to_writer(&mut buf, &receipt_content)
			.expect("Failed to serialize Receipt EDU to JSON vec");

		(Some(buf), since.1)
	}

	/// Look for read receipts in this room
	#[tracing::instrument(name = "receipts", level = "trace", skip(self, since, num))]
	async fn select_edus_receipts_room(
		&self,
		room_id: &RoomId,
		since: (u64, u64),
		num: &std::sync::atomic::AtomicUsize,
	) -> ReceiptMap {
		info!(
			target: "receipt_debug",
			%room_id, since_lower = since.0, since_upper = since.1,
			"select_edus_receipts_room: scanning"
		);
		let receipts_stream = self
			.services
			.read_receipt
			.readreceipts_since(room_id, Some(since.0));

		pin_mut!(receipts_stream);
		let mut collected = Vec::new();
		while let Some((user_id, count, read_receipt)) = receipts_stream.next().await {
			trace!(
				target: "receipt_debug",
				%room_id, %user_id, count, since_upper = since.1,
				"select_edus_receipts_room: saw candidate"
			);
			if count > since.1 {
				trace!(
					target: "receipt_debug",
					%room_id, %user_id, count, since_upper = since.1,
					"select_edus_receipts_room: stopping, candidate is past since_upper"
				);
				break;
			}
			if self.services.globals.user_is_local(&user_id) {
				let Ok(event) =
					serde_json::from_str::<AnySyncEphemeralRoomEvent>(read_receipt.json().get())
				else {
					continue;
				};
				let AnySyncEphemeralRoomEvent::Receipt(receipt) = event else {
					continue;
				};
				let Some((event_id, _)) = receipt.content.0.iter().next() else {
					continue;
				};
				let Ok(event_count) = self.services.timeline.get_pdu_count(event_id).await else {
					continue;
				};
				collected.push((
					user_id,
					count,
					read_receipt.json().get().to_owned(),
					event_count,
				));
			}
		}

		collected.sort_by_key(|(_, stream_count, _, event_count)| (*event_count, *stream_count));
		info!(
			target: "receipt_debug",
			%room_id, collected = collected.len(),
			"select_edus_receipts_room: done scanning"
		);

		build_receipt_map(
			collected
				.into_iter()
				.map(|(user_id, count, read_receipt_json, _)| (user_id, count, read_receipt_json))
				.collect(),
			since,
			SELECT_RECEIPT_LIMIT,
			num,
		)
	}

	/// Look for presence
	// TODO: presence updates are batched per server via `pending_updates`,
	// but server_sees_user still hits the DB per user to check shared rooms.
	// Consider caching which servers each user is visible to, to avoid these DB calls.
	#[tracing::instrument(name = "presence", level = "trace", skip(self, server_name, since))]
	async fn select_edus_presence(
		&self,
		server_name: &ServerName,
		since: (u64, u64),
	) -> (Option<EduBuf>, u64) {
		let Some((_, mut users)) = self.services.presence.pending_updates.remove(server_name)
		else {
			return (None, since.1);
		};

		if users.is_empty() {
			return (None, since.1);
		}

		let mut presence_updates = Vec::with_capacity(users.len().min(SELECT_PRESENCE_LIMIT));
		let mut attempted_users = Vec::with_capacity(SELECT_PRESENCE_LIMIT);
		let mut loop_count = 0_usize;
		for user_id in users.iter().cloned() {
			loop_count = loop_count.saturating_add(1);
			if loop_count > SELECT_PRESENCE_LIMIT * 5 {
				break;
			}

			let Ok((_, presence_event)) = self
				.services
				.presence
				.get_presence_with_count(&user_id)
				.await
				.log_err()
			else {
				attempted_users.push(user_id.clone());
				continue;
			};

			attempted_users.push(user_id.clone());

			// Send-time visibility check. Only send to servers that still see the user.
			if !self
				.services
				.state_cache
				.server_sees_user(server_name, &user_id)
				.await
			{
				continue;
			}

			let update = PresenceUpdate {
				user_id,
				presence: presence_event.content.presence,
				currently_active: presence_event.content.currently_active.unwrap_or(false),
				status_msg: presence_event.content.status_msg,
				last_active_ago: presence_event
					.content
					.last_active_ago
					.unwrap_or_else(|| uint!(0)),
			};

			presence_updates.push(update);
			if presence_updates.len() >= SELECT_PRESENCE_LIMIT {
				break;
			}
		}

		// remove only the users we attempted to process from this batch
		if !attempted_users.is_empty() {
			let attempted_users_set: HashSet<_> = attempted_users.iter().collect();
			users.retain(|user_id| !attempted_users_set.contains(user_id));
		}

		// put any remaining users in line for next batch
		if !users.is_empty() {
			self.services
				.presence
				.pending_updates
				.entry(server_name.to_owned())
				.or_default()
				.extend(users);
		}

		if presence_updates.is_empty() {
			return (None, since.1);
		}

		self.stats
			.outgoing_presence
			.fetch_add(presence_updates.len().try_into().unwrap_or(u64::MAX), Ordering::Relaxed);

		let presence_content = Edu::Presence(PresenceContent { push: presence_updates });

		let mut buf = EduBuf::new();
		serde_json::to_writer(&mut buf, &presence_content)
			.expect("failed to serialize Presence EDU to JSON");

		(Some(buf), since.1)
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
			return Err(Box::new((
				Destination::Appservice(id.clone()),
				err!(Database(warn!(?id, "Missing appservice registration"))),
			)));
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
			| Err(e) => Err(Box::new((Destination::Appservice(id), e))),
		}
	}

	#[tracing::instrument(
		name = "push",
		level = "trace",
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
			return Err(Box::new((
				Destination::Push(user_id.clone(), pushkey.clone()),
				err!(Database(error!(%user_id, ?pushkey, "Missing pusher"))),
			)));
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
				let thread_counts = self
					.services
					.user
					.thread_notification_counts(&user_id, &room_id)
					.await;
				let thread_total_notifications = thread_counts
					.values()
					.map(|(notifications, _)| *notifications)
					.fold(0_u64, u64::saturating_add);
				self.services
					.user
					.notification_count(&user_id, &room_id)
					.await
					.saturating_add(thread_total_notifications)
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
				.map_err(|e| Box::new((Destination::Push(user_id.clone(), pushkey.clone()), e)));
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
			.map(|edu_buf| {
				let res = serde_json::from_slice(edu_buf);
				if let Err(ref e) = res {
					tracing::error!(
						"Failed to deserialize EDU: {} - JSON: {}",
						e,
						String::from_utf8_lossy(edu_buf)
					);
				}
				res
			})
			.filter_map(Result::ok)
			.collect();

		if pdus.is_empty() && edus.is_empty() {
			if let Some(count) = edu_count {
				info!(
					target: "receipt_debug",
					%server, count,
					"send_events: nothing to send, advancing educount watermark anyway"
				);
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

		let now = MilliSecondsSinceUnixEpoch::now();
		// Monotonic counter seeded from unix-ms at startup, incremented per
		// transaction -- same scheme Synapse's TransactionManager uses. Content
		// hashing isn't needed for uniqueness: the counter alone guarantees each
		// transaction ID this process emits is distinct, and per-process
		// uniqueness is all the spec requires (`origin` + `txn_id` together
		// identify a transaction).
		let txn_id = self.next_txn_id.fetch_add(1, Ordering::Relaxed).to_string();
		let request = send_transaction_message::v1::Request {
			transaction_id: txn_id.clone().into(),
			origin: self.server.name.clone(),
			origin_server_ts: now,
			pdus,
			edus,
		};

		tracing::info!(target: "federation_debug", dest = ?server, "Sending federation request to server!");
		let result = self
			.send_federation_request_on(&self.services.client.sender, &server, request)
			.await;
		tracing::info!(target: "federation_debug", dest = ?server, "Finished sending federation request! Result: {:?}", result.is_ok());

		for (event_id, result) in result.iter().flat_map(|resp| resp.pdus.iter()) {
			if let Err(e) = result {
				let room_id = self
					.services
					.timeline
					.get_pdu(event_id)
					.await
					.ok()
					.and_then(|pdu| pdu.room_id_or_hash().map(|r| r.to_string()))
					.unwrap_or_default();
				info!(
					%txn_id,
					%server,
					%event_id,
					%room_id,
					remote_error=?e,
					"remote server encountered an error while processing an event"
				);
			}
		}

		match result {
			| Err(error) => {
				self.stats.outgoing_errors.fetch_add(1, Ordering::Relaxed);
				Err(Box::new((Destination::Federation(server), error)))
			},
			| Ok(_) => {
				if let Some(count) = edu_count {
					info!(
						target: "receipt_debug",
						%server, count,
						"send_events: transaction delivered, advancing educount watermark"
					);
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

pub(crate) fn build_device_list_edus(
	all_changes: BTreeMap<u64, HashSet<OwnedUserId>>,
	user_devices: &HashMap<OwnedUserId, Vec<OwnedDeviceId>>,
	since: (u64, u64),
	limit: usize,
) -> (EduVec, u64) {
	let mut events = EduVec::new();
	let mut device_list_changes = HashSet::<OwnedUserId>::new();
	let mut max_processed_count = since.0;
	let mut limited = false;

	for (count, users) in all_changes {
		let mut consumed_all = true;
		for user_id in users {
			if !device_list_changes.insert(user_id.clone()) {
				continue;
			}

			let devices = user_devices.get(&user_id);
			if let Some(devices) = devices.filter(|d| !d.is_empty()) {
				let mut user_consumed_all = true;
				for device_id in devices {
					if events.len() >= limit {
						limited = true;
						user_consumed_all = false;
						break;
					}

					let edu = Edu::DeviceListUpdate(DeviceListUpdateContent {
						user_id: user_id.clone(),
						device_id: device_id.clone(),
						device_display_name: Some("Placeholder".to_owned()),
						stream_id: UInt::try_from(count).unwrap_or_else(|_| uint!(1)),
						prev_id: Vec::new(),
						deleted: None,
						keys: None,
					});

					let mut buf = EduBuf::new();
					serde_json::to_writer(&mut buf, &edu)
						.expect("failed to serialize device list update to JSON");

					events.push(buf);
				}

				if !user_consumed_all {
					consumed_all = false;
					device_list_changes.remove(&user_id);
					break;
				}
			} else {
				if events.len() >= limit {
					limited = true;
					consumed_all = false;
					device_list_changes.remove(&user_id);
					break;
				}

				// Empty prev id forces synapse to resync; because synapse resyncs,
				// we can just insert placeholder data. The stream_id uses the actual
				// change count to ensure each update produces a unique EDU payload,
				// preventing transaction cache poisoning on the remote server.
				let edu = Edu::DeviceListUpdate(DeviceListUpdateContent {
					user_id,
					device_id: device_id!("placeholder").to_owned(),
					device_display_name: Some("Placeholder".to_owned()),
					stream_id: UInt::try_from(count).unwrap_or_else(|_| uint!(1)),
					prev_id: Vec::new(),
					deleted: None,
					keys: None,
				});

				let mut buf = EduBuf::new();
				serde_json::to_writer(&mut buf, &edu)
					.expect("failed to serialize device list update to JSON");

				events.push(buf);
			}
		}

		if consumed_all {
			max_processed_count = count;
		}

		if limited {
			break;
		}
	}

	if !limited {
		max_processed_count = since.1;
	}

	(events, max_processed_count)
}

pub(crate) fn build_receipt_map(
	receipts: Vec<(OwnedUserId, u64, String)>,
	since: (u64, u64),
	limit: usize,
	num: &std::sync::atomic::AtomicUsize,
) -> ReceiptMap {
	use std::{collections::btree_map::Entry, sync::atomic::Ordering};
	let mut read = BTreeMap::<OwnedUserId, ReceiptData>::new();

	for (user_id, count, read_receipt_json) in receipts {
		if count > since.1 {
			continue;
		}

		let Ok(event) = serde_json::from_str::<AnySyncEphemeralRoomEvent>(&read_receipt_json)
		else {
			continue;
		};

		let AnySyncEphemeralRoomEvent::Receipt(r) = event else {
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

		let is_unthreaded =
			matches!(receipt.thread, ruma::events::receipt::ReceiptThread::Unthreaded);
		let receipt_data = ReceiptData {
			data: receipt,
			event_ids: vec![event_id.clone()],
		};

		match read.entry(user_id) {
			| Entry::Vacant(e) => {
				let mut current = num.load(Ordering::Relaxed);
				loop {
					if current >= limit {
						break;
					}
					match num.compare_exchange_weak(
						current,
						current.saturating_add(1),
						Ordering::Relaxed,
						Ordering::Relaxed,
					) {
						| Ok(_) => break,
						| Err(observed) => current = observed,
					}
				}
				if current >= limit {
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
	use std::{
		collections::{BTreeMap, HashMap, HashSet},
		sync::atomic::AtomicUsize,
	};

	use ruma::user_id;

	use super::*;

	#[test]
	fn test_build_device_list_edus_empty() {
		let all_changes = BTreeMap::new();
		let since = (10, 20);
		let (events, max_processed_count) =
			build_device_list_edus(all_changes, &HashMap::new(), since, 100);
		assert!(events.is_empty());
		assert_eq!(max_processed_count, 20);
	}

	#[test]
	fn test_build_device_list_edus_under_limit() {
		let mut all_changes = BTreeMap::new();
		let mut users_15 = HashSet::new();
		users_15.insert(user_id!("@alice:example.com").to_owned());
		all_changes.insert(15, users_15);

		let mut users_18 = HashSet::new();
		users_18.insert(user_id!("@bob:example.com").to_owned());
		all_changes.insert(18, users_18);

		let since = (10, 20);
		let (events, max_processed_count) =
			build_device_list_edus(all_changes, &HashMap::new(), since, 100);
		assert_eq!(events.len(), 2);
		assert_eq!(max_processed_count, 20);
	}

	#[test]
	fn test_build_device_list_edus_over_limit_exact_count_boundary() {
		let mut all_changes = BTreeMap::new();

		let mut users_15 = HashSet::new();
		users_15.insert(user_id!("@alice:example.com").to_owned());
		users_15.insert(user_id!("@bob:example.com").to_owned());
		all_changes.insert(15, users_15);

		let mut users_18 = HashSet::new();
		users_18.insert(user_id!("@charlie:example.com").to_owned());
		all_changes.insert(18, users_18);

		let since = (10, 20);
		let (events, max_processed_count) =
			build_device_list_edus(all_changes, &HashMap::new(), since, 2);
		assert_eq!(events.len(), 2);
		assert_eq!(max_processed_count, 15);
	}

	#[test]
	fn test_build_device_list_edus_over_limit_mid_count_boundary() {
		let mut all_changes = BTreeMap::new();

		let mut users_15 = HashSet::new();
		users_15.insert(user_id!("@alice:example.com").to_owned());
		users_15.insert(user_id!("@bob:example.com").to_owned());
		users_15.insert(user_id!("@charlie:example.com").to_owned());
		all_changes.insert(15, users_15);

		let since = (10, 20);
		let (events, max_processed_count) =
			build_device_list_edus(all_changes, &HashMap::new(), since, 2);
		assert_eq!(events.len(), 2);
		assert_eq!(max_processed_count, 10);
	}

	#[test]
	fn test_build_device_list_edus_deduplication() {
		let mut all_changes = BTreeMap::new();

		let mut users_15 = HashSet::new();
		users_15.insert(user_id!("@alice:example.com").to_owned());
		all_changes.insert(15, users_15);

		let mut users_18 = HashSet::new();
		// Same user!
		users_18.insert(user_id!("@alice:example.com").to_owned());
		users_18.insert(user_id!("@bob:example.com").to_owned());
		all_changes.insert(18, users_18);

		let since = (10, 20);
		let (events, max_processed_count) =
			build_device_list_edus(all_changes, &HashMap::new(), since, 100);
		// Alice should only produce 1 event, plus bob = 2 events total
		assert_eq!(events.len(), 2);
		assert_eq!(max_processed_count, 20);
	}

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
		assert!(matches!(data.data.thread, ruma::events::receipt::ReceiptThread::Unthreaded));
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
		assert!(matches!(data.data.thread, ruma::events::receipt::ReceiptThread::Unthreaded));
		assert_eq!(data.data.ts.map(|t| t.0.into()), Some(12345_u64));
	}

	#[test]
	fn test_build_receipt_map_out_of_order_count_does_not_skip_in_range_receipt() {
		let mut receipts = Vec::new();
		let user_id = user_id!("@alice:example.com").to_owned();

		// An out-of-order receipt with a higher stream count comes first.
		let json_late = serde_json::json!({
			"type": "m.receipt",
			"content": {
				"$event1": {
					"m.read": {
						"@alice:example.com": {
							"ts": 10000
						}
					}
				}
			}
		});
		receipts.push((user_id.clone(), 30, json_late.to_string()));

		// The in-range receipt should still be considered even though it appears later.
		let json_in_range = serde_json::json!({
			"type": "m.receipt",
			"content": {
				"$event2": {
					"m.read": {
						"@alice:example.com": {
							"ts": 12345
						}
					}
				}
			}
		});
		receipts.push((user_id.clone(), 12, json_in_range.to_string()));

		let since = (10, 20);
		let num = AtomicUsize::new(0);
		let map = build_receipt_map(receipts, since, 100, &num);

		assert_eq!(map.read.len(), 1);
		assert_eq!(num.load(Ordering::Relaxed), 1);
		assert_eq!(map.read[&user_id].event_ids, vec!["$event2".to_owned()]);
	}
}
