mod data;
mod presence;

use std::{collections::HashSet, sync::Arc, time::Duration};

use async_trait::async_trait;
use conduwuit::{
	Error, Result, Server, checked, debug, debug_info, error, info, result::LogErr, utils, warn,
};
use dashmap::DashMap;
use database::Database;
use futures::{Stream, StreamExt, TryFutureExt};
use loole::{Receiver, Sender};
use ruma::{
	OwnedServerName, OwnedUserId, UInt, UserId, events::presence::PresenceEvent,
	presence::PresenceState,
};
use tokio::time::Instant;

use self::{data::Data, presence::Presence};
use crate::{Dep, globals, users};

pub struct Service {
	timer_channel: (Sender<TimerType>, Receiver<TimerType>),
	pub(super) pending_updates: DashMap<OwnedServerName, HashSet<OwnedUserId>>,
	pub(super) queued_users: dashmap::DashSet<OwnedUserId>,
	timeout_remote_users: bool,
	idle_timeout: u64,
	offline_timeout: u64,
	db: Data,
	services: Services,
}

struct Services {
	server: Arc<Server>,
	db: Arc<Database>,
	globals: Dep<globals::Service>,
	users: Dep<users::Service>,
	sending: Dep<crate::sending::Service>,
	state_cache: Dep<crate::rooms::state_cache::Service>,
}

type TimerType = (OwnedUserId, Option<Duration>);

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let config = &args.server.config;
		let idle_timeout_s = config.presence_idle_timeout_s;
		let offline_timeout_s = config.presence_offline_timeout_s;
		Ok(Arc::new(Self {
			timer_channel: loole::unbounded(),
			pending_updates: DashMap::new(),
			queued_users: dashmap::DashSet::new(),
			timeout_remote_users: config.presence_timeout_remote_users,
			idle_timeout: checked!(idle_timeout_s * 1_000)?,
			offline_timeout: checked!(offline_timeout_s * 1_000)?,
			db: Data::new(&args),
			services: Services {
				server: args.server.clone(),
				db: args.db.clone(),
				globals: args.depend::<globals::Service>("globals"),
				users: args.depend::<users::Service>("users"),
				sending: args.depend::<crate::sending::Service>("sending"),
				state_cache: args
					.depend::<crate::rooms::state_cache::Service>("rooms::state_cache"),
			},
		}))
	}

	async fn clear_cache(&self) { self.db.clear_cache(); }

	async fn worker(self: Arc<Self>) -> Result<()> {
		let receiver = self.timer_channel.1.clone();

		// Resetting dormant online/away statuses to offline on startup
		let startup_task = if self.services.server.config.allow_local_presence {
			let self_ = Arc::clone(&self);
			Some(self.services.server.runtime().spawn(async move {
				self_.unset_all_presence().await;
			}))
		} else {
			None
		};

		let mut presence_timers =
			std::collections::HashMap::<OwnedUserId, tokio::task::JoinHandle<()>>::new();
		let mut events_received: u64 = 0;
		let mut next_tally = Instant::now()
			.checked_add(Duration::from_mins(5))
			.unwrap_or_else(Instant::now);

		let self_flush = Arc::clone(&self);
		let flush_task = self.services.server.runtime().spawn(async move {
			let mut interval = tokio::time::interval(Duration::from_secs(
				self_flush
					.services
					.server
					.config
					.federation_presence_interval_s,
			));
			loop {
				interval.tick().await;
				if !self_flush.services.server.running() {
					break;
				}

				let mut users: Vec<_> = Vec::new();
				self_flush.queued_users.retain(|user_id| {
					users.push(user_id.clone());
					false
				});

				if users.len() > 50 {
					info!(
						target: "stats_verbose",
						"Presence flush task collected {} local users for outbound federation",
						users.len()
					);
				}

				let mut room_users: std::collections::HashMap<
					ruma::OwnedRoomId,
					Vec<OwnedUserId>,
				> = std::collections::HashMap::new();

				let mut iterations = 0_u8;
				for user_id in users {
					let mut joined_rooms = self_flush.services.state_cache.rooms_joined(&user_id);
					while let Some(room_id) = joined_rooms.next().await {
						iterations = iterations.wrapping_add(1);
						if iterations == 0 {
							tokio::task::yield_now().await;
						}
						room_users
							.entry(room_id.to_owned())
							.or_default()
							.push(user_id.clone());
					}
				}

				for (room_id, user_ids) in room_users {
					let mut room_servers = self_flush.services.state_cache.room_servers(&room_id);
					while let Some(server) = room_servers.next().await {
						iterations = iterations.wrapping_add(1);
						if iterations == 0 {
							tokio::task::yield_now().await;
						}
						if !self_flush.services.globals.server_is_ours(server) {
							let mut entry = self_flush
								.pending_updates
								.entry(server.to_owned())
								.or_default();

							for user_id in &user_ids {
								entry.insert(user_id.clone());
							}
						}
					}
				}

				self_flush
					.services
					.server
					.metrics
					.presence_pending_updates
					.store(
						u64::try_from(self_flush.pending_updates.len())
							.expect("failed conversion"),
						std::sync::atomic::Ordering::Relaxed,
					);

				let mut servers: Vec<_> = self_flush
					.pending_updates
					.iter()
					.map(|kv| kv.key().clone())
					.collect();

				// Prevent flooding the sender worker channels by limiting the number of
				// servers flushed per tick.
				// TODO: no magic number (100)
				servers.truncate(
					self_flush
						.services
						.server
						.config
						.sender_workers
						.max(1)
						.saturating_mul(100),
				);

				if !servers.is_empty() {
					let server_refs = servers.iter().map(AsRef::as_ref);
					self_flush
						.services
						.sending
						.flush_servers(futures::stream::iter(server_refs))
						.await
						.ok();
				}
				tokio::task::yield_now().await;
			}
		});

		while !receiver.is_closed() {
			let event = receiver.recv_async().await;
			match event {
				| Err(_) => break,
				| Ok((user_id, Some(timeout))) => {
					events_received = events_received.saturating_add(1);
					let self_clone = Arc::clone(&self);
					let user_id_clone = user_id.clone();

					let new_task = self.services.server.runtime().spawn(async move {
						tokio::time::sleep(timeout).await;
						self_clone
							.process_presence_timer(&user_id_clone)
							.await
							.log_err()
							.ok();

						tokio::task::yield_now().await;
					});

					if let Some(old_task) = presence_timers.insert(user_id, new_task) {
						old_task.abort();
					}
				},
				| Ok((user_id, None)) =>
					if let Some(task) = presence_timers.remove(&user_id) {
						task.abort();
					},
			}

			// Periodic tally
			if Instant::now() >= next_tally {
				presence_timers.retain(|_, task| !task.is_finished());
				info!(
					target: "stats",
					"Presence stats: {} active timers, {} received",
					presence_timers.len(),
					events_received
				);
				events_received = 0;
				next_tally = Instant::now()
					.checked_add(Duration::from_mins(5))
					.unwrap_or_else(Instant::now);
			}
		}

		flush_task.abort();

		for (_, handle) in presence_timers {
			handle.abort();
		}

		if let Some(task) = startup_task {
			_ = task.await;
		}

		Ok(())
	}

	fn interrupt(&self) {
		let (timer_sender, _) = &self.timer_channel;
		if !timer_sender.is_closed() {
			timer_sender.close();
		}
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Returns the latest presence event for the given user.
	#[inline]
	pub async fn get_presence(&self, user_id: &UserId) -> Result<PresenceEvent> {
		self.db
			.get_presence(user_id)
			.map_ok(|(_, presence)| presence)
			.await
	}

	/// Returns user's latest presence event, along with stream ID.
	#[inline]
	pub async fn get_presence_with_count(
		&self,
		user_id: &UserId,
	) -> Result<(u64, PresenceEvent)> {
		self.db.get_presence(user_id).await
	}

	/// Pings the presence of the given user, setting the specified state.
	// TODO: this is called on every profile update, read marker, typing notif,
	// etc. Each call still performs a presence lookup here and additional work
	// in set_presence (including constructing PresenceEvent and any related
	// profile/presence updates), even though get_presence may now be served
	// from an in-memory cache rather than always hitting the DB. Consider
	// reducing how often this is invoked or caching the derived data.
	pub async fn ping_presence(&self, user_id: &UserId, new_state: &PresenceState) -> Result<()> {
		const REFRESH_TIMEOUT: u64 = 60 * 1000;

		// Raw payload is smaller/cheaper in memory (and hits the fast MokaCache)
		let last_presence = self.db.get_presence_raw(user_id).await;
		let state_changed = match last_presence {
			| Err(_) => true,
			| Ok((_, ref presence)) => presence.state != *new_state,
		};

		let now = utils::millis_since_unix_epoch();
		let last_last_active_ago = match last_presence {
			| Err(_) => 0_u64,
			| Ok((_, ref presence)) => now.saturating_sub(presence.last_active_ts),
		};

		if !state_changed && last_last_active_ago < REFRESH_TIMEOUT {
			return Ok(());
		}

		let status_msg = match last_presence {
			| Ok((_, ref presence)) => presence.status_msg.clone(),
			| Err(_) => None,
		};

		let last_active_ago = UInt::new(0);
		let currently_active = *new_state == PresenceState::Online;
		let _cork = self.services.db.cork();
		self.db
			.set_presence(user_id, new_state, Some(currently_active), last_active_ago, status_msg)
			.await?;

		self.schedule_timeout(user_id, new_state)?;
		self.notify_presence_change(user_id).log_err().ok();

		Ok(())
	}

	/// Adds a presence event which will be saved until a new event replaces it.
	///
	/// External callers for APIs and federation. Previous state fetch if
	/// unknown.
	pub async fn set_presence(
		&self,
		user_id: &UserId,
		state: &PresenceState,
		currently_active: Option<bool>,
		last_active_ago: Option<UInt>,
		status_msg: Option<String>,
	) -> Result<()> {
		let presence_state = match state.as_str() {
			| "" => &PresenceState::Offline, // default an empty string to 'offline'
			| &_ => state,
		};

		let _cork = self.services.db.cork();
		self.db
			.set_presence(user_id, presence_state, currently_active, last_active_ago, status_msg)
			.await?;

		self.schedule_timeout(user_id, presence_state)?;
		self.notify_presence_change(user_id).log_err().ok();

		Ok(())
	}

	/// Schedules a presence timeout timer for the given user if applicable.
	fn schedule_timeout(&self, user_id: &UserId, presence_state: &PresenceState) -> Result<()> {
		if (self.timeout_remote_users || self.services.globals.user_is_local(user_id))
			&& user_id != self.services.globals.server_user
		{
			let mut timeout = match presence_state {
				| PresenceState::Online => self.services.server.config.presence_idle_timeout_s,
				| _ => self.services.server.config.presence_offline_timeout_s,
			};

			let jitter = rand::random_range(0..=timeout.max(10) / 10);
			timeout = timeout.saturating_add(jitter);

			self.timer_channel
				.0
				.send((user_id.to_owned(), Some(Duration::from_secs(timeout))))
				.map_err(|e| {
					error!("Failed to add presence timer: {}", e);
					Error::bad_database("Failed to add presence timer")
				})?;
		}

		Ok(())
	}

	/// Removes the presence record for the given user from the database.
	///
	/// TODO: Why is this not used?
	#[allow(dead_code)]
	pub async fn remove_presence(&self, user_id: &UserId) {
		self.db.remove_presence(user_id).await;
	}

	// Unset online/unavailable presence to offline on startup
	pub async fn unset_all_presence(&self) {
		use futures::StreamExt;

		debug_info!("Resetting presence for active users...");
		let mut reset = 0_usize;
		let mut iterations = 0_u8;

		let mut presence_stream = Box::pin(self.db.presence_since(0));
		while let Some((user_id, count, bytes)) = presence_stream.next().await {
			iterations = iterations.wrapping_add(1);
			if iterations == 0 {
				tokio::task::yield_now().await;
			}

			if !self.services.server.running() {
				info!("Shutdown requested during presence reset.");
				break;
			}

			if !self.services.globals.user_is_local(user_id)
				|| user_id == self.services.globals.server_user
			{
				continue;
			}

			let Ok(mut presence) = Presence::from_json_bytes(bytes) else {
				continue;
			};

			if !matches!(
				presence.state,
				PresenceState::Unavailable | PresenceState::Online | PresenceState::Busy
			) {
				continue;
			}

			let user_id = user_id.to_owned();

			presence.state = PresenceState::Offline;
			presence.currently_active = false;

			reset = reset.saturating_add(1);

			self.db.set_offline_fast(&user_id, count, presence);

			// Broadcast offline transition to remotely clear federation backoff timers!
			// This triggers the automated catch-up of missed events from downtime.
			self.notify_presence_change(&user_id).log_err().ok();
		}

		warn!("Presence reset complete: {reset} users reset to offline.");
	}

	/// Returns the most recent presence updates that happened after the event
	/// with id `since`.
	pub fn presence_since(
		&self,
		since: u64,
	) -> impl Stream<Item = (&UserId, u64, &[u8])> + Send + '_ {
		self.db.presence_since(since)
	}

	#[inline]
	pub async fn from_json_bytes_to_event(
		&self,
		bytes: &[u8],
		user_id: &UserId,
	) -> Result<PresenceEvent> {
		let presence = Presence::from_json_bytes(bytes)?;
		let event = presence
			.to_presence_event(user_id, &self.services.users)
			.await;

		Ok(event)
	}

	async fn process_presence_timer(&self, user_id: &OwnedUserId) -> Result<()> {
		let mut presence_state = PresenceState::Offline;
		let mut last_active_ago = None;
		let mut status_msg = None;

		let raw = self.db.get_presence_raw(user_id).await;

		if let Ok((_count, ref presence)) = raw {
			presence_state = presence.state.clone();
			let now = utils::millis_since_unix_epoch();
			last_active_ago =
				Some(UInt::new_saturating(now.saturating_sub(presence.last_active_ts)));
			status_msg.clone_from(&presence.status_msg);
		}

		let new_state = match (&presence_state, last_active_ago.map(u64::from)) {
			| (PresenceState::Online, Some(ago)) if ago >= self.idle_timeout =>
				Some(PresenceState::Unavailable),
			| (PresenceState::Unavailable, Some(ago)) if ago >= self.offline_timeout =>
				Some(PresenceState::Offline),
			| _ => None,
		};

		debug!(
			"Processed presence timer for user '{user_id}': Old state = {presence_state}, New \
			 state = {new_state:?}"
		);

		if let Some(new_state) = new_state {
			let _cork = self.services.db.cork();
			self.db
				.set_presence(user_id, &new_state, Some(false), last_active_ago, status_msg)
				.await?;

			self.schedule_timeout(user_id, &new_state)?;

			// We notify for idle/offline transitions so remote servers eventually
			// see the updated presence state. We have capped the outbound concurrent
			// sending futures to 128 per worker to ensure that this batching doesn't
			// create an outbound I/O storm or deplete DNS resources.
			self.notify_presence_change(user_id).log_err().ok();
		}

		Ok(())
	}

	/// Intelligently batches user presence updates to remote servers
	fn notify_presence_change(&self, user_id: &UserId) -> Result<()> {
		if !self.services.globals.user_is_local(user_id) {
			return Ok(());
		}

		if !self.services.server.config.allow_outgoing_presence {
			return Ok(());
		}

		self.queued_users.insert(user_id.to_owned());

		// 5-sec flusher task in worker() broadcasts updates and wakes up sender
		Ok(())
	}
}
