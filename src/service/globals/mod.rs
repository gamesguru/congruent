mod data;

use std::{collections::HashMap, fmt::Write, mem::size_of, sync::Arc, time::Instant};

use async_trait::async_trait;
use conduwuit::{Result, Server, SyncRwLock, error, utils::bytes::pretty};
use data::Data;
use regex::RegexSet;
use ruma::{OwnedEventId, OwnedRoomAliasId, OwnedServerName, OwnedUserId, ServerName, UserId};

use crate::service;

pub struct Service {
	pub db: Data,
	server: Arc<Server>,

	pub bad_event_ratelimiter: Arc<SyncRwLock<HashMap<OwnedEventId, RateLimitState>>>,
	pub server_user: OwnedUserId,
	pub admin_alias: OwnedRoomAliasId,
	pub turn_secret: String,
}

type RateLimitState = (Instant, u32); // Time if last failed try, number of failed tries

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let db = Data::new(&args);
		let config = &args.server.config;

		let turn_secret = config.turn_secret_file.as_ref().map_or_else(
			|| config.turn_secret.clone(),
			|path| match std::fs::read_to_string(path) {
				| Ok(secret) => secret.trim().to_owned(),
				| Err(e) => {
					error!("Failed to read the TURN secret file: {e}");

					config.turn_secret.clone()
				},
			},
		);

		Ok(Arc::new(Self {
			db,
			server: args.server.clone(),
			bad_event_ratelimiter: Arc::new(SyncRwLock::new(HashMap::new())),
			admin_alias: OwnedRoomAliasId::try_from(format!("#admins:{}", args.server.name))
				.expect("#admins:server_name is valid alias name"),
			server_user: UserId::parse_with_server_name(
				String::from("conduit"),
				&args.server.name,
			)
			.expect("@conduit:server_name is valid"),
			turn_secret,
		}))
	}

	async fn memory_usage(&self, out: &mut (dyn Write + Send)) -> Result {
		let (ber_count, ber_bytes) = self.bad_event_ratelimiter.read().iter().fold(
			(0_usize, 0_usize),
			|(mut count, mut bytes), (event_id, _)| {
				bytes = bytes.saturating_add(event_id.capacity());
				bytes = bytes.saturating_add(size_of::<RateLimitState>());
				count = count.saturating_add(1);
				(count, bytes)
			},
		);

		writeln!(out, "bad_event_ratelimiter: {ber_count} ({})", pretty(ber_bytes))?;

		Ok(())
	}

	async fn clear_cache(&self) { self.bad_event_ratelimiter.write().clear(); }

	fn name(&self) -> &str { service::make_name(std::module_path!()) }
}

impl Service {
	#[inline]
	pub fn next_count(&self) -> Result<u64> { self.db.next_count() }

	#[inline]
	pub fn next_count_batch(&self, diff: u64) -> Result<u64> { self.db.next_count_batch(diff) }

	#[inline]
	pub fn current_count(&self) -> Result<u64> { Ok(self.db.current_count()) }

	#[inline]
	#[must_use]
	pub fn server_name(&self) -> &ServerName { self.server.name.as_ref() }

	#[must_use]
	pub fn allow_public_room_directory_over_federation(&self) -> bool {
		self.server
			.config
			.allow_public_room_directory_over_federation
	}

	#[must_use]
	pub fn allow_device_name_federation(&self) -> bool {
		self.server.config.allow_device_name_federation
	}

	#[must_use]
	pub fn allow_room_creation(&self) -> bool { self.server.config.allow_room_creation }

	#[must_use]
	pub fn new_user_displayname_suffix(&self) -> &String {
		&self.server.config.new_user_displayname_suffix
	}

	#[must_use]
	pub fn allow_announcements_check(&self) -> bool {
		self.server.config.allow_announcements_check
	}

	#[must_use]
	pub fn trusted_servers(&self) -> &[OwnedServerName] { &self.server.config.trusted_servers }

	#[must_use]
	pub fn turn_password(&self) -> &String { &self.server.config.turn_password }

	#[must_use]
	pub fn turn_ttl(&self) -> u64 { self.server.config.turn_ttl }

	#[must_use]
	pub fn turn_uris(&self) -> &[String] { &self.server.config.turn_uris }

	#[must_use]
	pub fn turn_username(&self) -> &String { &self.server.config.turn_username }

	#[must_use]
	pub fn notification_push_path(&self) -> &String { &self.server.config.notification_push_path }

	#[must_use]
	pub fn url_preview_domain_contains_allowlist(&self) -> &Vec<String> {
		&self.server.config.url_preview_domain_contains_allowlist
	}

	#[must_use]
	pub fn url_preview_domain_explicit_allowlist(&self) -> &Vec<String> {
		&self.server.config.url_preview_domain_explicit_allowlist
	}

	#[must_use]
	pub fn url_preview_domain_explicit_denylist(&self) -> &Vec<String> {
		&self.server.config.url_preview_domain_explicit_denylist
	}

	#[must_use]
	pub fn url_preview_url_contains_allowlist(&self) -> &Vec<String> {
		&self.server.config.url_preview_url_contains_allowlist
	}

	#[must_use]
	pub fn url_preview_max_spider_size(&self) -> usize {
		self.server.config.url_preview_max_spider_size
	}

	#[must_use]
	pub fn url_preview_check_root_domain(&self) -> bool {
		self.server.config.url_preview_check_root_domain
	}

	#[must_use]
	pub fn url_preview_allow_audio_video(&self) -> bool {
		self.server.config.url_preview_allow_audio_video
	}

	#[must_use]
	pub fn forbidden_alias_names(&self) -> &RegexSet { &self.server.config.forbidden_alias_names }

	#[must_use]
	pub fn forbidden_usernames(&self) -> &RegexSet { &self.server.config.forbidden_usernames }

	/// checks if `user_id` is local to us via server_name comparison
	#[inline]
	#[must_use]
	pub fn user_is_local(&self, user_id: &UserId) -> bool {
		self.server_is_ours(user_id.server_name())
	}

	#[inline]
	#[must_use]
	pub fn server_is_ours(&self, server_name: &ServerName) -> bool {
		server_name == self.server_name()
	}
}
