use std::sync::Arc;

use conduwuit::{
	Result, debug_warn, utils,
	utils::{ReadyExt, stream::TryIgnore},
};
use database::{Deserialized, Json, Map};
use futures::Stream;
use moka::sync::Cache;
use ruma::{UInt, UserId, events::presence::PresenceEvent, presence::PresenceState};

use super::Presence;
use crate::{Dep, globals, users};

pub(crate) struct Data {
	presenceid_presence: Arc<Map>,
	userid_presenceid: Arc<Map>,
	presence_cache: Cache<ruma::OwnedUserId, Arc<(u64, Presence)>>,
	services: Services,
}

struct Services {
	globals: Dep<globals::Service>,
	users: Dep<users::Service>,
}

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;

		let cache_capacity = utils::math::usize_from_f64(
			f64::from(args.server.config.presence_cache_capacity)
				* args.server.config.cache_capacity_modifier,
		)
		.expect("valid cache size")
		.try_into()
		.unwrap_or(args.server.config.presence_cache_capacity);

		Self {
			presenceid_presence: db["presenceid_presence"].clone(),
			userid_presenceid: db["userid_presenceid"].clone(),
			presence_cache: Cache::builder().max_capacity(cache_capacity.into()).build(),
			services: Services {
				globals: args.depend::<globals::Service>("globals"),
				users: args.depend::<users::Service>("users"),
			},
		}
	}

	pub(super) async fn get_presence_raw(&self, user_id: &UserId) -> Result<(u64, Presence)> {
		if let Some(cached) = self.presence_cache.get(&user_id.to_owned()) {
			return Ok((*cached).clone());
		}

		let count = self
			.userid_presenceid
			.get(user_id)
			.await
			.deserialized::<u64>()?;

		let key = presenceid_key(count, user_id);
		let bytes = self.presenceid_presence.get(&key).await?;
		let presence = Presence::from_json_bytes(&bytes)?;

		self.presence_cache
			.insert(user_id.to_owned(), Arc::new((count, presence.clone())));

		Ok((count, presence))
	}

	pub(super) async fn get_presence(&self, user_id: &UserId) -> Result<(u64, PresenceEvent)> {
		let raw = self.get_presence_raw(user_id).await?;
		let event = raw.1.to_presence_event(user_id, &self.services.users).await;
		Ok((raw.0, event))
	}

	pub(super) fn clear_cache(&self) { self.presence_cache.invalidate_all(); }

	pub(super) async fn set_presence(
		&self,
		user_id: &UserId,
		presence_state: &PresenceState,
		currently_active: Option<bool>,
		last_active_ago: Option<UInt>,
		status_msg: Option<String>,
	) -> Result<()> {
		let last_presence = self.get_presence_raw(user_id).await;
		let state_changed = match last_presence {
			| Err(_) => true,
			| Ok(ref presence) => presence.1.state != *presence_state,
		};

		let status_msg_changed = match last_presence {
			| Err(_) => true,
			| Ok(ref last_presence) => {
				let old_msg = last_presence.1.status_msg.clone().unwrap_or_default();

				let new_msg = status_msg.clone().unwrap_or_default();

				new_msg != old_msg
			},
		};

		let now = utils::millis_since_unix_epoch();
		let last_last_active_ts = match last_presence {
			| Err(_) => 0,
			| Ok((_, ref presence)) => presence.last_active_ts,
		};

		let last_active_ts = match last_active_ago {
			| None => now,
			| Some(last_active_ago) => now.saturating_sub(last_active_ago.into()),
		};

		// TODO: tighten for state flicker?
		if !status_msg_changed && !state_changed && last_active_ts < last_last_active_ts {
			debug_warn!(
				"presence spam {user_id:?} last_active_ts:{last_active_ts:?} < \
				 {last_last_active_ts:?}",
			);
			return Ok(());
		}

		let status_msg = if status_msg.as_ref().is_some_and(String::is_empty) {
			None
		} else {
			status_msg
		};

		let presence = Presence::new(
			presence_state.to_owned(),
			currently_active.unwrap_or(false),
			last_active_ts,
			status_msg,
		);

		let count = self.services.globals.next_count()?;
		let key = presenceid_key(count, user_id);

		self.presenceid_presence.raw_put(key, Json(&presence));
		self.userid_presenceid.raw_put(user_id, count);

		self.presence_cache
			.insert(user_id.to_owned(), Arc::new((count, presence)));

		if let Ok((last_count, _)) = last_presence {
			let key = presenceid_key(last_count, user_id);
			self.presenceid_presence.remove(&key);
		}

		Ok(())
	}

	pub(super) fn set_offline_fast(&self, user_id: &UserId, count: u64, presence: Presence) {
		let key = presenceid_key(count, user_id);
		self.presenceid_presence.raw_put(key, Json(&presence));
		self.presence_cache
			.insert(user_id.to_owned(), Arc::new((count, presence)));
	}

	pub(super) async fn remove_presence(&self, user_id: &UserId) {
		self.presence_cache.invalidate(&user_id.to_owned());
		let Ok(count) = self
			.userid_presenceid
			.get(user_id)
			.await
			.deserialized::<u64>()
		else {
			return;
		};

		let key = presenceid_key(count, user_id);
		self.presenceid_presence.remove(&key);
		self.userid_presenceid.remove(user_id);
	}

	#[inline]
	pub(super) fn presence_since(
		&self,
		since: u64,
	) -> impl Stream<Item = (&UserId, u64, &[u8])> + Send + '_ {
		self.presenceid_presence
			.raw_stream_from(&(since.saturating_add(1)).to_be_bytes())
			.ignore_err()
			.ready_filter_map(move |(key, presence)| {
				let (count, user_id) = presenceid_parse(key).ok()?;
				(count > since).then_some((user_id, count, presence))
			})
	}
}

#[inline]
fn presenceid_key(count: u64, user_id: &UserId) -> Vec<u8> {
	let cap = size_of::<u64>().saturating_add(user_id.as_bytes().len());
	let mut key = Vec::with_capacity(cap);
	key.extend_from_slice(&count.to_be_bytes());
	key.extend_from_slice(user_id.as_bytes());
	key
}

#[inline]
fn presenceid_parse(key: &[u8]) -> Result<(u64, &UserId)> {
	let (count, user_id) = key.split_at(8);
	let user_id = user_id_from_bytes(user_id)?;
	let count = utils::u64_from_u8(count);

	Ok((count, user_id))
}

/// Parses a `UserId` from bytes.
fn user_id_from_bytes(bytes: &[u8]) -> Result<&UserId> {
	let str: &str = utils::str_from_bytes(bytes)?;
	let user_id: &UserId = str.try_into()?;

	Ok(user_id)
}
