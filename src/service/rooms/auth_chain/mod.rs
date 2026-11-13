mod data;

use std::{fmt::Debug, sync::Arc, time::Instant};

use conduwuit::{
	Result, debug, implement, info,
	utils::{IterStream, MutexMap, stream::TryBroadbandExt},
};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use rezzy::bitmap::Bitmap64;
use slipstream::{EventId, OwnedEventId, RoomId};

use self::data::Data;
use crate::{
	Dep, rooms,
	rooms::short::{ShortEventId, ShortRoomId},
};

/// An auth chain and whether every reachable event has recorded auth edges.
/// Partial chains are intended for diagnostics, not federation responses.
#[derive(Debug)]
pub struct AuthChain<T = Vec<ShortEventId>> {
	pub ids: T,
	pub complete: bool,
}

impl<T> AuthChain<T> {
	/// Reject partial ancestry before exposing a federation response.
	pub fn require_complete(self) -> Result<T> {
		if !self.complete {
			return Err(conduwuit::err!(Database("auth chain is incomplete")));
		}
		Ok(self.ids)
	}
}

pub struct Service {
	services: Services,
	db: Data,
	mutex_fetch: MutexMap<OwnedEventId, ()>,
}

struct Services {
	short: Dep<rooms::short::Service>,
	timeline: Dep<rooms::timeline::Service>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: Services {
				short: args.depend::<rooms::short::Service>("rooms::short"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
			},
			db: Data::new(&args),
			mutex_fetch: MutexMap::new(),
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
pub fn event_ids_iter<'a, I>(
	&'a self,
	room_id: &'a RoomId,
	starting_events: I,
) -> impl Stream<Item = Result<OwnedEventId>> + Send + 'a
where
	I: Iterator<Item = &'a EventId> + Clone + Debug + ExactSizeIterator + Send + 'a,
{
	self.get_auth_chain(room_id, starting_events)
		.and_then(move |chain| async move {
			let chain = chain.require_complete()?;
			Ok(self
				.services
				.short
				.multi_get_eventid_from_short(chain.into_iter().stream()))
		})
		.try_flatten_stream()
}

#[implement(Service)]
pub async fn get_auth_chain<'a, I>(
	&'a self,
	room_id: &RoomId,
	starting_events: I,
) -> Result<AuthChain>
where
	I: Iterator<Item = &'a EventId> + Clone + Debug + ExactSizeIterator + Send + 'a,
{
	let chain = self.get_auth_chain_bitmap(room_id, starting_events).await?;
	Ok(AuthChain {
		ids: chain.ids.iter().collect(),
		complete: chain.complete,
	})
}

#[implement(Service)]
pub async fn get_auth_chain_bitmap<'a, I>(
	&'a self,
	room_id: &RoomId,
	starting_events: I,
) -> Result<AuthChain<Bitmap64>>
where
	I: Iterator<Item = &'a EventId> + Clone + Debug + ExactSizeIterator + Send + 'a,
{
	let started = Instant::now();
	let shortroomid = self.services.short.get_shortroomid(room_id).await?;
	let starting_ids: Vec<(ShortEventId, &EventId)> = self
		.services
		.short
		.multi_get_or_create_shorteventid(starting_events.clone())
		.zip(starting_events.stream())
		.collect()
		.await;

	debug!(
		starting_events = ?starting_ids.len(),
		elapsed = ?started.elapsed(),
		"start",
	);

	let mut full_auth_chain = Bitmap64::new();
	let mut complete = true;
	let mut uncached = Vec::new();

	// Parallel check for starting events already in cache
	let cache_checks = starting_ids
		.into_iter()
		.try_stream::<conduwuit::Error>()
		.broad_and_then(|(shortid, event_id)| async move {
			// Do not independently translate each cold persisted closure.
			let key = [shortroomid, shortid];
			let res = self
				.db
				.auth_chain_cache
				.lock()
				.get_mut(key.as_slice())
				.cloned()
				.ok_or_else(|| conduwuit::err!(Request(NotFound("auth chain not in RAM"))));
			Ok((shortid, event_id, res))
		})
		.try_collect::<Vec<_>>()
		.await?;

	for (shortid, event_id, cache_res) in cache_checks {
		if let Ok(cached) = cache_res {
			full_auth_chain |= cached.as_ref();
			full_auth_chain.insert(shortid);
		} else {
			uncached.push((shortid, event_id));
		}
	}

	// Merge cold closures in room-local space before translating their union.
	if uncached.len() > 1 {
		if let Some(union) = self
			.db
			.get_cached_authchain_union(shortroomid, &uncached)
			.await?
		{
			full_auth_chain |= union.as_ref();
			for (id, _) in &uncached {
				full_auth_chain.insert(*id);
			}
			uncached.clear();
		}
	}

	// Sequential walk for uncached starting events
	for (shortid, event_id) in uncached {
		let _guard = self.mutex_fetch.lock(event_id).await;

		// Re-check cache under lock in case a concurrent walk populated it
		if let Some(cached) = self
			.get_optional_cached_authchain(shortroomid, shortid)
			.await?
		{
			full_auth_chain |= cached.as_ref();
			full_auth_chain.insert(shortid);
			continue;
		}

		let (auth_chain, is_complete) = self
			.get_auth_chain_inner(room_id, shortroomid, event_id, shortid)
			.await?;
		complete &= is_complete;
		if is_complete {
			self.cache_auth_chain_bitmap(shortroomid, shortid, &auth_chain);
		}

		full_auth_chain |= &auth_chain;
		full_auth_chain.insert(shortid);
	}

	// Cached chains may predate the derived create event being recorded.
	complete &= self
		.add_derived_create_event(room_id, None, &mut full_auth_chain)
		.await;

	info!(
		chain_length = ?full_auth_chain.len(),
		elapsed = ?started.elapsed(),
		"done",
	);

	Ok(AuthChain { ids: full_auth_chain, complete })
}

#[implement(Service)]
async fn get_auth_chain_inner(
	&self,
	room_id: &RoomId,
	shortroomid: ShortRoomId,
	event_id: &EventId,
	shortid: ShortEventId,
) -> Result<(Bitmap64, bool)> {
	let mut todo = vec![shortid];
	let mut found = Bitmap64::new();
	let mut is_complete = true;

	let started = Instant::now();
	let mut last_progress = Instant::now();

	while !todo.is_empty() {
		if last_progress.elapsed().as_secs() >= 30 {
			info!(%room_id, found = found.len(), queue = todo.len(), elapsed = ?started.elapsed(), "auth_chain walk in progress");
			last_progress = Instant::now();
		}

		let current_batch = std::mem::take(&mut todo);
		let mut next_short_ids = Vec::new();

		let batch_results: Vec<_> = self
			.services
			.timeline
			.multi_get_shortauthevents(futures::stream::iter(current_batch))
			.collect()
			.await;

		for res in batch_results {
			match res {
				| Ok(auth_shorts) =>
					for auth_short in auth_shorts {
						if found.insert(auth_short) {
							if let Some(cached) = self
								.get_optional_cached_authchain(shortroomid, auth_short)
								.await?
							{
								found |= cached.as_ref();
							} else {
								next_short_ids.push(auth_short);
							}
						}
					},
				| Err(error) if error.is_not_found() => {
					// Auth edges come only from mtxdb. A missing locator or edge row
					// means the offline import was incomplete; never reconstruct the
					// walk from legacy PDU data.
					debug!(%room_id, ?error, "Could not read mtxdb auth edges; marking auth chain as incomplete");
					is_complete = false;
				},
				| Err(error) => return Err(error),
			}
		}

		todo.extend(next_short_ids);
	}

	if !self
		.add_derived_create_event(room_id, Some(event_id), &mut found)
		.await
	{
		is_complete = false;
	}

	Ok((found, is_complete))
}

/// Adds the `m.room.create` event whose ID is derived from the room ID (room
/// versions where the room ID is the create event's hash) to `chain`.
///
/// Returns `false` when a short-ID mapping for the create event exists but its
/// PDU is unavailable, meaning the chain would be incomplete and must not be
/// cached. Rooms with no derivable create event are unaffected and return
/// `true`.
#[implement(Service)]
async fn add_derived_create_event(
	&self,
	room_id: &RoomId,
	exclude: Option<&EventId>,
	chain: &mut Bitmap64,
) -> bool {
	let Some(room_id_without_sigil) = room_id.as_str().strip_prefix('!') else {
		return true;
	};
	let create_id = format!("${room_id_without_sigil}");
	let Ok(create_id) = EventId::parse(create_id.as_str()) else {
		return true;
	};
	if exclude.is_some_and(|event_id| event_id == &create_id) {
		return true;
	}
	let Ok(create_short) = self.services.short.get_shorteventid(&create_id).await else {
		return true;
	};
	if self
		.services
		.timeline
		.get_pdu_in_room(Some(room_id), &create_id)
		.is_err()
	{
		return false;
	}

	chain.insert(create_short);
	true
}

#[implement(Service)]
#[inline]
pub async fn get_cached_eventid_authchain(
	&self,
	shortroomid: ShortRoomId,
	shorteventid: ShortEventId,
) -> Result<Arc<Bitmap64>> {
	self.db
		.get_cached_eventid_authchain(shortroomid, shorteventid)
		.await
}

/// A missing closure permits an edge walk; a failed storage read does not.
#[implement(Service)]
async fn get_optional_cached_authchain(
	&self,
	room: ShortRoomId,
	event: ShortEventId,
) -> Result<Option<Arc<Bitmap64>>> {
	match self.get_cached_eventid_authchain(room, event).await {
		| Ok(chain) => Ok(Some(chain)),
		| Err(error) if error.is_not_found() => Ok(None),
		| Err(error) => Err(error),
	}
}

/// Reload the persisted closure for an event, replacing any RAM entry.
#[implement(Service)]
pub async fn reload_persisted_eventid_authchain(
	&self,
	shortroomid: ShortRoomId,
	shorteventid: ShortEventId,
) -> Result<Arc<Bitmap64>> {
	self.db
		.load_persisted_authchain(shortroomid, shorteventid)
		.await
}

#[implement(Service)]
pub fn cache_auth_chain_bitmap(
	&self,
	shortroomid: ShortRoomId,
	shorteventid: ShortEventId,
	auth_chain: &Bitmap64,
) {
	self.db
		.cache_auth_chain(shortroomid, shorteventid, Arc::new(auth_chain.clone()));
}

#[implement(Service)]
pub fn get_cache_usage(&self) -> (usize, usize) {
	let cache = self.db.auth_chain_cache.lock();

	(cache.len(), cache.capacity())
}

#[implement(Service)]
pub fn clear_cache(&self) { self.db.auth_chain_cache.lock().clear(); }

#[implement(Service)]
pub async fn clear_db_cache(&self) { self.db.clear_db_cache().await; }

#[cfg(test)]
mod tests;
