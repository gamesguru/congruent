mod data;

use std::{collections::HashSet, fmt::Debug, sync::Arc, time::Instant};

use conduwuit::{
	Err, Result, debug, implement, info, trace,
	utils::{
		IterStream, MutexMap,
		stream::{ReadyExt, TryBroadbandExt},
	},
	warn,
};
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use roaring::RoaringTreemap;
use slipstream::{EventId, OwnedEventId, RoomId};

use self::data::Data;
use crate::{
	Dep, rooms,
	rooms::short::{ShortEventId, ShortRoomId},
};

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
		.map_ok(|chain| {
			self.services
				.short
				.multi_get_eventid_from_short(chain.into_iter().stream())
				.ready_filter(Result::is_ok)
		})
		.try_flatten_stream()
}

#[implement(Service)]
#[tracing::instrument(name = "auth_chain", level = "debug", skip_all, fields(room_id = %room_id))]
pub async fn get_auth_chain<'a, I>(
	&'a self,
	room_id: &RoomId,
	starting_events: I,
) -> Result<Vec<ShortEventId>>
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

	let mut full_auth_chain = RoaringTreemap::new();
	let mut uncached = Vec::new();

	// Parallel check for starting events already in cache
	let cache_checks = starting_ids
		.into_iter()
		.try_stream::<conduwuit::Error>()
		.broad_and_then(|(shortid, event_id)| async move {
			let res = self
				.get_cached_eventid_authchain(shortroomid, shortid)
				.await;
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

	// Sequential walk for uncached starting events
	for (shortid, event_id) in uncached {
		let _guard = self.mutex_fetch.lock(event_id).await;

		// Re-check cache under lock in case a concurrent walk populated it
		if let Ok(cached) = self
			.get_cached_eventid_authchain(shortroomid, shortid)
			.await
		{
			full_auth_chain |= cached.as_ref();
			full_auth_chain.insert(shortid);
			continue;
		}

		let (auth_chain, is_complete) = self
			.get_auth_chain_inner(room_id, shortroomid, event_id, shortid)
			.await?;
		if is_complete {
			self.cache_auth_chain_bitmap(shortroomid, shortid, &auth_chain);
		}

		full_auth_chain |= &auth_chain;
		full_auth_chain.insert(shortid);
	}

	// Cached chains may predate the derived create event being recorded.
	self.add_derived_create_event(room_id, None, &mut full_auth_chain)
		.await;

	let full_auth_chain: Vec<ShortEventId> = full_auth_chain.iter().collect();

	info!(
		chain_length = ?full_auth_chain.len(),
		elapsed = ?started.elapsed(),
		"done",
	);

	Ok(full_auth_chain)
}

#[implement(Service)]
#[tracing::instrument(name = "auth_chain_bitmap", level = "debug", skip_all, fields(room_id = %room_id))]
pub async fn get_auth_chain_bitmap<'a, I>(
	&'a self,
	room_id: &RoomId,
	starting_events: I,
) -> Result<RoaringTreemap>
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

	let mut full_auth_chain = RoaringTreemap::new();
	let mut uncached = Vec::new();

	// Parallel check for starting events already in cache
	let cache_checks = starting_ids
		.into_iter()
		.try_stream::<conduwuit::Error>()
		.broad_and_then(|(shortid, event_id)| async move {
			let res = self
				.get_cached_eventid_authchain(shortroomid, shortid)
				.await;
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

	// Sequential walk for uncached starting events
	for (shortid, event_id) in uncached {
		let _guard = self.mutex_fetch.lock(event_id).await;

		// Re-check cache under lock in case a concurrent walk populated it
		if let Ok(cached) = self
			.get_cached_eventid_authchain(shortroomid, shortid)
			.await
		{
			full_auth_chain |= cached.as_ref();
			full_auth_chain.insert(shortid);
			continue;
		}

		let (auth_chain, is_complete) = self
			.get_auth_chain_inner(room_id, shortroomid, event_id, shortid)
			.await?;
		if is_complete {
			self.cache_auth_chain_bitmap(shortroomid, shortid, &auth_chain);
		}

		full_auth_chain |= &auth_chain;
		full_auth_chain.insert(shortid);
	}

	// Cached chains may predate the derived create event being recorded.
	self.add_derived_create_event(room_id, None, &mut full_auth_chain)
		.await;

	info!(
		chain_length = ?full_auth_chain.len(),
		elapsed = ?started.elapsed(),
		"done",
	);

	Ok(full_auth_chain)
}

#[implement(Service)]
#[tracing::instrument(name = "inner", level = "trace", skip(self, room_id))]
async fn get_auth_chain_inner(
	&self,
	room_id: &RoomId,
	shortroomid: ShortRoomId,
	event_id: &EventId,
	shortid: ShortEventId,
) -> Result<(RoaringTreemap, bool)> {
	let mut todo = vec![(shortid, event_id.to_owned())];
	let mut found = RoaringTreemap::new();
	let mut is_complete = true;

	let started = Instant::now();
	let mut last_progress = Instant::now();

	while !todo.is_empty() {
		if last_progress.elapsed().as_secs() >= 30 {
			info!(%room_id, found = found.len(), queue = todo.len(), elapsed = ?started.elapsed(), "auth_chain walk in progress");
			last_progress = Instant::now();
		}

		let current_batch = std::mem::take(&mut todo);
		let (short_ids, event_ids): (Vec<_>, Vec<_>) = current_batch.into_iter().unzip();

		let mut next_short_ids = Vec::new();
		let mut missing_events = Vec::new();

		let batch_results: Vec<_> = self
			.services
			.timeline
			.multi_get_shortauthevents(futures::stream::iter(short_ids.clone()))
			.collect()
			.await;

		for (idx, (res, _short_id)) in batch_results
			.into_iter()
			.zip(short_ids.into_iter())
			.enumerate()
		{
			match res {
				| Ok(auth_shorts) =>
					for auth_short in auth_shorts {
						if found.insert(auth_short) {
							if let Ok(cached) = self
								.get_cached_eventid_authchain(shortroomid, auth_short)
								.await
							{
								found |= cached.as_ref();
							} else {
								next_short_ids.push(auth_short);
							}
						}
					},
				| Err(_) => {
					missing_events.push(event_ids[idx].clone());
				},
			}
		}

		if !next_short_ids.is_empty() {
			let resolved_events: Vec<_> = futures::stream::iter(next_short_ids.clone())
				.zip(
					self.services
						.short
						.multi_get_eventid_from_short::<OwnedEventId, _>(futures::stream::iter(
							next_short_ids,
						)),
				)
				.collect()
				.await;

			for (auth_short, res) in resolved_events {
				if let Ok(auth_event_id) = res {
					todo.push((auth_short, auth_event_id));
				} else {
					is_complete = false;
				}
			}
		}

		if !missing_events.is_empty() {
			let results: Vec<_> = missing_events
				.into_iter()
				.try_stream::<conduwuit::Error>()
				.broad_and_then(|missing_event_id| async move {
					trace!(%missing_event_id, "processing legacy auth event");

					let pdu_result = self
						.services
						.timeline
						.get_pdu_in_room(Some(room_id), &missing_event_id)
						.await;

					Ok((missing_event_id, pdu_result))
				})
				.try_collect()
				.await?;

			let mut new_auth_events = HashSet::new();
			for (missing_event_id, pdu_result) in results {
				match pdu_result {
					| Err(e) => {
						info!(%missing_event_id, ?e, "Could not find pdu mentioned in auth events; marking chain as incomplete");
						is_complete = false;
					},
					| Ok(pdu) => {
						if let Some(claimed_room_id) = pdu.room_id.clone() {
							if claimed_room_id != *room_id {
								return Err!(Request(Forbidden(error!(
									%missing_event_id,
									%room_id,
									wrong_room_id = ?pdu.room_id.unwrap(),
									"auth event for incorrect room"
								))));
							}
						}

						for auth_event in &pdu.auth_events {
							new_auth_events.insert(auth_event.clone());
						}
					},
				}
			}

			let new_auth_events: Vec<_> = new_auth_events.into_iter().collect();
			if new_auth_events.is_empty() {
				continue;
			}

			let mut legacy_short_ids = self
				.services
				.short
				.multi_get_or_create_shorteventid(new_auth_events.iter())
				.zip(futures::stream::iter(new_auth_events.clone()))
				.boxed();

			while let Some((sauthevent, auth_event)) = legacy_short_ids.next().await {
				if found.insert(sauthevent) {
					if let Ok(cached) = self
						.get_cached_eventid_authchain(shortroomid, sauthevent)
						.await
					{
						found |= cached.as_ref();
					} else {
						trace!(?auth_event, "adding legacy auth event to processing queue");
						todo.push((sauthevent, auth_event));
					}
				}
			}
		}
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
	chain: &mut RoaringTreemap,
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
		.await
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
) -> Result<Arc<RoaringTreemap>> {
	self.db
		.get_cached_eventid_authchain(shortroomid, shorteventid)
		.await
}

#[implement(Service)]
#[tracing::instrument(skip_all, level = "debug")]
pub fn cache_auth_chain_bitmap(
	&self,
	shortroomid: ShortRoomId,
	shorteventid: ShortEventId,
	auth_chain: &RoaringTreemap,
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
