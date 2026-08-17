use std::{collections::HashMap, fmt::Write, iter::once, mem::size_of, sync::Arc};

use async_trait::async_trait;
use conduwuit::{RoomVersion, debug, info};
use conduwuit_core::{
	Event, PduEvent, Result,
	result::FlatOk,
	state_res::StateMap,
	utils::{
		IterStream, MutexMap, MutexMapGuard, ReadyExt, calculate_hash,
		stream::{BroadbandExt, TryIgnore},
	},
	warn,
};
use conduwuit_database::{Deserialized, Ignore, Interfix, Map};
use futures::{
	FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt, future::join_all, pin_mut,
};
use ruma::{
	EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, RoomVersionId, UserId,
	events::{
		AnyStrippedStateEvent, StateEventType, TimelineEventType,
		room::create::RoomCreateEventContent,
	},
	serde::Raw,
};

use crate::{
	Dep, globals, rooms,
	rooms::{
		short::{ShortEventId, ShortStateHash},
		state_compressor::{CompressedState, parse_compressed_state_event},
	},
};

pub struct Service {
	pub mutex: RoomMutexMap,
	services: Services,
	db: Data,
}

struct Services {
	globals: Dep<globals::Service>,
	short: Dep<rooms::short::Service>,
	spaces: Dep<rooms::spaces::Service>,
	state_cache: Dep<rooms::state_cache::Service>,
	state_accessor: Dep<rooms::state_accessor::Service>,
	state_compressor: Dep<rooms::state_compressor::Service>,
	timeline: Dep<rooms::timeline::Service>,
}

struct Data {
	shorteventid_shortstatehash: Arc<Map>,
	roomid_shortstatehash: Arc<Map>,
	roomid_pduleaves: Arc<Map>,
}

type RoomMutexMap = MutexMap<OwnedRoomId, ()>;
pub type RoomMutexGuard = MutexMapGuard<OwnedRoomId, ()>;

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			mutex: RoomMutexMap::new(),
			services: Services {
				globals: args.depend::<globals::Service>("globals"),
				short: args.depend::<rooms::short::Service>("rooms::short"),
				spaces: args.depend::<rooms::spaces::Service>("rooms::spaces"),
				state_cache: args.depend::<rooms::state_cache::Service>("rooms::state_cache"),
				state_accessor: args
					.depend::<rooms::state_accessor::Service>("rooms::state_accessor"),
				state_compressor: args
					.depend::<rooms::state_compressor::Service>("rooms::state_compressor"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
			},
			db: Data {
				shorteventid_shortstatehash: args.db["shorteventid_shortstatehash"].clone(),
				roomid_shortstatehash: args.db["roomid_shortstatehash"].clone(),
				roomid_pduleaves: args.db["roomid_pduleaves"].clone(),
			},
		}))
	}

	async fn memory_usage(&self, out: &mut (dyn Write + Send)) -> Result {
		let mutex = self.mutex.len();
		writeln!(out, "state_mutex: {mutex}")?;

		Ok(())
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Set the room to the given statehash and update caches.
	pub async fn force_state(
		&self,
		room_id: &RoomId,
		shortstatehash: u64,
		statediffnew: Arc<CompressedState>,
		statediffremoved: Arc<CompressedState>,
		state_lock: &RoomMutexGuard,
	) -> Result {
		self.force_state_inner(
			room_id,
			shortstatehash,
			statediffnew,
			statediffremoved,
			state_lock,
			true,
			None,
		)
		.await
	}

	/// Same as `force_state`, but for callers that already hold
	/// `rooms::timeline::Service::mutex_insert` for this room (currently only
	/// `append_pdu`). Passing that guard through lets the outlier-demotion
	/// step below skip re-acquiring the same non-reentrant lock, which would
	/// otherwise deadlock the calling task against itself.
	pub async fn force_state_insert_locked(
		&self,
		room_id: &RoomId,
		shortstatehash: u64,
		statediffnew: Arc<CompressedState>,
		statediffremoved: Arc<CompressedState>,
		state_lock: &RoomMutexGuard,
		insert_lock: &rooms::timeline::InsertMutexGuard,
	) -> Result {
		self.force_state_inner(
			room_id,
			shortstatehash,
			statediffnew,
			statediffremoved,
			state_lock,
			true,
			Some(insert_lock),
		)
		.await
	}

	/// Admin-only: set room state without triggering per-member cache updates
	/// or outbound federation notifications (presence, device lists, etc).
	/// The caller must rebuild the membership cache afterwards.
	pub async fn force_state_quiet(
		&self,
		room_id: &RoomId,
		shortstatehash: u64,
		statediffnew: Arc<CompressedState>,
		statediffremoved: Arc<CompressedState>,
		state_lock: &RoomMutexGuard,
	) -> Result {
		self.force_state_inner(
			room_id,
			shortstatehash,
			statediffnew,
			statediffremoved,
			state_lock,
			false,
			None,
		)
		.await
	}

	#[allow(clippy::too_many_arguments)]
	async fn force_state_inner(
		&self,
		room_id: &RoomId,
		shortstatehash: u64,
		statediffnew: Arc<CompressedState>,
		statediffremoved: Arc<CompressedState>,
		state_lock: &RoomMutexGuard,
		update_cache: bool,
		insert_lock: Option<&rooms::timeline::InsertMutexGuard>,
	) -> Result {
		info!(
			target: "force_state",
			"processing {} new, {} removed state events for {room_id} (cache_update={update_cache})",
			statediffnew.len(),
			statediffremoved.len()
		);

		if !update_cache {
			// Admin bypass: Skip membership churn and outbound federation notifications.
			// The admin command will manually rebuild the cache via silent_bulk_sync.
			self.set_room_state(room_id, shortstatehash, state_lock);
			self.services.state_cache.update_joined_count(room_id).await;
			info!(target: "force_state", "quiet mode: state pointer set for {room_id}");
			return Ok(());
		}

		let new_state_events: HashMap<_, _> = statediffnew
			.iter()
			.map(|&new| parse_compressed_state_event(new))
			.collect();

		let new_event_ids = new_state_events
			.values()
			.copied()
			.stream()
			.then(|shorteventid| {
				self.services
					.short
					.get_eventid_from_short::<Box<_>>(shorteventid)
			})
			.ignore_err();

		let removed_events = statediffremoved
			.iter()
			.stream()
			.map(|&old| parse_compressed_state_event(old));

		let mut new_processed = 0_usize;
		let mut new_members = 0_usize;
		let mut new_skipped = 0_usize;
		pin_mut!(new_event_ids);
		while let Some(event_id) = new_event_ids.next().await {
			new_processed = new_processed.saturating_add(1);
			let pdu = match self
				.services
				.timeline
				.get_pdu_in_room(Some(room_id), &event_id)
				.await
			{
				| Ok(pdu) => pdu,
				| Err(_) => match self
					.services
					.timeline
					.get_pdu_in_room(None, &event_id)
					.await
				{
					| Ok(pdu) => {
						warn!(
							target: "force_state",
							"PDU {event_id} not found with room_id filter, recovered without"
						);
						pdu
					},
					| Err(_) => {
						new_skipped = new_skipped.saturating_add(1);
						continue;
					},
				},
			};

			match pdu.kind {
				| TimelineEventType::RoomMember => {
					let Some(user_id) = pdu.state_key.as_ref().map(UserId::parse).flat_ok()
					else {
						continue;
					};

					self.services
						.state_cache
						.update_membership(room_id, user_id, &pdu, false)
						.await?;

					// Membership changes can affect restricted room accessibility
					self.services
						.spaces
						.roomid_spacehierarchy_cache
						.lock()
						.await
						.remove(room_id);

					new_members = new_members.saturating_add(1);
					if new_members.is_multiple_of(1000) {
						info!(
							target: "force_state",
							"processed {new_members} members, {new_processed} total, {new_skipped} skipped"
						);
					}
				},
				| TimelineEventType::SpaceChild => {
					self.services
						.spaces
						.roomid_spacehierarchy_cache
						.lock()
						.await
						.remove(room_id);
				},
				| _ => continue,
			}
		}
		info!(
			target: "force_state",
			"new events done: {new_processed} processed, {new_members} members, {new_skipped} skipped"
		);

		// Collected once (not per removed member) so marking removed members as
		// left doesn't turn into O(removed * members) work below.
		let prior_members: Vec<_> = self
			.services
			.state_cache
			.room_members(room_id)
			.map(ToOwned::to_owned)
			.collect()
			.await;
		let local_prior_members: Vec<&OwnedUserId> = prior_members
			.iter()
			.filter(|member| self.services.globals.user_is_local(member))
			.collect();
		let mut removed_members: Vec<(OwnedUserId, u64)> = Vec::new();

		pin_mut!(removed_events);
		while let Some((shortstatekey, shorteventid)) = removed_events.next().await {
			// Process cache updates using shortstatekey (PDU-free!)
			// This guarantees we update the cache even if the historical PDU
			// JSON has been pruned from the database.
			if let Ok((event_type, state_key)) = self
				.services
				.short
				.get_statekey_from_short(shortstatekey)
				.await
			{
				if event_type == StateEventType::RoomMember {
					if let Ok(user_id) = UserId::parse(&*state_key) {
						// Replacement member events were already processed by the new-events
						// loop above. Without a replacement, the user is absent from the new
						// state and must be marked left.
						//
						// Use the reconciled variant here so we do not wipe a just-added
						// invite when state resolution is transitioning from leave/ban ->
						// invite.
						if !new_state_events.contains_key(&shortstatekey) {
							let left_count = self
								.services
								.state_cache
								.mark_as_left_reconciled(user_id, room_id)
								.await;
							removed_members.push((user_id.to_owned(), left_count));
						}
					}
				} else if event_type == StateEventType::SpaceChild {
					self.services
						.spaces
						.roomid_spacehierarchy_cache
						.lock()
						.await
						.remove(room_id);
				}
			}

			// Demote to outlier if possible (best-effort, not required for cache)
			let Ok(event_id) = self
				.services
				.short
				.get_eventid_from_short::<Box<_>>(shorteventid)
				.await
			else {
				continue;
			};

			let pdu_json = self.services.timeline.get_pdu_json(&event_id).await;
			if let Ok(pdu_json) = &pdu_json {
				match insert_lock {
					| Some(insert_lock) => self.services.timeline.add_pdu_outlier_locked(
						&event_id,
						pdu_json,
						Some(room_id),
						insert_lock,
					),
					| None =>
						self.services
							.timeline
							.add_pdu_outlier(&event_id, pdu_json, Some(room_id))
							.await,
				}
			}
		}

		if !removed_members.is_empty() {
			self.services
				.state_cache
				.mark_device_list_lefts_batch(
					&removed_members,
					&prior_members,
					&local_prior_members,
				)
				.await;
		}

		info!(target: "force_state", "removed events done, updating joined count");
		self.set_room_state(room_id, shortstatehash, state_lock);
		self.services.state_cache.update_joined_count(room_id).await;

		info!(target: "force_state", "complete for {room_id}");
		Ok(())
	}

	/// Generates a new StateHash and associates it with the incoming event.
	///
	/// This adds all current state events (not including the incoming event)
	/// to `stateid_pduid` and adds the incoming event to `eventid_statehash`.
	#[tracing::instrument(skip(self, state_ids_compressed), level = "debug")]
	pub async fn set_event_state(
		&self,
		event_id: &EventId,
		room_id: &RoomId,
		state_ids_compressed: Arc<CompressedState>,
	) -> Result<ShortStateHash> {
		const KEY_LEN: usize = size_of::<ShortEventId>();
		const VAL_LEN: usize = size_of::<ShortStateHash>();

		let shorteventid = self
			.services
			.short
			.get_or_create_shorteventid(event_id)
			.await;

		let previous_shortstatehash = self.get_room_shortstatehash(room_id).await;

		let state_hash = calculate_hash(state_ids_compressed.iter().map(|s| &s[..]));

		let (shortstatehash, already_existed) = self
			.services
			.short
			.get_or_create_shortstatehash(&state_hash)
			.await;

		if !already_existed {
			let states_parents = match previous_shortstatehash {
				| Ok(p) if p != 0 =>
					self.services
						.state_compressor
						.load_shortstatehash_info(p)
						.await?,
				| _ => Vec::new(),
			};

			let (statediffnew, statediffremoved) =
				if let Some(parent_stateinfo) = states_parents.last() {
					let statediffnew: CompressedState = state_ids_compressed
						.difference(
							parent_stateinfo
								.full_state
								.as_ref()
								.expect("top frame must have full_state"),
						)
						.copied()
						.collect();

					let statediffremoved: CompressedState = parent_stateinfo
						.full_state
						.as_ref()
						.expect("top frame must have full_state")
						.difference(&state_ids_compressed)
						.copied()
						.collect();

					(Arc::new(statediffnew), Arc::new(statediffremoved))
				} else {
					(state_ids_compressed, Arc::new(CompressedState::new()))
				};
			self.services.state_compressor.save_state_from_diff(
				shortstatehash,
				statediffnew,
				statediffremoved,
				1_000_000, // high number because no state will be based on this one
				states_parents,
			)?;
		}

		self.db
			.shorteventid_shortstatehash
			.aput::<KEY_LEN, VAL_LEN, _, _>(shorteventid, shortstatehash);
		self.services
			.short
			.shorteventid_shortstatehash_cache
			.insert(shorteventid, shortstatehash);

		Ok(shortstatehash)
	}

	/// Overwrites the shortstatehash for a specific event. Used by admin
	/// commands to fix stale pdu_shortstatehash entries after force-setting
	/// room state.
	pub fn set_pdu_shortstatehash(&self, shorteventid: u64, shortstatehash: u64) {
		const BUFSIZE: usize = size_of::<u64>();

		self.db
			.shorteventid_shortstatehash
			.aput::<BUFSIZE, BUFSIZE, _, _>(shorteventid, shortstatehash);
		self.services
			.short
			.shorteventid_shortstatehash_cache
			.insert(shorteventid, shortstatehash);
	}

	/// Batch-overwrite event state snapshots. Used by rebuild-state to avoid
	/// one RocksDB write per event.
	pub fn set_pdu_shortstatehash_batch(&self, entries: &[(u64, u64)]) {
		if entries.is_empty() {
			return;
		}

		let mut batch = conduwuit_database::Batch::new();
		for &(shorteventid, shortstatehash) in entries {
			let key = shorteventid.to_be_bytes();
			let val = shortstatehash.to_be_bytes();
			self.db
				.shorteventid_shortstatehash
				.batch_put(&mut batch, &key, val);
			self.services
				.short
				.shorteventid_shortstatehash_cache
				.insert(shorteventid, shortstatehash);
		}
		self.db.shorteventid_shortstatehash.apply_batch(batch);
	}

	/// Generates a new StateHash and associates it with the incoming event.
	///
	/// This adds all current state events (not including the incoming event)
	/// to `stateid_pduid` and adds the incoming event to `eventid_statehash`.
	#[tracing::instrument(skip(self, new_pdu), level = "debug")]
	pub async fn append_to_state(&self, new_pdu: &PduEvent, room_id: &RoomId) -> Result<u64> {
		const BUFSIZE: usize = size_of::<u64>();

		let shorteventid = self
			.services
			.short
			.get_or_create_shorteventid(&new_pdu.event_id)
			.await;

		let previous_shortstatehash = self.get_room_shortstatehash(room_id).await.unwrap_or(0);

		self.db
			.shorteventid_shortstatehash
			.aput::<BUFSIZE, BUFSIZE, _, _>(shorteventid, previous_shortstatehash);
		self.services
			.short
			.shorteventid_shortstatehash_cache
			.insert(shorteventid, previous_shortstatehash);

		match &new_pdu.state_key {
			| Some(state_key) => {
				let shortstatekey = self
					.services
					.short
					.get_or_create_shortstatekey(&new_pdu.kind.to_string().into(), state_key)
					.await;

				let new_ssh = self
					.services
					.state_compressor
					.append_state_pdu(
						previous_shortstatehash,
						shortstatekey,
						&new_pdu.event_id,
						|| self.services.globals.next_count(),
					)
					.await?;

				Ok(new_ssh.unwrap_or(previous_shortstatehash))
			},
			| _ => {
				assert!(
					previous_shortstatehash != 0,
					"first event in room must be a state event"
				);
				Ok(previous_shortstatehash)
			},
		}
	}

	#[tracing::instrument(skip_all, level = "debug")]
	pub async fn summary_stripped<'a, E>(
		&self,
		event: &'a E,
		room_id: &RoomId,
	) -> Vec<Raw<AnyStrippedStateEvent>>
	where
		E: Event + Send + Sync,
		&'a E: Event + Send,
	{
		let cells = [
			(&StateEventType::RoomCreate, ""),
			(&StateEventType::RoomJoinRules, ""),
			(&StateEventType::RoomCanonicalAlias, ""),
			(&StateEventType::RoomName, ""),
			(&StateEventType::RoomAvatar, ""),
			(&StateEventType::RoomMember, event.sender().as_str()), // Add recommended events
			(&StateEventType::RoomEncryption, ""),
			(&StateEventType::RoomTopic, ""),
		];

		let fetches = cells.into_iter().map(|(event_type, state_key)| {
			self.services
				.state_accessor
				.room_state_get(room_id, event_type, state_key)
		});

		join_all(fetches)
			.await
			.into_iter()
			.filter_map(Result::ok)
			.map(Event::into_format)
			.chain(once(event.to_format()))
			.collect()
	}

	/// Set the state hash to a new version, but does not update state_cache.
	#[tracing::instrument(skip(self, _mutex_lock), level = "debug")]
	pub fn set_room_state(
		&self,
		room_id: &RoomId,
		shortstatehash: u64,
		// Take mutex guard to make sure users get the room state mutex
		_mutex_lock: &RoomMutexGuard,
	) {
		const BUFSIZE: usize = size_of::<u64>();

		self.db
			.roomid_shortstatehash
			.raw_aput::<BUFSIZE, _, _>(room_id, shortstatehash);
	}

	/// Returns the room's version.
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn get_room_version(&self, room_id: &RoomId) -> Result<RoomVersionId> {
		if let Ok(version) = self.services.short.get_room_version(room_id).await {
			return Ok(version);
		}

		// Try the current room state snapshot first.
		if let Ok(content) = self
			.services
			.state_accessor
			.room_state_get_content::<RoomCreateEventContent>(
				room_id,
				&StateEventType::RoomCreate,
				"",
			)
			.await
		{
			let version = content.room_version;
			self.services.short.set_room_version(room_id, &version);
			return Ok(version);
		}

		// Fallback: the create event might be an outlier (not in the state
		// snapshot). Scan outliers for this room to find it.
		let mut outlier_stream = Box::pin(self.services.timeline.room_outlier_stream(room_id));
		while let Some((_eid, pdu)) = outlier_stream.next().await {
			if pdu.kind == TimelineEventType::RoomCreate {
				if let Ok(content) = pdu.get_content::<RoomCreateEventContent>() {
					let version = content.room_version;
					self.services.short.set_room_version(room_id, &version);
					return Ok(version);
				}
			}
		}

		Err(conduwuit::err!(Request(NotFound(
			"No create event found for room (checked state + outliers)"
		))))
	}

	pub async fn get_shortstatehash(&self, shorteventid: ShortEventId) -> Result<ShortStateHash> {
		if let Some(shortstatehash) = self
			.services
			.short
			.shorteventid_shortstatehash_cache
			.get(&shorteventid)
		{
			return Ok(shortstatehash);
		}

		let shortstatehash: ShortStateHash = self
			.db
			.shorteventid_shortstatehash
			.qry(&shorteventid)
			.await
			.deserialized()?;

		self.services
			.short
			.shorteventid_shortstatehash_cache
			.insert(shorteventid, shortstatehash);
		Ok(shortstatehash)
	}

	pub async fn get_room_shortstatehash(&self, room_id: &RoomId) -> Result<ShortStateHash> {
		self.db
			.roomid_shortstatehash
			.get(room_id)
			.await
			.deserialized()
	}

	pub fn get_forward_extremities<'a>(
		&'a self,
		room_id: &'a RoomId,
	) -> impl Stream<Item = OwnedEventId> + Send + 'a {
		let prefix = (room_id, Interfix);

		self.db
			.roomid_pduleaves
			.keys_prefix(&prefix)
			.map_ok(|(_, event_id): (Ignore, &EventId)| event_id.to_owned())
			.ignore_err()
	}

	/// Returns true if the given event_id is a current forward extremity
	/// (DAG tip) for the room.
	pub async fn is_forward_extremity(&self, room_id: &RoomId, event_id: &EventId) -> bool {
		self.get_forward_extremities(room_id)
			.any(|eid| futures::future::ready(eid == *event_id))
			.await
	}

	pub async fn set_forward_extremities<'a, I>(
		&'a self,
		room_id: &'a RoomId,
		event_ids: I,
		trusted_new_event: Option<&'a EventId>,
		_state_lock: &'a RoomMutexGuard,
	) where
		I: Iterator<Item = OwnedEventId> + Send + 'a,
	{
		// Only events that are actually accepted into the timeline may become
		// citable forward extremities. Outliers, rejected, and soft-failed
		// events cannot be relied upon to ever converge, so admitting them
		// here means every future event/state-res pass in this room pays to
		// re-walk their dependencies indefinitely. This is the single write
		// path to `roomid_pduleaves`, so enforcing eligibility here covers
		// all callers (including `recalculate_extremities` and reorder).
		//
		// `trusted_new_event`, if given, is the one event currently being
		// appended by this same operation: its `eventid_metadata` entry is
		// written moments after this call returns (see
		// `timeline::append_pdu`), so a metadata lookup for it here would
		// always miss. It is exempted from the DB check and trusted
		// directly, since by construction it is being newly accepted into
		// the timeline right now, not an outlier/rejected/soft-failed event.
		let mut eligible: Vec<OwnedEventId> = Vec::new();
		for event_id in event_ids {
			if trusted_new_event.is_some_and(|trusted| trusted.as_str() == event_id.as_str()) {
				eligible.push(event_id);
				continue;
			}

			match self.services.timeline.get_event_metadata(&event_id).await {
				| Ok(metadata)
					if !metadata.is_outlier
						&& metadata.status == rooms::pdu_metadata::EventStatus::Accepted =>
				{
					eligible.push(event_id);
				},
				| Ok(_) => {
					debug!(
						%room_id, %event_id,
						"Refusing to persist ineligible (outlier/rejected/soft-failed) \
						 event as a forward extremity",
					);
				},
				| Err(_) => {
					debug!(
						%room_id, %event_id,
						"Refusing to persist forward extremity with unknown event metadata",
					);
				},
			}
		}

		if eligible.is_empty() {
			warn!(
				%room_id,
				"set_forward_extremities: all candidate tips were ineligible \
				 (outlier/rejected/soft-failed/unknown); leaving existing forward \
				 extremities unchanged",
			);
			return;
		}

		let prefix = (room_id, Interfix);
		self.db
			.roomid_pduleaves
			.keys_prefix_raw(&prefix)
			.ignore_err()
			.ready_for_each(|key| self.db.roomid_pduleaves.remove(key))
			.await;

		// Enforce a hard cap at the DB writer level. Callers may pass more
		// tips than this (e.g. from recalculate_extremities),
		// Keeping the newest tips is preferred since they are most likely to
		// be merged by future events.
		let max_extremities = self.services.globals.max_forward_extremities();
		let start = eligible.len().saturating_sub(max_extremities);
		for event_id in &eligible[start..] {
			let key = (room_id, &**event_id);
			self.db.roomid_pduleaves.put_raw(key, &**event_id);
		}
	}

	/// This fetches auth events from the current state.
	#[tracing::instrument(skip(self, content, room_version), level = "trace")]
	pub async fn get_auth_events(
		&self,
		room_id: &RoomId,
		kind: &TimelineEventType,
		sender: &UserId,
		state_key: Option<&str>,
		content: &serde_json::value::RawValue,
		room_version: &RoomVersion,
	) -> Result<StateMap<PduEvent>> {
		let Ok(shortstatehash) = self.get_room_shortstatehash(room_id).await else {
			return Ok(HashMap::new());
		};

		let content_val: serde_json::Value =
			serde_json::from_str(content.get()).unwrap_or(serde_json::Value::Null);
		// For auth_types_for_event, V2 vs V2_1+ is the only distinction
		// (whether `m.room.create` is included). V2_1_1 and V2_2 behave the same.
		let version = if room_version.room_ids_as_hashes {
			rezzy::StateResVersion::V2_1
		} else {
			// TODO: what about StateRes V1? V1 rooms?
			rezzy::StateResVersion::V2
		};
		let auth_types_raw = rezzy::auth::auth_types_for_event(
			&kind.to_string(),
			sender.as_str(),
			state_key,
			&content_val,
			// MSC4291 (v12+): auth_events must NOT reference m.room.create
			version,
		);
		let auth_types: Vec<(StateEventType, conduwuit_core::matrix::StateKey)> = auth_types_raw
			.into_iter()
			.map(|(ty, sk)| (ty.into(), sk.into()))
			.collect();
		debug!(?auth_types, "Auth types for event");
		let sauthevents: HashMap<_, _> = auth_types
			.iter()
			.stream()
			.broad_filter_map(|(event_type, state_key)| {
				self.services
					.short
					.get_shortstatekey(event_type, state_key)
					.map_ok(move |ssk| (ssk, (event_type, state_key)))
					.map(Result::ok)
			})
			.collect()
			.await;
		debug!(?sauthevents, "Auth events to fetch");

		let (state_keys, event_ids): (Vec<_>, Vec<_>) = self
			.services
			.state_accessor
			.state_full_shortids(shortstatehash)
			.ready_filter_map(Result::ok)
			.ready_filter_map(|(shortstatekey, shorteventid)| {
				sauthevents
					.get(&shortstatekey)
					.map(|(ty, sk)| ((ty, sk), shorteventid))
			})
			.unzip()
			.await;
		debug!(?state_keys, ?event_ids, "Auth events found in state");
		self.services
			.short
			.multi_get_eventid_from_short(event_ids.into_iter().stream())
			.zip(state_keys.into_iter().stream())
			.ready_filter_map(|(event_id, (ty, sk))| Some(((ty, sk), event_id.ok()?)))
			.broad_filter_map(|((ty, sk), event_id): (_, OwnedEventId)| async move {
				self.services
					.timeline
					.get_pdu(&event_id)
					.await
					.map(move |pdu| (((*ty).clone(), (*sk).clone()), pdu))
					.inspect_err(|e| warn!("Failed to get auth event {event_id}: {e:?}"))
					.ok()
			})
			.collect()
			.map(Ok)
			.await
	}
}
