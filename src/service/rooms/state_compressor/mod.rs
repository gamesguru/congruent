use std::{
	collections::{BTreeSet, HashMap},
	fmt::{Debug, Write},
	mem::size_of,
	sync::Arc,
};

use async_trait::async_trait;
use conduwuit::{
	Result, SyncMutex,
	arrayvec::ArrayVec,
	at, checked, err, expected, implement, utils,
	utils::{bytes, math::usize_from_f64, stream::IterStream},
};
use database::Map;
use futures::{Stream, StreamExt};
use lru_cache::LruCache;
use ruma::{EventId, RoomId};

use crate::{
	Dep, rooms,
	rooms::short::{ShortEventId, ShortId, ShortStateHash, ShortStateKey},
};

pub struct Service {
	pub stateinfo_cache: SyncMutex<StateInfoLruCache>,
	db: Data,
	services: Services,
}

struct Services {
	short: Dep<rooms::short::Service>,
	state: Dep<rooms::state::Service>,
}

struct Data {
	shortstatehash_statediff: Arc<Map>,
}

#[derive(Clone)]
struct StateDiff {
	parent: Option<ShortStateHash>,
	added: Arc<CompressedState>,
	removed: Arc<CompressedState>,
}

/// One frame in a compressed state stack.
///
/// `full_state` is only guaranteed to be `Some(...)` for the top-most frame
/// returned by `load_shortstatehash_info` and related builders. Parent frames
/// intentionally drop it once their children have been derived, so callers must
/// only dereference the last frame's `full_state`.
#[derive(Clone, Default)]
pub struct ShortStateInfo {
	pub shortstatehash: ShortStateHash,
	pub full_state: Option<Arc<CompressedState>>,
	pub added: Arc<CompressedState>,
	pub removed: Arc<CompressedState>,
}

#[derive(Clone, Default)]
pub struct HashSetCompressStateEvent {
	pub shortstatehash: ShortStateHash,
	pub added: Arc<CompressedState>,
	pub removed: Arc<CompressedState>,
}

type StateInfoLruCache = LruCache<ShortStateHash, Arc<ShortStateInfoVec>>;
type ShortStateInfoVec = Vec<ShortStateInfo>;
type ParentStatesVec = Vec<ShortStateInfo>;

pub type CompressedState = BTreeSet<CompressedStateEvent>;
pub type CompressedStateEvent = [u8; 2 * size_of::<ShortId>()];

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let config = &args.server.config;
		let cache_capacity =
			f64::from(config.stateinfo_cache_capacity) * config.cache_capacity_modifier;
		Ok(Arc::new(Self {
			stateinfo_cache: LruCache::new(usize_from_f64(cache_capacity)?).into(),
			db: Data {
				shortstatehash_statediff: args.db["shortstatehash_statediff"].clone(),
			},
			services: Services {
				short: args.depend::<rooms::short::Service>("rooms::short"),
				state: args.depend::<rooms::state::Service>("rooms::state"),
			},
		}))
	}

	async fn memory_usage(&self, out: &mut (dyn Write + Send)) -> Result {
		let (cache_len, ents) = {
			let cache = self.stateinfo_cache.lock();
			let ents = cache
				.iter()
				.map(at!(1))
				.flat_map(|arc_vec| arc_vec.iter())
				.fold(HashMap::new(), |mut ents, ssi| {
					ents.insert(Arc::as_ptr(&ssi.added), compressed_state_size(&ssi.added));
					ents.insert(Arc::as_ptr(&ssi.removed), compressed_state_size(&ssi.removed));
					if let Some(ref fs) = ssi.full_state {
						ents.insert(Arc::as_ptr(fs), compressed_state_size(fs));
					}

					ents
				});

			(cache.len(), ents)
		};

		let ents_len = ents.len();
		let bytes = ents.values().copied().fold(0_usize, usize::saturating_add);

		let bytes = bytes::pretty(bytes);
		writeln!(out, "stateinfo_cache: {cache_len} {ents_len} ({bytes})")?;

		Ok(())
	}

	async fn clear_cache(&self) { self.stateinfo_cache.lock().clear(); }

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Returns a stack with info on shortstatehash, full state, added diff and
/// removed diff for the selected shortstatehash and each parent layer.
#[implement(Service)]
#[tracing::instrument(name = "load", level = "debug", skip(self))]
pub async fn load_shortstatehash_info(
	&self,
	shortstatehash: ShortStateHash,
) -> Result<ShortStateInfoVec> {
	if shortstatehash == 0 {
		return Ok(vec![ShortStateInfo {
			shortstatehash: 0,
			full_state: Some(Arc::new(CompressedState::new())),
			added: Arc::new(CompressedState::new()),
			removed: Arc::new(CompressedState::new()),
		}]);
	}

	if let Some(r) = self.stateinfo_cache.lock().get_mut(&shortstatehash) {
		// Arc clone is just a refcount bump — no Vec/BTreeSet allocation
		return Ok((**r).clone());
	}

	let stack = self.new_shortstatehash_info(shortstatehash).await?;

	self.cache_shortstatehash_info(shortstatehash, stack.clone())
		.await?;

	Ok(stack)
}

/// Returns a stack with info on shortstatehash, full state, added diff and
/// removed diff for the selected shortstatehash and each parent layer.
#[implement(Service)]
#[tracing::instrument(
		name = "cache",
		level = "debug",
		skip_all,
		fields(
			?shortstatehash,
			stack = stack.len(),
		),
	)]
async fn cache_shortstatehash_info(
	&self,
	shortstatehash: ShortStateHash,
	stack: ShortStateInfoVec,
) -> Result {
	self.stateinfo_cache
		.lock()
		.insert(shortstatehash, Arc::new(stack));

	Ok(())
}

#[implement(Service)]
async fn new_shortstatehash_info(
	&self,
	shortstatehash: ShortStateHash,
) -> Result<ShortStateInfoVec> {
	// Iterative chain walk: collect the chain of diffs from leaf to root,
	// then reconstruct full state bottom-up. Avoids recursive Box::pin
	// allocations and unbounded stack depth.
	let mut chain: Vec<(ShortStateHash, Arc<CompressedState>, Arc<CompressedState>)> = Vec::new();
	let mut current = shortstatehash;

	loop {
		// Check cache for this node — if found, use it as the base
		if let Some(cached) = self.stateinfo_cache.lock().get_mut(&current) {
			// Build on top of the cached stack
			let mut stack: ShortStateInfoVec = (**cached).clone();

			// Apply collected diffs in reverse (root-to-leaf) order
			for (ssh, added, removed) in chain.into_iter().rev() {
				let top = stack.last_mut().expect("at least one frame");
				let mut full_state = (**top
					.full_state
					.as_ref()
					.expect("top frame must have full_state"))
				.clone();

				top.full_state = None;

				full_state.extend(added.iter().copied());
				let removed_set = (*removed).clone();
				for r in &removed_set {
					full_state.remove(r);
				}

				stack.push(ShortStateInfo {
					shortstatehash: ssh,
					added,
					removed: Arc::new(removed_set),
					full_state: Some(Arc::new(full_state)),
				});
			}

			return Ok(stack);
		}

		let StateDiff { parent, added, removed } = self.get_statediff(current).await?;

		let parent_hash = parent.unwrap_or(0);
		if parent_hash == 0 {
			// Root node: build the initial stack
			let mut stack = vec![ShortStateInfo {
				shortstatehash: current,
				full_state: Some(added.clone()),
				added,
				removed,
			}];

			// Apply collected diffs in reverse (root-to-leaf) order
			for (ssh, added, removed) in chain.into_iter().rev() {
				let top = stack.last_mut().expect("at least one frame");
				let mut full_state = (**top
					.full_state
					.as_ref()
					.expect("top frame must have full_state"))
				.clone();

				top.full_state = None;

				full_state.extend(added.iter().copied());
				let removed_set = (*removed).clone();
				for r in &removed_set {
					full_state.remove(r);
				}

				stack.push(ShortStateInfo {
					shortstatehash: ssh,
					added,
					removed: Arc::new(removed_set),
					full_state: Some(Arc::new(full_state)),
				});
			}

			return Ok(stack);
		}

		chain.push((current, added, removed));
		current = parent_hash;
	}
}

#[implement(Service)]
pub fn compress_state_events<'a, I>(
	&'a self,
	state: I,
) -> impl Stream<Item = CompressedStateEvent> + Send + 'a
where
	I: Iterator<Item = (&'a ShortStateKey, &'a EventId)> + Clone + Debug + Send + 'a,
{
	let event_ids = state.clone().map(at!(1));

	let short_event_ids = self
		.services
		.short
		.multi_get_or_create_shorteventid(event_ids);

	state
		.stream()
		.map(at!(0))
		.zip(short_event_ids)
		.map(|(shortstatekey, shorteventid)| compress_state_event(*shortstatekey, shorteventid))
}

#[implement(Service)]
pub async fn compress_state_event(
	&self,
	shortstatekey: ShortStateKey,
	event_id: &EventId,
) -> CompressedStateEvent {
	let shorteventid = self
		.services
		.short
		.get_or_create_shorteventid(event_id)
		.await;

	compress_state_event(shortstatekey, shorteventid)
}

/// Appends a state event to the state diff, returning the new shortstatehash if
/// it changed, or None if the state event is already in the previous state.
#[implement(Service)]
#[tracing::instrument(skip(self, new_shortstatehash), level = "debug")]
pub async fn append_state_pdu<F: FnOnce() -> Result<ShortStateHash>>(
	&self,
	previous_shortstatehash: ShortStateHash,
	shortstatekey: ShortStateKey,
	event_id: &EventId,
	new_shortstatehash: F,
) -> Result<Option<ShortStateHash>> {
	let states_parents = if previous_shortstatehash != 0 {
		self.load_shortstatehash_info(previous_shortstatehash)
			.await?
	} else {
		Vec::new()
	};

	let new = self.compress_state_event(shortstatekey, event_id).await;

	let replaces = states_parents.last().and_then(|info| {
		info.full_state
			.as_ref()
			.expect("top frame must have full_state")
			.iter()
			.find(|bytes| bytes.starts_with(&shortstatekey.to_be_bytes()))
	});

	if Some(&new) == replaces {
		return Ok(None);
	}

	let shortstatehash = new_shortstatehash()?;
	let mut statediffnew = CompressedState::new();
	statediffnew.insert(new);

	let mut statediffremoved = CompressedState::new();
	if let Some(replaces) = replaces {
		statediffremoved.insert(*replaces);
	}

	self.save_state_from_diff(
		shortstatehash,
		Arc::new(statediffnew),
		Arc::new(statediffremoved),
		2,
		states_parents,
	)?;

	Ok(Some(shortstatehash))
}

/// Creates a new shortstatehash that often is just a diff to an already
/// existing shortstatehash and therefore very efficient.
///
/// There are multiple layers of diffs. The bottom layer 0 always contains
/// the full state. Layer 1 contains diffs to states of layer 0, layer 2
/// diffs to layer 1 and so on. If layer n > 0 grows too big, it will be
/// combined with layer n-1 to create a new diff on layer n-1 that's
/// based on layer n-2. If that layer is also too big, it will recursively
/// fix above layers too.
///
/// * `shortstatehash` - Shortstatehash of this state
/// * `statediffnew` - Added to base. Each vec is shortstatekey+shorteventid
/// * `statediffremoved` - Removed from base. Each vec is
///   shortstatekey+shorteventid
/// * `diff_to_sibling` - Approximately how much the diff grows each time for
///   this layer
/// * `parent_states` - A stack with info on shortstatehash, full state, added
///   diff and removed diff for each parent layer
#[implement(Service)]
pub fn save_state_from_diff(
	&self,
	shortstatehash: ShortStateHash,
	statediffnew: Arc<CompressedState>,
	statediffremoved: Arc<CompressedState>,
	diff_to_sibling: usize,
	mut parent_states: ParentStatesVec,
) -> Result {
	let statediffnew_len = statediffnew.len();
	let statediffremoved_len = statediffremoved.len();
	let diffsum = checked!(statediffnew_len + statediffremoved_len)?;

	if parent_states.len() > 3 {
		// Number of layers
		// To many layers, we have to go deeper
		let parent = parent_states.pop().expect("parent must have a state");

		let mut parent_new = (*parent.added).clone();
		let mut parent_removed = (*parent.removed).clone();

		for removed in statediffremoved.iter() {
			if !parent_new.remove(removed) {
				// It was not added in the parent and we removed it
				parent_removed.insert(*removed);
			}
			// Else it was added in the parent and we removed it again. We
			// can forget this change
		}

		for new in statediffnew.iter() {
			if !parent_removed.remove(new) {
				// It was not touched in the parent and we added it
				parent_new.insert(*new);
			}
			// Else it was removed in the parent and we added it again. We
			// can forget this change
		}

		self.save_state_from_diff(
			shortstatehash,
			Arc::new(parent_new),
			Arc::new(parent_removed),
			diffsum,
			parent_states,
		)?;

		return Ok(());
	}

	if parent_states.is_empty() {
		// There is no parent layer, create a new state
		self.save_statediff(shortstatehash, &StateDiff {
			parent: None,
			added: statediffnew,
			removed: statediffremoved,
		});

		return Ok(());
	}

	// Else we have two options.
	// 1. We add the current diff on top of the parent layer.
	// 2. We replace a layer above

	let parent = parent_states.pop().expect("parent must have a state");
	let parent_added_len = parent.added.len();
	let parent_removed_len = parent.removed.len();
	let parent_diff = checked!(parent_added_len + parent_removed_len)?;

	if checked!(diffsum * diffsum)? >= checked!(2 * diff_to_sibling * parent_diff)? {
		// Diff too big, we replace above layer(s)
		let mut parent_new = (*parent.added).clone();
		let mut parent_removed = (*parent.removed).clone();

		for removed in statediffremoved.iter() {
			if !parent_new.remove(removed) {
				// It was not added in the parent and we removed it
				parent_removed.insert(*removed);
			}
			// Else it was added in the parent and we removed it again. We
			// can forget this change
		}

		for new in statediffnew.iter() {
			if !parent_removed.remove(new) {
				// It was not touched in the parent and we added it
				parent_new.insert(*new);
			}
			// Else it was removed in the parent and we added it again. We
			// can forget this change
		}

		self.save_state_from_diff(
			shortstatehash,
			Arc::new(parent_new),
			Arc::new(parent_removed),
			diffsum,
			parent_states,
		)?;
	} else {
		// Diff small enough, we add diff as layer on top of parent
		self.save_statediff(shortstatehash, &StateDiff {
			parent: Some(parent.shortstatehash),
			added: statediffnew,
			removed: statediffremoved,
		});
	}

	Ok(())
}

/// Returns the new shortstatehash, and the state diff from the previous
/// room state
#[implement(Service)]
pub async fn save_state(
	&self,
	room_id: &RoomId,
	new_state_ids_compressed: Arc<CompressedState>,
) -> Result<HashSetCompressStateEvent> {
	let previous_shortstatehash = self
		.services
		.state
		.get_room_shortstatehash(room_id)
		.await
		.ok();

	self.save_state_with_parent(room_id, previous_shortstatehash, new_state_ids_compressed)
		.await
}

/// Returns the new shortstatehash, and the state diff from the previous
/// room state
#[implement(Service)]
#[tracing::instrument(skip(self, new_state_ids_compressed), level = "debug")]
pub async fn save_state_with_parent(
	&self,
	room_id: &RoomId,
	previous_shortstatehash: Option<ShortStateHash>,
	new_state_ids_compressed: Arc<CompressedState>,
) -> Result<HashSetCompressStateEvent> {
	let state_hash =
		utils::calculate_hash(new_state_ids_compressed.iter().map(|bytes| &bytes[..]));

	let (new_shortstatehash, already_existed) = self
		.services
		.short
		.get_or_create_shortstatehash(&state_hash)
		.await;

	if Some(new_shortstatehash) == previous_shortstatehash {
		return Ok(HashSetCompressStateEvent {
			shortstatehash: new_shortstatehash,
			..Default::default()
		});
	}

	let states_parents = if let Some(p) = previous_shortstatehash.filter(|&p| p != 0) {
		self.load_shortstatehash_info(p).await.unwrap_or_default()
	} else {
		ShortStateInfoVec::new()
	};

	let (statediffnew, statediffremoved) = if let Some(parent_stateinfo) = states_parents.last() {
		let statediffnew: CompressedState = new_state_ids_compressed
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
			.difference(&new_state_ids_compressed)
			.copied()
			.collect();

		(Arc::new(statediffnew), Arc::new(statediffremoved))
	} else {
		(new_state_ids_compressed, Arc::new(CompressedState::new()))
	};

	if !already_existed {
		self.save_state_from_diff(
			new_shortstatehash,
			statediffnew.clone(),
			statediffremoved.clone(),
			2, // every state change is 2 event changes on average
			states_parents,
		)?;
	}

	Ok(HashSetCompressStateEvent {
		shortstatehash: new_shortstatehash,
		added: statediffnew,
		removed: statediffremoved,
	})
}

/// Like `save_state`, but writes the new state as a fresh root node in the
/// diff chain rather than computing a diff against the room's full history.
///
/// `save_state` must load the entire ancestor chain via
/// `load_shortstatehash_info` to compute an incremental diff — for rooms with
/// thousands of historical state changes this is an O(depth × state_size)
/// traversal that can take minutes or hang the task entirely.
///
/// This variant skips that traversal and stores the full state unconditionally
/// as a new root (parent = None). The diff tree becomes slightly larger on
/// disk (future saves will still diff against this root), but the operation
/// completes in O(state_size) time regardless of history depth.
///
/// Use this for administrative force operations where correctness takes
/// precedence over diff-chain efficiency.
#[implement(Service)]
#[tracing::instrument(skip(self, new_state_ids_compressed), level = "debug")]
pub async fn save_state_as_root(
	&self,
	room_id: &RoomId,
	new_state_ids_compressed: Arc<CompressedState>,
) -> Result<HashSetCompressStateEvent> {
	let previous_shortstatehash = self
		.services
		.state
		.get_room_shortstatehash(room_id)
		.await
		.ok();

	let state_hash =
		utils::calculate_hash(new_state_ids_compressed.iter().map(|bytes| &bytes[..]));

	let (new_shortstatehash, already_existed) = self
		.services
		.short
		.get_or_create_shortstatehash(&state_hash)
		.await;

	// Fast-path: same state hash → nothing to do.
	if Some(new_shortstatehash) == previous_shortstatehash {
		return Ok(HashSetCompressStateEvent {
			shortstatehash: new_shortstatehash,
			..Default::default()
		});
	}

	// Compute the diff against the previous shortstatehash if we can load it
	// cheaply from the cache. Otherwise write the full state as a new root —
	// this avoids the O(depth) chain traversal that causes hangs on large rooms.
	let (statediffnew, statediffremoved, states_parents) =
		if let Some(prev) = previous_shortstatehash {
			if let Some(cached) = self
				.stateinfo_cache
				.lock()
				.get_mut(&prev)
				.map(|c| (**c).clone())
			{
				// Parent state was already in cache — diff is cheap.
				let parent = cached.last().expect("at least one frame");
				let full = parent
					.full_state
					.as_ref()
					.expect("top frame must have full_state");

				let added: CompressedState =
					new_state_ids_compressed.difference(full).copied().collect();
				let removed: CompressedState = full
					.difference(&new_state_ids_compressed)
					.copied()
					.collect();

				(Arc::new(added), Arc::new(removed), cached)
			} else {
				// Parent not cached — write the full state as a fresh root to
				// avoid the expensive recursive chain walk.
				(
					new_state_ids_compressed.clone(),
					Arc::new(CompressedState::new()),
					ShortStateInfoVec::new(),
				)
			}
		} else {
			// No previous state at all — this is a cold bootstrap.
			(
				new_state_ids_compressed,
				Arc::new(CompressedState::new()),
				ShortStateInfoVec::new(),
			)
		};

	if !already_existed {
		self.save_state_from_diff(
			new_shortstatehash,
			statediffnew.clone(),
			statediffremoved.clone(),
			2,
			states_parents,
		)?;
	}

	Ok(HashSetCompressStateEvent {
		shortstatehash: new_shortstatehash,
		added: statediffnew,
		removed: statediffremoved,
	})
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug", name = "get")]
async fn get_statediff(&self, shortstatehash: ShortStateHash) -> Result<StateDiff> {
	const BUFSIZE: usize = size_of::<ShortStateHash>();
	const STRIDE: usize = size_of::<ShortStateHash>();

	let value = self
		.db
		.shortstatehash_statediff
		.aqry::<BUFSIZE, _>(&shortstatehash)
		.await
		.map_err(|e| {
			err!(Database("Failed to find StateDiff from short {shortstatehash:?}: {e}"))
		})?;

	let parent = utils::u64_from_bytes(&value[0..size_of::<u64>()])
		.ok()
		.take_if(|parent| *parent != 0);

	debug_assert!(value.len().is_multiple_of(STRIDE), "value not aligned to stride");
	let _num_values = value.len() / STRIDE;

	let mut add_mode = true;
	let mut added = CompressedState::new();
	let mut removed = CompressedState::new();

	let mut i = STRIDE;
	while let Some(v) = value.get(i..expected!(i + 2 * STRIDE)) {
		if add_mode && v.starts_with(&0_u64.to_be_bytes()) {
			add_mode = false;
			i = expected!(i + STRIDE);
			continue;
		}
		if add_mode {
			added.insert(v.try_into()?);
		} else {
			removed.insert(v.try_into()?);
		}
		i = expected!(i + 2 * STRIDE);
	}

	Ok(StateDiff {
		parent,
		added: Arc::new(added),
		removed: Arc::new(removed),
	})
}

#[implement(Service)]
fn save_statediff(&self, shortstatehash: ShortStateHash, diff: &StateDiff) {
	let mut value = Vec::<u8>::with_capacity(
		2_usize
			.saturating_add(diff.added.len())
			.saturating_add(diff.removed.len()),
	);

	let parent = diff.parent.unwrap_or(0_u64);
	value.extend_from_slice(&parent.to_be_bytes());

	for new in diff.added.iter() {
		value.extend_from_slice(&new[..]);
	}

	if !diff.removed.is_empty() {
		value.extend_from_slice(&0_u64.to_be_bytes());
		for removed in diff.removed.iter() {
			value.extend_from_slice(&removed[..]);
		}
	}

	self.db
		.shortstatehash_statediff
		.insert(&shortstatehash.to_be_bytes(), &value);
}

/// Convenience: load full state for a shortstatehash, returning None if the
/// shortstatehash is 0 or doesn't exist. Wraps the common pattern of
/// `load_shortstatehash_info(ssh).last().full_state.as_ref()`.
#[implement(Service)]
pub async fn get_full_state(
	&self,
	shortstatehash: ShortStateHash,
) -> Option<Arc<CompressedState>> {
	if shortstatehash == 0 {
		return None;
	}
	let info = self.load_shortstatehash_info(shortstatehash).await.ok()?;
	info.last()?.full_state.clone()
}

/// Compute the set difference between two state snapshots.
/// Returns `(added, removed)` compressed state sets.
#[implement(Service)]
pub async fn diff_full_state(
	&self,
	old_ssh: ShortStateHash,
	new_ssh: ShortStateHash,
) -> (Arc<CompressedState>, Arc<CompressedState>) {
	let empty = Arc::new(CompressedState::new());
	let old_full = self.get_full_state(old_ssh).await;
	let new_full = self.get_full_state(new_ssh).await;

	match (old_full, new_full) {
		| (Some(old), Some(new)) => {
			let added: CompressedState = new.difference(&old).copied().collect();
			let removed: CompressedState = old.difference(&new).copied().collect();
			(Arc::new(added), Arc::new(removed))
		},
		| (None, Some(new)) => (new, empty),
		| (Some(old), None) => (empty, old),
		| (None, None) => (empty.clone(), empty),
	}
}

#[inline]
#[must_use]
pub(crate) fn compress_state_event(
	shortstatekey: ShortStateKey,
	shorteventid: ShortEventId,
) -> CompressedStateEvent {
	const SIZE: usize = size_of::<CompressedStateEvent>();

	let mut v = ArrayVec::<u8, SIZE>::new();
	v.extend(shortstatekey.to_be_bytes());
	v.extend(shorteventid.to_be_bytes());
	v.as_ref()
		.try_into()
		.expect("failed to create CompressedStateEvent")
}

#[inline]
#[must_use]
pub fn parse_compressed_state_event(
	compressed_event: CompressedStateEvent,
) -> (ShortStateKey, ShortEventId) {
	use utils::u64_from_u8;

	let shortstatekey = u64_from_u8(&compressed_event[0..size_of::<ShortStateKey>()]);
	let shorteventid = u64_from_u8(&compressed_event[size_of::<ShortStateKey>()..]);

	(shortstatekey, shorteventid)
}

#[inline]
fn compressed_state_size(compressed_state: &CompressedState) -> usize {
	compressed_state
		.len()
		.checked_mul(size_of::<CompressedStateEvent>())
		.expect("CompressedState size overflow")
}
