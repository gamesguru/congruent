mod acquire;
mod get;
mod keypair;
mod request;
mod sign;
mod validate;
mod verify;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use conduwuit::{
	Result, Server, debug_error, debug_warn, err, implement, trace,
	utils::{IterStream, MutexMap, timepoint_from_now},
};
use database::{Deserialized, Json, Map};
use futures::StreamExt;
use ruma::{
	CanonicalJsonObject, MilliSecondsSinceUnixEpoch, OwnedServerName, OwnedServerSigningKeyId,
	RoomVersionId, ServerName, ServerSigningKeyId,
	api::federation::discovery::{OldVerifyKey, ServerSigningKeys, VerifyKey},
	serde::Raw,
	signatures::{Ed25519KeyPair, PublicKeyMap, PublicKeySet},
};
use serde_json::value::RawValue as RawJsonValue;
use tokio::sync::RwLock;

use crate::{Dep, globals, sending};

#[derive(Clone, Copy, Debug)]
struct BackoffState {
	expires: std::time::Instant,
	failures: u32,
}

const BACKOFF_FAILURE_MEMORY: Duration = Duration::from_secs(24 * 60 * 60);
const MAX_FETCH_BACKOFF_ENTRIES: usize = 4096;

pub struct Service {
	keypair: Box<Ed25519KeyPair>,
	verify_keys: VerifyKeys,
	minimum_valid: Duration,
	/// Tracks servers that recently failed key fetches, including the instant
	/// the backoff expires and how many consecutive failures have occurred.
	fetch_backoff: RwLock<BTreeMap<OwnedServerName, BackoffState>>,
	/// Deduplicates concurrent in-flight key fetches per server name.
	/// Uses MutexMap (same pattern as resolver) — concurrent calls for the
	/// same server serialize on the mutex; the second caller re-checks cache.
	fetching: MutexMap<OwnedServerName, ()>,
	services: Services,
	db: Data,
}

struct Services {
	globals: Dep<globals::Service>,
	sending: Dep<sending::Service>,
	server: Arc<Server>,
}

struct Data {
	server_signingkeys: Arc<Map>,
}

pub type VerifyKeys = BTreeMap<OwnedServerSigningKeyId, VerifyKey>;
pub type PubKeyMap = PublicKeyMap;
pub type PubKeys = PublicKeySet;

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let minimum_valid = Duration::from_secs(3600);

		let (keypair, verify_keys) = keypair::init(args.db)?;
		debug_assert!(verify_keys.len() == 1, "only one active verify_key supported");

		Ok(Arc::new(Self {
			keypair,
			verify_keys,
			minimum_valid,
			fetch_backoff: RwLock::new(BTreeMap::new()),
			fetching: MutexMap::new(),
			services: Services {
				globals: args.depend::<globals::Service>("globals"),
				sending: args.depend::<sending::Service>("sending"),
				server: args.server.clone(),
			},
			db: Data {
				server_signingkeys: args.db["server_signingkeys"].clone(),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

/// Returns true if the server is currently in backoff (a recent fetch failed).
#[implement(Service)]
pub async fn is_in_backoff(&self, server: &ServerName) -> bool {
	let now = std::time::Instant::now();
	self.fetch_backoff
		.read()
		.await
		.get(server)
		.is_some_and(|state| now < state.expires)
}

/// Records a fetch failure, starting a backoff period for the server.
#[implement(Service)]
pub async fn record_backoff(&self, server: &ServerName) {
	let base_secs =
		bounded_msc4499_backoff_secs(self.services.server.config.msc4499_backoff_secs);
	let now = std::time::Instant::now();
	let mut backoff = self.fetch_backoff.write().await;
	backoff.retain(|_, state| {
		state
			.expires
			.checked_add(BACKOFF_FAILURE_MEMORY)
			.is_some_and(|horizon| now < horizon)
	});

	let state = backoff
		.entry(server.into())
		.or_insert(BackoffState { expires: now, failures: 0 });
	state.failures = state.failures.saturating_add(1);

	let shift = state.failures.saturating_sub(1).min(63);
	let multiplier = 1_u64.checked_shl(shift).unwrap_or(u64::MAX);
	let delay_secs = base_secs.saturating_mul(multiplier).min(3600);

	let expires = now
		.checked_add(Duration::from_secs(delay_secs))
		.or_else(|| now.checked_add(Duration::from_secs(86400)))
		.unwrap_or(now);
	state.expires = expires;

	while backoff.len() > MAX_FETCH_BACKOFF_ENTRIES {
		let Some(evict) = backoff
			.keys()
			.find(|key| key.as_str() != server.as_str())
			.cloned()
		else {
			break;
		};
		backoff.remove(&evict);
	}
}

/// Clears the backoff state for a server after a successful fetch.
#[implement(Service)]
pub async fn clear_backoff(&self, server: &ServerName) {
	self.fetch_backoff.write().await.remove(server);
}

/// Performs a `server_request` with fetch coalescing: concurrent calls for
/// the same server serialize on a per-server mutex. The second caller
/// re-evaluates freshness after the first finishes, avoiding redundant
/// network requests while still allowing sequential re-fetches when the
/// cached result is stale.
#[implement(Service)]
pub async fn server_request_coalesced(
	&self,
	server: &ServerName,
	minimum_valid_until_ts: Option<MilliSecondsSinceUnixEpoch>,
	requested_key_ids: &[&ServerSigningKeyId],
) -> Result<Raw<ServerSigningKeys>> {
	let _guard = self.fetching.lock(server).await;

	if self.is_in_backoff(server).await {
		return Err(err!(Request(NotFound("origin is in fetch backoff"))));
	}

	// Re-check cache — a concurrent caller may have already fetched.
	// Evaluate using the same freshness criteria as the caller.
	if let Ok(cached) = self.merged_signing_keys_for(server).await {
		let missing_key = requested_key_ids.iter().any(|kid| {
			!cached.verify_keys.contains_key(*kid) && !cached.old_verify_keys.contains_key(*kid)
		});

		let stale = minimum_valid_until_ts.is_some_and(|min| cached.valid_until_ts < min);

		if !missing_key && !stale {
			return self.raw_signing_keys_for(server).await;
		}
	}

	match self.server_request(server).await {
		| Ok(keys) => {
			self.clear_backoff(server).await;
			Ok(keys)
		},
		| Err(e) => {
			self.record_backoff(server).await;
			Err(e)
		},
	}
}

/// Constructs the database key for the historical/cumulative signing keys
/// record. Centralizes the `origin\0historical` key format to avoid
/// fragile hand-crafted key construction throughout the codebase.
pub(super) fn historical_db_key(origin: &ServerName) -> Vec<u8> {
	let mut key = origin.as_bytes().to_vec();
	key.extend_from_slice(b"\0historical");
	key
}

/// Constructs the database key for the set of key IDs whose binding is
/// still provisional (learned only via a notary, not yet confirmed by a
/// direct fetch). See MSC4499 "Notary fallback (two-tier binding)".
fn provisional_db_key(origin: &ServerName) -> Vec<u8> {
	let mut key = origin.as_bytes().to_vec();
	key.extend_from_slice(b"\0provisional");
	key
}

/// Where a key observation came from. Only a direct fetch can promote a
/// provisional (notary-learned) binding to permanent; see MSC4499 "Notary
/// fallback (two-tier binding)".
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchSource {
	/// A direct fetch from the origin's `/_matrix/key/v2/server`.
	Direct,
	/// A relayed observation from a `/_matrix/key/v2/query` notary.
	Notary,
}

#[implement(Service)]
#[inline]
pub fn keypair(&self) -> &Ed25519KeyPair { &self.keypair }

#[implement(Service)]
#[inline]
pub fn active_key_id(&self) -> &ServerSigningKeyId { self.active_verify_key().0 }

#[implement(Service)]
#[inline]
pub fn active_verify_key(&self) -> (&ServerSigningKeyId, &VerifyKey) {
	debug_assert!(self.verify_keys.len() <= 1, "more than one active verify_key");
	self.verify_keys
		.iter()
		.next()
		.map(|(id, key)| (id.as_ref(), key))
		.expect("missing active verify_key")
}

#[implement(Service)]
pub async fn add_signing_keys(
	&self,
	raw_new_keys: &Raw<ServerSigningKeys>,
	source: FetchSource,
) -> Result<ServerSigningKeys> {
	let mut new_keys: ServerSigningKeys = raw_new_keys
		.deserialize()
		.map_err(|e| err!(BadServerResponse("{e}")))?;
	let origin = &new_keys.server_name;

	// MSC4499: "A future expired_ts (beyond a 5-minute clock-skew allowance) MUST
	// be treated as malformed for that specific key entry, but MUST NOT poison
	// the rest of the response payload."
	let now_plus_skew_tp =
		timepoint_from_now(Duration::from_secs(300)).expect("SystemTime should not overflow");
	let now_plus_skew = MilliSecondsSinceUnixEpoch::from_system_time(now_plus_skew_tp)
		.expect("UInt should not overflow");

	new_keys.old_verify_keys.retain(|key_id, ok| {
		if ok.expired_ts > now_plus_skew {
			conduwuit::warn!(
				"Ignoring malformed old_verify_key {key_id} for {origin}: expired_ts {ts:?} is \
				 in the future",
				ts = ok.expired_ts
			);
			return false;
		}
		true
	});

	// Intra-payload collision verification (MSC 4499)
	for (key_id, verify_key) in &new_keys.verify_keys {
		if let Some(old_verify_key) = new_keys.old_verify_keys.get(key_id) {
			if verify_key.key != old_verify_key.key {
				return Err(err!(Request(InvalidParam(
					"Intra-payload Key ID collision detected"
				))));
			}
		}
	}

	// Load the historical, cumulative keys under `origin\0historical`
	let historical_key = historical_db_key(origin);

	let historical_keys_res = self
		.db
		.server_signingkeys
		.get(&historical_key)
		.await
		.deserialized::<ServerSigningKeys>();

	let mut historical_keys = match historical_keys_res {
		| Ok(keys) => keys,
		| Err(ref e) if e.is_not_found() => {
			// Backward-compat: older versions stored merged keys directly under `origin`.
			match self
				.db
				.server_signingkeys
				.get(origin)
				.await
				.deserialized::<ServerSigningKeys>()
			{
				| Ok(keys) => keys,
				| Err(ref e) if e.is_not_found() =>
					ServerSigningKeys::new(origin.to_owned(), MilliSecondsSinceUnixEpoch::now()),
				| Err(e) => return Err(e),
			}
		},
		| Err(e) => return Err(e),
	};

	// MSC4499 "Notary fallback (two-tier binding)": key IDs whose binding is
	// still provisional (learned only via a notary). Only a direct fetch that
	// still finds the binding live in verify_keys (i.e. not yet retired to
	// old_verify_keys) may promote it to permanent; this dual condition is
	// this schema's stand-in for "not expired and not retired", since
	// per-key valid_until_ts isn't tracked separately from retirement here.
	let provisional_key = provisional_db_key(origin);
	let mut provisional: std::collections::BTreeSet<OwnedServerSigningKeyId> = self
		.db
		.server_signingkeys
		.get(&provisional_key)
		.await
		.deserialized()
		.unwrap_or_default();
	let mut provisional_changed = false;
	let originally_known_key_ids: std::collections::BTreeSet<OwnedServerSigningKeyId> =
		historical_keys
			.verify_keys
			.keys()
			.chain(historical_keys.old_verify_keys.keys())
			.cloned()
			.collect();

	// Helper to compute sha256 hex string for fingerprint logging
	let get_fingerprint = |base64_key: &ruma::serde::Base64| -> String {
		use sha2::{Digest, Sha256};
		let digest = Sha256::digest(base64_key.as_bytes());
		let mut s = String::with_capacity(digest.len().saturating_mul(2));
		for b in digest {
			use std::fmt::Write as _;
			let _ = write!(s, "{b:02x}");
		}
		s
	};

	let enforce_fsw = self.services.server.config.msc4499_first_seen_wins;
	let mut rejected_collision = false;

	// Merging with Collision Detection (First Seen Wins)
	let mut filtered_verify_keys = new_keys.verify_keys.clone();
	let mut filtered_old_verify_keys = new_keys.old_verify_keys.clone();
	let collision_action = if enforce_fsw {
		"Retaining cached key."
	} else {
		"Not enforcing because msc4499_first_seen_wins is disabled."
	};

	for (key_id, new_key) in &new_keys.verify_keys {
		if let Some(existing_key) = historical_keys.verify_keys.get(key_id) {
			if existing_key.key != new_key.key {
				if source == FetchSource::Direct && provisional.contains(key_id) {
					// MSC4499 "Notary fallback (two-tier binding)": a direct fetch
					// overriding a still-live provisional (notary-learned) binding is
					// promotion, not a collision. Leave filtered_verify_keys untouched
					// so the new (direct) body wins the merge below.
					conduwuit::warn!(
						"MSC4499: direct fetch overrides provisional notary-learned key \
						 {key_id} for {origin} (two-tier binding promotion); this becomes the \
						 permanent binding"
					);
					provisional.remove(key_id);
					provisional_changed = true;
				} else {
					let existing_fp = get_fingerprint(&existing_key.key);
					let new_fp = get_fingerprint(&new_key.key);
					conduwuit::warn!(
						"Key ID collision detected for server {origin} on active key {key_id}! \
						 Cached fingerprint: {existing_fp}, conflicting fingerprint: {new_fp}. \
						 {collision_action}"
					);
					if enforce_fsw {
						rejected_collision = true;
						filtered_verify_keys.remove(key_id);
					}
				}
			}
		} else if let Some(existing_old_key) = historical_keys.old_verify_keys.get(key_id) {
			if existing_old_key.key != new_key.key {
				let existing_fp = get_fingerprint(&existing_old_key.key);
				let new_fp = get_fingerprint(&new_key.key);
				conduwuit::warn!(
					"Key ID collision detected for server {origin} on active/old key {key_id}! \
					 Cached fingerprint: {existing_fp}, conflicting fingerprint: {new_fp}. \
					 {collision_action}"
				);
				if enforce_fsw {
					rejected_collision = true;
					filtered_verify_keys.remove(key_id);
				}
			}
		}
	}

	for (key_id, new_old_key) in &new_keys.old_verify_keys {
		if let Some(existing_key) = historical_keys.verify_keys.get(key_id) {
			if existing_key.key != new_old_key.key {
				let existing_fp = get_fingerprint(&existing_key.key);
				let new_fp = get_fingerprint(&new_old_key.key);
				conduwuit::warn!(
					"Key ID collision detected for server {origin} on old/active key {key_id}! \
					 Cached fingerprint: {existing_fp}, conflicting fingerprint: {new_fp}. \
					 {collision_action}"
				);
				if enforce_fsw {
					rejected_collision = true;
					filtered_old_verify_keys.remove(key_id);
				}
			}
		} else if let Some(existing_old_key) = historical_keys.old_verify_keys.get(key_id) {
			if existing_old_key.key != new_old_key.key {
				let existing_fp = get_fingerprint(&existing_old_key.key);
				let new_fp = get_fingerprint(&new_old_key.key);
				conduwuit::warn!(
					"Key ID collision detected for server {origin} on old key {key_id}! Cached \
					 fingerprint: {existing_fp}, conflicting fingerprint: {new_fp}. \
					 {collision_action}"
				);
				if enforce_fsw {
					rejected_collision = true;
					filtered_old_verify_keys.remove(key_id);
				}
			}
		}
	}

	// Merge and clean up: if a key exists in both, the new verify_keys takes
	// precedence and we remove it from historical_keys.old_verify_keys.
	// Conversely, if a key is in old_verify_keys, we ensure it's not in
	// verify_keys.
	for key_id in filtered_verify_keys.keys() {
		historical_keys.old_verify_keys.remove(key_id);
	}
	for key_id in filtered_old_verify_keys.keys() {
		historical_keys.verify_keys.remove(key_id);
	}

	let now = MilliSecondsSinceUnixEpoch::now();

	// Any key in historical_keys.verify_keys that is NOT in filtered_verify_keys
	// has been retired. We must move it to old_verify_keys with a fixed expired_ts.
	let mut retired_keys = Vec::new();
	for (key_id, key) in &historical_keys.verify_keys {
		if !filtered_verify_keys.contains_key(key_id) {
			retired_keys.push((key_id.clone(), key.clone()));
		}
	}
	for (key_id, key) in retired_keys {
		historical_keys.verify_keys.remove(&key_id);
		historical_keys
			.old_verify_keys
			.entry(key_id)
			.or_insert_with(|| OldVerifyKey { key: key.key, expired_ts: now });
	}

	// Store the filtered/merged historical keys
	historical_keys.verify_keys.extend(filtered_verify_keys);
	for (key_id, old_key) in filtered_old_verify_keys {
		if enforce_fsw {
			historical_keys
				.old_verify_keys
				.entry(key_id)
				.or_insert(old_key);
		} else {
			historical_keys.old_verify_keys.insert(key_id, old_key);
		}
	}

	// MSC4499: retain at most 3,000 retired keys in old_verify_keys.
	// Keys in verify_keys are exempt from this quota (verify_keys itself is capped).
	let old_keys = historical_keys.old_verify_keys.len();
	if old_keys > 3000 {
		let to_evict = old_keys.saturating_sub(3000);
		conduwuit::debug!(
			"MSC4499: Evicting {to_evict} oldest old_verify_keys for {origin} to respect the 3,000-key retired-key quota"
		);

		// Collect keys to evict: oldest first (lowest expired_ts).
		// For ties, break by key_id descending (so smaller identifiers are retained).
		let mut ovks: Vec<_> = historical_keys.old_verify_keys.iter().collect();
		ovks.sort_by(|(id_a, ok_a), (id_b, ok_b)| {
			ok_a.expired_ts
				.cmp(&ok_b.expired_ts)
				.then_with(|| id_b.cmp(id_a))
		});

		let to_evict_ids: Vec<_> = ovks
			.iter()
			.take(to_evict)
			.map(|(id, _)| (*id).to_owned())
			.collect();

		for id in to_evict_ids {
			conduwuit::warn!(
				"MSC4499: evicted old_verify_key {id} for {origin} due to 3,000-key quota"
			);
			historical_keys.old_verify_keys.remove(&id);
			new_keys.old_verify_keys.remove(&id);
			if provisional.remove(&id) {
				provisional_changed = true;
			}
		}
	}

	// MSC4499 "Notary fallback (two-tier binding)": a key ID observed for the
	// very first time via a notary starts provisional; one observed directly
	// starts (and stays) permanent, so it's never added here.
	if source == FetchSource::Notary {
		for key_id in new_keys
			.verify_keys
			.keys()
			.chain(new_keys.old_verify_keys.keys())
		{
			if !originally_known_key_ids.contains(key_id) && provisional.insert(key_id.clone()) {
				provisional_changed = true;
			}
		}
	}

	if provisional_changed {
		self.db
			.server_signingkeys
			.raw_put(&provisional_key, Json(&provisional));
	}

	self.db
		.server_signingkeys
		.raw_put(&historical_key, Json(&historical_keys));

	// MSC4499 First-Seen-Wins enforcement on the origin record.
	// When enabled, replace any colliding keys in new_keys with their first-seen
	// values before storing. This ensures the notary never serves replaced keys.
	// Collisions are always logged above regardless of this setting.
	// Note: historical_keys now contains the complete merged state after extend().
	if enforce_fsw {
		for (key_id, vk) in &mut new_keys.verify_keys {
			let first_seen = historical_keys
				.verify_keys
				.get(key_id)
				.map(|k| &k.key)
				.or_else(|| historical_keys.old_verify_keys.get(key_id).map(|k| &k.key));

			if let Some(first_seen) = first_seen {
				if vk.key != *first_seen {
					vk.key = first_seen.clone();
				}
			}
		}
		for (key_id, ok) in &mut new_keys.old_verify_keys {
			let first_seen = historical_keys
				.verify_keys
				.get(key_id)
				.map(|k| &k.key)
				.or_else(|| historical_keys.old_verify_keys.get(key_id).map(|k| &k.key));

			if let Some(first_seen) = first_seen {
				if ok.key != *first_seen {
					ok.key = first_seen.clone();
				}
			}
		}
	}

	// Preserve the last raw payload that matched the accepted first-seen bindings.
	// A rejected collision must not replace the per-origin record, since that raw
	// blob is later re-signed and served by our notary endpoints.
	if !rejected_collision {
		self.db
			.server_signingkeys
			.raw_put(origin, Json(raw_new_keys));
	}

	Ok(new_keys)
}

#[implement(Service)]
#[tracing::instrument(skip(self, object), level = "debug")]
pub async fn required_keys_exist(
	&self,
	object: &CanonicalJsonObject,
	version: &RoomVersionId,
) -> bool {
	use ruma::signatures::required_keys;

	trace!(?object, "Checking required keys exist");
	let Ok(required_keys) = required_keys(object, version) else {
		debug_error!("Failed to determine required keys");
		return false;
	};
	trace!(?required_keys, "Required keys to verify event");
	required_keys
		.iter()
		.flat_map(|(server, key_ids)| key_ids.iter().map(move |key_id| (server, key_id)))
		.stream()
		.all(|(server, key_id)| self.verify_key_exists(server, key_id))
		.await
}

#[implement(Service)]
#[tracing::instrument(skip(self), level = "debug")]
pub async fn verify_key_exists(&self, origin: &ServerName, key_id: &ServerSigningKeyId) -> bool {
	type KeysMap<'a> = BTreeMap<&'a ServerSigningKeyId, &'a RawJsonValue>;

	let historical_key = historical_db_key(origin);

	if let Ok(keys) = self
		.db
		.server_signingkeys
		.get(&historical_key)
		.await
		.deserialized::<Raw<ServerSigningKeys>>()
	{
		if let Ok(Some(verify_keys)) = keys.get_field::<KeysMap<'_>>("verify_keys") {
			if verify_keys.contains_key(key_id) {
				return true;
			}
		}

		if let Ok(Some(old_verify_keys)) = keys.get_field::<KeysMap<'_>>("old_verify_keys") {
			if old_verify_keys.contains_key(key_id) {
				return true;
			}
		}
	}

	if let Ok(keys) = self
		.db
		.server_signingkeys
		.get(origin)
		.await
		.deserialized::<Raw<ServerSigningKeys>>()
	{
		if let Ok(Some(verify_keys)) = keys.get_field::<KeysMap<'_>>("verify_keys") {
			if verify_keys.contains_key(key_id) {
				return true;
			}
		}

		if let Ok(Some(old_verify_keys)) = keys.get_field::<KeysMap<'_>>("old_verify_keys") {
			if old_verify_keys.contains_key(key_id) {
				return true;
			}
		}
	}

	debug_warn!("Key {key_id} not found for {origin}");
	false
}

#[implement(Service)]
pub async fn verify_keys_for(&self, origin: &ServerName) -> VerifyKeys {
	let historical_key = historical_db_key(origin);

	let mut keys = BTreeMap::new();

	if let Ok(historical_keys) = self
		.db
		.server_signingkeys
		.get(&historical_key)
		.await
		.deserialized::<ServerSigningKeys>()
	{
		keys.extend(merge_old_keys(historical_keys).verify_keys);
	}

	if let Ok(origin_keys) = self.signing_keys_for(origin).await {
		for (key_id, verify_key) in merge_old_keys(origin_keys).verify_keys {
			keys.entry(key_id).or_insert(verify_key);
		}
	}

	if self.services.globals.server_is_ours(origin) {
		keys.extend(self.verify_keys.clone().into_iter());
	}

	keys
}

#[implement(Service)]
pub async fn signing_keys_for(&self, origin: &ServerName) -> Result<ServerSigningKeys> {
	self.raw_signing_keys_for(origin)
		.await?
		.deserialize()
		.map_err(|e| err!(BadServerResponse("{e}")))
}

#[implement(Service)]
pub async fn raw_signing_keys_for(&self, origin: &ServerName) -> Result<Raw<ServerSigningKeys>> {
	self.db.server_signingkeys.get(origin).await.deserialized()
}

#[implement(Service)]
pub async fn merged_signing_keys_for(&self, origin: &ServerName) -> Result<ServerSigningKeys> {
	let mut keys: ServerSigningKeys = self
		.raw_signing_keys_for(origin)
		.await?
		.deserialize()
		.map_err(|e| err!(BadServerResponse("{e}")))?;

	// Augment with historical keys if they exist. We prioritize the latest keys.
	let historical_key = historical_db_key(origin);
	if let Ok(historical_keys) = self
		.db
		.server_signingkeys
		.get(&historical_key)
		.await
		.deserialized::<ServerSigningKeys>()
	{
		// Augment with historical keys if they exist. We prioritize the latest keys.
		// We merge historical old_verify_keys into the latest payload so historical
		// key material remains available for verification and notary responses.
		// Preserve the first-seen record for each key ID instead of letting a
		// later payload overwrite an earlier expired_ts.
		let mut merged_ovks = historical_keys.old_verify_keys;
		for (key_id, old_key) in keys.old_verify_keys {
			merged_ovks.entry(key_id).or_insert(old_key);
		}

		keys.old_verify_keys = merged_ovks;
	}

	Ok(keys)
}

#[implement(Service)]
fn minimum_valid_ts(&self) -> MilliSecondsSinceUnixEpoch {
	let timepoint =
		timepoint_from_now(self.minimum_valid).expect("SystemTime should not overflow");
	MilliSecondsSinceUnixEpoch::from_system_time(timepoint).expect("UInt should not overflow")
}

fn merge_old_keys(mut keys: ServerSigningKeys) -> ServerSigningKeys {
	keys.verify_keys.extend(
		keys.old_verify_keys
			.clone()
			.into_iter()
			.map(|(key_id, old)| (key_id, VerifyKey::new(old.key))),
	);

	keys
}

fn extract_key(mut keys: ServerSigningKeys, key_id: &ServerSigningKeyId) -> Option<VerifyKey> {
	keys.verify_keys.remove(key_id).or_else(|| {
		keys.old_verify_keys
			.remove(key_id)
			.map(|old| VerifyKey::new(old.key))
	})
}

fn key_exists(keys: &ServerSigningKeys, key_id: &ServerSigningKeyId) -> bool {
	keys.verify_keys.contains_key(key_id) || keys.old_verify_keys.contains_key(key_id)
}

fn bounded_msc4499_backoff_secs(secs: u64) -> u64 { secs.clamp(1, 3600) }

#[cfg(test)]
mod tests {
	use super::bounded_msc4499_backoff_secs;

	#[test]
	fn msc4499_backoff_keeps_positive_lower_bound() {
		assert_eq!(bounded_msc4499_backoff_secs(0), 1);
		assert_eq!(bounded_msc4499_backoff_secs(2), 2);
		assert_eq!(bounded_msc4499_backoff_secs(3601), 3600);
	}
}
