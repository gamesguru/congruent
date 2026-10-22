mod acquire;
mod get;
mod keypair;
mod request;
mod sign;
mod validate;
mod verify;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use async_lock::RwLock;
use conduwuit::{
	Result, Server, debug_error, err, implement, trace,
	utils::{IterStream, MutexMap, timepoint_from_now},
};
use futures::StreamExt;
use slipstream::{
	CanonicalJsonObject, MilliSecondsSinceUnixEpoch, OwnedServerName, OwnedServerSigningKeyId,
	RoomVersionId, ServerName, ServerSigningKeyId,
	api::federation::discovery::{OldVerifyKey, ServerSigningKeys, VerifyKey},
	signatures::{Ed25519KeyPair, PublicKeyMap, PublicKeySet},
	sswire::Raw,
};

use crate::{Dep, globals, sending};

#[derive(Clone, Copy, Debug)]
struct BackoffState {
	expires: std::time::Instant,
	failures: u32,
}

const BACKOFF_FAILURE_MEMORY: Duration = Duration::from_hours(24);
const MAX_FETCH_BACKOFF_ENTRIES: usize = 4096;

/// MSC4499's retired-key ceiling: the maximum number of `old_verify_keys`
/// entries retained per origin, both as the storage-layer eviction target
/// here and as the per-response rejection threshold in `validate.rs`. The two
/// must agree -- if the storage layer ever accepted more than the per-response
/// check allows, a single oversized response could fill the whole quota in one
/// shot.
pub(super) const MSC4499_RETIRED_KEY_CEILING: usize = 3000;

/// How long a notary-learned binding stays open to a direct-fetch override.
/// MSC4499 leaves the exact window to the implementation; seven days matches
/// the key cache's normal refresh horizon.
const MSC4499_PROMOTION_WINDOW_MS: u64 = 604_800_000;

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
	mtxdb_keys: conduwuit_mtxdb::server_keys::ServerKeysAdapter,
}

pub type VerifyKeys = BTreeMap<OwnedServerSigningKeyId, VerifyKey>;
pub type PubKeyMap = PublicKeyMap;
pub type PubKeys = PublicKeySet;

fn signing_keys_from_native(
	origin: &ServerName,
	keys: &[conduwuit_mtxdb::server_keys::ServerKey],
) -> Result<ServerSigningKeys> {
	let mut out = ServerSigningKeys::new(origin.to_owned(), MilliSecondsSinceUnixEpoch(0));
	for key in keys {
		if key.is_evicted() {
			continue;
		}
		let key_id = std::str::from_utf8(&key.key_id)
			.map_err(|error| err!(Database("Invalid signing-key id in mtxdb: {error}")))?;
		let key_id = OwnedServerSigningKeyId::parse(key_id)
			.map_err(|error| err!(Database("Invalid signing-key id in mtxdb: {error}")))?;
		out.valid_until_ts =
			MilliSecondsSinceUnixEpoch(out.valid_until_ts.0.max(key.valid_until_ts));
		if let Some(expired_ts) = key.retirement_ts() {
			out.old_verify_keys.insert(
				key_id,
				OldVerifyKey::new(
					MilliSecondsSinceUnixEpoch(expired_ts),
					slipstream::sswire::Base64::new(key.key.clone()),
				),
			);
		} else {
			out.verify_keys
				.insert(key_id, VerifyKey::new(slipstream::sswire::Base64::new(key.key.clone())));
		}
	}
	Ok(out)
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let minimum_valid = Duration::from_hours(1);

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
				mtxdb_keys: conduwuit_mtxdb::server_keys::ServerKeysAdapter::new(
					args.mtxdb.clone(),
				),
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
		.or_else(|| now.checked_add(Duration::from_hours(24)))
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

/// MSC4499 "Corroboration tier" retired-key eviction ordering: given the
/// full retired-key set for a remote server and the subset of those key IDs
/// we've independently corroborated (previously observed active), returns
/// the key IDs to evict to bring the set down to `cap` entries.
///
/// Corroborated bindings are retained ahead of uncorroborated ones
/// regardless of raw recency; within each tier, the most-recently-retired
/// bindings are retained first (ties broken by key ID, ascending, so a
/// smaller identifier wins the tie). This recomputes the full retained set
/// from scratch every call rather than only comparing new candidates
/// against the existing tail, so the result is deterministic regardless of
/// arrival order.
///
/// A no-op (empty result) if `old_verify_keys.len() <= cap`.
#[cfg(test)]
fn select_old_verify_keys_to_evict(
	old_verify_keys: &BTreeMap<OwnedServerSigningKeyId, OldVerifyKey>,
	corroborated: &std::collections::BTreeSet<OwnedServerSigningKeyId>,
	cap: usize,
) -> Vec<OwnedServerSigningKeyId> {
	if old_verify_keys.len() <= cap {
		return Vec::new();
	}

	// Descending by expired_ts (most-recently-retired sorts first, i.e. kept);
	// on a tie, ascending by key_id (smaller identifier sorts first, i.e. kept)
	// -- both directions put the *retained* element first in the resulting
	// vector, since the caller skips the first `cap` entries as "kept" and
	// evicts the rest.
	let by_recency =
		|(id_a, ok_a): &(&OwnedServerSigningKeyId, &OldVerifyKey),
		 (id_b, ok_b): &(&OwnedServerSigningKeyId, &OldVerifyKey)| {
			ok_b.expired_ts
				.cmp(&ok_a.expired_ts)
				.then_with(|| id_a.cmp(id_b))
		};

	let (mut corroborated_ovks, mut uncorroborated_ovks): (Vec<_>, Vec<_>) = old_verify_keys
		.iter()
		.partition(|(id, _)| corroborated.contains(*id));
	corroborated_ovks.sort_by(by_recency);
	uncorroborated_ovks.sort_by(by_recency);

	corroborated_ovks
		.into_iter()
		.chain(uncorroborated_ovks)
		.skip(cap)
		.map(|(id, _)| id.to_owned())
		.collect()
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
		.expect("missing active verify_key")
}

#[implement(Service)]
pub fn add_signing_keys(
	&self,
	raw_new_keys: &Raw<ServerSigningKeys>,
	source: FetchSource,
) -> std::future::Ready<Result<ServerSigningKeys>> {
	std::future::ready(self.add_signing_keys_sync(raw_new_keys, source))
}

#[implement(Service)]
fn add_signing_keys_sync(
	&self,
	raw_new_keys: &Raw<ServerSigningKeys>,
	source: FetchSource,
) -> Result<ServerSigningKeys> {
	let new_keys: ServerSigningKeys = raw_new_keys
		.deserialize()
		.map_err(|e| err!(BadServerResponse("{e}")))?;
	let origin = &new_keys.server_name;

	// MSC4499: a single payload must not carry two bodies for one key id.
	for (key_id, verify_key) in &new_keys.verify_keys {
		if let Some(old_verify_key) = new_keys.old_verify_keys.get(key_id) {
			if verify_key.key != old_verify_key.key {
				return Err(err!(Request(InvalidParam(
					"Intra-payload Key ID collision detected"
				))));
			}
		}
	}

	let mut observed = Vec::with_capacity(
		new_keys
			.verify_keys
			.len()
			.saturating_add(new_keys.old_verify_keys.len()),
	);
	for (key_id, verify_key) in &new_keys.verify_keys {
		observed.push(conduwuit_mtxdb::server_keys::ObservedKey {
			key_id: key_id.as_str().as_bytes().to_vec(),
			key: verify_key.key.as_bytes().to_vec(),
			valid_until_ts: new_keys.valid_until_ts.0,
			expired_ts: None,
		});
	}
	for (key_id, old_verify_key) in &new_keys.old_verify_keys {
		observed.push(conduwuit_mtxdb::server_keys::ObservedKey {
			key_id: key_id.as_str().as_bytes().to_vec(),
			key: old_verify_key.key.as_bytes().to_vec(),
			valid_until_ts: new_keys.valid_until_ts.0,
			expired_ts: Some(old_verify_key.expired_ts.0),
		});
	}

	let sighting = match source {
		| FetchSource::Direct => conduwuit_mtxdb::server_keys::KeySighting::Direct,
		| FetchSource::Notary => conduwuit_mtxdb::server_keys::KeySighting::Notary,
	};
	let update = conduwuit_mtxdb::server_keys::KeyUpdate {
		origin: origin.as_bytes(),
		sighting,
		source: match source {
			| FetchSource::Direct => b"direct",
			| FetchSource::Notary => b"notary",
		},
		observed: &observed,
		now_ms: MilliSecondsSinceUnixEpoch::now().0,
		notary: Some(raw_new_keys.get().as_bytes()),
		promotion_window_ms: MSC4499_PROMOTION_WINDOW_MS,
		snapshot: source == FetchSource::Direct,
		limits: conduwuit_mtxdb::server_keys::KeyLimits {
			retired_retained: MSC4499_RETIRED_KEY_CEILING,
			..Default::default()
		},
	};

	let merged = self.db.mtxdb_keys.update(&update).map_err(|error| {
		err!(Database("Failed to update mtxdb signing keys for {origin}: {error}"))
	})?;
	for issue in &merged.issues {
		let key_id = String::from_utf8_lossy(&issue.key_id);
		match issue.kind {
			| conduwuit_mtxdb::server_keys::KeyIssueKind::Collision =>
				conduwuit::warn!("MSC4499: rejected colliding key {key_id} for {origin}"),
			| conduwuit_mtxdb::server_keys::KeyIssueKind::Displaced => conduwuit::warn!(
				"MSC4499: direct fetch overrode provisional key {key_id} for {origin}"
			),
			| conduwuit_mtxdb::server_keys::KeyIssueKind::ExpiredTsConflict => conduwuit::warn!(
				"MSC4499: ignored a conflicting expired_ts for {key_id} of {origin}"
			),
			| conduwuit_mtxdb::server_keys::KeyIssueKind::FutureExpiredTs => conduwuit::warn!(
				"Ignoring malformed old_verify_key {key_id} for {origin}: expired_ts is in the \
				 future"
			),
			| conduwuit_mtxdb::server_keys::KeyIssueKind::DigestCapReached => conduwuit::warn!(
				"MSC4499: refused new key {key_id} for {origin}: digest cap reached"
			),
			| conduwuit_mtxdb::server_keys::KeyIssueKind::Evicted =>
				conduwuit::debug!("MSC4499: evicted old_verify_key {key_id} for {origin}"),
			| conduwuit_mtxdb::server_keys::KeyIssueKind::EvictedCorroborated =>
				conduwuit::warn!(
					"MSC4499: evicted corroborated old_verify_key {key_id} for {origin}"
				),
		}
	}

	let mut out = signing_keys_from_native(origin, &merged.keys)?;
	out.signatures = new_keys.signatures;
	out.valid_until_ts = new_keys.valid_until_ts;
	Ok(out)
}

/// What a keys export wrote. `historical` counts permanent-tier records.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct KeysExportCounts {
	pub historical: u64,
	pub provisional: u64,
	pub corroborated: u64,
	pub notary: u64,
}

/// Exports all signing key records as JSONL. Each line is a `server_keys`
/// object: one native per-key record (provisional or permanent), or the
/// origin's verbatim Notary payload. The mtxdb CLI importer reads this layout
/// directly.
#[implement(Service)]
pub fn export_keys_jsonl<W: std::io::Write + Send>(
	&self,
	out: &mut W,
) -> std::future::Ready<Result<KeysExportCounts>> {
	std::future::ready(self.export_keys_jsonl_sync(out))
}

#[implement(Service)]
fn export_keys_jsonl_sync<W: std::io::Write + Send>(
	&self,
	out: &mut W,
) -> Result<KeysExportCounts> {
	use base64::{Engine as _, engine::general_purpose::STANDARD};
	use conduwuit_mtxdb::export_schema::{field, key_kind, tag};
	use slipstream::json::{Object, Value};

	fn line<W: std::io::Write>(out: &mut W, record: Object) -> Result<()> {
		let text = slipstream::codec::to_string(&Value::Object(record));
		out.write_all(text.as_bytes())
			.and_then(|()| out.write_all(b"\n"))
			.map_err(|e| err!(Database("export write failed: {e}")))
	}

	fn record(tag: &str) -> Object {
		let mut object = Object::new();
		object.insert(field::T.to_owned(), Value::String(tag.to_owned()));
		object
	}

	let mut counts = KeysExportCounts::default();
	let origins =
		self.db.mtxdb_keys.origins().map_err(|error| {
			err!(Database("Failed to list mtxdb signing-key origins: {error}"))
		})?;

	for origin_bytes in origins {
		let origin = std::str::from_utf8(&origin_bytes)
			.map_err(|error| err!(Database("Invalid signing-key origin in mtxdb: {error}")))?;
		let keys = self
			.db
			.mtxdb_keys
			.read_keys(&origin_bytes)
			.map_err(|error| {
				err!(Database("Failed to read mtxdb signing keys for {origin}: {error}"))
			})?;

		for key in keys {
			let mut obj = record(tag::SERVER_KEYS);
			obj.insert(field::ORIGIN.to_owned(), Value::String(origin.to_owned()));
			let kind = match key.tier {
				| conduwuit_mtxdb::server_keys::KeyTier::Provisional => key_kind::PROVISIONAL,
				| conduwuit_mtxdb::server_keys::KeyTier::Permanent => key_kind::PERMANENT,
			};
			obj.insert(field::KIND.to_owned(), Value::String(kind.to_owned()));
			obj.insert(
				field::KEY_ID.to_owned(),
				Value::String(String::from_utf8_lossy(&key.key_id).into_owned()),
			);
			if !key.key.is_empty() {
				obj.insert(field::KEY.to_owned(), Value::String(STANDARD.encode(&key.key)));
			}
			obj.insert(field::DIGEST.to_owned(), Value::String(STANDARD.encode(key.digest)));
			obj.insert(field::VALID_UNTIL_TS.to_owned(), Value::from(key.valid_until_ts));
			obj.insert(
				field::SOURCE.to_owned(),
				Value::String(String::from_utf8_lossy(&key.source).into_owned()),
			);
			obj.insert(field::TS_ADDED_MS.to_owned(), Value::from(key.ts_added_ms));
			obj.insert(field::FIRST_SEEN_MS.to_owned(), Value::from(key.first_seen_ms));
			obj.insert(field::LAST_ACTIVE_MS.to_owned(), Value::from(key.last_active_ms));
			obj.insert(field::PROMOTE_BY_MS.to_owned(), Value::from(key.promote_by_ms));
			obj.insert(field::CORROBORATED.to_owned(), Value::Bool(key.corroborated));
			obj.insert(field::INFERRED_RETIRED.to_owned(), Value::Bool(key.inferred_retired));
			if let Some(expired_ts) = key.expired_ts {
				obj.insert(field::EXPIRED_TS.to_owned(), Value::from(expired_ts));
			}

			match key.tier {
				| conduwuit_mtxdb::server_keys::KeyTier::Provisional => {
					counts.provisional = counts.provisional.saturating_add(1);
				},
				| conduwuit_mtxdb::server_keys::KeyTier::Permanent => {
					counts.historical = counts.historical.saturating_add(1);
				},
			}
			if key.corroborated {
				counts.corroborated = counts.corroborated.saturating_add(1);
			}
			line(out, obj)?;
		}

		if let Some(payload) = self
			.db
			.mtxdb_keys
			.read_notary(&origin_bytes)
			.map_err(|error| {
				err!(Database("Failed to read mtxdb notary keys for {origin}: {error}"))
			})? {
			let mut obj = record(tag::SERVER_KEYS);
			obj.insert(field::ORIGIN.to_owned(), Value::String(origin.to_owned()));
			obj.insert(field::KIND.to_owned(), Value::String(key_kind::NOTARY.to_owned()));
			obj.insert(field::PAYLOAD.to_owned(), Value::String(STANDARD.encode(payload)));
			counts.notary = counts.notary.saturating_add(1);
			line(out, obj)?;
		}
	}

	out.flush()
		.map_err(|e| err!(Database("export flush failed: {e}")))?;
	Ok(counts)
}

#[implement(Service)]
pub async fn required_keys_exist(
	&self,
	object: &CanonicalJsonObject,
	version: &RoomVersionId,
) -> bool {
	use slipstream::signatures::required_keys;

	trace!(?object, "Checking required keys exist");
	let Ok(required_keys) = required_keys(object, version) else {
		debug_error!("Failed to determine required keys");
		return false;
	};
	trace!(?required_keys, "Required keys to verify event");
	let keys: Vec<(OwnedServerName, OwnedServerSigningKeyId)> = required_keys
		.iter()
		.flat_map(|(server, key_ids)| {
			key_ids
				.iter()
				.map(move |key_id| (server.to_owned(), key_id.to_owned()))
		})
		.collect();
	keys.iter()
		.stream()
		.all(|(server, key_id)| self.verify_key_exists(server, key_id))
		.await
}

#[implement(Service)]
pub fn verify_key_exists(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
) -> std::future::Ready<bool> {
	std::future::ready(self.verify_key_exists_sync(origin, key_id))
}

#[implement(Service)]
fn verify_key_exists_sync(&self, origin: &ServerName, key_id: &ServerSigningKeyId) -> bool {
	// Our own active signing key is held in memory, not necessarily in the DB
	if self.services.globals.server_is_ours(origin) && self.verify_keys.contains_key(key_id) {
		return true;
	}

	self.db
		.mtxdb_keys
		.read_key(origin.as_bytes(), key_id.as_bytes())
		.unwrap_or_else(|error| panic!("Failed to read mtxdb signing keys for {origin}: {error}"))
		.is_some()
}

#[implement(Service)]
pub fn verify_keys_for(&self, origin: &ServerName) -> std::future::Ready<VerifyKeys> {
	std::future::ready(self.verify_keys_for_sync(origin))
}

#[implement(Service)]
fn verify_keys_for_sync(&self, origin: &ServerName) -> VerifyKeys {
	let native = self
		.db
		.mtxdb_keys
		.read_keys(origin.as_bytes())
		.unwrap_or_else(|error| {
			panic!("Failed to read mtxdb signing keys for {origin}: {error}")
		});
	let mut keys = signing_keys_from_native(origin, &native)
		.map_or_else(
			|error| panic!("Failed to decode mtxdb signing keys for {origin}: {error}"),
			merge_old_keys,
		)
		.verify_keys;
	if self.services.globals.server_is_ours(origin) {
		keys.extend(self.verify_keys.clone());
	}

	keys
}

/// Returns only the active (non-retired) verify keys for the given origin.
/// Retired keys in `old_verify_keys` are excluded.
#[implement(Service)]
pub fn active_verify_keys_for(&self, origin: &ServerName) -> std::future::Ready<VerifyKeys> {
	std::future::ready(self.active_verify_keys_for_sync(origin))
}

#[implement(Service)]
fn active_verify_keys_for_sync(&self, origin: &ServerName) -> VerifyKeys {
	let native = self
		.db
		.mtxdb_keys
		.read_keys(origin.as_bytes())
		.unwrap_or_else(|error| {
			panic!("Failed to read mtxdb signing keys for {origin}: {error}")
		});
	let mut keys = signing_keys_from_native(origin, &native)
		.unwrap_or_else(|error| {
			panic!("Failed to decode mtxdb signing keys for {origin}: {error}")
		})
		.verify_keys;
	if self.services.globals.server_is_ours(origin) {
		keys.extend(self.verify_keys.clone());
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
pub fn raw_signing_keys_for(
	&self,
	origin: &ServerName,
) -> std::future::Ready<Result<Raw<ServerSigningKeys>>> {
	std::future::ready(self.raw_signing_keys_for_sync(origin))
}

#[implement(Service)]
fn raw_signing_keys_for_sync(&self, origin: &ServerName) -> Result<Raw<ServerSigningKeys>> {
	let payload = self
		.db
		.mtxdb_keys
		.read_notary(origin.as_bytes())
		.map_err(|error| {
			err!(Database("Failed to read mtxdb notary keys for {origin}: {error}"))
		})?
		.ok_or_else(|| err!(Request(NotFound("No signing-key record for {origin}"))))?;
	let payload = std::str::from_utf8(&payload)
		.map_err(|error| err!(Database("Invalid notary payload for {origin}: {error}")))?;
	Raw::from_json_string(payload.to_owned())
		.map_err(|error| err!(Database("Invalid notary payload for {origin}: {error}")))
}

#[implement(Service)]
pub fn merged_signing_keys_for(
	&self,
	origin: &ServerName,
) -> std::future::Ready<Result<ServerSigningKeys>> {
	std::future::ready(self.merged_signing_keys_for_sync(origin))
}

#[implement(Service)]
fn merged_signing_keys_for_sync(&self, origin: &ServerName) -> Result<ServerSigningKeys> {
	let native = self
		.db
		.mtxdb_keys
		.read_keys(origin.as_bytes())
		.map_err(|error| {
			err!(Database("Failed to read mtxdb signing keys for {origin}: {error}"))
		})?;
	if native.is_empty() {
		return Err(err!(Request(NotFound("No signing-key record for {origin}"))));
	}
	signing_keys_from_native(origin, &native)
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
	use std::collections::BTreeSet;

	use slipstream::{MilliSecondsSinceUnixEpoch, OwnedServerSigningKeyId, sswire::Base64};

	use super::{
		BTreeMap, OldVerifyKey, bounded_msc4499_backoff_secs, select_old_verify_keys_to_evict,
	};

	#[test]
	fn msc4499_backoff_keeps_positive_lower_bound() {
		assert_eq!(bounded_msc4499_backoff_secs(0), 1);
		assert_eq!(bounded_msc4499_backoff_secs(2), 2);
		assert_eq!(bounded_msc4499_backoff_secs(3601), 3600);
	}

	fn key_id(name: &str) -> OwnedServerSigningKeyId {
		format!("ed25519:{name}").try_into().unwrap()
	}

	fn old_verify_key(expired_ts_ms: u64) -> OldVerifyKey {
		OldVerifyKey::new(MilliSecondsSinceUnixEpoch(expired_ts_ms), Base64::new(vec![0_u8; 32]))
	}

	#[test]
	fn evict_is_noop_under_cap() {
		let mut ovks = BTreeMap::new();
		ovks.insert(key_id("a"), old_verify_key(100));
		ovks.insert(key_id("b"), old_verify_key(200));

		assert!(select_old_verify_keys_to_evict(&ovks, &BTreeSet::new(), 2).is_empty());
		assert!(select_old_verify_keys_to_evict(&ovks, &BTreeSet::new(), 5).is_empty());
	}

	#[test]
	fn evict_picks_oldest_first_when_uncorroborated() {
		let mut ovks = BTreeMap::new();
		ovks.insert(key_id("oldest"), old_verify_key(100));
		ovks.insert(key_id("middle"), old_verify_key(200));
		ovks.insert(key_id("newest"), old_verify_key(300));

		let evicted = select_old_verify_keys_to_evict(&ovks, &BTreeSet::new(), 2);
		assert_eq!(evicted, vec![key_id("oldest")]);
	}

	#[test]
	fn evict_prefers_evicting_uncorroborated_over_older_corroborated() {
		let mut ovks = BTreeMap::new();
		// Corroborated, but far older than the uncorroborated entries below --
		// MSC4499 requires it survive ahead of them anyway.
		ovks.insert(key_id("corroborated_ancient"), old_verify_key(1));
		ovks.insert(key_id("uncorroborated_newer_1"), old_verify_key(1000));
		ovks.insert(key_id("uncorroborated_newer_2"), old_verify_key(2000));

		let mut corroborated = BTreeSet::new();
		corroborated.insert(key_id("corroborated_ancient"));

		// Cap of 2: normally (pure recency) the ancient corroborated key would be
		// the first evicted. Corroboration must override that.
		let evicted = select_old_verify_keys_to_evict(&ovks, &corroborated, 2);
		assert_eq!(evicted, vec![key_id("uncorroborated_newer_1")]);
	}

	#[test]
	fn evict_fills_cap_from_corroborated_tier_before_touching_uncorroborated() {
		let mut ovks = BTreeMap::new();
		ovks.insert(key_id("corroborated_1"), old_verify_key(100));
		ovks.insert(key_id("corroborated_2"), old_verify_key(200));
		ovks.insert(key_id("uncorroborated"), old_verify_key(300));

		let mut corroborated = BTreeSet::new();
		corroborated.insert(key_id("corroborated_1"));
		corroborated.insert(key_id("corroborated_2"));

		// Cap of 1: the corroborated tier (2 entries) alone already exceeds it,
		// so it claims the only slot -- the most-recently-retired corroborated
		// entry survives, the older corroborated entry is evicted, and the
		// uncorroborated entry gets none of the remaining slots (there are
		// none) despite being the most recently retired of all three.
		let evicted = select_old_verify_keys_to_evict(&ovks, &corroborated, 1);
		assert_eq!(evicted, vec![key_id("corroborated_1"), key_id("uncorroborated")]);
	}

	#[test]
	fn evict_breaks_recency_ties_retaining_smaller_key_id() {
		let mut ovks = BTreeMap::new();
		ovks.insert(key_id("aaa"), old_verify_key(500));
		ovks.insert(key_id("zzz"), old_verify_key(500));

		// Same expired_ts: the smaller key_id ("aaa") is retained, "zzz" evicted.
		let evicted = select_old_verify_keys_to_evict(&ovks, &BTreeSet::new(), 1);
		assert_eq!(evicted, vec![key_id("zzz")]);
	}
}
