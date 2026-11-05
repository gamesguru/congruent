use std::{
	borrow::Borrow,
	collections::{BTreeSet, HashMap},
	sync::Arc,
};

use conduwuit::{Err, Result, debug_error, debug_warn, err, implement, trace, warn};
use slipstream::{
	CanonicalJsonObject, MilliSecondsSinceUnixEpoch, OwnedServerName, RoomVersionId, ServerName,
	ServerSigningKeyId, api::federation::discovery::VerifyKey, sswire::Base64,
};

use super::{PubKeyMap, PubKeys, extract_key};

/// Whether event signature verification enforces a key's `valid_until_ts`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ValidUntilPolicy {
	/// Ignore `valid_until_ts`, as required by room versions 1 through 4.
	Ignore,
	/// Require the key's validity window to cover the event timestamp.
	Enforce,
}

fn validity_policy_for_version(version: &RoomVersionId) -> ValidUntilPolicy {
	if matches!(
		version,
		RoomVersionId::V1 | RoomVersionId::V2 | RoomVersionId::V3 | RoomVersionId::V4
	) {
		ValidUntilPolicy::Ignore
	} else {
		ValidUntilPolicy::Enforce
	}
}

/// Signing-key metadata read once for every signing server in a fetched state
/// batch. Per-event timestamp checks then run against this immutable snapshot.
#[derive(Clone, Default)]
pub(crate) struct EventKeyMetadataCache {
	records: Arc<HashMap<OwnedServerName, Vec<conduwuit_mtxdb::server_keys::ServerKey>>>,
}

impl EventKeyMetadataCache {
	fn get(&self, origin: &ServerName) -> Option<&[conduwuit_mtxdb::server_keys::ServerKey]> {
		self.records.get(origin).map(Vec::as_slice)
	}
}

#[implement(super::Service)]
pub(crate) async fn event_key_metadata_for_events(
	&self,
	events: &[(slipstream::OwnedEventId, CanonicalJsonObject)],
	version: &RoomVersionId,
) -> EventKeyMetadataCache {
	use slipstream::signatures::required_keys;

	let origins: BTreeSet<OwnedServerName> = events
		.iter()
		.filter_map(|(_, event)| required_keys(event, version).ok())
		.flat_map(std::collections::BTreeMap::into_keys)
		.collect();
	let adapter = self.db.mtxdb_keys.clone();
	let metadata = self
		.services
		.server
		.runtime()
		.spawn_blocking(move || {
			origins
				.into_iter()
				.filter_map(|origin| match adapter.read_keys(origin.as_bytes()) {
					| Ok(keys) if keys.iter().any(|key| !key.is_evicted()) =>
						Some((origin, keys)),
					| Ok(_) => None,
					| Err(error) => {
						debug_warn!(%origin, "Failed to preload signing-key metadata: {error}");
						None
					},
				})
				.collect()
		})
		.await
		.unwrap_or_default();

	EventKeyMetadataCache { records: Arc::new(metadata) }
}

#[implement(super::Service)]
pub async fn get_event_keys(
	&self,
	object: &CanonicalJsonObject,
	version: &RoomVersionId,
) -> Result<PubKeyMap> {
	use slipstream::signatures::required_keys;

	let required = match required_keys(object, version) {
		| Ok(required) => required,
		| Err(e) => {
			debug_error!("Failed to determine keys required to verify: {e}");
			return Err!(BadServerResponse("Failed to determine keys required to verify: {e}"));
		},
	};
	trace!(?required, "Keys required to verify event");

	// Extract origin_server_ts to enforce expired key rejection per MSC4499.
	// Events signed by a key whose expired_ts <= origin_server_ts must be rejected.
	// origin_server_ts is required on all Matrix events; reject if absent/malformed
	// to prevent bypassing the expiry check via crafted events.
	let origin_server_ts = object
		.get("origin_server_ts")
		.and_then(slipstream::json::Value::as_i64)
		.and_then(|ts| u64::try_from(ts).ok())
		.map(MilliSecondsSinceUnixEpoch)
		.ok_or_else(|| err!(BadServerResponse("Event missing or malformed origin_server_ts")))?;

	// Room versions 5 and later require the signing key's valid_until_ts to
	// cover the event's origin_server_ts; versions 1-4 ignore valid_until_ts.
	let validity_policy = validity_policy_for_version(version);

	let mut keys = PubKeyMap::new();
	for (server, key_ids) in &required {
		let pubkeys = self
			.get_pubkeys_for_event(
				server,
				key_ids.iter().map(Borrow::borrow),
				origin_server_ts,
				validity_policy,
			)
			.await;
		keys.insert(server.to_owned(), pubkeys);
	}

	Ok(keys)
}

#[implement(super::Service)]
pub(crate) async fn get_event_keys_with_metadata(
	&self,
	object: &CanonicalJsonObject,
	version: &RoomVersionId,
	metadata: &EventKeyMetadataCache,
) -> Result<PubKeyMap> {
	use slipstream::signatures::required_keys;

	let required = required_keys(object, version).map_err(|error| {
		err!(BadServerResponse("Failed to determine keys required to verify: {error}"))
	})?;
	let event_ts = object
		.get("origin_server_ts")
		.and_then(slipstream::json::Value::as_i64)
		.and_then(|ts| u64::try_from(ts).ok())
		.map(MilliSecondsSinceUnixEpoch)
		.ok_or_else(|| err!(BadServerResponse("Event missing or malformed origin_server_ts")))?;
	let validity_policy = validity_policy_for_version(version);
	let mut keys = PubKeyMap::new();
	for (origin, key_ids) in &required {
		let cached = metadata.get(origin).and_then(|server_keys| {
			pubkeys_from_metadata(server_keys, key_ids, event_ts, validity_policy)
		});
		let pubkeys = match cached {
			| Some(pubkeys) => pubkeys,
			| None =>
				self.get_pubkeys_for_event(
					origin,
					key_ids.iter().map(Borrow::borrow),
					event_ts,
					validity_policy,
				)
				.await,
		};
		keys.insert(origin.to_owned(), pubkeys);
	}

	Ok(keys)
}

fn pubkeys_from_metadata(
	server_keys: &[conduwuit_mtxdb::server_keys::ServerKey],
	key_ids: &[slipstream::OwnedServerSigningKeyId],
	event_ts: MilliSecondsSinceUnixEpoch,
	validity_policy: ValidUntilPolicy,
) -> Option<PubKeys> {
	let mut keys = PubKeys::new();
	for key_id in key_ids {
		let key = server_keys
			.iter()
			.find(|key| !key.is_evicted() && key.key_id == key_id.as_str().as_bytes())?;

		if let Some(expired_ts) = key.retirement_ts() {
			if expired_ts <= event_ts.0 {
				return None;
			}
			keys.insert(key_id.clone(), Base64::new(key.key.clone()));
			continue;
		}

		// MSC4499: each key record carries its own validity window. The
		// reconstructed payload's top-level `valid_until_ts` is only the
		// newest of them and must not make an older, expired key look valid.
		if validity_policy == ValidUntilPolicy::Enforce && key.valid_until_ts < event_ts.0 {
			// Preserve the normal path's one-time refresh when a cached key record
			// does not cover this event timestamp.
			return None;
		}
		keys.insert(key_id.clone(), Base64::new(key.key.clone()));
	}

	Some(keys)
}

#[implement(super::Service)]
pub async fn get_pubkeys<'a, S, K>(&self, batch: S) -> PubKeyMap
where
	S: Iterator<Item = (&'a ServerName, K)> + Send,
	K: Iterator<Item = &'a ServerSigningKeyId> + Send,
{
	let mut keys = PubKeyMap::new();
	for (server, key_ids) in batch {
		let pubkeys = self.get_pubkeys_for(server, key_ids).await;
		keys.insert(server.into(), pubkeys);
	}

	keys
}

#[implement(super::Service)]
pub async fn get_pubkeys_for<'a, I>(&self, origin: &ServerName, key_ids: I) -> PubKeys
where
	I: Iterator<Item = &'a ServerSigningKeyId> + Send,
{
	let mut keys = PubKeys::new();
	for key_id in key_ids {
		if let Ok(verify_key) = self.get_verify_key(origin, key_id).await {
			keys.insert(key_id.into(), verify_key.key);
		}
	}

	keys
}

/// Like `get_pubkeys_for`, but filters keys based on the event timestamp.
/// Per MSC4499, keys in `old_verify_keys` whose `expired_ts` is at or before
/// `origin_server_ts` are excluded. `valid_until_ts` is enforced according to
/// `validity_policy`: when enforced, the event timestamp must be covered by
/// the key's validity window.
#[implement(super::Service)]
pub(super) async fn get_pubkeys_for_event<'a, I>(
	&self,
	origin: &ServerName,
	key_ids: I,
	origin_server_ts: MilliSecondsSinceUnixEpoch,
	validity_policy: ValidUntilPolicy,
) -> PubKeys
where
	I: Iterator<Item = &'a ServerSigningKeyId> + Send,
{
	let mut keys = PubKeys::new();

	for key_id in key_ids {
		if let Ok(verify_key) = self.get_verify_key(origin, key_id).await {
			if self
				.is_key_expired_for_event(origin, key_id, origin_server_ts)
				.await
			{
				debug_warn!(
					%origin, %key_id,
					"Rejecting expired key for event verification \
					 (key expired before event origin_server_ts)"
				);
				continue;
			}
			if validity_policy == ValidUntilPolicy::Enforce
				&& !self
					.is_key_valid_for_event(origin, key_id, origin_server_ts)
					.await
			{
				debug_warn!(
					%origin, %key_id,
					"Rejecting key for event verification (key valid_until_ts is before \
					 event origin_server_ts)"
				);
				continue;
			}
			keys.insert(key_id.into(), verify_key.key);
		}
	}

	keys
}

/// Whether the origin's key record is valid until at least the event's
/// `origin_server_ts` (room version 5 and later). A record that does not cover
/// it may only be stale, so the origin is asked for a fresh one before the key
/// is refused. A retired key is judged by its `expired_ts` instead, and an
/// origin without a stored record has no validity window to enforce. Other
/// cache read errors fail closed and reject the key.
#[implement(super::Service)]
async fn is_key_valid_for_event(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
	event_ts: MilliSecondsSinceUnixEpoch,
) -> bool {
	let stale = match self
		.db
		.mtxdb_keys
		.read_key(origin.as_bytes(), key_id.as_str().as_bytes())
	{
		| Ok(Some(key)) => {
			// A retired key is judged by its `expired_ts` instead.
			key.retirement_ts().is_none() && key.valid_until_ts < event_ts.0
		},
		// An origin without a stored record has no validity window to enforce.
		| Ok(None) => return true,
		| Err(error) => {
			warn!(%origin, "Failed to read cached signing-key validity; refusing key: {error}");
			return false;
		},
	};
	if !stale {
		return true;
	}

	let Ok(raw) = self
		.server_request_coalesced(origin, Some(event_ts), &[key_id])
		.await
	else {
		return false;
	};
	if let Err(e) = self
		.add_signing_keys(&raw, super::FetchSource::Direct)
		.await
	{
		debug_error!("Failed to add refreshed signing keys: {e}");
		return false;
	}

	match self
		.db
		.mtxdb_keys
		.read_key(origin.as_bytes(), key_id.as_str().as_bytes())
	{
		| Ok(Some(key)) => {
			// MSC4499: use this key's own validity timestamp, not the newest
			// timestamp among all of the origin's keys.
			key.retirement_ts().is_some() || key.valid_until_ts >= event_ts.0
		},
		| Ok(None) => true,
		| Err(error) => {
			warn!(%origin, "Failed to read refreshed signing-key validity; refusing key: {error}");
			false
		},
	}
}

/// Checks if a key from old_verify_keys has an expired_ts that is at or before
/// the given event timestamp, meaning it should not be used to verify that
/// event.
#[implement(super::Service)]
async fn is_key_expired_for_event(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
	event_ts: MilliSecondsSinceUnixEpoch,
) -> bool {
	// Check the origin key record (including merged historical old_verify_keys).
	if let Ok(server_keys) = self.merged_signing_keys_for(origin).await {
		if let Some(old_key) = server_keys.old_verify_keys.get(key_id) {
			return old_key.expired_ts <= event_ts;
		}
	}

	false
}

#[implement(super::Service)]
pub async fn get_verify_key(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
) -> Result<VerifyKey> {
	let notary_first = self.services.server.config.query_trusted_key_servers_first;
	let notary_only = self.services.server.config.only_query_trusted_key_servers;

	// .remove() takes ownership from the temporary map, avoiding a clone
	if let Some(result) = self.verify_keys_for(origin).await.remove(key_id) {
		trace!("Found key in cache");
		return Ok(result);
	}

	if notary_first {
		if let Ok(result) = self.get_verify_key_from_notaries(origin, key_id).await {
			return Ok(result);
		}
	}

	if !notary_only {
		if let Ok(result) = self.get_verify_key_from_origin(origin, key_id).await {
			return Ok(result);
		}
	}

	if !notary_first {
		if let Ok(result) = self.get_verify_key_from_notaries(origin, key_id).await {
			return Ok(result);
		}
	}

	Err!(BadServerResponse(debug_error!(
		%key_id,
		%origin,
		"Failed to fetch federation signing-key"
	)))
}

#[implement(super::Service)]
async fn get_verify_key_from_notaries(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
) -> Result<VerifyKey> {
	for notary in self.services.globals.trusted_servers() {
		if let Ok(server_keys) = self.notary_request(notary, origin).await {
			for server_key in server_keys {
				let server_key = match self
					.add_signing_keys(&server_key, super::FetchSource::Notary)
					.await
				{
					| Ok(patched) => patched,
					| Err(e) => {
						debug_error!("Failed to add signing keys: {e}");
						continue;
					},
				};

				if let Some(result) = extract_key(server_key, key_id) {
					return Ok(result);
				}
			}
		}
	}

	Err!(Request(NotFound("Failed to fetch signing-key from notaries")))
}

#[implement(super::Service)]
async fn get_verify_key_from_origin(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
) -> Result<VerifyKey> {
	if let Ok(server_key) = self.server_request(origin).await {
		let server_key = match self
			.add_signing_keys(&server_key, super::FetchSource::Direct)
			.await
		{
			| Ok(patched) => patched,
			| Err(e) => {
				debug_error!("Failed to add signing keys: {e}");
				return Err!(BadServerResponse("Failed to add signing keys: {e}"));
			},
		};

		if let Some(result) = extract_key(server_key, key_id) {
			return Ok(result);
		}
	}

	Err!(Request(NotFound("Failed to fetch signing-key from origin")))
}

/// Retrieves currently active (non-retired) verify keys for request authentication.
/// Per MSC4499 / Matrix federation specification, requests must only be authenticated
/// with active keys in `verify_keys`, not retired keys in `old_verify_keys`.
#[implement(super::Service)]
pub async fn get_active_verify_key(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
) -> Result<VerifyKey> {
	let notary_first = self.services.server.config.query_trusted_key_servers_first;
	let notary_only = self.services.server.config.only_query_trusted_key_servers;

	if let Ok(cached) = self.merged_signing_keys_for(origin).await {
		// Only trust the cache while the response is still valid; otherwise
		// re-query so a key the origin has since retired is not accepted.
		if cached.valid_until_ts >= self.minimum_valid_ts() {
			if let Some(result) = self.active_verify_keys_for(origin).await.remove(key_id) {
				trace!("Found active key in cache");
				return Ok(result);
			}
		}

		// A cached binding says this key is retired: fail fast rather than
		// triggering outbound fetches for every request signed with it.
		if cached.old_verify_keys.contains_key(key_id)
			&& !self
				.active_verify_keys_for(origin)
				.await
				.contains_key(key_id)
		{
			return Err!(Request(Forbidden("Signing key {key_id} of {origin} is retired")));
		}
	} else if let Some(result) = self.active_verify_keys_for(origin).await.remove(key_id) {
		return Ok(result);
	}

	if notary_first {
		if let Ok(result) = self
			.get_active_verify_key_from_notaries(origin, key_id)
			.await
		{
			return Ok(result);
		}
	}

	if !notary_only {
		if let Ok(result) = self.get_active_verify_key_from_origin(origin, key_id).await {
			return Ok(result);
		}
	}

	if !notary_first {
		if let Ok(result) = self
			.get_active_verify_key_from_notaries(origin, key_id)
			.await
		{
			return Ok(result);
		}
	}

	Err!(BadServerResponse(debug_error!(
		%key_id,
		%origin,
		"Failed to fetch active federation signing-key"
	)))
}

#[implement(super::Service)]
async fn get_active_verify_key_from_notaries(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
) -> Result<VerifyKey> {
	for notary in self.services.globals.trusted_servers() {
		if let Ok(server_keys) = self.notary_request(notary, origin).await {
			for server_key in server_keys {
				let mut server_key = match self
					.add_signing_keys(&server_key, super::FetchSource::Notary)
					.await
				{
					| Ok(patched) => patched,
					| Err(e) => {
						debug_error!("Failed to add signing keys: {e}");
						continue;
					},
				};

				if let Some(result) = server_key.verify_keys.remove(key_id) {
					return Ok(result);
				}
			}
		}
	}

	Err!(Request(NotFound("Failed to fetch active signing-key from notaries")))
}

#[implement(super::Service)]
async fn get_active_verify_key_from_origin(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
) -> Result<VerifyKey> {
	if let Ok(server_key) = self.server_request(origin).await {
		let mut server_key = match self
			.add_signing_keys(&server_key, super::FetchSource::Direct)
			.await
		{
			| Ok(patched) => patched,
			| Err(e) => {
				debug_error!("Failed to add signing keys: {e}");
				return Err!(BadServerResponse("Failed to add signing keys: {e}"));
			},
		};

		if let Some(result) = server_key.verify_keys.remove(key_id) {
			return Ok(result);
		}
	}

	Err!(Request(NotFound("Failed to fetch active signing-key from origin")))
}

#[cfg(test)]
mod tests {
	use slipstream::RoomVersionId;

	use super::{ValidUntilPolicy, validity_policy_for_version};

	#[test]
	fn valid_until_policy_matches_room_version_rules() {
		for version in
			[RoomVersionId::V1, RoomVersionId::V2, RoomVersionId::V3, RoomVersionId::V4]
		{
			assert_eq!(validity_policy_for_version(&version), ValidUntilPolicy::Ignore);
		}
		assert_eq!(validity_policy_for_version(&RoomVersionId::V5), ValidUntilPolicy::Enforce);
	}
}
