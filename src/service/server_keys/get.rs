use std::borrow::Borrow;

use conduwuit::{Err, Result, debug_error, debug_warn, err, implement, trace};
use slipstream::{
	CanonicalJsonObject, MilliSecondsSinceUnixEpoch, RoomVersionId, ServerName,
	ServerSigningKeyId, api::federation::discovery::VerifyKey,
};

use super::{PubKeyMap, PubKeys, extract_key};

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
	let enforce_valid_until = !matches!(
		version,
		RoomVersionId::V1 | RoomVersionId::V2 | RoomVersionId::V3 | RoomVersionId::V4
	);

	let mut keys = PubKeyMap::new();
	for (server, key_ids) in &required {
		let pubkeys = self
			.get_pubkeys_for_event(
				server,
				key_ids.iter().map(Borrow::borrow),
				origin_server_ts,
				enforce_valid_until,
			)
			.await;
		keys.insert(server.to_owned(), pubkeys);
	}

	Ok(keys)
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

/// Like `get_pubkeys_for`, but filters out expired keys based on the event
/// timestamp. Per MSC4499: an event signed at time T is valid iff T <
/// expired_ts. Keys in `old_verify_keys` whose `expired_ts` <=
/// `origin_server_ts` are excluded.
#[implement(super::Service)]
pub async fn get_pubkeys_for_event<'a, I>(
	&self,
	origin: &ServerName,
	key_ids: I,
	origin_server_ts: MilliSecondsSinceUnixEpoch,
	enforce_valid_until: bool,
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
			if enforce_valid_until
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
/// origin without a stored record has no validity window to enforce.
#[implement(super::Service)]
async fn is_key_valid_for_event(
	&self,
	origin: &ServerName,
	key_id: &ServerSigningKeyId,
	event_ts: MilliSecondsSinceUnixEpoch,
) -> bool {
	let Ok(cached) = self.merged_signing_keys_for(origin).await else {
		return true;
	};
	if cached.old_verify_keys.contains_key(key_id) || cached.valid_until_ts >= event_ts {
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

	self.merged_signing_keys_for(origin)
		.await
		.is_ok_and(|fresh| {
			fresh.old_verify_keys.contains_key(key_id) || fresh.valid_until_ts >= event_ts
		})
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
