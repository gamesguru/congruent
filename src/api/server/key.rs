use std::{
	mem::take,
	time::{Duration, SystemTime},
};

use axum::{Json, extract::State, response::IntoResponse};
use conduwuit::{Result, err, utils::timepoint_from_now};
use ruma::{
	MilliSecondsSinceUnixEpoch, Signatures,
	api::{
		OutgoingResponse,
		federation::discovery::{
			OldVerifyKey, ServerSigningKeys, get_remote_server_keys,
			get_remote_server_keys_batch, get_server_keys,
		},
	},
	serde::Raw,
};

use crate::Ruma;

/// # `GET /_matrix/key/v2/server`
///
/// Gets the public signing keys of this server.
///
/// - Matrix does not support invalidating public keys, so the key returned by
///   this will be valid forever.
// Response type for this endpoint is Json because we need to calculate a
// signature for the response
pub(crate) async fn get_server_keys_route(
	State(services): State<crate::State>,
) -> Result<impl IntoResponse> {
	let server_key = get_our_signing_keys(&services).await;
	let server_key = Raw::new(&server_key)?;
	let mut response = get_server_keys::v2::Response::new(server_key)
		.try_into_http_response::<Vec<u8>>()
		.map(|mut response| take(response.body_mut()))
		.and_then(|body| serde_json::from_slice(&body).map_err(Into::into))?;

	services.server_keys.sign_json(&mut response)?;

	Ok(Json(response))
}

fn valid_until_ts() -> MilliSecondsSinceUnixEpoch {
	let dur = Duration::from_secs(86400 * 7);
	let timepoint = timepoint_from_now(dur).expect("SystemTime should not overflow");
	MilliSecondsSinceUnixEpoch::from_system_time(timepoint).expect("UInt should not overflow")
}

fn expires_ts() -> MilliSecondsSinceUnixEpoch {
	let timepoint = SystemTime::now();
	MilliSecondsSinceUnixEpoch::from_system_time(timepoint).expect("UInt should not overflow")
}

/// # `GET /_matrix/key/v2/server/{keyId}`
///
/// Gets the public signing keys of this server.
///
/// - Matrix does not support invalidating public keys, so the key returned by
///   this will be valid forever.
pub(crate) async fn get_server_keys_deprecated_route(
	State(services): State<crate::State>,
) -> impl IntoResponse {
	get_server_keys_route(State(services)).await
}

async fn get_our_signing_keys(services: &crate::State) -> ServerSigningKeys {
	let server_name = services.globals.server_name();
	let active_key_id = services.server_keys.active_key_id();
	let mut all_keys = services.server_keys.verify_keys_for(server_name).await;

	let verify_keys = all_keys
		.remove_entry(active_key_id)
		.expect("active verify_key is missing");

	let old_verify_keys = all_keys
		.into_iter()
		.map(|(id, key)| (id, OldVerifyKey::new(expires_ts(), key.key)))
		.collect();

	ServerSigningKeys {
		verify_keys: [verify_keys].into(),
		old_verify_keys,
		server_name: server_name.to_owned(),
		valid_until_ts: valid_until_ts(),
		signatures: Signatures::new(),
	}
}

async fn sign_signing_keys(
	services: &crate::State,
	server_keys: &Raw<ServerSigningKeys>,
) -> Result<Raw<ServerSigningKeys>> {
	let mut keys_obj: ruma::CanonicalJsonObject = serde_json::from_str(server_keys.json().get())?;
	services.server_keys.sign_json(&mut keys_obj)?;
	let raw_value = serde_json::value::to_raw_value(&keys_obj)?;
	Ok(Raw::from_json(raw_value))
}

fn select_server_key_response(
	raw_server_key: Option<Raw<ServerSigningKeys>>,
	merged_server_key: Option<ServerSigningKeys>,
) -> Result<Raw<ServerSigningKeys>> {
	if let Some(keys) = merged_server_key {
		Raw::new(&keys).map_err(Into::into)
	} else if let Some(keys) = raw_server_key {
		Ok(keys)
	} else {
		Err(err!(Request(NotFound("Signing keys not found for server"))))
	}
}

async fn get_signing_keys_for(
	services: &crate::State,
	server_name: &ruma::ServerName,
	minimum_valid_until_ts: Option<MilliSecondsSinceUnixEpoch>,
	requested_key_ids: &[&ruma::ServerSigningKeyId],
) -> Result<Raw<ServerSigningKeys>> {
	if services.globals.server_is_ours(server_name) {
		return Raw::new(&get_our_signing_keys(services).await).map_err(Into::into);
	}

	let mut raw_server_key = match services.server_keys.raw_signing_keys_for(server_name).await {
		| Ok(keys) => Some(keys),
		| Err(ref e) if e.is_not_found() => None,
		| Err(e) => return Err(e),
	};
	let mut merged_server_key = match services
		.server_keys
		.merged_signing_keys_for(server_name)
		.await
	{
		| Ok(keys) => Some(keys),
		| Err(ref e) if e.is_not_found() => None,
		| Err(e) => return Err(e),
	};

	let needs_fetch = match &merged_server_key {
		| Some(keys) => {
			// Re-fetch if any requested key ID is missing from the cached payload
			let missing_requested_key = requested_key_ids.iter().any(|kid| {
				!keys.verify_keys.contains_key(*kid) && !keys.old_verify_keys.contains_key(*kid)
			});

			if missing_requested_key {
				true
			} else if let Some(min_valid) = minimum_valid_until_ts {
				keys.valid_until_ts < min_valid
			} else {
				false
			}
		},
		| None => true,
	};

	if needs_fetch {
		match services
			.server_keys
			.server_request_coalesced(server_name, minimum_valid_until_ts, requested_key_ids)
			.await
		{
			| Ok(new_keys) => match services
				.server_keys
				.add_signing_keys(&new_keys, conduwuit_service::server_keys::FetchSource::Direct)
				.await
			{
				| Ok(_) => {
					merged_server_key = match services
						.server_keys
						.merged_signing_keys_for(server_name)
						.await
					{
						| Ok(keys) => Some(keys),
						| Err(e) => {
							conduwuit::warn!(
								"merged_signing_keys_for failed for {server_name} after fetch: \
								 {e}"
							);
							raw_server_key = Some(new_keys);
							None
						},
					};
				},
				| Err(e) => conduwuit::warn!("add_signing_keys failed for {server_name}: {e}"),
			},
			| Err(e) => {
				conduwuit::warn!("server_request_coalesced failed for {server_name}: {e}");
			},
		}
	}

	select_server_key_response(raw_server_key, merged_server_key)
}

/// # `GET /_matrix/key/v2/query/{serverName}`
///
/// Query keys of a remote server via this notary server.
pub(crate) async fn get_remote_server_keys_route(
	State(services): State<crate::State>,
	body: Ruma<get_remote_server_keys::v2::Request>,
) -> Result<get_remote_server_keys::v2::Response> {
	let server_key =
		get_signing_keys_for(&services, &body.server_name, Some(body.minimum_valid_until_ts), &[
		])
		.await?;
	let signed_key = sign_signing_keys(&services, &server_key).await?;

	Ok(get_remote_server_keys::v2::Response { server_keys: vec![signed_key] })
}

/// # `POST /_matrix/key/v2/query`
///
/// Query keys of multiple remote servers via this notary server.
pub(crate) async fn get_remote_server_keys_batch_route(
	State(services): State<crate::State>,
	body: Ruma<get_remote_server_keys_batch::v2::Request>,
) -> Result<get_remote_server_keys_batch::v2::Response> {
	let mut response_keys = Vec::new();

	for (server_name, key_ids) in &body.server_keys {
		let min_valid = key_ids
			.values()
			.filter_map(|c| c.minimum_valid_until_ts)
			.max();

		let requested: Vec<&ruma::ServerSigningKeyId> =
			key_ids.keys().map(AsRef::as_ref).collect();

		if let Ok(server_key) =
			get_signing_keys_for(&services, server_name, min_valid, &requested).await
		{
			match sign_signing_keys(&services, &server_key).await {
				| Ok(signed_key) => response_keys.push(signed_key),
				| Err(e) => {
					conduwuit::warn!("Failed to sign server keys for {server_name}: {e}");
				},
			}
		}
	}

	Ok(get_remote_server_keys_batch::v2::Response { server_keys: response_keys })
}

#[cfg(test)]
mod tests {
	use ruma::{
		MilliSecondsSinceUnixEpoch, OwnedServerSigningKeyId, Signatures,
		api::federation::discovery::{OldVerifyKey, ServerSigningKeys, VerifyKey},
		serde::{Base64, Raw},
	};

	use super::select_server_key_response;

	fn key_payload(
		verify_key_id: &str,
		verify_key: &str,
		old_key_id: Option<&str>,
		old_key: Option<&str>,
	) -> ServerSigningKeys {
		let mut verify_keys = std::collections::BTreeMap::new();
		verify_keys.insert(
			verify_key_id.try_into().unwrap(),
			VerifyKey::new(Base64::new(verify_key.as_bytes().to_vec())),
		);

		let old_verify_keys = match (old_key_id, old_key) {
			| (Some(id), Some(key)) => {
				let mut keys = std::collections::BTreeMap::new();
				keys.insert(
					id.try_into().unwrap(),
					OldVerifyKey::new(
						MilliSecondsSinceUnixEpoch::now(),
						Base64::new(key.as_bytes().to_vec()),
					),
				);
				keys
			},
			| _ => std::collections::BTreeMap::new(),
		};

		ServerSigningKeys {
			server_name: "example.com".try_into().unwrap(),
			valid_until_ts: MilliSecondsSinceUnixEpoch::now(),
			verify_keys,
			old_verify_keys,
			signatures: Signatures::new(),
		}
	}

	#[test]
	fn cache_hit_prefers_merged_historical_keys() {
		let raw = Raw::new(&key_payload("ed25519:active", "AAA", None, None)).unwrap();
		let merged = key_payload("ed25519:active", "AAA", Some("ed25519:old"), Some("BBB"));

		let selected = select_server_key_response(Some(raw), Some(merged)).unwrap();
		let selected: ServerSigningKeys = selected.deserialize().unwrap();
		let active_key_id: OwnedServerSigningKeyId = "ed25519:active".try_into().unwrap();
		let old_key_id: OwnedServerSigningKeyId = "ed25519:old".try_into().unwrap();

		assert!(selected.verify_keys.contains_key(&active_key_id));
		assert!(selected.old_verify_keys.contains_key(&old_key_id));
	}

	#[test]
	fn post_fetch_uses_merged_historical_keys() {
		let fetched = Raw::new(&key_payload("ed25519:active", "AAA", None, None)).unwrap();
		let merged =
			key_payload("ed25519:active", "AAA", Some("ed25519:historical"), Some("BBB"));

		let selected = select_server_key_response(Some(fetched), Some(merged)).unwrap();
		let selected: ServerSigningKeys = selected.deserialize().unwrap();
		let active_key_id: OwnedServerSigningKeyId = "ed25519:active".try_into().unwrap();
		let historical_key_id: OwnedServerSigningKeyId = "ed25519:historical".try_into().unwrap();

		assert!(selected.verify_keys.contains_key(&active_key_id));
		assert!(selected.old_verify_keys.contains_key(&historical_key_id));
	}
}
