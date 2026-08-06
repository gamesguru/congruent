use std::{collections::BTreeMap, fmt::Debug};

use conduwuit::{Err, Result, debug, debug_warn, implement};
use ruma::{
	OwnedServerName, OwnedServerSigningKeyId, ServerName, ServerSigningKeyId,
	api::federation::discovery::{
		ServerSigningKeys, get_remote_server_keys,
		get_remote_server_keys_batch::{self, v2::QueryCriteria},
		get_server_keys,
	},
	serde::Raw,
};

use super::validate::check_no_duplicate_json_keys;

/// MSC4499: Validate raw JSON before any typed deserialization.
/// Shared by all key ingestion paths (direct fetch, notary, batch notary).
fn validate_raw(raw: &Raw<ServerSigningKeys>, strict: bool) -> bool {
	if let Err(e) = check_no_duplicate_json_keys(raw.json().get(), strict) {
		debug_warn!("Rejecting key response with duplicate JSON keys: {e}");
		return false;
	}

	true
}

#[implement(super::Service)]
pub(super) async fn batch_notary_request<'a, S, K>(
	&self,
	notary: &ServerName,
	batch: S,
) -> Result<Vec<Raw<ServerSigningKeys>>>
where
	S: Iterator<Item = (&'a ServerName, K)> + Send,
	K: Iterator<Item = &'a ServerSigningKeyId> + Send,
{
	use get_remote_server_keys_batch::v2::Request;
	type RumaBatch = BTreeMap<OwnedServerName, BTreeMap<OwnedServerSigningKeyId, QueryCriteria>>;

	let criteria = QueryCriteria {
		minimum_valid_until_ts: Some(self.minimum_valid_ts()),
	};

	let mut server_keys = batch.fold(RumaBatch::new(), |mut batch, (server, key_ids)| {
		batch
			.entry(server.into())
			.or_default()
			.extend(key_ids.map(|key_id| (key_id.into(), criteria.clone())));

		batch
	});

	debug_assert!(!server_keys.is_empty(), "empty batch request to notary");

	let mut results = Vec::new();
	while let Some(batch) = server_keys
		.keys()
		.rev()
		.take(self.services.server.config.trusted_server_batch_size)
		.next_back()
		.cloned()
	{
		let request = Request {
			server_keys: server_keys.split_off(&batch),
		};

		debug!(
			?notary,
			?batch,
			remaining = %server_keys.len(),
			requesting = ?request.server_keys.keys(),
			"notary request"
		);

		let batch_response = self
			.services
			.sending
			.send_synapse_request(notary, request)
			.await?;

		let response = batch_response
			.server_keys
			.iter()
			.filter(|raw| validate_raw(raw, self.services.server.config.msc4499_strict_caching))
			.cloned();

		results.extend(response);
	}

	Ok(results)
}

#[implement(super::Service)]
pub async fn notary_request(
	&self,
	notary: &ServerName,
	target: &ServerName,
) -> Result<impl Iterator<Item = Raw<ServerSigningKeys>> + Clone + Debug + Send + use<>> {
	use get_remote_server_keys::v2::Request;

	let request = Request {
		server_name: target.into(),
		minimum_valid_until_ts: self.minimum_valid_ts(),
	};

	let notary_response = self
		.services
		.sending
		.send_federation_request(notary, request)
		.await?;

	Ok(notary_response
		.server_keys
		.iter()
		.filter(|raw| validate_raw(raw, self.services.server.config.msc4499_strict_caching))
		.cloned()
		.collect::<Vec<_>>()
		.into_iter())
}

#[implement(super::Service)]
pub async fn server_request(&self, target: &ServerName) -> Result<Raw<ServerSigningKeys>> {
	use get_server_keys::v2::Request;

	let response = self
		.services
		.sending
		.send_federation_request(target, Request::new())
		.await?;

	// MSC4499: Check raw JSON for duplicate keys before serde_json dedup
	check_no_duplicate_json_keys(
		response.server_key.json().get(),
		self.services.server.config.msc4499_strict_caching,
	)?;

	let server_signing_key: ServerSigningKeys = response
		.server_key
		.deserialize()
		.map_err(|e| conduwuit::err!(BadServerResponse("{e}")))?;

	if server_signing_key.server_name != target {
		return Err!(BadServerResponse(debug_warn!(
			requested = ?target,
			response = ?server_signing_key.server_name,
			"Server responded with bogus server_name"
		)));
	}

	Ok(response.server_key)
}
