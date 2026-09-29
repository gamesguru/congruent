use std::{
	borrow::Borrow,
	collections::{BTreeMap, BTreeSet},
	time::Duration,
};

use conduwuit::{debug, debug_error, debug_warn, error, implement, info, trace, warn};
use futures::{StreamExt, stream::FuturesUnordered};
use ruma::{
	OwnedServerName, OwnedServerSigningKeyId, ServerName, ServerSigningKeyId,
	api::federation::discovery::ServerSigningKeys, serde::Raw,
};
use serde_json::value::RawValue as RawJsonValue;

use super::key_exists;

type Batch = BTreeMap<OwnedServerName, Vec<OwnedServerSigningKeyId>>;

#[implement(super::Service)]
pub async fn acquire_events_pubkeys<'a, I>(&self, events: I)
where
	I: Iterator<Item = &'a Box<RawJsonValue>> + Send,
{
	type Batch = BTreeMap<OwnedServerName, BTreeSet<OwnedServerSigningKeyId>>;
	type Signatures = BTreeMap<OwnedServerName, BTreeMap<OwnedServerSigningKeyId, String>>;

	#[derive(serde::Deserialize)]
	struct EventSignatures {
		signatures: Option<Signatures>,
	}

	let mut batch = Batch::new();
	events
		.filter_map(|event| serde_json::from_str::<EventSignatures>(event.get()).ok())
		.filter_map(|event| event.signatures)
		.flat_map(IntoIterator::into_iter)
		.for_each(|(server, sigs)| {
			batch.entry(server).or_default().extend(sigs.into_keys());
		});

	let batch = batch
		.iter()
		.map(|(server, keys)| (server.borrow(), keys.iter().map(Borrow::borrow)));

	self.acquire_pubkeys(batch).await;
}

#[implement(super::Service)]
pub async fn acquire_pubkeys<'a, S, K>(&self, batch: S)
where
	S: Iterator<Item = (&'a ServerName, K)> + Send + Clone,
	K: Iterator<Item = &'a ServerSigningKeyId> + Send + Clone,
{
	let notary_only = self.services.server.config.only_query_trusted_key_servers;
	let notary_first_always = self.services.server.config.query_trusted_key_servers_first;
	let notary_first_on_join = self
		.services
		.server
		.config
		.query_trusted_key_servers_first_on_join;

	let requested_servers = batch.clone().count();
	let requested_keys = batch.clone().flat_map(|(_, key_ids)| key_ids).count();

	debug!("acquire {requested_keys} keys from {requested_servers}");

	let mut missing = self.acquire_locals(batch).await;
	let mut missing_keys = keys_count(&missing);
	let mut missing_servers = missing.len();
	if missing_servers == 0 {
		return;
	}

	info!("{missing_keys} keys for {missing_servers} servers will be acquired");

	if notary_first_always || notary_first_on_join {
		missing = self.acquire_notary(missing.into_iter()).await;
		missing_keys = keys_count(&missing);
		missing_servers = missing.len();
		if missing_keys == 0 {
			return;
		}

		warn!(
			"missing {missing_keys} keys for {missing_servers} servers from all notaries first"
		);
	}

	if !notary_only {
		missing = self.acquire_origins(missing.into_iter()).await;
		missing_keys = keys_count(&missing);
		missing_servers = missing.len();
		if missing_keys == 0 {
			return;
		}

		debug_warn!("missing {missing_keys} keys for {missing_servers} servers unreachable");
	}

	if !notary_first_always && !notary_first_on_join {
		missing = self.acquire_notary(missing.into_iter()).await;
		missing_keys = keys_count(&missing);
		missing_servers = missing.len();
		if missing_keys == 0 {
			return;
		}

		debug_warn!(
			"still missing {missing_keys} keys for {missing_servers} servers from all notaries."
		);
	}

	if missing_keys > 0 {
		warn!(
			"did not obtain {missing_keys} keys for {missing_servers} servers out of \
			 {requested_keys} total keys for {requested_servers} total servers."
		);
	}

	for (server, key_ids) in missing {
		debug_warn!(%server, ?key_ids, "missing");
	}
}

#[implement(super::Service)]
async fn acquire_locals<'a, S, K>(&self, batch: S) -> Batch
where
	S: Iterator<Item = (&'a ServerName, K)> + Send,
	K: Iterator<Item = &'a ServerSigningKeyId> + Send,
{
	let mut missing = Batch::new();
	for (server, key_ids) in batch {
		for key_id in key_ids {
			if !self.verify_key_exists(server, key_id).await {
				missing
					.entry(server.into())
					.or_default()
					.push(key_id.into());
			}
		}
	}

	missing
}

#[implement(super::Service)]
async fn acquire_origins<I>(&self, batch: I) -> Batch
where
	I: Iterator<Item = (OwnedServerName, Vec<OwnedServerSigningKeyId>)> + Send,
{
	let mut requests: FuturesUnordered<_> = batch
		.map(|(origin, key_ids)| self.acquire_origin(origin, key_ids))
		.collect();

	let mut missing = Batch::new();
	while let Some((origin, key_ids)) = requests.next().await {
		if !key_ids.is_empty() {
			missing.insert(origin, key_ids);
		}
	}

	missing
}

#[implement(super::Service)]
async fn acquire_origin(
	&self,
	origin: OwnedServerName,
	mut key_ids: Vec<OwnedServerSigningKeyId>,
) -> (OwnedServerName, Vec<OwnedServerSigningKeyId>) {
	// Our own keys are available locally; never query ourselves over federation
	if self.services.globals.server_is_ours(&origin) {
		return (origin, key_ids);
	}

	let timeout = Duration::from_secs(self.services.server.config.server_key_fetch_timeout);

	match tokio::time::timeout(timeout, self.server_request(&origin)).await {
		| Err(e) => debug_warn!(%origin, "timed out: {e}"),
		| Ok(Err(e)) => debug_error!(%origin, "{e}"),
		| Ok(Ok(server_keys)) => {
			trace!(
				%origin,
				?key_ids,
				?server_keys,
				"received server_keys"
			);

			match self
				.add_signing_keys(&server_keys, super::FetchSource::Direct)
				.await
			{
				| Ok(server_keys) => {
					key_ids.retain(|key_id| !key_exists(&server_keys, key_id));
				},
				| Err(e) => {
					debug_error!("Failed to add signing keys: {e}");
				},
			}
		},
	}

	(origin, key_ids)
}

#[implement(super::Service)]
async fn acquire_notary<I>(&self, batch: I) -> Batch
where
	I: Iterator<Item = (OwnedServerName, Vec<OwnedServerSigningKeyId>)> + Send,
{
	let mut missing: Batch = batch.collect();
	let trusted_servers = self.services.globals.trusted_servers();
	if trusted_servers.is_empty() {
		return missing;
	}

	let mut requests = FuturesUnordered::new();
	for notary in trusted_servers {
		let batch: Vec<_> = missing
			.iter()
			.map(|(s, k)| (s.clone(), k.clone()))
			.collect();

		requests.push(async move {
			let req_batch = batch
				.iter()
				.map(|(server, keys)| (server.borrow(), keys.iter().map(Borrow::borrow)));

			(notary, self.batch_notary_request(notary, req_batch).await)
		});
	}

	while let Some((notary, result)) = requests.next().await {
		match result {
			| Err(e) => error!("Failed to contact notary {notary:?}: {e}"),
			| Ok(results) => {
				for server_keys in results {
					self.acquire_notary_result(&mut missing, server_keys).await;
				}

				if keys_count(&missing) == 0 {
					break;
				}
			},
		}
	}

	missing
}

#[implement(super::Service)]
async fn acquire_notary_result(&self, missing: &mut Batch, server_keys: Raw<ServerSigningKeys>) {
	let Ok(server) = server_keys.get_field::<OwnedServerName>("server_name") else {
		return;
	};
	let Some(server) = server else {
		return;
	};

	match self
		.add_signing_keys(&server_keys, super::FetchSource::Notary)
		.await
	{
		| Ok(server_keys) =>
			if let Some(key_ids) = missing.get_mut(&server) {
				key_ids.retain(|key_id| !key_exists(&server_keys, key_id));
				if key_ids.is_empty() {
					missing.remove(&server);
				}
			},
		| Err(e) => {
			debug_error!("Failed to add signing keys: {e}");
		},
	}
}

fn keys_count(batch: &Batch) -> usize {
	batch.values().flat_map(|key_ids| key_ids.iter()).count()
}
