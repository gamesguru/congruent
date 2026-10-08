use std::{
	collections::{BTreeMap, HashMap, HashSet},
	net::IpAddr,
	sync::Arc,
	time::{Duration, Instant},
};

use conduwuit::{
	Err, Error, Result, debug, debug_warn, defer, err, error, info,
	result::LogErr,
	state_res::lexicographical_topological_sort,
	trace,
	utils::{
		IterStream, ReadyExt, millis_since_unix_epoch,
		stream::{BroadbandExt, TryBroadbandExt, automatic_width},
	},
	warn,
};
use conduwuit_service::{
	Services,
	rooms::state_accessor::{InputCache, StateHashes},
	sending::{EDU_LIMIT, PDU_LIMIT},
};
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt};
use http::StatusCode;
use itertools::Itertools;
use service::transactions::{
	FederationTxnState, TransactionError, TxnKey, WrappedTransactionResponse,
};
use slipstream::{
	CanonicalJsonObject, MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedRoomId, OwnedServerName,
	OwnedUserId, RoomId, ServerName, UInt, UserId,
	api::{
		client::error::{ErrorKind, ErrorKind::LimitExceeded},
		federation::{
			device::get_devices,
			transactions::{
				edu::{
					DeviceListUpdateContent, DirectDeviceContent, Edu, PresenceContent,
					PresenceUpdate, ReceiptContent, ReceiptData, ReceiptMap,
					SigningKeyUpdateContent, TypingContent,
				},
				send_transaction_message,
			},
		},
	},
	codec,
	encryption::DeviceKeys,
	events::receipt::{ReceiptEvent, ReceiptEventContent, ReceiptType},
	int,
	json::Value,
	sswire::{JsonObject, Raw},
	to_device::DeviceIdOrAllDevices,
};
use tokio::sync::watch::{Receiver, Sender};

use crate::{
	Ruma,
	router::{
		ApiError,
		extract::{ClientIp, State},
	},
};

type ResolvedMap = BTreeMap<OwnedEventId, Result>;
type Pdu = (OwnedRoomId, OwnedEventId, CanonicalJsonObject);

/// # `PUT /_matrix/federation/v1/send/{txnId}`
///
/// Push EDUs and PDUs to this server.
pub(crate) async fn send_transaction_message_route(
	State(services): State<crate::State>,
	ClientIp(client): ClientIp,
	body: Ruma<send_transaction_message::v1::Request>,
) -> std::result::Result<crate::router::response::Response, ApiError> {
	if *body.origin() != body.body.origin {
		return Err!(Request(Forbidden(
			"Not allowed to send transactions on behalf of other servers"
		)))
		.map_err(Into::into);
	}

	if body.pdus.len() > PDU_LIMIT {
		return Err!(Request(Forbidden(
			"Not allowed to send more than {PDU_LIMIT} PDUs in one transaction"
		)))
		.map_err(Into::into);
	}

	if body.edus.len() > EDU_LIMIT {
		return Err!(Request(Forbidden(
			"Not allowed to send more than {EDU_LIMIT} EDUs in one transaction"
		)))
		.map_err(Into::into);
	}

	let txn_key = (body.origin().to_owned(), body.transaction_id.clone());

	// Atomically check cache, join active, or start new transaction
	match services
		.transactions
		.get_or_start_federation_txn(txn_key.clone())
	{
		| Ok(FederationTxnState::Cached(response)) => {
			// Already responded
			Ok(crate::json_util::json_response(response))
		},
		| Ok(FederationTxnState::Active(receiver)) => {
			// Another thread is processing
			wait_for_result(receiver).await.map_err(Into::into)
		},
		| Ok(FederationTxnState::Started { receiver, sender }) => {
			// We're the first, spawn the processing task
			drop(
				services
					.server
					.runtime()
					.spawn(process_inbound_transaction(services, body, client, txn_key, sender)),
			);
			// and wait for it
			wait_for_result(receiver).await.map_err(Into::into)
		},
		| Err(e) => {
			if matches!(e, Error::BadRequest(LimitExceeded { .. }, _)) {
				// We're rejecting the transaction due to load.
				// Process the EDUs anyway so we don't miss ephemeral keys that might otherwise
				// be dropped by the sender if they eventually give up retrying!
				let edus: Vec<_> = body.body.edus.clone();
				let origin = body.origin().to_owned();

				let services = services.services();
				let runtime = services.server.runtime().clone();
				drop(runtime.spawn(async move {
					let edus_stream = edus
						.into_iter()
						.map(|edu| edu.get().to_owned())
						.map(|json_str| codec::from_str::<Edu>(&json_str))
						.filter_map(Result::ok)
						.stream();

					edus_stream
						.for_each_concurrent(automatic_width(), move |edu| {
							let origin = origin.clone();
							let services = Arc::clone(&services);
							async move { handle_edu(services, client, origin.clone(), edu).await }
						})
						.await;
				}));
			}

			Err(ApiError(e))
		},
	}
}

async fn wait_for_result(
	mut recv: Receiver<WrappedTransactionResponse>,
) -> Result<crate::router::response::Response> {
	if conduwuit::timeout(Duration::from_secs(50), recv.changed())
		.await
		.is_err()
	{
		// Took too long, return 429 to encourage the sender to try again
		return Err(Error::BadRequest(
			LimitExceeded { retry_after: None },
			"Transaction is being still being processed. Please try again later.",
		));
	}
	let value = recv.borrow_and_update();
	match value.clone() {
		| Some(Ok(response)) => Ok(crate::json_util::json_response(response)),
		| Some(Err(err)) => Err(transaction_error_to_response(&err)),
		| None => Err(Error::Request(
			ErrorKind::Unknown,
			"Transaction processing failed unexpectedly".into(),
			StatusCode::INTERNAL_SERVER_ERROR,
		)),
	}
}

async fn process_inbound_transaction(
	services: crate::State,
	body: Ruma<send_transaction_message::v1::Request>,
	client: IpAddr,
	txn_key: TxnKey,
	sender: Sender<WrappedTransactionResponse>,
) {
	let state = services;
	let services: &Services = &state;
	let edu_services = state.services();
	let state_hashes = body.json_body.as_ref().and_then(|json| {
		let obj = json.as_object()?;
		let hashes = obj
			.get("state_hashes")
			.or_else(|| obj.get("tk.nutra.msc4500.state_hashes"))?;
		codec::from_value::<StateHashes>(hashes).ok()
	});
	let txn_start_time = Instant::now();
	let origin = body.origin().to_owned();
	let pdu_count = body.pdus.len();
	let edu_count = body.edus.len();
	let pdu_ids: Vec<_> = body
		.pdus
		.iter()
		.filter_map(|pdu| codec::from_str::<Value>(pdu.get()).ok())
		.filter_map(|pdu| {
			pdu.get("event_id")
				.and_then(|e| e.as_str())
				.map(ToOwned::to_owned)
		})
		.collect();
	let pdus = body
		.body
		.pdus
		.into_iter()
		.stream()
		.broad_then(
			|pdu| async move { services.rooms.event_handler.parse_incoming_pdu(&pdu).await },
		)
		.inspect_err(|e| warn!("Could not parse incoming PDU: {e}"))
		.ready_filter_map(Result::ok);

	let edus = body
		.body
		.edus
		.into_iter()
		.map(|edu| edu.get().to_owned())
		.map(|json| codec::from_str::<Edu>(&json))
		.filter_map(Result::ok)
		.collect::<Vec<_>>()
		.into_iter()
		.stream();

	info!(
		pdus = pdu_count,
		edus = edu_count,
		pdu_ids = ?pdu_ids,
		"Processing transaction"
	);
	services
		.server
		.metrics
		.transactions_processed
		.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

	// Process EDUs concurrently with the PDU pipeline, but don't acknowledge the
	// transaction until both sides have actually committed. Returning 200 before
	// receipt/typing/device-list EDUs land creates an ack-before-commit race:
	// the sender advances its EDU watermark while the receiver may still not
	// have applied the write or an ACL gate for that same transaction.
	let edu_origin = origin.clone();
	let edu_processing = async move {
		edus.for_each_concurrent(automatic_width(), move |edu| {
			let origin = edu_origin.clone();
			let services = Arc::clone(&edu_services);
			async move { handle_edu(services, client, origin, edu).await }
		})
		.await;
	};

	let ((), results) = futures::join!(edu_processing, handle(services, &client, &origin, pdus));
	let results = match results {
		| Ok(results) => results,
		| Err(err) => {
			fail_federation_txn(state, &txn_key, &sender, err);
			return;
		},
	};

	for (id, result) in &results {
		if let Err(e) = result {
			if matches!(e, Error::BadRequest(ErrorKind::NotFound, _)) {
				debug_warn!("Incoming PDU failed {id}: {e:?}");
			}
		}
	}

	let elapsed = txn_start_time.elapsed();

	// Record transaction timing metrics
	let elapsed_us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
	services
		.server
		.metrics
		.transactions_time
		.fetch_add(elapsed_us, std::sync::atomic::Ordering::Relaxed);
	services
		.server
		.metrics
		.transactions_max_time_1m
		.fetch_max(elapsed_us, std::sync::atomic::Ordering::Relaxed);
	if elapsed > Duration::from_secs(1) {
		services
			.server
			.metrics
			.transactions_slow_1s
			.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	}
	if elapsed > Duration::from_secs(10) {
		services
			.server
			.metrics
			.transactions_slow_10s
			.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	}
	if elapsed > Duration::from_secs(100) {
		services
			.server
			.metrics
			.transactions_slow_100s
			.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
	}

	// Print update
	if elapsed < Duration::from_millis(50) {
		debug!(
			target: "federation",
			pdus = pdu_count,
			edus = edu_count,
			?elapsed,
			"Nominal txn"
		);
	} else if elapsed < Duration::from_secs(1) {
		info!(
			target: "federation",
			pdus = pdu_count,
			edus = edu_count,
			?elapsed,
			"Nominal txn"
		);
	} else if elapsed < Duration::from_secs(10) {
		info!(
			target: "federation",
			pdus = pdu_count,
			edus = edu_count,
			?elapsed,
			"Slow txn"
		);
	} else if elapsed < Duration::from_secs(100) {
		warn!(
			target: "federation",
			pdus = pdu_count,
			edus = edu_count,
			?elapsed,
			"Very slow txn"
		);
	} else {
		warn!(
			target: "federation",
			pdus = pdu_count,
			edus = edu_count,
			?elapsed,
			"Stalled txn"
		);
	}

	// Bundle response
	let pdus = results
		.into_iter()
		.map(|(e, r)| {
			let mut obj = slipstream::json::Object::new();
			if let Err(err) = r {
				obj.insert("error".to_owned(), Value::String(error::sanitized_message(err)));
			}
			(e.to_string(), Value::Object(obj))
		})
		.collect::<slipstream::json::Object>();
	let mut response_builder = slipstream::ObjectBuilder::new();
	response_builder.field("pdus", &pdus);
	let mut response_json = response_builder.finish();

	inject_state_hash_mismatches(services, state_hashes, &mut response_json).await;

	services
		.transactions
		.finish_federation_txn(txn_key, sender, response_json);
}

async fn inject_state_hash_mismatches(
	services: &Services,
	state_hashes: Option<StateHashes>,
	response_json: &mut Value,
) {
	let Some(state_hashes) = state_hashes else { return };

	// One algorithm governs the whole transaction. An unrecognized one defers
	// validation of every entry rather than skipping them one at a time.
	if !state_hashes.is_known_algorithm() {
		info!(
			target: "state_hashes",
			algorithm = %state_hashes.algorithm,
			"skipping state hash validation for unrecognized algorithm"
		);
		return;
	}

	// Only compute the (costly) input closure when this server also opted in.
	let check_inputs = state_hashes.has_resolution_inputs()
		&& services
			.server
			.config
			.experimental_features
			.msc4500_resolution_inputs;
	let mut inputs_cache = InputCache::new();

	for (event_id, entry) in state_hashes.entries {
		let Some(pdu_has_error) = response_json
			.get("pdus")
			.and_then(|p| p.as_object())
			.and_then(|p| p.get(event_id.as_str()))
			.and_then(|p| p.as_object())
			.map(|pdu| pdu.contains_key("error"))
		else {
			continue;
		};
		if pdu_has_error {
			continue;
		}

		// `limited` is an explicit deferral. An entry missing a required digest is
		// malformed: it is not an assertion that the overlay is empty, and it is
		// not a mismatch either.
		let Some((_, after, _, redactions_after)) = entry.required_digests() else {
			if !entry.limited {
				warn!(
					target: "state_hashes",
					%event_id,
					"state_hashes entry is missing a required digest; deferring"
				);
			}
			continue;
		};
		let after = after.to_owned();
		let redactions_after = redactions_after.to_owned();

		// An unresolved DAG point is deferred rather than reported as a mismatch.
		let Some(local) = services
			.rooms
			.state_accessor
			.msc4500_pdu_digests(&event_id)
			.await
		else {
			continue;
		};

		// Absent and `null` both mean the sender made no input assertion. Compute
		// the local value anyway so mismatch diagnostics retain the expected
		// digest; the comparison below only treats two present values as a
		// mismatch.
		let received_inputs = entry.resolution_inputs().map(ToOwned::to_owned);
		let local_inputs = if check_inputs {
			match services.rooms.timeline.get_pdu(&event_id).await {
				| Ok(pdu) =>
					services
						.rooms
						.state_accessor
						.msc4500_resolution_inputs_digest(&pdu, &mut inputs_cache)
						.await,
				| Err(_) => None,
			}
		} else {
			None
		};
		let inputs_differ = matches!(
			(received_inputs.as_deref(), local_inputs.as_deref()),
			(Some(received), Some(local)) if received != local
		);

		let after_differs = local.after.primary != after;
		let redactions_differ = local.after.redactions != redactions_after;
		if !after_differs && !redactions_differ && !inputs_differ {
			continue;
		}

		let mut mismatch_builder = slipstream::ObjectBuilder::new();
		mismatch_builder.field("algorithm", &state_hashes.algorithm);
		mismatch_builder.field("expected_after", &local.after.primary);
		mismatch_builder.field("received_after", &after);
		mismatch_builder.field("expected_redactions_after", &local.after.redactions);
		mismatch_builder.field("received_redactions_after", &redactions_after);
		let mut mismatch = mismatch_builder.finish();
		if check_inputs {
			if let Value::Object(object) = &mut mismatch {
				object.insert(
					"expected_resolution_inputs_before".to_owned(),
					codec::to_value(&local_inputs),
				);
				object.insert(
					"received_resolution_inputs_before".to_owned(),
					codec::to_value(&received_inputs),
				);
			}
		}
		if let Some(pdu_res) = response_json
			.get_mut("pdus")
			.and_then(|p| p.as_object_mut())
			.and_then(|p| p.get_mut(event_id.as_str()))
			.and_then(|p| p.as_object_mut())
		{
			pdu_res.insert("state_hash_mismatch".to_owned(), mismatch);
		}
	}
}

/// Handles a failed federation transaction by sending the error through
/// the channel and cleaning up the transaction state. This allows waiters to
/// receive an appropriate error response.
fn fail_federation_txn(
	services: crate::State,
	txn_key: &TxnKey,
	sender: &Sender<WrappedTransactionResponse>,
	err: TransactionError,
) {
	debug!("Transaction failed: {err}");

	// Remove from active state so the transaction can be retried
	services.transactions.remove_federation_txn(txn_key);

	// Send the error to any waiters
	if let Err(e) = sender.send(Some(Err(err))) {
		debug_warn!("Failed to send transaction error to receivers: {e}");
	}
}

/// Converts a TransactionError into an appropriate HTTP error response.
fn transaction_error_to_response(err: &TransactionError) -> Error {
	match err {
		| TransactionError::ShuttingDown => Error::Request(
			ErrorKind::Unknown,
			"Server is shutting down, please retry later".into(),
			StatusCode::SERVICE_UNAVAILABLE,
		),
		| TransactionError::Transient(e) => Error::Request(
			ErrorKind::Unknown,
			format!("Transient error, please retry: {e}").into(),
			StatusCode::INTERNAL_SERVER_ERROR,
		),
	}
}
async fn handle(
	services: &Services,
	client: &IpAddr,
	origin: &ServerName,
	pdus: impl Stream<Item = Pdu> + Send,
) -> std::result::Result<ResolvedMap, TransactionError> {
	// group pdus by room
	let pdus = pdus
		.collect()
		.map(|mut pdus: Vec<_>| {
			pdus.sort_by(|(room_a, ..), (room_b, ..)| room_a.cmp(room_b));
			pdus.into_iter()
				.into_grouping_map_by(|(room_id, ..)| room_id.clone())
				.collect()
		})
		.await;

	// we can evaluate rooms concurrently
	let results: ResolvedMap = pdus
		.into_iter()
		.try_stream()
		.broad_and_then(|(room_id, pdus): (_, Vec<_>)| {
			handle_room(services, &client, origin, room_id, pdus.into_iter())
				.map_ok(Vec::into_iter)
				.map_ok(IterStream::try_stream)
		})
		.try_flatten()
		.try_collect()
		.boxed()
		.await?;

	Ok(results)
}

/// Attempts to build a localised directed acyclic graph out of the given PDUs,
/// returning them in a topologically sorted order.
///
/// This is used to attempt to process PDUs in an order that respects their
/// dependencies, however it is ultimately the sender's responsibility to send
/// them in a processable order, so this is just a best effort attempt. It does
/// not account for power levels or other tie breaks.
async fn build_local_dag(
	pdu_map: &HashMap<OwnedEventId, CanonicalJsonObject>,
) -> Result<Vec<OwnedEventId>> {
	debug_assert!(pdu_map.len() >= 2, "needless call to build_local_dag with less than 2 PDUs");
	let mut dag: HashMap<OwnedEventId, HashSet<OwnedEventId>> =
		HashMap::with_capacity(pdu_map.len());
	let mut id_origin_ts: HashMap<OwnedEventId, _> = HashMap::with_capacity(pdu_map.len());

	for (event_id, value) in pdu_map {
		// We already checked that these properties are correct in parse_incoming_pdu,
		// so it's safe to unwrap here.
		// We also filter to remove any prev_events that are not in this pdu_map, as we
		// need to have at least one event with zero out degrees for the lexico-topo
		// sort below. If there are multiple events with omitted prevs, they will be
		// ordered by timestamp, then event ID. At that point though, it's unlikely to
		// matter.
		let mut prev_events: HashSet<OwnedEventId> = value
			.get("prev_events")
			.unwrap()
			.as_array()
			.unwrap()
			.iter()
			.filter_map(|v| v.as_str())
			.filter_map(|s| OwnedEventId::parse(s).ok())
			.filter(|id| pdu_map.contains_key(id))
			.collect();

		if let Some(auth_events) = value.get("auth_events").and_then(|v| v.as_array()) {
			prev_events.extend(
				auth_events
					.iter()
					.filter_map(|v| v.as_str())
					.filter_map(|s| OwnedEventId::parse(s).ok())
					.filter(|id| pdu_map.contains_key(id)),
			);
		}

		dag.insert(event_id.clone(), prev_events);
		let origin_server_ts = value
			.get("origin_server_ts")
			.and_then(rezzy::JsonValue::as_i64)
			.unwrap_or_default();
		id_origin_ts.insert(event_id.clone(), origin_server_ts);
	}

	debug!(count = dag.len(), "Sorting incoming events with partial graph");
	lexicographical_topological_sort(&dag, &async |node_id| {
		// Note: we don't bother fetching power levels because that would massively slow
		// this function down. This is a best-effort attempt to order events correctly
		// for processing, however ultimately that should be the sender's job.
		let ts = id_origin_ts
			.get(&node_id)
			.copied()
			.unwrap_or_else(|| int!(0))
			.to_string()
			.parse::<u64>()
			.ok()
			.and_then(|value| UInt::try_from(value).ok())
			.unwrap_or_default();
		Ok((int!(0), MilliSecondsSinceUnixEpoch(ts)))
	})
	.await
	.inspect(|sorted| {
		debug_assert_eq!(
			sorted.len(),
			pdu_map.len(),
			"Sorted graph was not the same size as the input graph"
		);
	})
	.map_err(|e| err!("failed to resolve local graph: {e}"))
}

async fn handle_room(
	services: &Services,
	_client: &IpAddr,
	origin: &ServerName,
	room_id: OwnedRoomId,
	pdus: impl Iterator<Item = Pdu> + Send,
) -> std::result::Result<Vec<(OwnedEventId, Result)>, TransactionError> {
	services
		.server
		.metrics
		.federation_active_rooms
		.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

	defer! {{
		services
			.server
			.metrics
			.federation_active_rooms
			.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
	}}

	let _room_lock = services
		.rooms
		.event_handler
		.mutex_federation
		.lock(&room_id)
		.await;

	let room_id = &room_id;
	let pdu_map: HashMap<OwnedEventId, CanonicalJsonObject> = pdus
		.into_iter()
		.map(|(_, event_id, value)| (event_id, value))
		.collect();
	// Try to sort PDUs by their dependencies, but fall back to arbitrary order on
	// failure (e.g., cycles). This is best-effort; proper ordering is the sender's
	// responsibility.
	let sorted_event_ids = if pdu_map.len() >= 2 {
		build_local_dag(&pdu_map).await.unwrap_or_else(|e| {
			debug_warn!("Failed to build local DAG for room {room_id}: {e}");
			pdu_map.keys().cloned().collect()
		})
	} else {
		pdu_map.keys().cloned().collect()
	};
	let mut results = Vec::with_capacity(sorted_event_ids.len());
	for event_id in sorted_event_ids {
		smol::future::yield_now().await;
		let value = pdu_map
			.get(&event_id)
			.expect("sorted event IDs must be from the original map")
			.clone();
		services
			.server
			.check_running()
			.map_err(|_| TransactionError::ShuttingDown)?;
		let result = Box::pin(
			services
				.rooms
				.event_handler
				.handle_incoming_pdu(&origin, room_id, &event_id, value, true, None),
		)
		.await
		.map(|_| ());

		if let Err(ref e) = result {
			// Only abort the entire transaction for truly local failures
			// (database errors, internal panics) — NOT for federation connection
			// errors to third-party servers or per-PDU auth rejections. Those are
			// per-PDU failures that won't be fixed by the sender retrying the
			// same transaction.
			if e.status_code().is_server_error()
				&& services.server.running()
				&& !e.to_string().contains("Federation connection error")
				&& !matches!(e, Error::MissingAuthEvents(_))
			{
				return Err(TransactionError::Transient(e.to_string()));
			}
		}

		results.push((event_id, result));
	}
	Ok(results)
}

async fn handle_edu(services: Arc<Services>, client: IpAddr, origin: OwnedServerName, edu: Edu) {
	match edu {
		| Edu::Presence(presence) if services.server.config.allow_incoming_presence => {
			handle_edu_presence(Arc::clone(&services), client, origin, presence).await;
		},

		| Edu::Receipt(receipt) if services.server.config.allow_incoming_read_receipts => {
			handle_edu_receipt(Arc::clone(&services), client, origin, receipt).await;
		},

		| Edu::Typing(typing) if services.server.config.allow_incoming_typing => {
			handle_edu_typing(&services, &client, &origin, typing).await;
		},

		| Edu::DeviceListUpdate(content) => {
			handle_edu_device_list_update(&services, &client, &origin, content).await;
		},

		| Edu::DirectToDevice(content) => {
			handle_edu_direct_to_device(&services, &client, &origin, content).await;
		},

		| Edu::SigningKeyUpdate(content) => {
			handle_edu_signing_key_update(&services, &client, &origin, content).await;
		},

		| Edu::_Custom(ref _custom) => debug_warn!(?edu, "received custom/unknown EDU"),

		| _ => trace!(?edu, "skipped"),
	}
}

async fn handle_edu_presence(
	services: Arc<Services>,
	_client: IpAddr,
	origin: OwnedServerName,
	presence: PresenceContent,
) {
	let fut = presence
		.push
		.into_iter()
		.stream()
		.for_each_concurrent(automatic_width(), |update| {
			handle_edu_presence_update(Arc::clone(&services), origin.clone(), update)
		});

	let timeout = services.server.config.federation_presence_interval_s;
	if conduwuit::timeout(Duration::from_secs(timeout), fut)
		.await
		.is_err()
	{
		info!(
			%origin,
			timeout,
			"Congestion: presence updates took too long, dropping remaining"
		);
	}
}

async fn handle_edu_presence_update(
	services: Arc<Services>,
	origin: OwnedServerName,
	update: PresenceUpdate,
) {
	smol::future::yield_now().await;

	if update.user_id.server_name() != origin {
		debug_warn!(
			%update.user_id, %origin,
			"received presence EDU for user not belonging to origin"
		);
		return;
	}

	services
		.presence
		.set_presence(
			&update.user_id,
			&update.presence,
			Some(update.currently_active),
			Some(update.last_active_ago),
			update.status_msg.clone(),
		)
		.await
		.log_err()
		.ok();
}

async fn handle_edu_receipt(
	services: Arc<Services>,
	_client: IpAddr,
	origin: OwnedServerName,
	receipt: ReceiptContent,
) {
	receipt
		.receipts
		.into_iter()
		.stream()
		.for_each_concurrent(automatic_width(), |(room_id, room_updates)| {
			handle_edu_receipt_room(Arc::clone(&services), origin.clone(), room_id, room_updates)
		})
		.await;
}

async fn handle_edu_receipt_room(
	services: Arc<Services>,
	origin: OwnedServerName,
	room_id: OwnedRoomId,
	room_updates: ReceiptMap,
) {
	if services
		.rooms
		.event_handler
		.acl_check(&origin, &room_id)
		.await
		.is_err()
	{
		debug_warn!(
			%origin, %room_id,
			"received read receipt EDU from ACL'd server"
		);
		return;
	}

	let room_id = &room_id;
	room_updates
		.read
		.into_iter()
		.stream()
		.for_each_concurrent(automatic_width(), |(user_id, user_updates)| {
			let services = Arc::clone(&services);
			let origin = origin.clone();
			let room_id = room_id.clone();
			async move {
				handle_edu_receipt_room_user(services, origin, room_id, user_id, user_updates)
					.await;
			}
		})
		.await;
}

async fn handle_edu_receipt_room_user(
	services: Arc<Services>,
	origin: OwnedServerName,
	room_id: OwnedRoomId,
	user_id: slipstream::OwnedUserId,
	user_updates: ReceiptData,
) {
	if user_id.server_name() != origin {
		debug_warn!(
			%user_id, %origin,
			"received read receipt EDU for user not belonging to origin"
		);
		return;
	}

	if !services
		.rooms
		.state_cache
		.server_in_room(&origin, &room_id)
		.await
	{
		debug_warn!(
			%user_id, %room_id, %origin,
			"received read receipt EDU from server who does not have a member in the room",
		);
		return;
	}

	let data = user_updates.data;
	user_updates
		.event_ids
		.into_iter()
		.stream()
		.for_each(|event_id| {
			let services = Arc::clone(&services);
			let user_id = user_id.clone();
			let room_id = room_id.clone();
			let data = data.clone();
			async move {
				let user_data = [(user_id.clone(), data.clone())];
				let receipts = [(ReceiptType::Read, BTreeMap::from(user_data))];
				let content = [(event_id.clone(), BTreeMap::from(receipts))];
				services
					.rooms
					.read_receipt
					.readreceipt_update(&user_id, &room_id, &ReceiptEvent {
						content: ReceiptEventContent(content.into()),
						room_id: room_id.clone(),
					})
					.await;
			}
		})
		.await;
}

async fn handle_edu_typing(
	services: &Services,
	_client: &IpAddr,
	origin: &ServerName,
	typing: TypingContent,
) {
	if typing.user_id.server_name() != origin {
		debug_warn!(
			%typing.user_id, %origin,
			"received typing EDU for user not belonging to origin"
		);
		return;
	}

	if services
		.rooms
		.event_handler
		.acl_check(&typing.user_id.server_name(), &typing.room_id)
		.await
		.is_err()
	{
		debug_warn!(
			%typing.user_id, %typing.room_id, %origin,
			"received typing EDU for ACL'd user's server"
		);
		return;
	}

	if !services
		.rooms
		.state_cache
		.is_joined(&typing.user_id, &typing.room_id)
		.await
	{
		debug_warn!(
			%typing.user_id, %typing.room_id, %origin,
			"received typing EDU for user not in room"
		);
		return;
	}

	if typing.typing {
		let secs = services.server.config.typing_federation_timeout_s;
		let timeout = millis_since_unix_epoch().saturating_add(secs.saturating_mul(1000));

		services
			.rooms
			.typing
			.typing_add(&typing.user_id, &typing.room_id, timeout)
			.await
			.log_err()
			.ok();
	} else {
		services
			.rooms
			.typing
			.typing_remove(&typing.user_id, &typing.room_id)
			.await
			.log_err()
			.ok();
	}
}

async fn handle_edu_device_list_update(
	services: &Services,
	_client: &IpAddr,
	origin: &ServerName,
	content: DeviceListUpdateContent,
) {
	let DeviceListUpdateContent {
		user_id,
		device_id,
		stream_id,
		prev_id,
		deleted,
		keys,
		device_display_name,
	} = content;

	if user_id.server_name() != origin {
		debug_warn!(
			%user_id, %origin,
			"received device list update EDU for user not belonging to origin"
		);
		return;
	}

	info!(%user_id, %origin, "Received DeviceListUpdate event");

	let incoming_stream_id = stream_id;
	let last_seen_stream_id = services.users.remote_device_list_stream_id(&user_id).await;

	if incoming_stream_id <= last_seen_stream_id {
		return;
	}

	if prev_id
		.iter()
		.copied()
		.any(|prev| prev > last_seen_stream_id && prev != incoming_stream_id)
	{
		// TODO: Synapse keeps a richer pending-update pipeline keyed by prev_id, which
		// lets it reconcile some out-of-order EDUs locally instead of forcing clients
		// to refetch. We intentionally keep this lighter for now and conservatively
		// surface a change.
		services
			.users
			.set_remote_device_list_stream_id(&user_id, incoming_stream_id);
		services.users.mark_device_key_update(&user_id).await;
		return;
	}

	if deleted == Some(true) {
		let had_cached_keys = services
			.users
			.get_device_keys(&user_id, &device_id)
			.await
			.is_ok();

		services
			.users
			.remove_remote_device_keys(&user_id, &device_id)
			.await;
		services
			.users
			.set_remote_device_list_stream_id(&user_id, incoming_stream_id);

		if had_cached_keys {
			services.users.mark_device_key_update(&user_id).await;
		}

		return;
	}

	let Some(incoming_keys) = keys else {
		let request = get_devices::v1::Request { user_id: user_id.clone() };

		let Ok(response) = services
			.sending
			.send_federation_request(&user_id.server_name(), request)
			.await
		else {
			// The EDU only carried a stream position, so we need a follow-up
			// /user/devices fetch to decide whether anything actually changed.
			// If that fetch fails, defer processing instead of fabricating a
			// local keychange. The sender will retry the EDU, and we can only
			// safely advance the remote cursor once we have confirmed state.
			conduwuit::warn!(
				%user_id,
				%origin,
				incoming_stream_id,
				last_seen_stream_id,
				"failed to fetch remote device list; deferring update"
			);
			return;
		};

		let fetched_stream_id = response.stream_id;
		if fetched_stream_id <= last_seen_stream_id {
			return;
		}

		// TODO: Synapse tracks and replays prev_id chains locally, which lets it avoid
		// some of these fallback /user/devices fetches and save federation bandwidth.
		let mut actually_changed = false;
		for device in response.devices {
			let incoming_keys = inject_device_display_name(
				device.keys.clone(),
				device.device_display_name.as_ref(),
			);

			let existing_keys = services
				.users
				.get_device_keys(&user_id, &device.device_id)
				.await
				.ok();
			let keys_changed = match existing_keys {
				| Some(existing_keys) =>
					remote_device_keys_differ(&existing_keys, &incoming_keys),
				| None => true,
			};

			if keys_changed {
				actually_changed = true;
			}

			services
				.users
				.cache_remote_device_keys(&user_id, &device.device_id, &incoming_keys)
				.await;
		}

		services
			.users
			.set_remote_device_list_stream_id(&user_id, fetched_stream_id);

		if actually_changed {
			services.users.mark_device_key_update(&user_id).await;
		}

		return;
	};

	let incoming_keys =
		inject_device_display_name(incoming_keys.cast(), device_display_name.as_ref());

	let existing_keys = services
		.users
		.get_device_keys(&user_id, &device_id)
		.await
		.ok();
	let keys_changed = match existing_keys {
		| Some(existing_keys) => remote_device_keys_differ(&existing_keys, &incoming_keys),
		| None => true,
	};

	services
		.users
		.cache_remote_device_keys(&user_id, &device_id, &incoming_keys)
		.await;
	services
		.users
		.set_remote_device_list_stream_id(&user_id, incoming_stream_id);

	if keys_changed {
		services.users.mark_device_key_update(&user_id).await;
	}
}

fn inject_device_display_name(
	mut keys: Raw<DeviceKeys>,
	display_name: Option<&String>,
) -> Raw<DeviceKeys> {
	let Ok(mut object) = keys.deserialize_as::<JsonObject>() else {
		return keys;
	};

	let mut modified = false;

	match display_name {
		| Some(name) => {
			let unsigned = object
				.entry("unsigned".to_owned())
				.or_insert_with(|| Value::Object(JsonObject::new()));
			if let Value::Object(unsigned_object) = unsigned {
				if unsigned_object
					.get("device_display_name")
					.and_then(|v| v.as_str())
					!= Some(name)
				{
					unsigned_object.insert("device_display_name".to_owned(), name.clone().into());
					modified = true;
				}
			}
		},
		| None =>
			if let Some(Value::Object(unsigned_object)) = object.get_mut("unsigned") {
				if unsigned_object.remove("device_display_name").is_some() {
					modified = true;
				}
			},
	}

	if modified {
		keys = Raw::from_value(&object);
	}

	keys
}

fn remote_device_keys_differ(
	existing_keys: &Raw<DeviceKeys>,
	incoming_keys: &Raw<DeviceKeys>,
) -> bool {
	match (existing_keys.deserialize(), incoming_keys.deserialize()) {
		| (Ok(existing_keys), Ok(incoming_keys)) =>
			codec::to_value(&existing_keys) != codec::to_value(&incoming_keys),
		| _ => existing_keys.get() != incoming_keys.get(),
	}
}

async fn handle_edu_direct_to_device(
	services: &Services,
	_client: &IpAddr,
	origin: &ServerName,
	content: DirectDeviceContent,
) {
	let DirectDeviceContent {
		ref sender,
		ref ev_type,
		ref message_id,
		messages,
	} = content;

	if sender.server_name() != origin {
		debug_warn!(
			%sender, %origin,
			"received direct to device EDU for user not belonging to origin"
		);
		return;
	}

	// Check if this is a new transaction id
	if services
		.transactions
		.get_client_txn(sender, None, message_id)
		.await
		.is_ok()
	{
		return;
	}

	// process messages concurrently for different users
	let ev_type = ev_type.clone();
	messages
		.into_iter()
		.stream()
		.broad_filter_map(|(target_user_id, map)| async move {
			services
				.users
				.is_active_local(&target_user_id)
				.await
				.then_some((target_user_id, map))
		})
		.for_each_concurrent(automatic_width(), |(target_user_id, map)| {
			handle_edu_direct_to_device_user(services, target_user_id, sender, &ev_type, map)
		})
		.await;

	// Save transaction id with empty data
	services
		.transactions
		.add_client_txnid(sender, None, message_id, &[]);
}

async fn handle_edu_direct_to_device_user<Event: Send + Sync>(
	services: &Services,
	target_user_id: OwnedUserId,
	sender: &UserId,
	ev_type: &str,
	map: BTreeMap<DeviceIdOrAllDevices, Raw<Event>>,
) {
	for (target_device_id_maybe, event) in map {
		let Ok(event) = event
			.deserialize_as()
			.map_err(|e| err!(Request(InvalidParam(error!("To-Device event is invalid: {e}")))))
		else {
			info!(
				%sender, %target_user_id, ?target_device_id_maybe, ev_type,
				"Failed to deserialize To-Device event, dropping"
			);
			continue;
		};

		info!(
			%sender, %target_user_id, ?target_device_id_maybe, ev_type,
			"Received To-Device event"
		);

		handle_edu_direct_to_device_event(
			services,
			&target_user_id,
			sender,
			target_device_id_maybe,
			ev_type,
			event,
		)
		.await;
	}
}

async fn handle_edu_direct_to_device_event(
	services: &Services,
	target_user_id: &UserId,
	sender: &UserId,
	target_device_id_maybe: DeviceIdOrAllDevices,
	ev_type: &str,
	event: Value,
) {
	match target_device_id_maybe {
		| DeviceIdOrAllDevices::DeviceId(ref target_device_id) => {
			services
				.users
				.add_to_device_event(sender, target_user_id, target_device_id, ev_type, event)
				.await;
		},

		| DeviceIdOrAllDevices::AllDevices => {
			services
				.users
				.all_device_ids(target_user_id)
				.for_each(|target_device_id| {
					let event = event.clone();
					async move {
						services
							.users
							.add_to_device_event(
								sender,
								target_user_id,
								&target_device_id,
								ev_type,
								event,
							)
							.await;
					}
				})
				.await;
		},
	}
}

async fn handle_edu_signing_key_update(
	services: &Services,
	_client: &IpAddr,
	origin: &ServerName,
	content: SigningKeyUpdateContent,
) {
	let SigningKeyUpdateContent { user_id, master_key, self_signing_key } = content;

	if user_id.server_name() != origin {
		debug_warn!(
			%user_id, %origin,
			"received signing key update EDU from server that does not belong to user's server"
		);
		return;
	}

	services
		.users
		.add_cross_signing_keys(
			&user_id,
			&master_key.as_ref().map(Raw::cast),
			&self_signing_key.as_ref().map(Raw::cast),
			&None,
			true,
		)
		.await
		.log_err()
		.ok();
}
