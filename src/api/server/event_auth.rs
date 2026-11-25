use std::{borrow::Borrow, iter::once};

use conduwuit::{Err, Error, Event, Result, err, info};
use futures::TryStreamExt;
use slipstream::api::{
	client::error::ErrorKind, federation::authorization::get_event_authorization,
};

use super::AccessCheck;
use crate::{Ruma, router::extract::State};

/// # `GET /_matrix/federation/v1/event_auth/{roomId}/{eventId}`
///
/// Retrieves the auth chain for a given event.
///
/// - This does not include the event itself
pub(crate) async fn get_event_authorization_route(
	State(services): State<crate::State>,
	body: Ruma<get_event_authorization::v1::Request>,
) -> Result<get_event_authorization::v1::Response> {
	AccessCheck {
		services: &services,
		origin: body.origin(),
		room_id: &body.room_id,
		event_id: None,
	}
	.check()
	.await?;

	info!(
		origin = body.origin().as_str(),
		room_id = %body.room_id,
		event_id = %body.event_id,
		"Serving event_auth request"
	);

	let services_for_read = services.services();
	let reply_event_id = body.event_id.clone();
	let runtime = services.server.runtime().clone();
	let event = runtime
		.spawn_blocking(move || services_for_read.rooms.timeline.get_pdu(&reply_event_id))
		.await
		.map_err(|_| Error::BadRequest(ErrorKind::NotFound, "Event not found."))?
		.map_err(|_| Error::BadRequest(ErrorKind::NotFound, "Event not found."))?;

	if event.room_id_or_hash().as_deref() != Some(body.room_id.as_ref()) {
		return Err!(Request(NotFound("Event does not belong to this room.")));
	}

	let auth_chain = services
		.rooms
		.auth_chain
		.event_ids_iter(&body.room_id, once(body.event_id.borrow()))
		.try_filter(|id| futures::future::ready(id != &body.event_id))
		.and_then(|id| {
			let services = services.services();
			let runtime = services.server.runtime().clone();
			async move {
				runtime
					.spawn_blocking(move || services.rooms.timeline.get_pdu_json(&id))
					.await
					.map_err(|error| err!(Database("blocking auth PDU lookup failed: {error}")))?
			}
		})
		.and_then(|pdu| async {
			Ok(services
				.sending
				.convert_to_outgoing_federation_event(pdu)
				.await)
		})
		.try_collect()
		.await?;

	Ok(get_event_authorization::v1::Response { auth_chain })
}

#[cfg(test)]
mod tests {
	use std::{sync::Arc, time::SystemTime};

	use bytes::Bytes;
	use conduwuit_core::{
		Server,
		config::{Config, RawConfig},
		log::{Log, LogLevelReloadHandles, capture::State as CaptureState},
		matrix::{Event, PduEvent},
	};
	use http::{Method, Request};
	use slipstream::{
		CanonicalJsonObject, CanonicalJsonValue, EventId, OwnedEventId, OwnedRoomId, OwnedUserId,
		RoomId, api::federation::authentication::XMatrix,
	};

	use super::super::test_server::TestServer;

	fn test_log() -> Log {
		Log {
			reload: LogLevelReloadHandles,
			capture: Arc::new(CaptureState::new()),
		}
	}

	fn dummy_pdu(room: &RoomId, event: &EventId, sender: &str, auth: &[&EventId]) -> PduEvent {
		let string = |s: &str| CanonicalJsonValue::String(s.to_owned());
		let mut json = CanonicalJsonObject::new();
		json.insert("room_id".into(), string(room.as_str()));
		json.insert("sender".into(), string(sender));
		json.insert("type".into(), string("m.room.message"));
		json.insert("content".into(), CanonicalJsonValue::Object(CanonicalJsonObject::new()));
		json.insert(
			"origin_server_ts".into(),
			CanonicalJsonValue::Number(123_456_789_u64.into()),
		);
		json.insert("depth".into(), CanonicalJsonValue::Number(2_u64.into()));
		json.insert("prev_events".into(), CanonicalJsonValue::Array(Vec::new()));
		json.insert(
			"auth_events".into(),
			CanonicalJsonValue::Array(auth.iter().map(|id| string(id.as_str())).collect()),
		);
		let mut hashes = CanonicalJsonObject::new();
		hashes.insert("sha256".into(), string("dummy"));
		json.insert("hashes".into(), CanonicalJsonValue::Object(hashes));
		PduEvent::from_id_val(event, json, Some(room)).expect("failed to create pdu")
	}

	#[conduwuit_macros::async_test]
	async fn incomplete_auth_chain_is_not_a_partial_success() {
		let suffix = SystemTime::now()
			.duration_since(SystemTime::UNIX_EPOCH)
			.expect("system clock is after the Unix epoch")
			.as_nanos();
		let temp_root = std::env::temp_dir()
			.join(format!("conduwuit-event-auth-test-{}-{suffix}", std::process::id()));
		let raw_config = RawConfig::from_toml(&format!(
			"server_name = \"test.conduwuit.local\"\nallow_federation = true\ndatabase_path = \
			 \"{}\"",
			temp_root.to_string_lossy().replace('\\', "/")
		))
		.expect("failed to parse test config");
		let config = Config::new(&raw_config).expect("failed to parse config");
		let server = Arc::new(Server::new(config, None::<&()>, test_log()));
		let services =
			conduwuit_service::Services::build(server.clone()).expect("failed to build services");

		let local = services.globals.server_name().to_owned();
		let room = OwnedRoomId::parse("!completeness:test.conduwuit.local").unwrap();
		let child = OwnedEventId::parse("$child:test.conduwuit.local").unwrap();
		let missing = OwnedEventId::parse("$missing:test.conduwuit.local").unwrap();
		let user = OwnedUserId::parse(format!("@alice:{local}")).unwrap();

		services
			.rooms
			.short
			.set_room_version(&room, &slipstream::RoomVersionId::V1)
			.expect("set room version");
		services
			.rooms
			.state_cache
			.mark_as_joined(&user, &room)
			.await;
		services.rooms.state_cache.update_joined_count(&room).await;

		// Record the child with an auth edge to a parent that was never written.
		conduwuit_mtxdb::events::EventAdapter::new(services.mtxdb.clone())
			.write_event_with_edges(room.as_bytes(), child.as_bytes(), b"child", &[], &[
				missing.as_bytes()
			])
			.expect("write event graph");

		let pdu = dummy_pdu(&room, &child, &format!("@alice:{local}"), &[&missing]);
		let value = pdu.to_canonical_object();
		services
			.rooms
			.timeline
			.force_insert_pdu(&room, &child, &pdu, &value, false)
			.await
			.expect("insert pdu");

		let (state, guard) = conduwuit_service::state::create(services.clone());
		let router =
			crate::router::build(crate::hyper_router::MinimalRouter::new(), &services.server);
		let mut test_server = TestServer::spawn(router, state).await;

		let uri = format!("/_matrix/federation/v1/event_auth/{room}/{child}");
		let authorization = XMatrix::sign(
			services.server_keys.keypair(),
			&local,
			&local,
			&Method::GET,
			&uri,
			None,
		)
		.expect("sign federation request");
		let request = Request::get(uri)
			.header(http::header::AUTHORIZATION, authorization.encode())
			.body(Bytes::new())
			.unwrap();

		let response = test_server.send(request).await.expect("send request");
		let status = response.status();
		let body = String::from_utf8_lossy(response.body());
		assert!(
			!status.is_success(),
			"incomplete ancestry must not return a partial success: {status} {body}"
		);
		assert!(
			body.contains("auth chain is incomplete"),
			"expected the completeness error rather than another rejection: {status} {body}"
		);

		test_server.task.abort();
		drop(guard);
		let _ = std::fs::remove_dir_all(temp_root);
	}
}
