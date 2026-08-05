mod args;
mod auth;
mod handler;
mod request;
mod response;

use std::str::FromStr;

use axum::{
	Router,
	response::{IntoResponse, Redirect},
	routing::{any, delete, get, post, put},
};
use conduwuit::{Server, err};
pub(super) use conduwuit_service::state::State;
use http::{Uri, uri};

use self::handler::RouterExt;
pub(super) use self::{
	args::{Args as Ruma, authenticate_user},
	response::RumaResponse,
};
use crate::{admin, client, server};

pub fn build(router: Router<State>, server: &Server) -> Router<State> {
	let config = &server.config;
	let mut router = router
        .ruma_route(&client::get_profile_key_route)
        .ruma_route(&client::set_profile_key_route)
        .ruma_route(&client::delete_profile_key_route)
        .ruma_route(&client::appservice_ping)
		.ruma_route(&client::get_supported_versions_route)
		.ruma_route(&client::get_register_available_route)
		.ruma_route(&client::register::register_route)
		.ruma_route(&client::register::request_registration_token_via_email_route)
		.ruma_route(&client::get_login_types_route)
		.ruma_route(&client::login_route)
		.ruma_route(&client::login_token_route)
		.ruma_route(&client::whoami_route)
		.ruma_route(&client::logout_route)
		.ruma_route(&client::logout_all_route)
		.ruma_route(&client::change_password_route)
		.ruma_route(&client::request_password_change_token_via_email_route)
		.ruma_route(&client::deactivate_route)
		.ruma_route(&client::threepid::third_party_route)
		.ruma_route(&client::threepid::request_3pid_management_token_via_email_route)
		.ruma_route(&client::threepid::request_3pid_management_token_via_msisdn_route)
		.ruma_route(&client::threepid::add_3pid_route)
		.ruma_route(&client::threepid::delete_3pid_route)
		.ruma_route(&client::check_registration_token_validity)
		.ruma_route(&client::get_capabilities_route)
		.ruma_route(&client::get_pushrules_all_route)
		.ruma_route(&client::get_pushrules_global_route)
		.ruma_route(&client::set_pushrule_route)
		.ruma_route(&client::get_pushrule_route)
		.ruma_route(&client::set_pushrule_enabled_route)
		.ruma_route(&client::get_pushrule_enabled_route)
		.ruma_route(&client::get_pushrule_actions_route)
		.ruma_route(&client::set_pushrule_actions_route)
		.ruma_route(&client::delete_pushrule_route)
		.ruma_route(&client::get_room_event_route)
		.ruma_route(&client::get_room_event_by_timestamp_route)
		.ruma_route(&client::get_room_aliases_route)
		.ruma_route(&client::get_filter_route)
		.ruma_route(&client::create_filter_route)
		.ruma_route(&client::create_openid_token_route)
		.ruma_route(&client::set_global_account_data_route)
		.ruma_route(&client::set_room_account_data_route)
		.ruma_route(&client::get_global_account_data_route)
		.ruma_route(&client::get_room_account_data_route)
		.route(
			"/_matrix/client/unstable/org.matrix.msc3391/user/{user_id}/account_data/{event_type}",
			delete(client::delete_global_account_data_msc3391_route),
		)
		.route(
			"/_matrix/client/unstable/org.matrix.msc3391/user/{user_id}/rooms/{room_id}/account_data/{event_type}",
			delete(client::delete_room_account_data_msc3391_route),
		)
		.ruma_route(&client::set_displayname_route)
		.ruma_route(&client::get_displayname_route)
		.ruma_route(&client::set_avatar_url_route)
		.ruma_route(&client::get_avatar_url_route)
		.ruma_route(&client::get_profile_route)
		.ruma_route(&client::set_presence_route)
		.ruma_route(&client::get_presence_route)
		.ruma_route(&client::upload_keys_route)
		.ruma_route(&client::get_keys_route)
		.ruma_route(&client::claim_keys_route)
		.ruma_route(&client::create_backup_version_route)
		.ruma_route(&client::update_backup_version_route)
		.ruma_route(&client::delete_backup_version_route)
		.ruma_route(&client::get_latest_backup_info_route)
		.ruma_route(&client::get_backup_info_route)
		.ruma_route(&client::add_backup_keys_route)
		.ruma_route(&client::add_backup_keys_for_room_route)
		.ruma_route(&client::add_backup_keys_for_session_route)
		.ruma_route(&client::delete_backup_keys_for_room_route)
		.ruma_route(&client::delete_backup_keys_for_session_route)
		.ruma_route(&client::delete_backup_keys_route)
		.ruma_route(&client::get_backup_keys_for_room_route)
		.ruma_route(&client::get_backup_keys_for_session_route)
		.ruma_route(&client::get_backup_keys_route)
		.ruma_route(&client::set_read_marker_route)
		.ruma_route(&client::create_receipt_route)
		.ruma_route(&client::create_typing_event_route)
		.ruma_route(&client::create_room_route)
		.ruma_route(&client::redact_event_route)
		.ruma_route(&client::report_event_route)
		.ruma_route(&client::report_room_route)
		.ruma_route(&client::report_user_route)
		.ruma_route(&client::create_alias_route)
		.ruma_route(&client::delete_alias_route)
		.ruma_route(&client::get_alias_route)
		.ruma_route(&client::join_room_by_id_route)
		.ruma_route(&client::join_room_by_id_or_alias_route)
		.route("/_matrix/client/v3/rooms/{room_id}/joined_members", get(client::joined_members_route))
		.route("/_matrix/client/r0/rooms/{room_id}/joined_members", get(client::joined_members_route))
		.ruma_route(&client::knock_room_route)
		.ruma_route(&client::leave_room_route)
		.ruma_route(&client::forget_room_route)
		.ruma_route(&client::joined_rooms_route)
		.ruma_route(&client::kick_user_route)
		.ruma_route(&client::ban_user_route)
		.ruma_route(&client::unban_user_route)
		.ruma_route(&client::invite_user_route)
		.ruma_route(&client::set_room_visibility_route)
		.ruma_route(&client::get_room_visibility_route)
		.merge(
			Router::new()
				.ruma_route(&client::get_public_rooms_route)
				.ruma_route(&client::get_public_rooms_filtered_route)
				.layer(axum::middleware::map_response(inject_public_join_rule)),
		)
		.ruma_route(&client::search_users_route)
		.ruma_route(&client::get_member_events_route)
		.ruma_route(&client::get_protocols_route)
		.route(
			"/_matrix/client/unstable/thirdparty/protocols",
			get(client::get_protocols_route_unstable),
		)
		.route(
			"/_matrix/client/v3/rooms/{room_id}/send/{event_type}/{txn_id}",
			put(client::send_message_event_route),
		)
		.route(
			"/_matrix/client/r0/rooms/{room_id}/send/{event_type}/{txn_id}",
			put(client::send_message_event_route),
		)
		.route(
			"/_matrix/client/v3/rooms/{room_id}/state/{event_type}/{state_key}",
			put(client::send_state_event_for_key_route),
		)
		.route(
			"/_matrix/client/r0/rooms/{room_id}/state/{event_type}/{state_key}",
			put(client::send_state_event_for_key_route),
		)
		.ruma_route(&client::get_state_events_route)
		.ruma_route(&client::get_state_events_for_key_route)
		// Ruma doesn't have support for multiple paths for a single endpoint yet, and these routes
		// share one Ruma request / response type pair with {get,send}_state_event_for_key_route
		.route(
			"/_matrix/client/r0/rooms/{room_id}/state/{event_type}",
			get(client::get_state_events_for_empty_key_route)
				.put(client::send_state_event_for_empty_key_route),
		)
		.route(
			"/_matrix/client/v3/rooms/{room_id}/state/{event_type}",
			get(client::get_state_events_for_empty_key_route)
				.put(client::send_state_event_for_empty_key_route),
		)
		// These two endpoints allow trailing slashes
		.route(
			"/_matrix/client/r0/rooms/{room_id}/state/{event_type}/",
			get(client::get_state_events_for_empty_key_route)
				.put(client::send_state_event_for_empty_key_route),
		)
		.route(
			"/_matrix/client/v3/rooms/{room_id}/state/{event_type}/",
			get(client::get_state_events_for_empty_key_route)
				.put(client::send_state_event_for_empty_key_route),
		)
		.route("/_matrix/client/r0/sync", get(client::sync_events_route))
		.route("/_matrix/client/v3/sync", get(client::sync_events_route))
		.route("/_matrix/client/v4/sync", post(client::sync_events_v5_route))
		.route("/_matrix/client/v5/sync", post(client::sync_events_v5_route))
		.route(
			"/_matrix/client/unstable/org.matrix.simplified_msc3575/sync",
			post(client::sync_events_unstable_msc3575_route),
		)
		.ruma_route(&client::get_context_route)
		.merge(
			Router::new()
				.ruma_route(&client::get_message_events_route)
				.layer(axum::middleware::from_fn(default_messages_dir)),
		)
		.merge(
			Router::new()
				.ruma_route(&client::search_events_route)
				.layer(axum::middleware::map_response(ensure_search_results_present)),
		)
		.ruma_route(&client::turn_server_route)
		.ruma_route(&client::send_event_to_device_route)
		.ruma_route(&client::create_content_route)
		.ruma_route(&client::get_content_thumbnail_route)
		.ruma_route(&client::get_content_route)
		.ruma_route(&client::get_content_as_filename_route)
		.route(
			"/_matrix/client/v1/media/download/{server_name}/{media_id}/",
			get(redirect_download_no_filename),
		)
		.route(
			"/_matrix/client/v3/media/download/{server_name}/{media_id}/",
			get(redirect_download_no_filename),
		)
		.route(
			"/_matrix/media/v3/download/{server_name}/{media_id}/",
			get(redirect_download_no_filename),
		)
		.route(
			"/_matrix/media/r0/download/{server_name}/{media_id}/",
			get(redirect_download_no_filename),
		)
		.ruma_route(&client::get_media_preview_route)
		.ruma_route(&client::get_media_config_route)
		.ruma_route(&client::get_devices_route)
		.ruma_route(&client::get_device_route)
		.ruma_route(&client::update_device_route)
		.ruma_route(&client::delete_device_route)
		.ruma_route(&client::delete_devices_route)
		.ruma_route(&client::put_dehydrated_device_route)
		.ruma_route(&client::delete_dehydrated_device_route)
		.ruma_route(&client::get_dehydrated_device_route)
		.ruma_route(&client::get_dehydrated_events_route)
		.route(
			"/_matrix/client/unstable/org.matrix.msc4140/delayed_events",
			get(client::get_all_delayed_events_route),
		)
		.route(
			"/_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}",
			get(client::get_delayed_event_route),
		)
		.route(
			"/_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}",
			post(client::update_delayed_event_without_action_route),
		)
		.route(
			"/_matrix/client/unstable/org.matrix.msc4140/delayed_events/{delay_id}/{action}",
			post(client::update_delayed_event_route),
		)
		.ruma_route(&client::get_tags_route)
		.ruma_route(&client::update_tag_route)
		.ruma_route(&client::delete_tag_route)
		.ruma_route(&client::upload_signing_keys_route)
		.ruma_route(&client::upload_signatures_route)
		.ruma_route(&client::get_key_changes_route)
		.ruma_route(&client::get_pushers_route)
		.ruma_route(&client::set_pushers_route)
		.ruma_route(&client::upgrade_room_route)
		.ruma_route(&client::get_threads_route)
		.ruma_route(&client::get_relating_events_with_rel_type_and_event_type_route)
		.ruma_route(&client::get_relating_events_with_rel_type_route)
		.ruma_route(&client::get_relating_events_route)
		.ruma_route(&client::get_hierarchy_route)
		.ruma_route(&client::get_mutual_rooms_route)
		.ruma_route(&client::get_event_relationships_route)
		.ruma_route(&client::get_room_summary)
		.route(
			"/_matrix/client/unstable/im.nheko.summary/rooms/{room_id_or_alias}/summary",
			get(client::get_room_summary_legacy)
		)
		.ruma_route(&client::get_suspended_status)
		.ruma_route(&client::put_suspended_status)
		.ruma_route(&client::well_known_support)
		.ruma_route(&client::well_known_client)
		.ruma_route(&client::get_rtc_transports)
		.route("/_conduwuit/server_version", get(client::conduwuit_server_version))
		.route("/_continuwuity/server_version", get(client::conduwuit_server_version))
		.ruma_route(&client::room_initial_sync_route)
		.route("/client/server.json", get(client::syncv3_client_server_json))
		.route("/_matrix/client/unstable/org.continuwuity.dag/{room_id}", get(client::get_room_dag_route))
		.ruma_route(&admin::rooms::ban::ban_room)
		.ruma_route(&admin::rooms::list::list_rooms);

	if config.allow_federation {
		router = router
			.ruma_route(&server::get_server_version_route)
			.route("/_matrix/key/v2/server", get(server::get_server_keys_route))
			.route(
				"/_matrix/key/v2/server/{key_id}",
				get(server::get_server_keys_deprecated_route),
			)
			.merge(
			Router::new()
				.ruma_route(&server::get_public_rooms_route)
				.ruma_route(&server::get_public_rooms_filtered_route)
				.layer(axum::middleware::map_response(inject_public_join_rule)),
		)
			.ruma_route(&server::send_transaction_message_route)
			.ruma_route(&server::get_event_route)
			.ruma_route(&server::get_backfill_route)
			.ruma_route(&server::get_missing_events_route)
			.ruma_route(&server::get_event_relationships_route)
			.ruma_route(&server::get_event_authorization_route)
			.ruma_route(&server::get_room_state_route)
			.ruma_route(&server::get_room_state_ids_route)
			.ruma_route(&server::create_leave_event_template_route)
			.ruma_route(&server::create_knock_event_template_route)
			.ruma_route(&server::create_leave_event_v1_route)
			.ruma_route(&server::create_leave_event_v2_route)
			.ruma_route(&server::create_knock_event_v1_route)
			.ruma_route(&server::create_join_event_template_route)
			.ruma_route(&server::create_join_event_v1_route)
			.ruma_route(&server::create_join_event_v2_route)
			.ruma_route(&server::create_invite_route)
			.ruma_route(&server::get_devices_route)
			.ruma_route(&server::get_room_information_route)
			.ruma_route(&server::get_profile_information_route)
			.ruma_route(&server::get_keys_route)
			.ruma_route(&server::claim_keys_route)
			.ruma_route(&server::get_openid_userinfo_route)
			.ruma_route(&server::get_hierarchy_route)
			.ruma_route(&server::get_event_by_timestamp_route)
			.ruma_route(&server::well_known_server)
			.ruma_route(&server::get_content_route)
			.ruma_route(&server::get_content_thumbnail_route)
			.ruma_route(&server::get_edutypes_route)
			// MSC0F01: Gossip-Based Federation Room Reconciliation
			.route(
				"/_matrix/federation/unstable/org.matrix.msc0f01/room_digest/{room_id}",
				get(server::room_digest::get_room_digest_route),
			)
			.route("/_conduwuit/local_user_count", get(client::conduwuit_local_user_count))
			.route("/_continuwuity/local_user_count", get(client::conduwuit_local_user_count));
	} else {
		router = router
			.route("/_matrix/federation/{*path}", any(federation_disabled))
			.route("/.well-known/matrix/server", any(federation_disabled))
			.route("/_matrix/key/{*path}", any(federation_disabled))
			.route("/_conduwuit/local_user_count", any(federation_disabled))
			.route("/_continuwuity/local_user_count", any(federation_disabled));
	}

	if config.allow_legacy_media {
		router = router
			.ruma_route(&client::create_mxc_uri_route)
			.ruma_route(&client::create_content_async_route)
			.ruma_route(&client::get_media_config_legacy_route)
			.ruma_route(&client::get_media_preview_legacy_route)
			.ruma_route(&client::get_content_legacy_route)
			.ruma_route(&client::get_content_as_filename_legacy_route)
			.ruma_route(&client::get_content_thumbnail_legacy_route)
			.route("/_matrix/media/v1/config", get(client::get_media_config_legacy_legacy_route))
			.route("/_matrix/media/v1/upload", post(client::create_content_legacy_route))
			.route(
				"/_matrix/media/v1/preview_url",
				get(client::get_media_preview_legacy_legacy_route),
			)
			.route(
				"/_matrix/media/v1/download/{server_name}/{media_id}",
				get(client::get_content_legacy_legacy_route),
			)
			.route(
				"/_matrix/media/v1/download/{server_name}/{media_id}/{file_name}",
				get(client::get_content_as_filename_legacy_legacy_route),
			)
			.route(
				"/_matrix/media/v1/thumbnail/{server_name}/{media_id}",
				get(client::get_content_thumbnail_legacy_legacy_route),
			);
	} else {
		router = router
			.route("/_matrix/media/v1/{*path}", any(legacy_media_disabled))
			.route("/_matrix/media/v3/config", any(legacy_media_disabled))
			.route("/_matrix/media/v3/download/{*path}", any(legacy_media_disabled))
			.route("/_matrix/media/v3/thumbnail/{*path}", any(legacy_media_disabled))
			.route("/_matrix/media/v3/preview_url", any(redirect_legacy_preview))
			.route("/_matrix/media/r0/config", any(legacy_media_disabled))
			.route("/_matrix/media/r0/download/{*path}", any(legacy_media_disabled))
			.route("/_matrix/media/r0/thumbnail/{*path}", any(legacy_media_disabled))
			.route("/_matrix/media/r0/preview_url", any(redirect_legacy_preview));
	}

	router
}

async fn redirect_download_no_filename(uri: Uri) -> impl IntoResponse {
	let path = uri.path().trim_end_matches('/');
	let query = uri.query().unwrap_or_default();

	let path_and_query = if query.is_empty() {
		path.to_owned()
	} else {
		format!("{path}?{query}")
	};

	let path_and_query = uri::PathAndQuery::from_str(&path_and_query)
		.expect("Failed to build PathAndQuery for media download redirect URI");

	let uri = uri::Builder::new()
		.path_and_query(path_and_query)
		.build()
		.expect("Failed to build URI for redirect")
		.to_string();

	Redirect::temporary(&uri)
}

async fn redirect_legacy_preview(uri: Uri) -> impl IntoResponse {
	let path = "/_matrix/client/v1/media/preview_url";
	let query = uri.query().unwrap_or_default();

	let path_and_query = format!("{path}?{query}");
	let path_and_query = uri::PathAndQuery::from_str(&path_and_query)
		.expect("Failed to build PathAndQuery for media preview redirect URI");

	let uri = uri::Builder::new()
		.path_and_query(path_and_query)
		.build()
		.expect("Failed to build URI for redirect")
		.to_string();

	Redirect::temporary(&uri)
}

async fn legacy_media_disabled() -> impl IntoResponse {
	err!(Request(Forbidden("Unauthenticated media is disabled.")))
}

async fn federation_disabled() -> impl IntoResponse {
	err!(Request(Forbidden("Federation is disabled.")))
}

async fn inject_public_join_rule(res: axum::response::Response) -> axum::response::Response {
	use axum::body::to_bytes;

	let (parts, body) = res.into_parts();

	let Ok(bytes) = to_bytes(body, usize::MAX).await else {
		return axum::response::Response::from_parts(parts, axum::body::Body::empty());
	};

	if let Ok(mut json) = serde_json::from_slice::<serde_json::Value>(&bytes) {
		if let Some(chunk) = json.get_mut("chunk").and_then(|c| c.as_array_mut()) {
			for room in chunk {
				if room.get("join_rule").is_none() {
					room["join_rule"] = serde_json::json!("public");
				}
			}
		}
		if let Ok(modified_bytes) = serde_json::to_vec(&json) {
			return axum::response::Response::from_parts(
				parts,
				axum::body::Body::from(modified_bytes),
			);
		}
	}

	axum::response::Response::from_parts(parts, axum::body::Body::from(bytes))
}

/// ruma's `ResultRoomEvents::results` has `skip_serializing_if =
/// "Vec::is_empty"`, so an empty page of search results serializes with the
/// `results` key dropped entirely rather than as `results: []`. Complement's
/// `Can back-paginate search results` test (and the spec's implied contract)
/// expects the key to always be present when `room_events` was requested.
/// Patched here at the response-body level instead of in the vendored ruma
/// crate, mirroring `inject_public_join_rule` above.
async fn ensure_search_results_present(
	res: axum::response::Response,
) -> axum::response::Response {
	use axum::body::to_bytes;

	let (parts, body) = res.into_parts();

	let Ok(bytes) = to_bytes(body, usize::MAX).await else {
		return axum::response::Response::from_parts(parts, axum::body::Body::empty());
	};

	if let Ok(mut json) = serde_json::from_slice::<serde_json::Value>(&bytes) {
		if let Some(room_events) = json
			.get_mut("search_categories")
			.and_then(|c| c.get_mut("room_events"))
			.and_then(|r| r.as_object_mut())
		{
			room_events
				.entry("results")
				.or_insert_with(|| serde_json::json!([]));
		}
		if let Ok(modified_bytes) = serde_json::to_vec(&json) {
			return axum::response::Response::from_parts(
				parts,
				axum::body::Body::from(modified_bytes),
			);
		}
	}

	axum::response::Response::from_parts(parts, axum::body::Body::from(bytes))
}

/// ruma's `get_message_events::v3::Request::dir` is a required `Direction`
/// (no `Option`, no `#[serde(default)]`), matching the letter of the spec
/// ("dir (Required)"). But Synapse treats it as optional and defaults to
/// forwards when absent (`PaginationConfig.from_request`,
/// `default_dir: Direction = Direction.FORWARDS`) -- the same kind of
/// spec-vs-reference-implementation gap already established for `from` by
/// MSC3567 ("Synapse already implements this, but it is not spec-compliant").
/// Complement tests against that lenient behavior (e.g. `TestRoomForget`'s
/// "Forgotten room messages cannot be paginated" omits `dir` entirely), so a
/// request missing `dir` would otherwise 400 with `M_BAD_JSON` before our
/// handler ever gets to run its own checks.
///
/// Since this is a required *request* field (not a response shape ruma
/// serializes for us), it can't be patched the same way as the
/// response-side workarounds above -- there's no body to fix up after the
/// fact, because ruma's deserializer rejects the request before our handler
/// runs. Instead this injects a default `dir=f` into the query string
/// ahead of extraction, mirroring Synapse's default.
async fn default_messages_dir(
	mut req: http::Request<axum::body::Body>,
	next: axum::middleware::Next,
) -> axum::response::Response {
	let uri = req.uri();
	let has_dir = uri
		.query()
		.is_some_and(|q| q.split('&').any(|kv| kv.split('=').next() == Some("dir")));

	if !has_dir {
		let path = uri.path();
		let query = match uri.query() {
			| Some(q) if !q.is_empty() => format!("{q}&dir=f"),
			| _ => "dir=f".to_owned(),
		};

		if let Ok(new_uri) = Uri::builder()
			.path_and_query(format!("{path}?{query}"))
			.build()
		{
			*req.uri_mut() = new_uri;
		}
	}

	next.run(req).await
}
