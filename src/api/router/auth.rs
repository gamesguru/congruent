use axum::RequestPartsExt;
use axum_extra::{
	TypedHeader,
	headers::{Authorization, authorization::Bearer},
	typed_header::TypedHeaderRejectionReason,
};
use conduwuit::{Err, Error, Result, debug, debug_error, err, warn};
use futures::{
	TryFutureExt,
	future::{
		Either::{Left, Right},
		select_ok,
	},
	pin_mut,
};
use ruma::{
	CanonicalJsonObject, CanonicalJsonValue, DeviceId, OwnedDeviceId, OwnedServerName,
	OwnedUserId, UserId,
	api::{
		AuthScheme, IncomingRequest, Metadata,
		client::{
			directory::get_public_rooms,
			error::ErrorKind,
			profile::{get_avatar_url, get_display_name, get_profile, get_profile_key},
			voip::get_turn_server_info,
		},
		federation::{authentication::XMatrix, openid::get_openid_userinfo},
	},
};
use service::{
	Services,
	server_keys::{PubKeyMap, PubKeys},
};

use super::request::Request;
use crate::service::appservice::RegistrationInfo;

enum Token {
	Appservice(Box<RegistrationInfo>),
	User((OwnedUserId, OwnedDeviceId)),
	Invalid,
	None,
}

pub(super) struct Auth {
	pub(super) origin: Option<OwnedServerName>,
	pub(super) sender_user: Option<OwnedUserId>,
	pub(super) sender_device: Option<OwnedDeviceId>,
	pub(super) appservice_info: Option<RegistrationInfo>,
}

pub(super) async fn auth(
	services: &Services,
	request: &mut Request,
	json_body: Option<&CanonicalJsonValue>,
	metadata: &Metadata,
) -> Result<Auth> {
	let expected_login_metadata = &ruma::api::client::session::login::v3::Request::METADATA;
	let expected_ping_metadata =
		&ruma::api::client::appservice::request_ping::v1::Request::METADATA;

	let stack_var = 0_u8;
	if request.parts.uri.path().contains("/login") {
		tracing::info!(
			"AUTH_DEBUG: URI: {} {}, Metadata ptr: {:p}, Stack pointer: {:p}, Expected login \
			 ptr: {:p}, Expected ping ptr: {:p}",
			&request.parts.method,
			&request.parts.uri,
			metadata,
			&stack_var,
			expected_login_metadata,
			expected_ping_metadata,
		);
	}

	let bearer: Option<TypedHeader<Authorization<Bearer>>> =
		request.parts.extract().await.unwrap_or(None);
	let token = match &bearer {
		| Some(TypedHeader(Authorization(bearer))) => Some(bearer.token()),
		| None => request.query.access_token.as_deref(),
	};

	let token = find_token(services, token).await?;

	if metadata.authentication == AuthScheme::None {
		match metadata {
			| &get_public_rooms::v3::Request::METADATA => {
				match token {
					| Token::Appservice(_) | Token::User(_) => {
						// we should have validated the token above
						// already
					},
					| Token::None | Token::Invalid => {
						return Err(Error::BadRequest(
							ErrorKind::MissingToken,
							"Missing or invalid access token.",
						));
					},
				}
			},
			| &get_profile::v3::Request::METADATA
			| &get_profile_key::unstable::Request::METADATA
			| &get_display_name::v3::Request::METADATA
			| &get_avatar_url::v3::Request::METADATA => {
				if services.server.config.require_auth_for_profile_requests {
					match token {
						| Token::Appservice(_) | Token::User(_) => {
							// we should have validated the token above
							// already
						},
						| Token::None | Token::Invalid => {
							return Err(Error::BadRequest(
								ErrorKind::MissingToken,
								"Missing or invalid access token.",
							));
						},
					}
				}
			},
			| _ => {},
		}
	}

	let authentication = if request.parts.uri.path().contains("/login") {
		AuthScheme::None
	} else {
		metadata.authentication
	};

	match (authentication, token) {
		| (AuthScheme::AccessToken, Token::Appservice(info)) =>
			Ok(auth_appservice(services, request, info).await?),
		| (
			AuthScheme::None | AuthScheme::AccessTokenOptional | AuthScheme::AppserviceToken,
			Token::Appservice(info),
		) => Ok(Auth {
			origin: None,
			sender_user: None,
			sender_device: None,
			appservice_info: Some(*info),
		}),
		| (AuthScheme::AccessToken, Token::None) => match metadata {
			| &get_turn_server_info::v3::Request::METADATA => {
				if services.server.config.turn_allow_guests {
					Ok(Auth {
						origin: None,
						sender_user: None,
						sender_device: None,
						appservice_info: None,
					})
				} else {
					Err(Error::BadRequest(ErrorKind::MissingToken, "Missing access token."))
				}
			},
			| _ => Err(Error::BadRequest(ErrorKind::MissingToken, "Missing access token.")),
		},
		| (
			AuthScheme::AccessToken | AuthScheme::AccessTokenOptional | AuthScheme::None,
			Token::User((user_id, device_id)),
		) => {
			let is_locked = services.users.is_locked(&user_id).await.map_err(|e| {
				err!(Request(Forbidden(warn!("Failed to check user lock status: {e}"))))
			})?;
			if is_locked {
				// Only /logout and /logout/all are allowed for locked users
				if !matches!(
					metadata,
					&ruma::api::client::session::logout::v3::Request::METADATA
						| &ruma::api::client::session::logout_all::v3::Request::METADATA
				) {
					return Err(Error::BadRequest(
						ErrorKind::UserLocked,
						"This account has been locked.",
					));
				}
			}
			Ok(Auth {
				origin: None,
				sender_user: Some(user_id),
				sender_device: Some(device_id),
				appservice_info: None,
			})
		},
		| (AuthScheme::ServerSignatures, Token::None) =>
			Ok(auth_server(services, request, json_body).await?),
		| (
			AuthScheme::None | AuthScheme::AppserviceToken | AuthScheme::AccessTokenOptional,
			Token::None,
		) => Ok(Auth {
			sender_user: None,
			sender_device: None,
			origin: None,
			appservice_info: None,
		}),
		| (AuthScheme::ServerSignatures, Token::Appservice(_) | Token::User(_)) =>
			Err(Error::BadRequest(
				ErrorKind::Unauthorized,
				"Only server signatures should be used on this endpoint.",
			)),
		| (AuthScheme::AppserviceToken, Token::User(_)) => {
			tracing::error!(
				"AUTH_CORRUPTION_DETECTED: metadata.authentication is AppserviceToken but token \
				 is User. URI: {} {}, Metadata pointer: {:p}, Authentication scheme: {:?}",
				&request.parts.method,
				&request.parts.uri,
				metadata,
				metadata.authentication,
			);
			Err(Error::BadRequest(
				ErrorKind::Unauthorized,
				"Only appservice access tokens should be used on this endpoint.",
			))
		},
		| (AuthScheme::None, Token::Invalid) => {
			// OpenID federation endpoint uses a query param with the same name, drop this
			// once query params for user auth are removed from the spec. This is
			// required to make integration manager work.
			if request.query.access_token.is_some()
				&& metadata == &get_openid_userinfo::v1::Request::METADATA
			{
				Ok(Auth {
					origin: None,
					sender_user: None,
					sender_device: None,
					appservice_info: None,
				})
			} else {
				Err(Error::BadRequest(
					ErrorKind::UnknownToken { soft_logout: false },
					"Unknown access token.",
				))
			}
		},
		| (_, Token::Invalid) => Err(Error::BadRequest(
			ErrorKind::UnknownToken { soft_logout: false },
			"Unknown access token.",
		)),
	}
}

async fn auth_appservice(
	services: &Services,
	request: &Request,
	info: Box<RegistrationInfo>,
) -> Result<Auth> {
	let user_id_default = || {
		UserId::parse_with_server_name(
			info.registration.sender_localpart.as_str(),
			services.globals.server_name(),
		)
	};

	let Ok(user_id) = request
		.query
		.user_id
		.clone()
		.map_or_else(user_id_default, OwnedUserId::parse)
	else {
		return Err!(Request(InvalidUsername("Username is invalid.")));
	};

	if !info.is_user_match(&user_id) {
		return Err!(Request(Exclusive("User is not in namespace.")));
	}

	// MSC3202/MSC4190: Handle device_id masquerading for appservices.
	// The device_id can be provided via `device_id` or
	// `org.matrix.msc3202.device_id` query parameter.
	let sender_device = if let Some(ref device_id_str) = request.query.device_id {
		let device_id: &DeviceId = device_id_str.as_str().into();

		// Verify the device exists for this user
		if services
			.users
			.get_device_metadata(&user_id, device_id)
			.await
			.is_err()
		{
			return Err!(Request(Forbidden(
				"Device does not exist for user or appservice cannot masquerade as this device."
			)));
		}

		Some(device_id.to_owned())
	} else {
		None
	};

	Ok(Auth {
		origin: None,
		sender_user: Some(user_id),
		sender_device,
		appservice_info: Some(*info),
	})
}

async fn auth_server(
	services: &Services,
	request: &mut Request,
	body: Option<&CanonicalJsonValue>,
) -> Result<Auth> {
	type Member = (String, CanonicalJsonValue);
	type Object = CanonicalJsonObject;
	type Value = CanonicalJsonValue;

	let x_matrix = parse_x_matrix(request).await?;
	auth_server_checks(services, &x_matrix)?;

	let destination = services.globals.server_name();
	let origin = &x_matrix.origin;
	let signature_uri = request
		.parts
		.uri
		.path_and_query()
		.expect("all requests have a path")
		.to_string();

	let signature: [Member; 1] =
		[(x_matrix.key.as_str().into(), Value::String(x_matrix.sig.to_string()))];

	let signatures: [Member; 1] = [(origin.as_str().into(), Value::Object(signature.into()))];

	let authorization: Object = if let Some(body) = body.cloned() {
		let authorization: [Member; 6] = [
			("content".into(), body),
			("destination".into(), Value::String(destination.into())),
			("method".into(), Value::String(request.parts.method.as_str().into())),
			("origin".into(), Value::String(origin.as_str().into())),
			("signatures".into(), Value::Object(signatures.into())),
			("uri".into(), Value::String(signature_uri)),
		];

		authorization.into()
	} else {
		let authorization: [Member; 5] = [
			("destination".into(), Value::String(destination.into())),
			("method".into(), Value::String(request.parts.method.as_str().into())),
			("origin".into(), Value::String(origin.as_str().into())),
			("signatures".into(), Value::Object(signatures.into())),
			("uri".into(), Value::String(signature_uri)),
		];

		authorization.into()
	};

	let key = services
		.server_keys
		.get_verify_key(origin, &x_matrix.key)
		.await
		.map_err(|e| err!(Request(Forbidden(warn!("Failed to fetch signing keys: {e}")))))?;

	let keys: PubKeys = [(x_matrix.key.to_string(), key.key)].into();
	let keys: PubKeyMap = [(origin.as_str().into(), keys)].into();
	if let Err(e) = ruma::signatures::verify_json(&keys, authorization) {
		debug_error!("Failed to verify federation request from {origin}: {e}");
		if request.parts.uri.to_string().contains('@') {
			warn!(
				"Request uri contained '@' character. Make sure your reverse proxy gives \
				 conduwuit the raw uri (apache: use nocanon)"
			);
		}

		return Err!(Request(Forbidden("Failed to verify X-Matrix signatures.")));
	}

	Ok(Auth {
		origin: origin.to_owned().into(),
		sender_user: None,
		sender_device: None,
		appservice_info: None,
	})
}

fn auth_server_checks(services: &Services, x_matrix: &XMatrix) -> Result<()> {
	auth_server_checks_impl(
		services.config.allow_federation,
		services.globals.server_name(),
		services
			.moderation
			.is_remote_server_forbidden(&x_matrix.origin),
		&x_matrix.origin,
		x_matrix.destination.as_deref(),
	)
}

fn auth_server_checks_impl(
	allow_federation: bool,
	server_name: &ruma::ServerName,
	is_forbidden: bool,
	x_matrix_origin: &ruma::ServerName,
	x_matrix_destination: Option<&ruma::ServerName>,
) -> Result<()> {
	if !allow_federation {
		return Err!(Config("allow_federation", "Federation is disabled."));
	}

	if let Some(dest) = x_matrix_destination {
		if dest != server_name {
			return Err!(Request(Forbidden(warn!(
				"Invalid destination. Expected: {}, Got: {}",
				server_name, dest
			))));
		}
	} else {
		// Matrix 1.4 introduced the `destination` field. Older servers may not send it.
		// We allow it to be missing for backwards compatibility. This fires before
		// signature verification for every pre-1.4 server, so keep it at debug to
		// avoid unauthenticated log-flood/noise in production.
		debug!(
			"Missing destination in X-Matrix header from {}. Allowing for backwards \
			 compatibility.",
			x_matrix_origin
		);
	}

	if is_forbidden {
		return Err!(Request(Forbidden(debug_warn!(
			"Federation requests from {} denied.",
			x_matrix_origin
		))));
	}

	Ok(())
}

async fn parse_x_matrix(request: &mut Request) -> Result<XMatrix> {
	let TypedHeader(Authorization(x_matrix)) = request
		.parts
		.extract::<TypedHeader<Authorization<XMatrix>>>()
		.await
		.map_err(|e| {
			let msg = match e.reason() {
				| TypedHeaderRejectionReason::Missing => "Missing Authorization header",
				| TypedHeaderRejectionReason::Error(_) => "Invalid X-Matrix signatures",
				| _ => "Unknown header-related error",
			};

			err!(Request(Forbidden(warn!(
				"{msg}: {e} for {} {}",
				&request.parts.method, &request.parts.uri
			))))
		})?;

	Ok(x_matrix)
}

async fn find_token(services: &Services, token: Option<&str>) -> Result<Token> {
	let Some(token) = token else {
		return Ok(Token::None);
	};
	if token.is_empty() {
		return Ok(Token::Invalid);
	}

	let user_token = services.users.find_from_token(token).map_ok(Token::User);

	let appservice_token = services
		.appservice
		.find_from_token(token)
		.map_ok(Box::new)
		.map_ok(Token::Appservice);

	pin_mut!(user_token, appservice_token);
	// Returns Ok if either token type succeeds, Err only if both fail
	match select_ok([Left(user_token), Right(appservice_token)]).await {
		| Err(e) if !e.is_not_found() => Err(e),
		| Ok((token, _)) => Ok(token),
		| _ => Ok(Token::Invalid),
	}
}

#[cfg(test)]
mod tests {
	use ruma::server_name;

	use super::*;

	#[test]
	fn test_auth_server_checks_impl_missing_destination() {
		let server_name = server_name!("local.com");
		let origin = server_name!("remote.com");
		let result = auth_server_checks_impl(true, server_name, false, origin, None);
		assert!(
			result.is_ok(),
			"Missing destination should be allowed for backwards compatibility"
		);
	}

	#[test]
	fn test_auth_server_checks_impl_valid_destination() {
		let server_name = server_name!("local.com");
		let origin = server_name!("remote.com");
		let result = auth_server_checks_impl(true, server_name, false, origin, Some(server_name));
		assert!(result.is_ok(), "Valid destination should be allowed");
	}

	#[test]
	fn test_auth_server_checks_impl_invalid_destination() {
		let server_name = server_name!("local.com");
		let origin = server_name!("remote.com");
		let wrong_dest = server_name!("wrong.com");
		let result = auth_server_checks_impl(true, server_name, false, origin, Some(wrong_dest));
		assert!(result.is_err(), "Invalid destination should be rejected");
	}

	#[test]
	fn test_auth_server_checks_impl_federation_disabled() {
		let server_name = server_name!("local.com");
		let origin = server_name!("remote.com");
		let result =
			auth_server_checks_impl(false, server_name, false, origin, Some(server_name));
		assert!(result.is_err(), "Disabled federation should be rejected");
	}

	#[test]
	fn test_auth_server_checks_impl_forbidden_server() {
		let server_name = server_name!("local.com");
		let origin = server_name!("remote.com");
		let result = auth_server_checks_impl(true, server_name, true, origin, Some(server_name));
		assert!(result.is_err(), "Forbidden origin server should be rejected");
	}
}
