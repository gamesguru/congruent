use std::time::SystemTime;

use conduwuit::{Err, Result, err};
use service::{mailer::messages, uiaa::Identity};
use slipstream::{
	MilliSecondsSinceUnixEpoch,
	api::client::account::{
		ThirdPartyIdRemovalStatus, add_3pid, delete_3pid, get_3pids,
		request_3pid_management_token_via_email, request_3pid_management_token_via_msisdn,
	},
	thirdparty::{Medium, ThirdPartyIdentifierInit},
};

use crate::{Ruma, router::extract::State};

/// # `GET _matrix/client/v3/account/3pid`
///
/// Get a list of third party identifiers associated with this account.
pub(crate) async fn third_party_route(
	State(services): State<crate::State>,
	body: Ruma<get_3pids::v3::Request>,
) -> Result<get_3pids::v3::Response> {
	let sender_user = body.sender_user();
	let mut threepids = vec![];

	if let Some(email) = services
		.threepid
		.get_email_for_localpart(sender_user.localpart())
		.await
	{
		threepids.push(
			ThirdPartyIdentifierInit {
				address: email,
				medium: Medium::Email,
				// We don't currently track these, and they aren't used for much
				validated_at: MilliSecondsSinceUnixEpoch::now(),
				added_at: MilliSecondsSinceUnixEpoch::from_system_time(SystemTime::UNIX_EPOCH)
					.unwrap(),
			}
			.into(),
		);
	}

	Ok(get_3pids::v3::Response { threepids })
}

/// # `POST /_matrix/client/v3/account/3pid/email/requestToken`
///
/// Requests a validation email for the purpose of changing an account's email.
pub(crate) async fn request_3pid_management_token_via_email_route(
	State(services): State<crate::State>,
	body: Ruma<request_3pid_management_token_via_email::v3::Request>,
) -> Result<request_3pid_management_token_via_email::v3::Response> {
	if !services.threepid.email_requirement().may_change() {
		return Err!(Request(Forbidden("You may not change your email address.")));
	}

	let email = body.email.clone();
	if !email.contains('@') {
		return Err!(Request(InvalidParam("Invalid email address.")));
	}

	if services
		.threepid
		.get_localpart_for_email(&email)
		.await
		.is_some()
	{
		return Err!(Request(ThreepidInUse("This email address is already in use.")));
	}

	let session = services
		.threepid
		.send_validation_email(
			email,
			|verification_link| messages::ChangeEmail {
				server_name: services.config.server_name.as_str(),
				user_id: body.sender_user_opt(),
				verification_link,
			},
			&slipstream::OwnedClientSecret::parse(&body.client_secret)
				.map_err(|_| err!(Request(InvalidParam("Invalid client_secret"))))?,
			body.send_attempt.try_into().unwrap(),
		)
		.await?;

	Ok(request_3pid_management_token_via_email::v3::Response { sid: session.to_string() })
}

/// # `POST /_matrix/client/v3/account/3pid/msisdn/requestToken`
///
/// "This API should be used to request validation tokens when adding an email
/// address to an account"
///
/// - 403 signals that The homeserver does not allow the third party identifier
///   as a contact option.
pub(crate) async fn request_3pid_management_token_via_msisdn_route(
	_body: Ruma<request_3pid_management_token_via_msisdn::v3::Request>,
) -> Result<request_3pid_management_token_via_msisdn::v3::Response> {
	Err!(Request(ThreepidMediumNotSupported(
		"MSISDN third-party identifiers are not supported."
	)))
}

/// # `POST /_matrix/client/v3/account/3pid/add`
pub(crate) async fn add_3pid_route(
	State(services): State<crate::State>,
	body: Ruma<add_3pid::v3::Request>,
) -> Result<add_3pid::v3::Response> {
	let sender_user = body.sender_user();

	if !services.threepid.email_requirement().may_change() {
		return Err!(Request(Forbidden("You may not change your email address.")));
	}

	// Require password auth to add an email
	let _ = services
		.uiaa
		.authenticate_password(&body.auth, Some(Identity::from_user_id(sender_user)))
		.await?;

	let sid = slipstream::OwnedSessionId::parse(&body.sid)
		.map_err(|_| err!(Request(InvalidParam("Invalid sid"))))?;
	let client_secret = slipstream::OwnedClientSecret::parse(&body.client_secret)
		.map_err(|_| err!(Request(InvalidParam("Invalid client_secret"))))?;
	let email = services
		.threepid
		.consume_valid_session(&sid, &client_secret)
		.await
		.map_err(|message| err!(Request(ThreepidAuthFailed("{message}"))))?;

	services
		.threepid
		.associate_localpart_email(sender_user.localpart(), &email)
		.await?;

	Ok(add_3pid::v3::Response {})
}

/// # `POST /_matrix/client/v3/account/3pid/delete`
pub(crate) async fn delete_3pid_route(
	State(services): State<crate::State>,
	body: Ruma<delete_3pid::v3::Request>,
) -> Result<delete_3pid::v3::Response> {
	let sender_user = body.sender_user();

	if body.medium != Medium::Email {
		return Ok(delete_3pid::v3::Response {
			id_server_unbind_result: ThirdPartyIdRemovalStatus::NoSupport,
		});
	}

	if !services.threepid.email_requirement().may_remove() {
		return Err!(Request(Forbidden("You may not remove your email address.")));
	}

	if services
		.threepid
		.disassociate_localpart_email(sender_user.localpart())
		.await
		.is_none()
	{
		return Err!(Request(ThreepidNotFound("Your account has no associated email.")));
	}

	Ok(delete_3pid::v3::Response {
		id_server_unbind_result: ThirdPartyIdRemovalStatus::Success,
	})
}
