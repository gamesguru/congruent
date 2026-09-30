use conduwuit::{Err, Result, implement, info, trace};
use conduwuit_database::{Deserialized, Json};
use ruma::{
	DeviceId, OwnedDeviceId, UserId,
	api::client::dehydrated_device::{
		DehydratedDeviceData, put_dehydrated_device::unstable::Request,
	},
	encryption::DeviceKeys,
	serde::Raw,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DehydratedDevice {
	/// Unique ID of the device.
	pub device_id: OwnedDeviceId,

	/// Contains serialized and encrypted private data.
	pub device_data: Raw<DehydratedDeviceData>,
}

/// Creates or recreates the user's dehydrated device.
///
/// Normalizes the uploaded `device_keys` by round-tripping through the typed
/// [`DeviceKeys`] struct. This strips non-spec fields like `"dehydrated":true`
/// which Element embeds but are not part of the [`DeviceKeys`] schema. Without
/// this, other clients deserialize through the same struct, recompute canonical
/// JSON *without* the extra field, and cross-signing signature verification
/// fails — causing the device to appear "Not trusted".
#[implement(super::Service)]
#[tracing::instrument(
	level = "info",
	skip_all,
	fields(
		%user_id,
		device_id = %request.device_id,
		display_name = ?request.initial_device_display_name,
	)
)]
pub async fn set_dehydrated_device(&self, user_id: &UserId, request: Request) -> Result {
	assert!(
		self.exists(user_id).await,
		"Tried to create dehydrated device for non-existent user"
	);

	let existing_id = self.get_dehydrated_device_id(user_id).await;

	if existing_id.is_err()
		&& self
			.get_device_metadata(user_id, &request.device_id)
			.await
			.is_ok()
	{
		return Err!("A hydrated device already exists with that ID.");
	}

	if let Ok(existing_id) = existing_id {
		info!("Replacing existing dehydrated device {existing_id}");
		self.remove_device(user_id, &existing_id).await;
	}

	self.create_device(
		user_id,
		&request.device_id,
		"",
		request.initial_device_display_name.clone(),
		None,
	)
	.await?;

	trace!(device_data = ?request.device_data);
	self.db.userid_dehydrateddevice.raw_put(
		user_id,
		Json(&DehydratedDevice {
			device_id: request.device_id.clone(),
			device_data: request.device_data,
		}),
	);

	// Normalize device_keys by round-tripping through the typed DeviceKeys struct
	// to strip non-spec fields (e.g. "dehydrated":true) that break cross-signing
	// signature verification on other clients.
	let device_keys = request
		.device_keys
		.deserialize()
		.map(|dk: DeviceKeys| Raw::new(&dk).expect("DeviceKeys round-trip"))
		.unwrap_or(request.device_keys);

	info!(device_id = %request.device_id, "Storing dehydrated device keys");
	self.add_device_keys(user_id, &request.device_id, &device_keys)
		.await;

	info!(count = request.one_time_keys.len(), "Storing one-time keys");
	for (one_time_key_key, one_time_key_value) in &request.one_time_keys {
		self.add_one_time_key(user_id, &request.device_id, one_time_key_key, one_time_key_value)
			.await?;
	}

	info!(count = request.fallback_keys.len(), "Storing fallback keys");
	for (fallback_key_id, fallback_key_value) in &request.fallback_keys {
		self.add_one_time_key(user_id, &request.device_id, fallback_key_id, fallback_key_value)
			.await?;
	}

	Ok(())
}

/// Removes a user's dehydrated device.
///
/// Calling this directly will remove the dehydrated data but leak the frontage
/// device. Thus this is called by the regular device interface such that the
/// dehydrated data will not leak instead.
///
/// If device_id is given, the user's dehydrated device must match or this is a
/// no-op, but an Err is still returned to indicate that. Otherwise returns the
/// removed dehydrated device_id.
#[implement(super::Service)]
#[tracing::instrument(
	level = "debug",
	skip_all,
	fields(
		%user_id,
		device_id = ?maybe_device_id,
	)
)]
pub(super) async fn remove_dehydrated_device(
	&self,
	user_id: &UserId,
	maybe_device_id: Option<&DeviceId>,
) -> Result<OwnedDeviceId> {
	let Ok(device_id) = self.get_dehydrated_device_id(user_id).await else {
		return Err!(Request(NotFound("No dehydrated device for this user.")));
	};

	if let Some(maybe_device_id) = maybe_device_id {
		if maybe_device_id != device_id {
			return Err!(Request(NotFound("Not the user's dehydrated device.")));
		}
	}

	self.db.userid_dehydrateddevice.remove(user_id);

	Ok(device_id)
}

/// Get the device_id of the user's dehydrated device.
#[implement(super::Service)]
#[tracing::instrument(
	level = "debug",
	skip_all,
	fields(%user_id)
)]
pub async fn get_dehydrated_device_id(&self, user_id: &UserId) -> Result<OwnedDeviceId> {
	self.get_dehydrated_device(user_id)
		.await
		.map(|device| device.device_id)
}

/// Get the dehydrated device private data
#[implement(super::Service)]
#[tracing::instrument(
	level = "debug",
	skip_all,
	fields(%user_id),
	ret,
)]
pub async fn get_dehydrated_device(&self, user_id: &UserId) -> Result<DehydratedDevice> {
	self.db
		.userid_dehydrateddevice
		.get(user_id)
		.await
		.deserialized()
		.map_err(|e| {
			if e.is_not_found() {
				conduwuit::Error::BadRequest(
					ruma::api::client::error::ErrorKind::NotFound,
					"No dehydrated device found.",
				)
			} else {
				e
			}
		})
}
