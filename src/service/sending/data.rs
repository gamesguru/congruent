use std::{fmt::Debug, sync::Arc};

use conduwuit::{
	Error, Result, at, utils,
	utils::{ReadyExt, stream::TryIgnore},
};
use database::{Database, Deserialized, Map};
use futures::Stream;
use ruma::{OwnedServerName, ServerName, UserId};

use super::{Destination, SendingEvent};
use crate::{Dep, globals};

pub(super) type OutgoingItem = (Key, SendingEvent, Destination);
pub(super) type SendingItem = (Key, SendingEvent);
pub(super) type QueueItem = (Key, SendingEvent);
pub(super) type Key = Vec<u8>;

#[derive(Clone)]
pub struct Data {
	pub(super) servercurrentevent_data: Arc<Map>,
	pub(super) servernameevent_data: Arc<Map>,
	pub(super) federation_outbound_to_device: Arc<Map>,
	servername_educount: Arc<Map>,
	pub(super) db: Arc<Database>,
	services: Services,
}

#[derive(Clone)]
struct Services {
	globals: Dep<globals::Service>,
}

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;
		Self {
			servercurrentevent_data: db["servercurrentevent_data"].clone(),
			servernameevent_data: db["servernameevent_data"].clone(),
			federation_outbound_to_device: db["federation_outbound_to_device"].clone(),
			servername_educount: db["servername_educount"].clone(),
			db: args.db.clone(),
			services: Services {
				globals: args.depend::<globals::Service>("globals"),
			},
		}
	}

	pub(super) fn delete_active_request(&self, key: &[u8]) {
		self.servercurrentevent_data.remove(key);
	}

	pub(super) async fn delete_all_active_requests_for(&self, destination: &Destination) {
		let prefix = destination.get_prefix();
		self.servercurrentevent_data
			.raw_keys_prefix(&prefix)
			.ignore_err()
			.ready_for_each(|key| self.servercurrentevent_data.remove(key))
			.await;
	}

	pub(super) async fn delete_all_requests_for(&self, destination: &Destination) {
		let prefix = destination.get_prefix();
		self.servercurrentevent_data
			.raw_keys_prefix(&prefix)
			.ignore_err()
			.ready_for_each(|key| self.servercurrentevent_data.remove(key))
			.await;

		self.servernameevent_data
			.raw_keys_prefix(&prefix)
			.ignore_err()
			.ready_for_each(|key| self.servernameevent_data.remove(key))
			.await;
	}

	pub(super) fn mark_as_active<'a, I>(&self, events: I)
	where
		I: Iterator<Item = &'a QueueItem>,
	{
		events
			.filter(|(key, _)| !key.is_empty())
			.for_each(|(key, val)| {
				let val = if let SendingEvent::Edu(val) = &val { &**val } else { &[] };

				self.servercurrentevent_data.insert(key, val);
				self.servernameevent_data.remove(key);
				self.federation_outbound_to_device.remove(key);
			});
	}

	#[inline]
	pub fn active_requests(&self) -> impl Stream<Item = OutgoingItem> + Send + '_ {
		self.servercurrentevent_data
			.raw_stream()
			.ignore_err()
			.ready_filter_map(|(key, val)| match parse_servercurrentevent(key, val) {
				| Ok((dest, event)) => Some((key.to_vec(), event, dest)),
				| Err(e) => {
					// Delete the corrupted key so it doesn't spam on every scan
					self.servercurrentevent_data.remove(key);
					conduwuit::warn!(
						"Removed corrupted servercurrentevent key ({} bytes): key_hex={:02x?} \
						 key_lossy={:?} val_len={} err={e}",
						key.len(),
						key,
						String::from_utf8_lossy(key),
						val.len(),
					);
					None
				},
			})
	}

	#[inline]
	pub fn active_requests_for(
		&self,
		destination: &Destination,
	) -> impl Stream<Item = SendingItem> + Send + '_ + use<'_> {
		let prefix = destination.get_prefix();
		self.servercurrentevent_data
			.raw_stream_from(&prefix)
			.ignore_err()
			.ready_take_while(move |(key, _)| key.starts_with(&prefix))
			.ready_filter_map(|(key, val)| match parse_servercurrentevent(key, val) {
				| Ok((_, event)) => Some((key.to_vec(), event)),
				| Err(e) => {
					self.servercurrentevent_data.remove(key);
					conduwuit::warn!(
						"Removed corrupted servercurrentevent key ({} bytes): key_hex={:02x?} \
						 val_len={} err={e}",
						key.len(),
						key,
						val.len(),
					);
					None
				},
			})
	}

	pub(super) fn queue_requests<'a, I>(&self, requests: I) -> Vec<Vec<u8>>
	where
		I: Iterator<Item = (&'a SendingEvent, &'a Destination)> + Clone + Debug + Send,
	{
		self.queue_requests_impl(requests, &self.servernameevent_data)
	}

	pub(super) fn queue_reliable_requests<'a, I>(&self, requests: I) -> Vec<Vec<u8>>
	where
		I: Iterator<Item = (&'a SendingEvent, &'a Destination)> + Clone + Debug + Send,
	{
		self.queue_requests_impl(requests, &self.federation_outbound_to_device)
	}

	fn queue_requests_impl<'a, I>(&self, requests: I, map: &Map) -> Vec<Vec<u8>>
	where
		I: Iterator<Item = (&'a SendingEvent, &'a Destination)> + Clone + Debug + Send,
	{
		let keys: Vec<_> = requests
			.clone()
			.map(|(event, dest)| {
				let mut key = dest.get_prefix();
				if let SendingEvent::Pdu(value) = event {
					key.extend(value.as_ref());
				} else {
					let count = self.services.globals.next_count().unwrap();
					key.extend(&count.to_be_bytes());
				}

				key
			})
			.collect();

		let mut batch = database::Batch::new();
		for (key, event) in keys.iter().map(Vec::as_slice).zip(requests.map(at!(0))) {
			let value = if let SendingEvent::Edu(value) = &event {
				&**value
			} else {
				&[]
			};
			map.batch_put(&mut batch, key, value);
		}
		map.apply_batch(batch);

		keys
	}

	pub fn queued_requests(
		&self,
		destination: &Destination,
	) -> impl Stream<Item = QueueItem> + Send + '_ {
		Self::queued_requests_impl(destination, &self.servernameevent_data, "servernameevent")
	}

	pub fn queued_reliable_requests(
		&self,
		destination: &Destination,
	) -> impl Stream<Item = QueueItem> + Send + '_ {
		Self::queued_requests_impl(
			destination,
			&self.federation_outbound_to_device,
			"federation_outbound_to_device",
		)
	}

	fn queued_requests_impl<'a>(
		destination: &Destination,
		map: &'a Arc<Map>,
		map_name: &'static str,
	) -> impl Stream<Item = QueueItem> + Send + 'a {
		let prefix = destination.get_prefix();
		map.raw_stream_from(&prefix)
			.ignore_err()
			.ready_take_while(move |(key, _)| key.starts_with(&prefix))
			.ready_filter_map(move |(key, val)| match parse_servercurrentevent(key, val) {
				| Ok((_, event)) => Some((key.to_vec(), event)),
				| Err(e) => {
					map.remove(key);
					conduwuit::warn!(
						"Removed corrupted {map_name} key ({} bytes): key_hex={:02x?} \
						 val_len={} err={e}",
						key.len(),
						key,
						val.len(),
					);
					None
				},
			})
	}

	#[inline]
	pub fn queued_request_destinations(&self) -> impl Stream<Item = Destination> + Send + '_ {
		Self::queued_request_destinations_impl(&self.servernameevent_data, "servernameevent")
	}

	#[inline]
	pub fn queued_reliable_request_destinations(
		&self,
	) -> impl Stream<Item = Destination> + Send + '_ {
		Self::queued_request_destinations_impl(
			&self.federation_outbound_to_device,
			"federation_outbound_to_device",
		)
	}

	fn queued_request_destinations_impl<'a>(
		map: &'a Arc<Map>,
		map_name: &'static str,
	) -> impl Stream<Item = Destination> + Send + 'a {
		map.raw_stream()
			.ignore_err()
			.ready_filter_map(move |(key, val)| match parse_servercurrentevent(key, val) {
				| Ok((dest, _)) => Some(dest),
				| Err(e) => {
					map.remove(key);
					conduwuit::warn!(
						"Removed corrupted {map_name} key ({} bytes): key_hex={:02x?} \
						 val_len={} err={e}",
						key.len(),
						key,
						val.len(),
					);
					None
				},
			})
	}

	pub(super) fn set_latest_educount(&self, server_name: &ServerName, last_count: u64) {
		self.servername_educount.raw_put(server_name, last_count);
	}

	pub async fn get_latest_educount(&self, server_name: &ServerName) -> u64 {
		self.servername_educount
			.get(server_name)
			.await
			.deserialized()
			.unwrap_or(0)
	}
}

fn parse_servercurrentevent(key: &[u8], value: &[u8]) -> Result<(Destination, SendingEvent)> {
	// Appservices start with a plus
	Ok::<_, Error>(if key.starts_with(b"+") {
		let mut parts = key[1..].splitn(2, |&b| b == 0xFF);

		let server = parts.next().expect("splitn always returns one element");
		let event = parts
			.next()
			.ok_or_else(|| Error::bad_database("Invalid bytes in servercurrentpdus."))?;

		let server = utils::string_from_bytes(server).map_err(|_| {
			Error::bad_database("Invalid server bytes in server_currenttransaction")
		})?;

		(
			Destination::Appservice(server),
			if value.is_empty() {
				SendingEvent::Pdu(event.into())
			} else {
				SendingEvent::Edu(value.into())
			},
		)
	} else if key.starts_with(b"$") {
		let mut parts = key[1..].splitn(3, |&b| b == 0xFF);

		let user = parts.next().expect("splitn always returns one element");
		let user_string = utils::str_from_bytes(user)
			.map_err(|_| Error::bad_database("Invalid user string in servercurrentevent"))?;
		let user_id = UserId::parse(user_string)
			.map_err(|_| Error::bad_database("Invalid user id in servercurrentevent"))?;

		let pushkey = parts
			.next()
			.ok_or_else(|| Error::bad_database("Invalid bytes in servercurrentpdus."))?;
		let pushkey_string = utils::string_from_bytes(pushkey)
			.map_err(|_| Error::bad_database("Invalid pushkey in servercurrentevent"))?;

		let event = parts
			.next()
			.ok_or_else(|| Error::bad_database("Invalid bytes in servercurrentpdus."))?;

		(
			Destination::Push(user_id.to_owned(), pushkey_string),
			if value.is_empty() {
				SendingEvent::Pdu(event.into())
			} else {
				// I'm pretty sure this should never be called
				SendingEvent::Edu(value.into())
			},
		)
	} else {
		let mut parts = key.splitn(2, |&b| b == 0xFF);

		let server = parts.next().expect("splitn always returns one element");
		let event = parts
			.next()
			.ok_or_else(|| Error::bad_database("Invalid bytes in servercurrentpdus."))?;

		let server = utils::string_from_bytes(server).map_err(|_| {
			Error::bad_database("Invalid server bytes in server_currenttransaction")
		})?;

		(
			Destination::Federation(OwnedServerName::parse(&server).map_err(|_| {
				Error::bad_database("Invalid server string in server_currenttransaction")
			})?),
			if value.is_empty() {
				SendingEvent::Pdu(event.into())
			} else {
				SendingEvent::Edu(value.into())
			},
		)
	})
}
