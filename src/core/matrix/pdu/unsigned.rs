use std::borrow::Borrow;

use slipstream::{
	MilliSecondsSinceUnixEpoch,
	codec::{from_str, to_value},
	json::{Object, Value},
};

use super::{Pdu, RawJson};
use crate::{Result, err, implement, result::LogErr};

/// Set the `unsigned` field of the PDU using only information in the PDU.
/// Some unsigned data is already set within the database (eg. prev events,
/// threads). Once this is done, other data must be calculated from the database
/// (eg. relations) This is for server-to-client events.
/// Backfill handles this itself.
#[implement(Pdu)]
pub fn set_unsigned(&mut self, user_id: Option<&slipstream::UserId>) {
	if Some(self.sender.borrow()) != user_id {
		self.remove_transaction_id().log_err().ok();
	}
	self.add_age().log_err().ok();
}

impl Pdu {
	/// Parse `unsigned` into an object; empty if unset.
	fn unsigned_object(&self) -> Result<Object> {
		let Some(unsigned) = &self.unsigned else {
			return Ok(Object::new());
		};

		match from_str::<Value>(unsigned.get()) {
			| Ok(Value::Object(object)) => Ok(object),
			| Ok(_) => Err(err!(Database("Invalid unsigned in pdu event: not an object"))),
			| Err(e) => Err(err!(Database("Invalid unsigned in pdu event: {e}"))),
		}
	}

	fn set_unsigned_object(&mut self, object: Object) -> Result {
		self.unsigned = Some(RawJson::from_value(&Value::Object(object)));

		Ok(())
	}
}

/// Set the `membership` key in `unsigned` to tell the client what the
/// requesting user's membership was at the time of this event.
/// Per spec §11.20.1.1, this SHOULD be included on events in `/sync`,
/// `/messages`, `/context`, and `/event`.
#[implement(Pdu)]
pub fn set_membership(&mut self, membership: &str) -> Result {
	let mut unsigned = self.unsigned_object()?;
	unsigned.insert("membership".into(), membership.into());
	self.set_unsigned_object(unsigned)
}

#[implement(Pdu)]
pub fn remove_transaction_id(&mut self) -> Result {
	if self.unsigned.is_none() {
		return Ok(());
	}

	let mut unsigned = self.unsigned_object()?;
	unsigned.remove("transaction_id");
	self.set_unsigned_object(unsigned)
}

#[implement(Pdu)]
pub fn add_age(&mut self) -> Result {
	let mut unsigned = self.unsigned_object()?;

	// deliberately allowing for the possibility of negative age
	let now: i128 = MilliSecondsSinceUnixEpoch::now().get().into();
	let then: i128 = self.origin_server_ts.into();
	let this_age = now.saturating_sub(then);

	unsigned.insert("age".into(), to_value(&i64::try_from(this_age).unwrap_or(i64::MAX)));
	self.set_unsigned_object(unsigned)
}

#[implement(Pdu)]
pub fn add_relation(&mut self, name: &str, pdu: Option<&Pdu>) -> Result {
	let mut unsigned = self.unsigned_object()?;

	let pdu = pdu.map_or_else(|| Value::Object(Object::new()), to_value);

	if let Value::Object(relations) = unsigned
		.entry("m.relations".into())
		.or_insert_with(|| Value::Object(Object::new()))
	{
		relations.insert(name.to_owned(), pdu);
	}

	self.set_unsigned_object(unsigned)
}
