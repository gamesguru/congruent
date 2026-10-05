use slipstream::codec::Serialize;

pub(crate) fn single_field<T: Serialize + ?Sized>(
	key: &str,
	value: &T,
) -> slipstream::json::Value {
	let mut object = slipstream::ObjectBuilder::new();
	object.field(key, value);
	object.finish()
}

pub(crate) fn empty_events() -> slipstream::json::Value {
	single_field("events", &Vec::<slipstream::json::Value>::new())
}

#[cfg(test)]
mod tests {
	#[test]
	fn single_field_preserves_null() {
		let value = super::single_field("profile_updates", &slipstream::json::Value::Null);
		assert_eq!(slipstream::codec::to_string(&value), r#"{"profile_updates":null}"#);
	}
}
