use serde::Deserialize;
use slipstream::events::relation::RelationType;

use super::Event;

fn deserialize_relation_type<'de, D>(deserializer: D) -> Result<RelationType, D::Error>
where
	D: serde::Deserializer<'de>,
{
	let value = serde_json::Value::deserialize(deserializer)?;
	slipstream::codec::from_str(&value.to_string()).map_err(serde::de::Error::custom)
}

pub trait RelationTypeEqual<E: Event> {
	fn relation_type_equal(&self, event: &E) -> bool;
}

#[derive(Debug, Deserialize)]
struct ExtractRelatesToEventId {
	#[serde(rename = "m.relates_to")]
	relates_to: ExtractRelType,
}

#[derive(Debug, Deserialize)]
struct ExtractRelType {
	#[serde(deserialize_with = "deserialize_relation_type")]
	rel_type: RelationType,
}

impl<E: Event> RelationTypeEqual<E> for RelationType {
	fn relation_type_equal(&self, event: &E) -> bool {
		event
			.get_content_serde()
			.map(|c: ExtractRelatesToEventId| c.relates_to.rel_type)
			.is_ok_and(|r| r == *self)
	}
}
