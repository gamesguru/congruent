use slipstream::events::relation::RelationType;

use super::Event;

pub trait RelationTypeEqual<E: Event> {
	fn relation_type_equal(&self, event: &E) -> bool;
}

impl<E: Event> RelationTypeEqual<E> for RelationType {
	fn relation_type_equal(&self, event: &E) -> bool {
		event
			.get_content_as_value()
			.get("m.relates_to")
			.and_then(|relates_to| relates_to.get("rel_type"))
			.and_then(|rel_type| slipstream::codec::from_value::<Self>(rel_type).ok())
			.is_some_and(|r| r == *self)
	}
}
