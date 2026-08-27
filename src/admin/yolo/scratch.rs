use conduwuit::Result;
use ruma::OwnedEventId;
use crate::admin_command;

#[admin_command]
pub(super) async fn check_event_info(&self, event_id: OwnedEventId) -> Result {
    let pdu = self.services.rooms.timeline.get_pdu(&event_id).await.unwrap();
    self.write_str(&format!("Event type: {}\nEvent content: {:?}", pdu.kind(), pdu.content())).await
}
