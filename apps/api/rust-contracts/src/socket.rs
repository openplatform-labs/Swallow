//! Narrow Socket.IO contract helpers, not a Socket.IO server.
/// Match the reference gateway's context routing, including its length and membership semantics.
pub fn context_matches(message: &[String], inbox: &[String]) -> bool {
    if message.is_empty() {
        return inbox.is_empty();
    }
    message.len() == inbox.len() && message.iter().all(|key| inbox.contains(key))
}

pub const NOTIFICATION_RECEIVED: &str = "notification_received";
pub const UNREAD_COUNT_CHANGED: &str = "unread_count_changed";
pub const UNSEEN_COUNT_CHANGED: &str = "unseen_count_changed";
pub const AGENT_EVENT: &str = "agent_event";
