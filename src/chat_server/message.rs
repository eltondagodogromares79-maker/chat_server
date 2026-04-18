use actix::prelude::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MessageKind {
    Text,
    Image,
    File,
    Audio,
    Video,
}

impl MessageKind {
    pub fn infer_from(content: &str) -> Self {
        let lower = content.to_lowercase();

        if lower.ends_with(".png")
            || lower.ends_with(".jpg")
            || lower.ends_with(".jpeg")
            || lower.ends_with(".gif")
            || lower.ends_with(".webp")
        {
            MessageKind::Image
        } else if lower.ends_with(".mp4")
            || lower.ends_with(".mov")
            || lower.ends_with(".webm")
        {
            MessageKind::Video
        } else if lower.ends_with(".mp3")
            || lower.ends_with(".wav")
            || lower.ends_with(".ogg")
        {
            MessageKind::Audio
        } else if lower.ends_with(".pdf")
            || lower.ends_with(".zip")
            || lower.ends_with(".docx")
            || lower.ends_with(".xlsx")
            || lower.ends_with(".txt")
        {
            MessageKind::File
        } else {
            MessageKind::Text
        }
    }
}

impl std::fmt::Display for MessageKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MessageKind::Text => write!(f, "text"),
            MessageKind::Image => write!(f, "image"),
            MessageKind::File => write!(f, "file"),
            MessageKind::Audio => write!(f, "audio"),
            MessageKind::Video => write!(f, "video"),
        }
    }
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Connect {
    pub user_id: Uuid,
    pub addr: Recipient<ServerMessage>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {
    pub user_id: Uuid,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct DirectMessage {
    pub from: Uuid,
    pub to: Uuid,
    pub room: String,
    pub content: String,
    pub kind: MessageKind,
    pub sent_at: DateTime<Utc>,
    pub reply_to_id: Option<Uuid>,
    pub reply_to_content: Option<String>,
    pub reply_to_sender: Option<String>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct GroupMessage {
    pub from: Uuid,
    pub room: String,
    pub content: String,
    pub kind: MessageKind,
    pub sent_at: DateTime<Utc>,
    pub reply_to_id: Option<Uuid>,
    pub reply_to_content: Option<String>,
    pub reply_to_sender: Option<String>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct JoinRoom {
    pub user_id: Uuid,
    pub room: String,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct CreateRoom {
    pub user_id: Uuid,
    pub room: String,
    pub room_type: String,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct TypingEvent {
    pub user_id: Uuid,
    pub room: String,
    pub is_typing: bool,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ReadEvent {
    pub user_id: Uuid,
    pub room: String,
    pub last_read_at: DateTime<Utc>,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ReactionEvent {
    pub user_id: Uuid,
    pub room: String,
    pub message_id: Uuid,
    pub emoji: String,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct EditMessage {
    pub user_id: Uuid,
    pub room: String,
    pub message_id: Uuid,
    pub content: String,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct DeleteMessage {
    pub user_id: Uuid,
    pub room: String,
    pub message_id: Uuid,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ServerMessage(pub String);

#[derive(Message)]
#[rtype(result = "()")]
pub struct ErrorMessage {
    pub user_id: Uuid,
    pub error: String,
}
