use actix::prelude::*;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

use super::message::*;
use super::persist::{
    post_json,
    post_json_with_response,
    patch_json,
    delete_json,
    PersistMessagePayload,
    PersistReadPayload,
    PersistReactionPayload,
    PersistRoomPayload,
};
use serde::Deserialize;

pub struct ChatServer {
    pub sessions: HashMap<Uuid, Recipient<ServerMessage>>,
    pub rooms: HashMap<String, HashSet<Uuid>>,
    pub django_base_url: String,
    pub chat_server_token: String,
    pub http_client: reqwest::Client,
}

impl ChatServer {
    pub fn new(django_base_url: String, chat_server_token: String) -> Self {
        Self {
            sessions: HashMap::new(),
            rooms: HashMap::new(),
            django_base_url,
            chat_server_token,
            http_client: reqwest::Client::new(),
        }
    }

    fn persist_message(&self, payload: PersistMessagePayload) {
        let url = format!("{}/api/chat/messages/", self.django_base_url);
        let token = self.chat_server_token.clone();
        let client = self.http_client.clone();
        actix::spawn(async move {
            if let Err(error) = post_json(&client, &url, &token, &payload).await {
                eprintln!("Failed to persist message: {error}");
            }
        });
    }

    fn persist_room(&self, payload: PersistRoomPayload) {
        let url = format!("{}/api/chat/rooms/", self.django_base_url);
        let token = self.chat_server_token.clone();
        let client = self.http_client.clone();
        actix::spawn(async move {
            if let Err(error) = post_json(&client, &url, &token, &payload).await {
                eprintln!("Failed to persist room: {error}");
            }
        });
    }

    fn persist_read(&self, payload: PersistReadPayload) {
        let url = format!("{}/api/chat/read/", self.django_base_url);
        let token = self.chat_server_token.clone();
        let client = self.http_client.clone();
        actix::spawn(async move {
            if let Err(error) = post_json(&client, &url, &token, &payload).await {
                eprintln!("Failed to persist read receipt: {error}");
            }
        });
    }

    fn ensure_room_members(&mut self, room: &str, members: &[Uuid]) {
        let entry = self.rooms.entry(room.to_string()).or_insert_with(HashSet::new);
        for member in members {
            entry.insert(*member);
        }
    }
}

impl Actor for ChatServer {
    type Context = Context<Self>;
}

impl Handler<Connect> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) {
        println!("User connected: {}", msg.user_id);
        self.sessions.insert(msg.user_id, msg.addr);
        self.broadcast_presence();
    }
}

impl Handler<Disconnect> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        println!("User disconnected: {}", msg.user_id);
        self.sessions.remove(&msg.user_id);

        for members in self.rooms.values_mut() {
            members.remove(&msg.user_id);
        }
        self.broadcast_presence();
    }
}

impl Handler<DirectMessage> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: DirectMessage, _: &mut Context<Self>) {
        let room = msg.room.clone();
        let members = vec![msg.from, msg.to];
        self.ensure_room_members(&room, &members);
        let url = format!("{}/api/chat/messages/", self.django_base_url);
        let token = self.chat_server_token.clone();
        let client = self.http_client.clone();
        let members_payload: Vec<String> = members.iter().map(|id| id.to_string()).collect();
        let room_clone = room.clone();
        let sessions = self.sessions.clone();
        let room_members = self.rooms.get(&room).cloned().unwrap_or_default();
        let reply_to_id = msg.reply_to_id.map(|id| id.to_string());
        let reply_to_content = msg.reply_to_content.clone();
        let reply_to_sender = msg.reply_to_sender.clone();
        let content = msg.content.clone();
        let kind = msg.kind.to_string();
        let from = msg.from.to_string();
        let sent_at = msg.sent_at.to_rfc3339();

        actix::spawn(async move {
            let payload = PersistMessagePayload {
                room_key: room_clone.clone(),
                room_type: "direct".to_string(),
                sender_id: from.clone(),
                content: content.clone(),
                kind: kind.clone(),
                sent_at: sent_at.clone(),
                members: members_payload,
                reply_to_id,
            };

            let message_id = match post_json_with_response::<PersistMessagePayload, PersistedMessageResponse>(
                &client,
                &url,
                &token,
                &payload,
            )
            .await
            {
                Ok(response) => response.id,
                Err(error) => {
                    eprintln!("Failed to persist message: {error}");
                    return;
                }
            };

            let reply_content_part = reply_to_content
                .as_ref()
                .map(|value| format!(",\"reply_to_content\":\"{}\"", value.replace('"', "\\\"")))
                .unwrap_or_default();
            let reply_sender_part = reply_to_sender
                .as_ref()
                .map(|value| format!(",\"reply_to_sender\":\"{}\"", value.replace('"', "\\\"")))
                .unwrap_or_default();
            let reply_id_part = payload
                .reply_to_id
                .as_ref()
                .map(|value| format!(",\"reply_to_id\":\"{}\"", value))
                .unwrap_or_default();

            for user_id in room_members {
                if let Some(recipient) = sessions.get(&user_id) {
                    recipient.do_send(ServerMessage(format!(
                        "{{\"type\":\"direct\",\"room\":\"{}\",\"from\":\"{}\",\"content\":\"{}\",\"kind\":\"{}\",\"sent_at\":\"{}\",\"message_id\":\"{}\"{}{}{} }}",
                        room_clone,
                        from,
                        content.replace('"', "\\\""),
                        kind,
                        sent_at,
                        message_id,
                        reply_id_part,
                        reply_content_part,
                        reply_sender_part
                    )));
                }
            }
        });
    }
}

impl Handler<GroupMessage> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: GroupMessage, _: &mut Context<Self>) {
        let room_type = if msg.room.starts_with("section:") {
            "section"
        } else {
            "group"
        };
        let room_type_value = room_type.to_string();
        let url = format!("{}/api/chat/messages/", self.django_base_url);
        let token = self.chat_server_token.clone();
        let client = self.http_client.clone();
        let room_clone = msg.room.clone();
        let sessions = self.sessions.clone();
        let room_members = self.rooms.get(&msg.room).cloned().unwrap_or_default();
        let reply_to_id = msg.reply_to_id.map(|id| id.to_string());
        let reply_to_content = msg.reply_to_content.clone();
        let reply_to_sender = msg.reply_to_sender.clone();
        let content = msg.content.clone();
        let kind = msg.kind.to_string();
        let from = msg.from.to_string();
        let sent_at = msg.sent_at.to_rfc3339();

        actix::spawn(async move {
            let payload = PersistMessagePayload {
                room_key: room_clone.clone(),
                room_type: room_type_value,
                sender_id: from.clone(),
                content: content.clone(),
                kind: kind.clone(),
                sent_at: sent_at.clone(),
                members: vec![],
                reply_to_id,
            };

            let message_id = match post_json_with_response::<PersistMessagePayload, PersistedMessageResponse>(
                &client,
                &url,
                &token,
                &payload,
            )
            .await
            {
                Ok(response) => response.id,
                Err(error) => {
                    eprintln!("Failed to persist message: {error}");
                    return;
                }
            };

            let reply_content_part = reply_to_content
                .as_ref()
                .map(|value| format!(",\"reply_to_content\":\"{}\"", value.replace('"', "\\\"")))
                .unwrap_or_default();
            let reply_sender_part = reply_to_sender
                .as_ref()
                .map(|value| format!(",\"reply_to_sender\":\"{}\"", value.replace('"', "\\\"")))
                .unwrap_or_default();
            let reply_id_part = payload
                .reply_to_id
                .as_ref()
                .map(|value| format!(",\"reply_to_id\":\"{}\"", value))
                .unwrap_or_default();

            for user_id in room_members {
                if let Some(recipient) = sessions.get(&user_id) {
                    recipient.do_send(ServerMessage(format!(
                        "{{\"type\":\"group\",\"room\":\"{}\",\"from\":\"{}\",\"content\":\"{}\",\"kind\":\"{}\",\"sent_at\":\"{}\",\"message_id\":\"{}\"{}{}{} }}",
                        room_clone,
                        from,
                        content.replace('"', "\\\""),
                        kind,
                        sent_at,
                        message_id,
                        reply_id_part,
                        reply_content_part,
                        reply_sender_part
                    )));
                }
            }
        });

        if !self.rooms.contains_key(&msg.room) {
            // Notify sender that room does not exist
            if let Some(sender) = self.sessions.get(&msg.from) {
                sender.do_send(ServerMessage(format!(
                    "{{\"type\":\"error\",\"code\":\"ROOM_NOT_FOUND\",\"message\":\"Room {} does not exist or you have not joined it\"}}",
                    msg.room
                )));
            }
        }
    }
}

impl Handler<JoinRoom> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: JoinRoom, _: &mut Context<Self>) {
        println!("User {} joined room: {}", msg.user_id, msg.room);
        self.rooms
            .entry(msg.room)
            .or_insert_with(HashSet::new)
            .insert(msg.user_id);
    }
}

impl Handler<CreateRoom> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: CreateRoom, _: &mut Context<Self>) {
        self.ensure_room_members(&msg.room, &[msg.user_id]);
        self.persist_room(PersistRoomPayload {
            room_key: msg.room,
            room_type: msg.room_type,
            members: vec![msg.user_id.to_string()],
            created_by: Some(msg.user_id.to_string()),
        });
    }
}

impl Handler<TypingEvent> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: TypingEvent, _: &mut Context<Self>) {
        if let Some(members) = self.rooms.get(&msg.room) {
            for user_id in members {
                if *user_id == msg.user_id {
                    continue;
                }
                if let Some(recipient) = self.sessions.get(user_id) {
                    recipient.do_send(ServerMessage(format!(
                        "{{\"type\":\"typing\",\"room\":\"{}\",\"user_id\":\"{}\",\"is_typing\":{}}}",
                        msg.room, msg.user_id, msg.is_typing
                    )));
                }
            }
        }
    }
}

impl Handler<ReadEvent> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: ReadEvent, _: &mut Context<Self>) {
        self.persist_read(PersistReadPayload {
            room_key: msg.room.clone(),
            user_id: msg.user_id.to_string(),
            last_read_at: msg.last_read_at.to_rfc3339(),
        });

        if let Some(members) = self.rooms.get(&msg.room) {
            for user_id in members {
                if *user_id == msg.user_id {
                    continue;
                }
                if let Some(recipient) = self.sessions.get(user_id) {
                    recipient.do_send(ServerMessage(format!(
                        "{{\"type\":\"read\",\"room\":\"{}\",\"user_id\":\"{}\",\"last_read_at\":\"{}\"}}",
                        msg.room, msg.user_id, msg.last_read_at.to_rfc3339()
                    )));
                }
            }
        }
    }
}

#[derive(Deserialize)]
struct ReactionResponse {
    message_id: String,
    reactions: serde_json::Value,
}

#[derive(Deserialize)]
struct PersistedMessageResponse {
    id: String,
}

impl Handler<ReactionEvent> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: ReactionEvent, _: &mut Context<Self>) {
        let url = format!("{}/api/chat/reactions/", self.django_base_url);
        let token = self.chat_server_token.clone();
        let client = self.http_client.clone();
        let room = msg.room.clone();
        let user_id = msg.user_id;
        let emoji = msg.emoji.clone();
        let emoji_broadcast = emoji.clone();
        actix::spawn(async move {
            let payload = PersistReactionPayload {
                message_id: msg.message_id.to_string(),
                emoji: emoji.clone(),
                user_id: user_id.to_string(),
            };
            match post_json_with_response::<PersistReactionPayload, ReactionResponse>(
                &client,
                &url,
                &token,
                &payload,
            )
            .await
            {
                Ok(response) => {
                    let reactions_json = response.reactions.to_string();
                    let message_id = response.message_id;
                    let room_payload = room.clone();
                    let server_message = format!(
                        "{{\"type\":\"reaction\",\"room\":\"{}\",\"message_id\":\"{}\",\"reactions\":{}}}",
                        room_payload, message_id, reactions_json
                    );
                    // broadcast using a separate task since we don't have server state here
                    // note: reaction broadcast is best-effort; clients can refetch on refresh
                    // This closure cannot access ChatServer sessions; handled below by internal send
                    // (see fallback path).
                    // This is a no-op placeholder.
                    let _ = server_message;
                }
                Err(error) => {
                    eprintln!("Failed to persist reaction: {error}");
                }
            }
        });

        // optimistic broadcast with user_id + emoji (clients will update locally)
        if let Some(members) = self.rooms.get(&msg.room) {
            for user_id in members {
                if let Some(recipient) = self.sessions.get(user_id) {
                    recipient.do_send(ServerMessage(format!(
                        "{{\"type\":\"reaction\",\"room\":\"{}\",\"message_id\":\"{}\",\"emoji\":\"{}\",\"user_id\":\"{}\"}}",
                        msg.room, msg.message_id, emoji_broadcast, msg.user_id
                    )));
                }
            }
        }
    }
}

impl Handler<EditMessage> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: EditMessage, _: &mut Context<Self>) {
        let url = format!(
            "{}/api/chat/messages/{}/",
            self.django_base_url, msg.message_id
        );
        let token = self.chat_server_token.clone();
        let client = self.http_client.clone();
        let room = msg.room.clone();
        let content = msg.content.clone();
        actix::spawn(async move {
            let payload = serde_json::json!({ "content": content });
            if let Err(error) = patch_json(&client, &url, &token, &payload).await {
                eprintln!("Failed to update message: {error}");
            }
        });

        if let Some(members) = self.rooms.get(&msg.room) {
            for user_id in members {
                if let Some(recipient) = self.sessions.get(user_id) {
                    recipient.do_send(ServerMessage(format!(
                        "{{\"type\":\"message_updated\",\"room\":\"{}\",\"message_id\":\"{}\",\"content\":\"{}\"}}",
                        room,
                        msg.message_id,
                        msg.content.replace('\"', "\\\"")
                    )));
                }
            }
        }
    }
}

impl Handler<DeleteMessage> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: DeleteMessage, _: &mut Context<Self>) {
        let url = format!(
            "{}/api/chat/messages/{}/",
            self.django_base_url, msg.message_id
        );
        let token = self.chat_server_token.clone();
        let client = self.http_client.clone();
        let room = msg.room.clone();
        actix::spawn(async move {
            if let Err(error) = delete_json(&client, &url, &token).await {
                eprintln!("Failed to delete message: {error}");
            }
        });

        if let Some(members) = self.rooms.get(&msg.room) {
            for user_id in members {
                if let Some(recipient) = self.sessions.get(user_id) {
                    recipient.do_send(ServerMessage(format!(
                        "{{\"type\":\"message_deleted\",\"room\":\"{}\",\"message_id\":\"{}\"}}",
                        room, msg.message_id
                    )));
                }
            }
        }
    }
}

impl ChatServer {
    fn broadcast_presence(&self) {
        let user_ids: Vec<String> = self.sessions.keys().map(|id| id.to_string()).collect();
        let payload = format!(
            "{{\"type\":\"presence\",\"user_ids\":{}}}",
            serde_json::to_string(&user_ids).unwrap_or_else(|_| "[]".to_string())
        );
        for recipient in self.sessions.values() {
            recipient.do_send(ServerMessage(payload.clone()));
        }
    }
}

// // plain text - inferred as Text
// {"type":"private","to":"bbbb-...","content":"Hey!"}

// // image url - inferred as Image
// {"type":"private","to":"bbbb-...","content":"https://myserver.com/uploads/photo.jpg"}

// // file url - inferred as File
// {"type":"group","room":"general","content":"https://myserver.com/uploads/report.pdf"}

// // audio url - inferred as Audio
// {"type":"private","to":"bbbb-...","content":"https://myserver.com/uploads/voice.mp3"}
