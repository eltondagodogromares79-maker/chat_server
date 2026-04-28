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

#[derive(Clone)]
pub struct SessionConnection {
    pub channel: ConnectionChannel,
    pub recipient: Recipient<ServerMessage>,
}

pub struct ChatServer {
    pub sessions: HashMap<Uuid, HashMap<Uuid, SessionConnection>>,
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

    fn send_to_channel(&self, user_id: &Uuid, payload: &str, channel: ConnectionChannel) {
        if let Some(recipients) = self.sessions.get(user_id) {
            for recipient in recipients.values() {
                if recipient.channel == channel {
                    recipient.recipient.do_send(ServerMessage(payload.to_string()));
                }
            }
        }
    }

    fn send_to_chat(&self, user_id: &Uuid, payload: &str) {
        self.send_to_channel(user_id, payload, ConnectionChannel::Chat);
    }

    fn send_to_notifications(&self, user_id: &Uuid, payload: &str) {
        self.send_to_channel(user_id, payload, ConnectionChannel::Notifications);
    }

    fn chat_recipients_for_room(&self, room: &str) -> Vec<Recipient<ServerMessage>> {
        self.rooms
            .get(room)
            .into_iter()
            .flat_map(|members| members.iter())
            .flat_map(|uid| self.sessions.get(uid).into_iter().flat_map(|conns| conns.values()))
            .filter(|connection| connection.channel == ConnectionChannel::Chat)
            .map(|connection| connection.recipient.clone())
            .collect()
    }

    fn has_chat_connection(&self, user_id: &Uuid) -> bool {
        self.sessions
            .get(user_id)
            .map(|connections| {
                connections
                    .values()
                    .any(|connection| connection.channel == ConnectionChannel::Chat)
            })
            .unwrap_or(false)
    }
}

impl Actor for ChatServer {
    type Context = Context<Self>;
}

impl Handler<Connect> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) {
        println!("User connected: {}", msg.user_id);
        self.sessions
            .entry(msg.user_id)
            .or_default()
            .insert(
                msg.connection_id,
                SessionConnection {
                    channel: msg.channel,
                    recipient: msg.addr,
                },
            );
        if msg.channel == ConnectionChannel::Chat {
            self.broadcast_presence();
        }
    }
}

impl Handler<Disconnect> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        println!("User disconnected: {}", msg.user_id);
        let removed_channel = if let Some(connections) = self.sessions.get_mut(&msg.user_id) {
            let removed = connections.remove(&msg.connection_id).map(|connection| connection.channel);
            let should_remove = connections.is_empty();
            if should_remove {
                self.sessions.remove(&msg.user_id);
            }
            removed
        } else {
            None
        };

        if !self.has_chat_connection(&msg.user_id) {
            for members in self.rooms.values_mut() {
                members.remove(&msg.user_id);
            }
        }
        if removed_channel == Some(ConnectionChannel::Chat) {
            self.broadcast_presence();
        }
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
        // Bug 3 fix: only clone recipients for room members, not the entire sessions map
        let recipients_snapshot = self.chat_recipients_for_room(&room);
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

            for recipient in recipients_snapshot {
                recipient.do_send(ServerMessage(format!(
                    "{{\"type\":\"direct\",\"room\":\"{}\",\"from\":\"{}\",\"content\":\"{}\",\"kind\":\"{}\",\"sent_at\":\"{}\",\"message_id\":\"{}\"{}{}{}}}",
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
        });
    }
}

impl Handler<GroupMessage> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: GroupMessage, _: &mut Context<Self>) {
        // Bug 2 fix: check room existence BEFORE spawning the async task
        if !self.rooms.contains_key(&msg.room) {
            let payload = format!(
                "{{\"type\":\"error\",\"code\":\"ROOM_NOT_FOUND\",\"message\":\"Room {} does not exist or you have not joined it\"}}",
                msg.room
            );
            self.send_to_chat(&msg.from, &payload);
            return;
        }
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
        // Bug 3 fix: only clone recipients for room members
        let recipients_snapshot = self.chat_recipients_for_room(&msg.room);
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

            for recipient in recipients_snapshot {
                recipient.do_send(ServerMessage(format!(
                    "{{\"type\":\"group\",\"room\":\"{}\",\"from\":\"{}\",\"content\":\"{}\",\"kind\":\"{}\",\"sent_at\":\"{}\",\"message_id\":\"{}\"{}{}{}}}",
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
        });
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
                let payload = format!(
                    "{{\"type\":\"typing\",\"room\":\"{}\",\"user_id\":\"{}\",\"is_typing\":{}}}",
                    msg.room, msg.user_id, msg.is_typing
                );
                self.send_to_chat(user_id, &payload);
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
                let payload = format!(
                    "{{\"type\":\"read\",\"room\":\"{}\",\"user_id\":\"{}\",\"last_read_at\":\"{}\"}}",
                    msg.room, msg.user_id, msg.last_read_at.to_rfc3339()
                );
                self.send_to_chat(user_id, &payload);
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
        // Bug 1 fix: snapshot recipients so the spawn can broadcast the authoritative reactions
        let reaction_recipients = self.chat_recipients_for_room(&msg.room);

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
                    // Bug 1 fix: actually broadcast the authoritative reactions from Django
                    let reactions_json = response.reactions.to_string();
                    let server_message = format!(
                        "{{\"type\":\"reaction\",\"room\":\"{}\",\"message_id\":\"{}\",\"reactions\":{}}}",
                        room, response.message_id, reactions_json
                    );
                    for recipient in reaction_recipients {
                        recipient.do_send(ServerMessage(server_message.clone()));
                    }
                }
                Err(error) => {
                    eprintln!("Failed to persist reaction: {error}");
                    // fallback: optimistic broadcast so UI isn't stuck
                    let fallback = format!(
                        "{{\"type\":\"reaction\",\"room\":\"{}\",\"message_id\":\"{}\",\"emoji\":\"{}\",\"user_id\":\"{}\"}}",
                        room, msg.message_id, emoji_broadcast, user_id
                    );
                    for recipient in reaction_recipients {
                        recipient.do_send(ServerMessage(fallback.clone()));
                    }
                }
            }
        });
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
                let payload = format!(
                    "{{\"type\":\"message_updated\",\"room\":\"{}\",\"message_id\":\"{}\",\"content\":\"{}\"}}",
                    room,
                    msg.message_id,
                    msg.content.replace('\"', "\\\"")
                );
                self.send_to_chat(user_id, &payload);
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
                let payload = format!(
                    "{{\"type\":\"message_deleted\",\"room\":\"{}\",\"message_id\":\"{}\"}}",
                    room, msg.message_id
                );
                self.send_to_chat(user_id, &payload);
            }
        }
    }
}

impl Handler<InternalNotificationEvent> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: InternalNotificationEvent, _: &mut Context<Self>) {
        self.send_to_notifications(&msg.user_id, &msg.payload);
    }
}

impl Handler<InternalNotificationBatchEvent> for ChatServer {
    type Result = ();

    fn handle(&mut self, msg: InternalNotificationBatchEvent, _: &mut Context<Self>) {
        for (user_id, payload) in msg.notifications {
            self.send_to_notifications(&user_id, &payload);
        }
    }
}

impl ChatServer {
    fn broadcast_presence(&self) {
        let user_ids: Vec<String> = self
            .sessions
            .iter()
            .filter_map(|(user_id, connections)| {
                connections
                    .values()
                    .any(|connection| connection.channel == ConnectionChannel::Chat)
                    .then(|| user_id.to_string())
            })
            .collect();
        let payload = format!(
            "{{\"type\":\"presence\",\"user_ids\":{}}}",
            serde_json::to_string(&user_ids).unwrap_or_else(|_| "[]".to_string())
        );
        for recipients in self.sessions.values() {
            for recipient in recipients.values() {
                if recipient.channel == ConnectionChannel::Chat {
                    recipient.recipient.do_send(ServerMessage(payload.clone()));
                }
            }
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
