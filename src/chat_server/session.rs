use actix::prelude::*;
use actix_web_actors::ws;
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use super::auth::validate_room_access;
use super::message::*;
use super::server::ChatServer;

pub struct ChatSession {
    pub user_id: Uuid,
    pub connection_id: Uuid,
    pub server_addr: Addr<ChatServer>,
    pub initial_rooms: Vec<String>,
    pub django_base_url: String,
    pub chat_server_token: String,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum ClientMessage {
    #[serde(rename = "direct", alias = "private")]
    Direct {
        to: String, // <-- String instead of Uuid
        content: String,
        reply_to_id: Option<String>,
        reply_to_content: Option<String>,
        reply_to_sender: Option<String>,
    },
    #[serde(rename = "join")]
    Join { room: String },
    #[serde(rename = "create_group")]
    CreateGroup { room: String },
    #[serde(rename = "group")]
    Group {
        room: String,
        content: String,
        reply_to_id: Option<String>,
        reply_to_content: Option<String>,
        reply_to_sender: Option<String>,
    },
    #[serde(rename = "typing")]
    Typing {
        room: String,
        is_typing: bool,
    },
    #[serde(rename = "read")]
    Read {
        room: String,
        last_read_at: Option<String>,
    },
    #[serde(rename = "reaction")]
    Reaction {
        room: String,
        message_id: String,
        emoji: String,
    },
    #[serde(rename = "edit_message")]
    EditMessage {
        room: String,
        message_id: String,
        content: String,
    },
    #[serde(rename = "delete_message")]
    DeleteMessage {
        room: String,
        message_id: String,
    },
}

impl Actor for ChatSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        let addr = ctx.address();

        self.server_addr.do_send(Connect {
            user_id: self.user_id,
            connection_id: self.connection_id,
            channel: ConnectionChannel::Chat,
            addr: addr.recipient(),
        });

        ctx.text(format!(
            "{{\"type\":\"connected\",\"user_id\":\"{}\"}}",
            self.user_id
        ));

        for room in self.initial_rooms.iter() {
            self.server_addr.do_send(JoinRoom {
                user_id: self.user_id,
                room: room.clone(),
            });
            ctx.text(format!(
                "{{\"type\":\"joined\",\"room\":\"{}\"}}",
                room
            ));
        }
    }

    fn stopped(&mut self, _: &mut Self::Context) {
        self.server_addr.do_send(Disconnect {
            user_id: self.user_id,
            connection_id: self.connection_id,
        });
    }
}

impl Handler<ServerMessage> for ChatSession {
    type Result = ();

    fn handle(&mut self, msg: ServerMessage, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for ChatSession {
    fn handle(
        &mut self,
        msg: Result<ws::Message, ws::ProtocolError>,
        ctx: &mut Self::Context,
    ) {
        match msg {
            Ok(ws::Message::Text(text)) => {
                println!("[{}] Received: {}", self.user_id, text);

                match serde_json::from_str::<ClientMessage>(&text) {
                    Ok(ClientMessage::Direct { to, content, reply_to_id, reply_to_content, reply_to_sender }) => {
                        // validate UUID format first
                        let to_uuid = match Uuid::parse_str(&to) {
                            Ok(id) => id,
                            Err(_) => {
                                ctx.text("{\"type\":\"error\",\"code\":\"INVALID_UUID\",\"message\":\"The provided user ID is not a valid UUID\"}");
                                return;
                            }
                        };

                        // prevent sending to yourself
                        if to_uuid == self.user_id {
                            ctx.text("{\"type\":\"error\",\"code\":\"INVALID_TARGET\",\"message\":\"You cannot send a message to yourself\"}");
                            return;
                        }

                        let room = if self.user_id < to_uuid {
                            format!("dm:{}:{}", self.user_id, to_uuid)
                        } else {
                            format!("dm:{}:{}", to_uuid, self.user_id)
                        };

                        let kind = MessageKind::infer_from(&content);

                        let reply_to = reply_to_id
                            .and_then(|value| Uuid::parse_str(&value).ok());

                        self.server_addr.do_send(DirectMessage {
                            from: self.user_id,
                            to: to_uuid,
                            room,
                            content,
                            kind,
                            sent_at: Utc::now(),
                            reply_to_id: reply_to,
                            reply_to_content,
                            reply_to_sender,
                        });
                    }

                    Ok(ClientMessage::Join { room }) => {
                        if room.trim().is_empty() {
                            ctx.text("{\"type\":\"error\",\"code\":\"INVALID_ROOM\",\"message\":\"Room name cannot be empty\"}");
                            return;
                        }
                        let user_id = self.user_id;
                        let server_addr = self.server_addr.clone();
                        let django_base_url = self.django_base_url.clone();
                        let chat_server_token = self.chat_server_token.clone();
                        let room_clone = room.clone();

                        ctx.spawn(
                            actix::fut::wrap_future(async move {
                                validate_room_access(
                                    &django_base_url,
                                    &chat_server_token,
                                    user_id,
                                    &room_clone,
                                )
                                .await
                            })
                            .map(move |result, _actor, ctx: &mut ws::WebsocketContext<Self>| match result {
                                Ok(true) => {
                                    server_addr.do_send(JoinRoom {
                                        user_id,
                                        room: room.clone(),
                                    });
                                    ctx.text(format!(
                                        "{{\"type\":\"joined\",\"room\":\"{}\"}}",
                                        room
                                    ));
                                }
                                Ok(false) => {
                                    ctx.text(format!(
                                        "{{\"type\":\"error\",\"code\":\"ROOM_FORBIDDEN\",\"message\":\"You do not have access to room {}\"}}",
                                        room
                                    ));
                                }
                                Err(_) => {
                                    ctx.text("{\"type\":\"error\",\"code\":\"ROOM_VALIDATION_FAILED\",\"message\":\"Unable to verify room access\"}");
                                }
                            }),
                        );
                    }

                    Ok(ClientMessage::CreateGroup { room }) => {
                        if room.trim().is_empty() {
                            ctx.text("{\"type\":\"error\",\"code\":\"INVALID_ROOM\",\"message\":\"Room name cannot be empty\"}");
                            return;
                        }

                        self.server_addr.do_send(CreateRoom {
                            user_id: self.user_id,
                            room: room.clone(),
                            room_type: "group".to_string(),
                        });

                        self.server_addr.do_send(JoinRoom {
                            user_id: self.user_id,
                            room: room.clone(),
                        });

                        ctx.text(format!(
                            "{{\"type\":\"group_created\",\"room\":\"{}\"}}",
                            room
                        ));
                    }

                    Ok(ClientMessage::Group { room, content, reply_to_id, reply_to_content, reply_to_sender }) => {
                        let kind = MessageKind::infer_from(&content);
                        let reply_to = reply_to_id
                            .and_then(|value| Uuid::parse_str(&value).ok());

                        self.server_addr.do_send(GroupMessage {
                            from: self.user_id,
                            room,
                            content,
                            kind,
                            sent_at: Utc::now(),
                            reply_to_id: reply_to,
                            reply_to_content,
                            reply_to_sender,
                        });
                    }

                    Ok(ClientMessage::Typing { room, is_typing }) => {
                        if room.trim().is_empty() {
                            return;
                        }

                        self.server_addr.do_send(TypingEvent {
                            user_id: self.user_id,
                            room,
                            is_typing,
                        });
                    }

                    Ok(ClientMessage::Read { room, last_read_at }) => {
                        if room.trim().is_empty() {
                            return;
                        }

                        let parsed = last_read_at
                            .and_then(|value| chrono::DateTime::parse_from_rfc3339(&value).ok())
                            .map(|dt| dt.with_timezone(&Utc))
                            .unwrap_or_else(Utc::now);

                        self.server_addr.do_send(ReadEvent {
                            user_id: self.user_id,
                            room,
                            last_read_at: parsed,
                        });
                    }

                    Ok(ClientMessage::Reaction { room, message_id, emoji }) => {
                        if room.trim().is_empty() || emoji.trim().is_empty() {
                            return;
                        }

                        let message_uuid = match Uuid::parse_str(&message_id) {
                            Ok(id) => id,
                            Err(_) => {
                                ctx.text("{\"type\":\"error\",\"code\":\"INVALID_UUID\",\"message\":\"Invalid message ID\"}");
                                return;
                            }
                        };

                        self.server_addr.do_send(ReactionEvent {
                            user_id: self.user_id,
                            room,
                            message_id: message_uuid,
                            emoji,
                        });
                    }

                    Ok(ClientMessage::EditMessage { room, message_id, content }) => {
                        if room.trim().is_empty() || content.trim().is_empty() {
                            return;
                        }

                        let message_uuid = match Uuid::parse_str(&message_id) {
                            Ok(id) => id,
                            Err(_) => {
                                ctx.text("{\"type\":\"error\",\"code\":\"INVALID_UUID\",\"message\":\"Invalid message ID\"}");
                                return;
                            }
                        };

                        self.server_addr.do_send(EditMessage {
                            user_id: self.user_id,
                            room,
                            message_id: message_uuid,
                            content,
                        });
                    }

                    Ok(ClientMessage::DeleteMessage { room, message_id }) => {
                        if room.trim().is_empty() {
                            return;
                        }

                        let message_uuid = match Uuid::parse_str(&message_id) {
                            Ok(id) => id,
                            Err(_) => {
                                ctx.text("{\"type\":\"error\",\"code\":\"INVALID_UUID\",\"message\":\"Invalid message ID\"}");
                                return;
                            }
                        };

                        self.server_addr.do_send(DeleteMessage {
                            user_id: self.user_id,
                            room,
                            message_id: message_uuid,
                        });
                    }

                    Err(_) => {
                        ctx.text("{\"type\":\"error\",\"code\":\"INVALID_FORMAT\",\"message\":\"Invalid message format. Expected JSON with a type field\"}");
                    }
                }
            }

            Ok(ws::Message::Ping(msg)) => {
                ctx.pong(&msg);
            }

            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }

            _ => {}
        }
    }
}
