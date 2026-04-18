use actix::Addr;
use crate::chat_server::server::ChatServer;

#[derive(Clone)]
pub struct AppState {
    pub chat_server: Addr<ChatServer>,
    pub django_base_url: String,
    pub chat_server_token: String,
}

impl AppState {
    pub fn new(
        chat_server: Addr<ChatServer>,
        django_base_url: String,
        chat_server_token: String,
    ) -> Self {
        Self {
            chat_server,
            django_base_url,
            chat_server_token,
        }
    }
}
