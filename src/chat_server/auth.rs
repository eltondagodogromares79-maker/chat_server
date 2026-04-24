use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::Deserialize;
use url::form_urlencoded;
use uuid::Uuid;

#[derive(Deserialize)]
struct DjangoSection {
    id: String,
    name: String,
}

#[derive(Deserialize)]
struct DjangoChatContext {
    id: String,
    role: String,
    sections: Vec<DjangoSection>,
}

#[derive(Deserialize)]
struct DjangoRoomAccess {
    allowed: bool,
}

pub struct ChatContext {
    pub user_id: Uuid,
    pub role: String,
    pub section_rooms: Vec<String>,
}

pub async fn resolve_chat_context(
    django_base_url: &str,
    cookie_header: Option<&str>,
    auth_header: Option<&str>,
) -> Result<ChatContext> {
    let client = Client::new();
    let url = format!("{}/api/users/chat-context/", django_base_url.trim_end_matches('/'));
    let mut request = client.get(url);

    if let Some(cookie) = cookie_header {
        request = request.header("cookie", cookie);
    }
    if let Some(auth) = auth_header {
        request = request.header("authorization", auth);
    }

    let response = request.send().await?;
    if !response.status().is_success() {
        return Err(anyhow!("Unauthorized"));
    }

    let profile: DjangoChatContext = response.json().await?;
    let user_id = Uuid::parse_str(&profile.id)?;
    let section_rooms = profile
        .sections
        .into_iter()
        .map(|section| format!("section:{}", section.id))
        .collect();
    Ok(ChatContext {
        user_id,
        role: profile.role,
        section_rooms,
    })
}

pub async fn validate_room_access(
    django_base_url: &str,
    token: &str,
    user_id: Uuid,
    room_key: &str,
) -> Result<bool> {
    let client = Client::new();
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("room_key", room_key)
        .append_pair("user_id", &user_id.to_string())
        .finish();
    let url = format!(
        "{}/api/chat/rooms/access/?{}",
        django_base_url.trim_end_matches('/'),
        query
    );
    let response = client
        .get(url)
        .header("X-Chat-Server-Token", token)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(anyhow!("Room access lookup failed"));
    }

    let payload: DjangoRoomAccess = response.json().await?;
    Ok(payload.allowed)
}
