use serde::Serialize;
use serde::de::DeserializeOwned;

#[derive(Serialize)]
pub struct PersistRoomPayload {
    pub room_key: String,
    pub room_type: String,
    pub members: Vec<String>,
    pub created_by: Option<String>,
}

#[derive(Serialize)]
pub struct PersistMessagePayload {
    pub room_key: String,
    pub room_type: String,
    pub sender_id: String,
    pub content: String,
    pub kind: String,
    pub sent_at: String,
    pub members: Vec<String>,
    pub reply_to_id: Option<String>,
}

#[derive(Serialize)]
pub struct PersistReadPayload {
    pub room_key: String,
    pub user_id: String,
    pub last_read_at: String,
}

#[derive(Serialize)]
pub struct PersistReactionPayload {
    pub message_id: String,
    pub emoji: String,
    pub user_id: String,
}

pub async fn post_json<T: Serialize>(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    payload: &T,
) -> anyhow::Result<()> {
    let response = client
        .post(url)
        .header("X-Chat-Server-Token", token)
        .json(payload)
        .send()
        .await?;

    response.error_for_status()?;
    Ok(())
}

pub async fn post_json_with_response<T: Serialize, R: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    payload: &T,
) -> anyhow::Result<R> {
    let response = client
        .post(url)
        .header("X-Chat-Server-Token", token)
        .json(payload)
        .send()
        .await?;

    let response = response.error_for_status()?;
    let data = response.json::<R>().await?;
    Ok(data)
}

pub async fn patch_json<T: Serialize>(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    payload: &T,
) -> anyhow::Result<()> {
    let response = client
        .patch(url)
        .header("X-Chat-Server-Token", token)
        .json(payload)
        .send()
        .await?;

    response.error_for_status()?;
    Ok(())
}

pub async fn delete_json(
    client: &reqwest::Client,
    url: &str,
    token: &str,
) -> anyhow::Result<()> {
    let response = client
        .delete(url)
        .header("X-Chat-Server-Token", token)
        .send()
        .await?;

    response.error_for_status()?;
    Ok(())
}
