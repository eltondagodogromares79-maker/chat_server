use actix_web::{HttpRequest, HttpResponse, web};
use serde::Deserialize;
use uuid::Uuid;

use crate::state::AppState;

use super::message::InternalNotificationEvent;

#[derive(Deserialize)]
pub struct InternalNotificationRequest {
    pub user_id: Uuid,
    pub payload: serde_json::Value,
}

fn is_authorized(req: &HttpRequest, expected_token: &str) -> bool {
    if expected_token.is_empty() {
        return false;
    }

    let provided = req
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim())
        .and_then(|value| value.strip_prefix("Bearer ").or(Some(value)));

    matches!(provided, Some(token) if token == expected_token)
}

pub async fn publish_notification(
    req: HttpRequest,
    state: web::Data<AppState>,
    payload: web::Json<InternalNotificationRequest>,
) -> HttpResponse {
    if !is_authorized(&req, &state.chat_server_token) {
        return HttpResponse::Unauthorized().finish();
    }

    state.chat_server.do_send(InternalNotificationEvent {
        user_id: payload.user_id,
        payload: payload.payload.to_string(),
    });

    HttpResponse::Accepted().finish()
}
