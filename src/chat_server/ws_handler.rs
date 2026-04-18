use actix_web::{web, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use crate::state::AppState;

use super::auth::resolve_chat_context;
use super::session::ChatSession;
use url::form_urlencoded;

pub async fn ws_route(
    req: HttpRequest,
    stream: web::Payload,
    state: web::Data<AppState>,
) -> Result<HttpResponse, actix_web::Error> {
    let cookie_header = req
        .headers()
        .get("cookie")
        .and_then(|value| value.to_str().ok());
    let mut auth_header = req
        .headers()
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string());

    let mut token_param: Option<String> = None;
    for (key, value) in form_urlencoded::parse(req.query_string().as_bytes()) {
        if key == "token" {
            token_param = Some(value.into_owned());
            break;
        }
    }

    if let Some(token) = token_param {
        auth_header = Some(format!("Bearer {}", token));
    }

    let chat_context = match resolve_chat_context(
        &state.django_base_url,
        cookie_header,
        auth_header.as_deref(),
    )
    .await
    {
        Ok(ctx) => ctx,
        Err(_) => return Ok(HttpResponse::Unauthorized().finish()),
    };

    let session = ChatSession {
        user_id: chat_context.user_id,
        server_addr: state.chat_server.clone(), 
        initial_rooms: chat_context.section_rooms,
    };

    ws::start(session, &req, stream)
}
