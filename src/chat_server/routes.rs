use actix_web::web::{self, ServiceConfig};

use crate::{chat_server::ws_handler::{ ws_route}};


pub fn chat_routes(cfg: &mut ServiceConfig) {
    cfg.service(
    web::scope("/ws/chat")

        .route("", web::get().to(ws_route))
        .route("/", web::get().to(ws_route))

    );
}