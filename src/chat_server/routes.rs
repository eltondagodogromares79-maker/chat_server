use actix_web::web::{self, ServiceConfig};

use crate::chat_server::{
    internal_notifications::{publish_notification, publish_notifications_bulk},
    ws_handler::{notifications_ws_route, ws_route},
};


pub fn chat_routes(cfg: &mut ServiceConfig) {
    cfg.service(
    web::scope("/ws/chat")

        .route("", web::get().to(ws_route))
        .route("/", web::get().to(ws_route))

    )
    .service(
    web::scope("/ws/notifications")

        .route("", web::get().to(notifications_ws_route))
        .route("/", web::get().to(notifications_ws_route))

    )
    .service(
    web::scope("/internal/notifications")

        .route("", web::post().to(publish_notification))
        .route("/", web::post().to(publish_notification))

    )
    .service(
    web::scope("/internal/notifications/bulk")

        .route("", web::post().to(publish_notifications_bulk))
        .route("/", web::post().to(publish_notifications_bulk))

    );
}
