use actix::Actor;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::{App, HttpServer};
use tracing::info;

use crate::chat_server::server::ChatServer;
use crate::config::server::ServerConfig;
use crate::state::AppState;
use crate::chat_server::routes::chat_routes;

pub async fn run_server() -> std::io::Result<()> {
    let server_config = ServerConfig::from_env();
    let django_base_url = std::env::var("DJANGO_API_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:8000".to_string());
    let chat_server_token = std::env::var("CHAT_SERVER_TOKEN").unwrap_or_default();
    let chat_server = ChatServer::new(django_base_url.clone(), chat_server_token.clone()).start();

    info!("Initializing application state...");
    info!(
        "Starting server on {}:{}",
        server_config.host, server_config.port
    );

    // Wrap in Data here so we clone cheaply inside the closure (Arc under the hood)
    let app_state = Data::new(AppState::new(
        chat_server,
        django_base_url,
        chat_server_token,
    ));

    HttpServer::new(move || {
        App::new()
            .app_data(app_state.clone())
            .wrap(Logger::default())
            .configure(chat_routes)
    })
    .bind(server_config.address())?
    .run()
    .await
}
