mod app;
mod state;
mod config;
mod chat_server;

use crate::app::run_server;
use crate::config::environment::init_environment;
use tracing_subscriber::EnvFilter;


#[actix_web::main]
async fn main() -> std::io::Result<()> {
  
    init_environment();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
        .init();
    run_server().await //This is the main entry for running your application
}
