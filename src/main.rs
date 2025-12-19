mod api;
mod db;
mod models;

use axum::Router;
use tokio::net::TcpListener;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting Vleer API...");

    let pool = match db::create_pool().await {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to initialize database: {}", e);
            std::process::exit(1);
        }
    };

    info!("Database initialized and migrations applied.");

    let app = Router::new().merge(api::app_router()).with_state(pool);

    let listener = TcpListener::bind("0.0.0.0:3000").await.unwrap();
    info!("Server listening on http://0.0.0.0:3000");
    axum::serve(listener, app).await.unwrap();
}
