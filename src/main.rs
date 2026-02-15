mod api;
mod db;
mod elasticsearch;
mod models;
mod rate_limit;

use crate::elasticsearch::SearchClient;
use crate::rate_limit::rate_limit;
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};
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

    let scrape_db_url = std::env::var("SCRAPE_DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5432/apple_music_scrape".to_string()
    });
    let scrape_pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&scrape_db_url)
        .await
    {
        Ok(p) => {
            info!("Scrape database pool created");
            Some(p)
        }
        Err(e) => {
            warn!("Scrape database unavailable, metadata endpoints will be disabled: {}", e);
            None
        }
    };

    let es_url =
        std::env::var("ELASTICSEARCH_URL").unwrap_or_else(|_| "http://localhost:9200".to_string());
    let search_client = match SearchClient::new(&es_url) {
        Ok(client) => {
            info!("Elasticsearch client created, connecting to: {}", es_url);
            let client = Arc::new(client);
            if let Err(e) = client.create_index().await {
                error!("Failed to create Elasticsearch index: {}", e);
            } else {
                match client.count().await {
                    Ok(count) => info!("Elasticsearch index ready. Indexed documents: {}", count),
                    Err(e) => info!("Elasticsearch index ready. Could not get count: {}", e),
                }
            }
            client
        }
        Err(e) => {
            error!("Failed to create Elasticsearch client: {}", e);
            std::process::exit(1);
        }
    };

    let app = Router::new()
        .merge(api::app_router(search_client, pool, scrape_pool))
        .layer(CorsLayer::permissive())
        .layer(rate_limit(20, 1000));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    info!("Server listening on http://0.0.0.0:3000");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    .unwrap();
}
