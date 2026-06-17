mod api;
mod db;
mod manticore;
mod models;
mod rate_limit;

use crate::manticore::SearchClient;
use crate::rate_limit::rate_limit;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderValue, Method, header};
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, CorsLayer};
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

    info!("starting vleer api");

    let pool = match db::create_pool().await {
        Ok(p) => p,
        Err(e) => {
            error!("failed to initialize database: {}", e);
            std::process::exit(1);
        }
    };

    info!("database initialized and migrations applied");

    let scrape_db_url = std::env::var("SCRAPE_DATABASE_URL").unwrap_or_else(|_| {
        "postgres://postgres:postgres@localhost:5432/apple_music_scrape".to_string()
    });
    let scrape_pool = match sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&scrape_db_url)
        .await
    {
        Ok(p) => {
            info!("scrape database pool created");
            Some(p)
        }
        Err(e) => {
            warn!(
                "scrape database unavailable, metadata endpoints will be disabled: {}",
                e
            );
            None
        }
    };

    let es_url =
        std::env::var("MANTICORE_URL").unwrap_or_else(|_| "http://localhost:9308".to_string());
    let search_client = match SearchClient::new(&es_url) {
        Ok(client) => {
            info!("manticore client created, connecting to {}", es_url);
            let client = Arc::new(client);
            if let Err(e) = client.create_index().await {
                error!("failed to create manticore table: {}", e);
            } else {
                match client.count().await {
                    Ok(count) => info!("manticore ready, indexed documents: {}", count),
                    Err(e) => info!("manticore ready, could not get count: {}", e),
                }
            }
            let ping_client = client.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
                loop {
                    interval.tick().await;
                    if let Err(e) = ping_client.ping().await {
                        tracing::warn!("manticore keepalive failed: {}", e);
                    }
                }
            });

            client
        }
        Err(e) => {
            error!("failed to create manticore client: {}", e);
            std::process::exit(1);
        }
    };

    let cors_origins: Vec<HeaderValue> = std::env::var("ALLOWED_ORIGINS")
        .unwrap_or_default()
        .split(',')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() {
                None
            } else {
                s.parse::<HeaderValue>().ok()
            }
        })
        .collect();

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(cors_origins))
        .allow_methods([Method::GET, Method::POST])
        .allow_headers([header::CONTENT_TYPE]);

    let app = Router::new()
        .merge(api::app_router(search_client, pool, scrape_pool))
        .layer(cors)
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(rate_limit(20, 1000));

    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:3000".to_string());
    let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
        Ok(l) => {
            info!("server listening on {}", bind_addr);
            l
        }
        Err(e) => {
            error!("failed to bind to {}: {}", bind_addr, e);
            std::process::exit(1);
        }
    };

    if let Err(e) = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await
    {
        error!("server error: {}", e);
        std::process::exit(1);
    }
}
