mod api;
mod db;
mod manticore;
mod models;
mod rate_limit;
mod search;

use crate::rate_limit::rate_limit;
use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::http::{HeaderValue, Method, header};
use std::net::SocketAddr;
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

    let metadata_enabled = std::env::var("METADATA_ENABLED")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "off" | "no"))
        .unwrap_or(true);
    if !metadata_enabled {
        warn!("METADATA_ENABLED is off, /metadata answers 503 and the scrape database is not touched");
    }

    let scrape_db_url = std::env::var("SCRAPE_DATABASE_URL").unwrap_or_else(|_| {
        tracing::warn!("SCRAPE_DATABASE_URL not set, falling back to localhost:5432");
        "postgres://postgres:postgres@localhost:5432/apple_music_scrape".to_string()
    });
    let scrape_pool = match metadata_enabled.then(|| scrape_db_url.parse::<sqlx::postgres::PgConnectOptions>()) {
        None => None,
        Some(Ok(opts)) => {
            let probe = opts.clone();
            tokio::spawn(async move {
                use sqlx::Connection;
                if let Err(e) = sqlx::postgres::PgConnection::connect_with(&probe).await {
                    warn!("scrape database unreachable: {}", e);
                }
            });
            info!("scrape database pool created (lazy)");
            Some(
                sqlx::postgres::PgPoolOptions::new()
                    .max_connections(8)
                    .acquire_timeout(std::time::Duration::from_secs(5))
                    .connect_lazy_with(opts.options([
                        ("statement_timeout", "10000"),
                        ("client_min_messages", "error"),
                    ])),
            )
        }
        Some(Err(e)) => {
            warn!(
                "scrape database unavailable, metadata endpoints will be disabled: {}",
                e
            );
            None
        }
    };

    let manticore = std::env::var("MANTICORE_URL")
        .ok()
        .filter(|u| metadata_enabled && !u.trim().is_empty())
        .and_then(|url| match manticore::Manticore::new(&url) {
            Ok(m) => {
                info!("search backend: manticore at {}", url);
                Some(std::sync::Arc::new(m))
            }
            Err(e) => {
                warn!("manticore unavailable, using postgres search: {}", e);
                None
            }
        });

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
        .merge(api::app_router(pool, scrape_pool, manticore))
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
