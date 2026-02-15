use crate::elasticsearch::SearchClient;
use axum::{Router, body::Body, extract::Request, routing::any};
use sqlx::PgPool;
use std::sync::Arc;

pub mod metadata;
pub mod telemetry;
pub mod validation;

pub fn app_router(search_client: Arc<SearchClient>, pool: PgPool, scrape_pool: PgPool) -> Router {
    Router::new()
        .nest("/metadata", metadata::router(search_client, scrape_pool))
        .nest("/telemetry", telemetry::router().with_state(pool))
        .route("/", any(|_: Request<Body>| async { "Healthy" }))
}
