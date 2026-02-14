use axum::Router;
use sqlx::PgPool;
use std::sync::Arc;
use crate::search::SearchClient;

pub mod search;
pub mod telemetry;

pub fn router(search_client: Arc<SearchClient>) -> Router<PgPool> {
    Router::new()
        .nest("/search", search::router().with_state(search_client))
        .nest("/telemetry", telemetry::router())
}
