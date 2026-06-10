use crate::manticore::SearchClient;
use axum::Router;
use sqlx::PgPool;
use std::sync::Arc;

pub mod v1;

pub fn router(search_client: Arc<SearchClient>, scrape_pool: PgPool) -> Router {
    Router::new().nest("/v1", v1::router(search_client, scrape_pool))
}
