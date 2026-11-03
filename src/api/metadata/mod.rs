use crate::index::SearchIndex;
use axum::Router;
use sqlx::PgPool;
use std::sync::Arc;

pub mod v1;

pub fn router(scrape_pool: PgPool, index: Option<Arc<SearchIndex>>) -> Router {
    Router::new().nest("/v1", v1::router(scrape_pool, index))
}
