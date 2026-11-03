pub mod metadata;
pub mod resource;

use crate::api::metadata::v1::metadata::SearchState;
use crate::index::SearchIndex;
use axum::Router;
use sqlx::PgPool;
use std::sync::Arc;

pub fn router(scrape_pool: PgPool, index: Option<Arc<SearchIndex>>) -> Router {
    metadata::router().with_state(SearchState { scrape_pool, index })
}
