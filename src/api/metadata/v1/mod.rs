pub mod metadata;
pub mod resource;

use crate::api::metadata::v1::metadata::SearchState;
use axum::Router;
use sqlx::PgPool;

pub fn router(scrape_pool: PgPool) -> Router {
    metadata::router().with_state(SearchState { scrape_pool })
}
