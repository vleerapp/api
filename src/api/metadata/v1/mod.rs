pub mod metadata;
pub mod resource;

use crate::api::metadata::v1::metadata::SearchState;
use crate::manticore::Manticore;
use axum::Router;
use sqlx::PgPool;
use std::sync::Arc;

pub fn router(scrape_pool: PgPool, manticore: Option<Arc<Manticore>>) -> Router {
    metadata::router().with_state(SearchState {
        scrape_pool,
        manticore,
    })
}
