pub mod metadata;

use crate::{api::metadata::v1::metadata::SearchState, elasticsearch::SearchClient};
use axum::Router;
use sqlx::PgPool;
use std::sync::Arc;

pub fn router(search_client: Arc<SearchClient>, scrape_pool: PgPool) -> Router {
    let search_state = SearchState {
        client: search_client,
        scrape_pool,
    };

    metadata::router().with_state(search_state)
}
