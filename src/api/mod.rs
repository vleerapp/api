use axum::{Router, body::Body, extract::Request, routing::any};
use sqlx::PgPool;
use std::sync::Arc;
use crate::search::SearchClient;

pub mod v1;
pub mod validation;

pub fn app_router(search_client: Arc<SearchClient>) -> Router<PgPool> {
    Router::new()
        .nest("/v1", v1::router(search_client))
        .route("/", any(|_: Request<Body>| async { "Healthy" }))
}
