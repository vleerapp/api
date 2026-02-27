use crate::manticore::SearchClient;
use axum::{Router, body::Body, extract::Request, routing::any};
use sqlx::PgPool;
use std::sync::Arc;

pub mod metadata;
pub mod telemetry;
pub mod validation;

pub fn app_router(search_client: Arc<SearchClient>, pool: PgPool, scrape_pool: Option<PgPool>) -> Router {
    let mut router = Router::new()
        .nest("/telemetry", telemetry::router().with_state(pool))
        .route("/", any(|_: Request<Body>| async { "Healthy" }));
    
    if let Some(pool) = scrape_pool {
        router = router.nest("/metadata", metadata::router(search_client, pool));
    }
    
    router
}
