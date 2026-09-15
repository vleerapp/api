use axum::{Router, body::Body, extract::Request, routing::any};
use sqlx::PgPool;

pub mod downloads;
pub mod metadata;
pub mod telemetry;
pub mod update;
pub mod validation;

pub fn app_router(pool: PgPool, scrape_pool: Option<PgPool>) -> Router {
    let mut router = Router::new()
        .nest("/telemetry", telemetry::router().with_state(pool))
        .nest("/update", update::router())
        .nest("/downloads", downloads::router())
        .route("/", any(|_: Request<Body>| async { "Healthy" }));

    if let Some(pool) = scrape_pool {
        router = router.nest("/metadata", metadata::router(pool));
    }

    router
}
