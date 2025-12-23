use axum::{Router, body::Body, extract::Request, routing::any};
use sqlx::PgPool;

pub mod v1;
pub mod validation;

pub fn app_router() -> Router<PgPool> {
    Router::new()
        .nest("/v1", v1::router())
        .route("/", any(|_: Request<Body>| async { "Healthy" }))
}
