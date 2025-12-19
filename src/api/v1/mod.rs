use axum::Router;
use sqlx::PgPool;

pub mod telemetry;

pub fn router() -> Router<PgPool> {
    Router::new()
        .nest("/telemetry", telemetry::router())
}
