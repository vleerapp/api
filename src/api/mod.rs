use axum::Router;
use sqlx::PgPool;

mod telemetry;

pub fn app_router() -> Router<PgPool> {
    Router::new().nest("/telemetry", telemetry::router())
}
