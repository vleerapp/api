use axum::Router;
use sqlx::PgPool;

pub mod v1;

pub fn router() -> Router<PgPool> {
    Router::new()
        .nest("/v1", v1::router())
}
