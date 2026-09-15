use axum::Router;
use sqlx::PgPool;

pub mod v1;

pub fn router(scrape_pool: PgPool) -> Router {
    Router::new().nest("/v1", v1::router(scrape_pool))
}
