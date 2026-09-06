use axum::Router;

pub mod v1;

pub fn router() -> Router {
    v1::router()
}
