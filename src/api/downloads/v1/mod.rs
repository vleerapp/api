pub mod downloads;

use axum::Router;

pub fn router() -> Router {
    downloads::router()
}
