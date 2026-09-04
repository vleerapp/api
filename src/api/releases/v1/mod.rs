pub mod releases;

use axum::Router;

pub fn router() -> Router {
    releases::router()
}
