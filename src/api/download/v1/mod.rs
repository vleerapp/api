pub mod download;

use axum::Router;

pub fn router() -> Router {
    download::router()
}
