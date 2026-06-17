pub mod update;

use axum::Router;

pub fn router() -> Router {
    update::router()
}
