use axum::{
    Json, Router,
    body::Body,
    extract::Request,
    http::{StatusCode, header},
    response::IntoResponse,
    routing::any,
};
use serde_json::json;
use crate::manticore::Manticore;
use sqlx::PgPool;
use std::sync::Arc;

pub mod download;
pub mod metadata;
pub mod telemetry;
pub mod update;
pub mod validation;

async fn metadata_unavailable() -> impl IntoResponse {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        [(header::RETRY_AFTER, "600")],
        Json(json!({ "error": { "status": 503, "message": "metadata is temporarily unavailable" } })),
    )
}

pub fn app_router(
    pool: PgPool,
    scrape_pool: Option<PgPool>,
    manticore: Option<Arc<Manticore>>,
) -> Router {
    let mut router = Router::new()
        .nest("/telemetry", telemetry::router().with_state(pool))
        .nest("/update", update::router())
        .nest("/download", download::router())
        .route("/", any(|_: Request<Body>| async { "Healthy" }));

    router = match scrape_pool {
        Some(pool) => router.nest("/metadata", metadata::router(pool, manticore)),
        None => router.nest("/metadata", Router::new().fallback(metadata_unavailable)),
    };

    router
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[tokio::test]
    async fn metadata_answers_503_while_the_rest_stays_up() {
        let pool = PgPool::connect_lazy("postgres://x@localhost/x").unwrap();
        let app = app_router(pool, None, None);

        let res = app
            .clone()
            .oneshot(Request::get("/metadata/v1/stats").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(res.headers()[header::RETRY_AFTER], "600");

        let res = app
            .oneshot(Request::get("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
    }
}
