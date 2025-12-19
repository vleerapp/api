use axum::{Json, Router, extract::State, routing::post};
use serde::Deserialize;
use sqlx::PgPool;
use tracing::{debug, error};
use uuid::Uuid;

#[derive(Deserialize)]
pub struct TelemetryPayload {
    pub user_id: Uuid,
    pub app_version: String,
    pub os_name: String,
    pub song_count: i64,
}

pub fn router() -> Router<PgPool> {
    Router::new().route("/", post(submit_telemetry))
}

async fn submit_telemetry(
    State(pool): State<PgPool>,
    Json(payload): Json<TelemetryPayload>,
) -> axum::http::StatusCode {
    debug!(
        user_id = %payload.user_id,
        version = %payload.app_version,
        "Processing telemetry submission"
    );

    let result = sqlx::query!(
        r#"
        INSERT INTO telemetry (user_id, app_version, os_name, song_count)
        VALUES ($1, $2, $3, $4)
        "#,
        payload.user_id,
        payload.app_version,
        payload.os_name,
        payload.song_count
    )
    .execute(&pool)
    .await;

    match result {
        Ok(_) => {
            debug!("Telemetry saved");
            axum::http::StatusCode::OK
        }
        Err(e) => {
            error!("Failed to insert telemetry: {}", e);
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}
