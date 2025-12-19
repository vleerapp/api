use axum::{
    Json, Router,
    extract::{Query, State},
    routing::post,
};
use serde::Deserialize;
use sqlx::PgPool;
use time::OffsetDateTime;
use tracing::{debug, error};

use crate::{
    api::validation::ValidatedJson,
    models::telemetry::{TelemetryStat, TelemetrySubmission},
};

pub fn router() -> Router<PgPool> {
    Router::new().route("/", post(submit_telemetry).get(get_telemetry_stats))
}

async fn submit_telemetry(
    State(pool): State<PgPool>,
    ValidatedJson(payload): ValidatedJson<TelemetrySubmission>,
) -> axum::http::StatusCode {
    debug!(user_id = %payload.user_id, "v1: Receiving telemetry");

    let result = sqlx::query(
        r#"
        INSERT INTO telemetry (user_id, app_version, os, song_count)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(payload.user_id)
    .bind(payload.app_version)
    .bind(payload.os.as_str())
    .bind(payload.song_count)
    .execute(&pool)
    .await;

    match result {
        Ok(_) => axum::http::StatusCode::OK,
        Err(e) => {
            error!("v1 insert error: {}", e);
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

#[derive(Deserialize)]
pub struct StatsQuery {
    #[serde(with = "time::serde::rfc3339::option")]
    pub from: Option<OffsetDateTime>,
}

async fn get_telemetry_stats(
    State(pool): State<PgPool>,
    Query(params): Query<StatsQuery>,
) -> Result<Json<Vec<TelemetryStat>>, axum::http::StatusCode> {
    let start_date = params
        .from
        .unwrap_or_else(|| OffsetDateTime::now_utc() - time::Duration::days(7));

    let stats = sqlx::query_as::<_, TelemetryStat>(
        r#"
        SELECT 
            time_bucket('1 day', time) AS bucket,
            os,
            CAST(AVG(song_count) AS FLOAT8) AS avg_songs,
            COUNT(DISTINCT user_id) AS user_count
        FROM telemetry 
        WHERE time >= $1
        GROUP BY time_bucket('1 day', time), os
        ORDER BY 1 ASC
        "#,
    )
    .bind(start_date)
    .fetch_all(&pool)
    .await
    .map_err(|e| {
        error!("v1 stats error: {}", e);
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(stats))
}
