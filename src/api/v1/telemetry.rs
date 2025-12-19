use axum::{
    Json, Router,
    extract::{Query, State},
    routing::{get, post},
};
use sqlx::PgPool;
use time::OffsetDateTime;
use tracing::{debug, error};

use crate::{
    api::validation::ValidatedJson,
    models::telemetry::{DistributionPoint, StatsQuery, TelemetrySubmission, TimeSeriesPoint},
};

pub fn router() -> Router<PgPool> {
    Router::new()
        .route("/", post(submit_telemetry))
        .route("/songs_over_time", get(get_songs_over_time))
        .route("/users_over_time", get(get_users_over_time))
        .route("/distribution/os", get(get_os_distribution))
        .route("/distribution/version", get(get_version_distribution))
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

async fn get_songs_over_time(
    State(pool): State<PgPool>,
    Query(params): Query<StatsQuery>,
) -> Result<Json<Vec<TimeSeriesPoint>>, axum::http::StatusCode> {
    let start = params
        .from
        .unwrap_or_else(|| OffsetDateTime::now_utc() - time::Duration::days(30));

    let initial_val: f64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(song_count), 0)::FLOAT8 FROM telemetry WHERE time < $1",
    )
    .bind(start)
    .fetch_one(&pool)
    .await
    .unwrap_or(0.0);

    let raw_points = sqlx::query_as::<_, TimeSeriesPoint>(
        r#"
        SELECT 
            time AS bucket, 
            CAST(song_count AS FLOAT8) AS value 
        FROM telemetry 
        WHERE time >= $1 
        ORDER BY time ASC
        "#,
    )
    .bind(start)
    .fetch_all(&pool)
    .await
    .map_err(|e| {
        error!("songs db error: {}", e);
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let mut final_points = Vec::new();

    final_points.push(TimeSeriesPoint {
        bucket: start,
        value: initial_val,
    });

    let mut running_total = initial_val;
    for p in raw_points {
        running_total += p.value;
        final_points.push(TimeSeriesPoint {
            bucket: p.bucket,
            value: running_total,
        });
    }

    final_points.push(TimeSeriesPoint {
        bucket: OffsetDateTime::now_utc(),
        value: running_total,
    });

    Ok(Json(final_points))
}

async fn get_users_over_time(
    State(pool): State<PgPool>,
    Query(params): Query<StatsQuery>,
) -> Result<Json<Vec<TimeSeriesPoint>>, axum::http::StatusCode> {
    let start = params
        .from
        .unwrap_or_else(|| OffsetDateTime::now_utc() - time::Duration::days(30));

    let initial_count: f64 =
        sqlx::query_scalar("SELECT COUNT(*)::FLOAT8 FROM telemetry WHERE time < $1")
            .bind(start)
            .fetch_one(&pool)
            .await
            .unwrap_or(0.0);

    let raw_points = sqlx::query_as::<_, TimeSeriesPoint>(
        r#"
        SELECT time AS bucket, 1.0::FLOAT8 AS value 
        FROM telemetry WHERE time >= $1 ORDER BY time ASC
        "#,
    )
    .bind(start)
    .fetch_all(&pool)
    .await
    .map_err(|e| {
        error!("users db error: {}", e);
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let mut final_points = Vec::new();

    final_points.push(TimeSeriesPoint {
        bucket: start,
        value: initial_count,
    });

    let mut running_total = initial_count;
    for p in raw_points {
        running_total += 1.0;
        final_points.push(TimeSeriesPoint {
            bucket: p.bucket,
            value: running_total,
        });
    }

    final_points.push(TimeSeriesPoint {
        bucket: OffsetDateTime::now_utc(),
        value: running_total,
    });

    Ok(Json(final_points))
}

async fn get_os_distribution(
    State(pool): State<PgPool>,
    Query(_): Query<StatsQuery>,
) -> Result<Json<Vec<DistributionPoint>>, axum::http::StatusCode> {
    let stats = sqlx::query_as::<_, DistributionPoint>(
        r#"
        SELECT os AS label, COUNT(DISTINCT user_id) AS count
        FROM telemetry
        -- REMOVED: WHERE time >= $1
        GROUP BY os
        ORDER BY count DESC
        "#,
    )
    .fetch_all(&pool)
    .await
    .map_err(|e| {
        error!("os stats error: {}", e);
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(stats))
}

async fn get_version_distribution(
    State(pool): State<PgPool>,
    Query(_): Query<StatsQuery>,
) -> Result<Json<Vec<DistributionPoint>>, axum::http::StatusCode> {
    let stats = sqlx::query_as::<_, DistributionPoint>(
        r#"
        SELECT app_version AS label, COUNT(DISTINCT user_id) AS count
        FROM telemetry
        -- REMOVED: WHERE time >= $1
        GROUP BY app_version
        ORDER BY count DESC
        "#,
    )
    .fetch_all(&pool)
    .await
    .map_err(|e| {
        error!("version stats error: {}", e);
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(stats))
}
