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
    rate_limit::rate_limit,
};

pub fn router() -> Router<PgPool> {
    let ingest_routes = Router::new()
        .route("/", post(submit_telemetry))
        .layer(rate_limit(1, 2000));

    let dashboard_routes = Router::new()
        .route("/songs_over_time", get(get_songs_over_time))
        .route("/users_over_time", get(get_users_over_time))
        .route("/distribution/os", get(get_os_distribution))
        .route("/distribution/version", get(get_version_distribution))
        .layer(rate_limit(20, 1000));

    Router::new().merge(ingest_routes).merge(dashboard_routes)
}

async fn submit_telemetry(
    State(pool): State<PgPool>,
    ValidatedJson(payload): ValidatedJson<TelemetrySubmission>,
) -> axum::http::StatusCode {
    debug!(user_id = %payload.user_id, "v1: Receiving telemetry");

    let result = sqlx::query(
        r#"
        INSERT INTO telemetry (user_id, app_version, os, song_count, time)
        VALUES ($1, $2, $3, $4, NOW())
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

    let points = sqlx::query_as::<_, TimeSeriesPoint>(
        r#"
        WITH baseline_state AS (
            SELECT DISTINCT ON (user_id) 
                user_id, 
                song_count::BIGINT as last_val
            FROM telemetry
            WHERE time < $1
            ORDER BY user_id, time DESC
        ),
        initial_global_sum AS (
            SELECT COALESCE(SUM(last_val), 0)::FLOAT8 as total_val 
            FROM baseline_state
        ),
        deltas AS (
            SELECT 
                t.time,
                (t.song_count::BIGINT - COALESCE(
                    LAG(t.song_count::BIGINT) OVER (PARTITION BY t.user_id ORDER BY t.time), 
                    b.last_val, 
                    0
                )) as change
            FROM telemetry t
            LEFT JOIN baseline_state b ON t.user_id = b.user_id
            WHERE t.time >= $1
        ),
        valid_changes AS (
            SELECT 
                time as bucket,
                (SUM(change) OVER (ORDER BY time) + (SELECT total_val FROM initial_global_sum))::FLOAT8 as value
            FROM deltas
            WHERE change > 0
        ),
        final_point AS (
            SELECT value FROM valid_changes ORDER BY bucket DESC LIMIT 1
        )
        SELECT 
            $1 as bucket, 
            (SELECT total_val FROM initial_global_sum) as value 
        UNION ALL
        SELECT bucket, value FROM valid_changes
        UNION ALL
        SELECT 
            NOW() as bucket, 
            COALESCE(
                (SELECT value FROM final_point), 
                (SELECT total_val FROM initial_global_sum)
            ) as value
        ORDER BY bucket ASC
        "#,
    )
    .bind(start)
    .fetch_all(&pool)
    .await
    .map_err(|e| {
        error!("songs db error: {}", e);
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(points))
}

async fn get_users_over_time(
    State(pool): State<PgPool>,
    Query(params): Query<StatsQuery>,
) -> Result<Json<Vec<TimeSeriesPoint>>, axum::http::StatusCode> {
    let start = params
        .from
        .unwrap_or_else(|| OffsetDateTime::now_utc() - time::Duration::days(30));

    let points = sqlx::query_as::<_, TimeSeriesPoint>(
        r#"
        WITH initial_stats AS (
            SELECT COUNT(DISTINCT user_id)::FLOAT8 as initial_count
            FROM telemetry
            WHERE time < $1
        ),
        new_user_events AS (
            SELECT 
                user_id, 
                MIN(time) as first_seen
            FROM telemetry
            GROUP BY user_id
            HAVING MIN(time) >= $1
        ),
        timeline AS (
            SELECT 
                first_seen as bucket,
                ((SELECT initial_count FROM initial_stats) + RANK() OVER (ORDER BY first_seen))::FLOAT8 as value
            FROM new_user_events
        ),
        final_point AS (
            SELECT value FROM timeline ORDER BY bucket DESC LIMIT 1
        )
        SELECT 
            $1 as bucket, 
            (SELECT initial_count FROM initial_stats) as value
        UNION ALL
        SELECT bucket, value FROM timeline
        UNION ALL
        SELECT 
            NOW() as bucket, 
            COALESCE(
                (SELECT value FROM final_point), 
                (SELECT initial_count FROM initial_stats)
            ) as value
        ORDER BY bucket ASC
        "#,
    )
    .bind(start)
    .fetch_all(&pool)
    .await
    .map_err(|e| {
        error!("users db error: {}", e);
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(points))
}

async fn get_os_distribution(
    State(pool): State<PgPool>,
    Query(_): Query<StatsQuery>,
) -> Result<Json<Vec<DistributionPoint>>, axum::http::StatusCode> {
    let stats = sqlx::query_as::<_, DistributionPoint>(
        r#"
        SELECT os AS label, COUNT(*) AS count
        FROM (
            SELECT DISTINCT ON (user_id) os
            FROM telemetry
            ORDER BY user_id, time DESC
        ) latest_states
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
        SELECT app_version AS label, COUNT(*) AS count
        FROM (
            SELECT DISTINCT ON (user_id) app_version
            FROM telemetry
            ORDER BY user_id, time DESC
        ) latest_states
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
