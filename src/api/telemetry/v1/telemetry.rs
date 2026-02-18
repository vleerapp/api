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

fn calculate_bucket_interval(from: &OffsetDateTime, to: &OffsetDateTime) -> i64 {
    let duration_secs = (to.unix_timestamp() - from.unix_timestamp()).max(1);

    let target_points = 150;
    let interval_secs = duration_secs / target_points;

    if interval_secs < 60 {
        if interval_secs < 20 { 10 } else { 30 }
    } else if interval_secs < 3600 {
        if interval_secs < 180 {
            60
        } else if interval_secs < 450 {
            300
        } else if interval_secs < 750 {
            600
        } else if interval_secs < 1350 {
            900
        } else {
            1800
        }
    } else if interval_secs < 86400 {
        if interval_secs < 5400 {
            3600
        } else if interval_secs < 10800 {
            7200
        } else if interval_secs < 18000 {
            10800
        } else if interval_secs < 32400 {
            21600
        } else {
            43200
        }
    } else {
        if interval_secs < 432000 {
            86400
        } else {
            604800
        }
    }
}

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
    let end = params.to.unwrap_or_else(|| OffsetDateTime::now_utc());

    // Calculate interval based on time range (Grafana-friendly bucketing)
    let interval_secs = calculate_bucket_interval(&start, &end);

    let points = sqlx::query_as::<_, TimeSeriesPoint>(
        r#"
        WITH baseline AS (
            -- Get the last known song count for each user before the time range
            SELECT DISTINCT ON (user_id)
                user_id,
                song_count::FLOAT8 as last_val
            FROM telemetry
            WHERE time < $1
            ORDER BY user_id, time DESC
        ),
        baseline_total AS (
            SELECT COALESCE(SUM(last_val), 0)::FLOAT8 as total
            FROM baseline
        ),
        bucketed AS (
            -- Aggregate telemetry into time buckets using TimescaleDB time_bucket
            SELECT 
                time_bucket($3::INTERVAL, time) as bucket,
                user_id,
                last(song_count, time)::FLOAT8 as song_count
            FROM telemetry
            WHERE time >= $1 AND time <= $2
            GROUP BY bucket, user_id
        ),
        bucket_totals AS (
            -- Sum across all users per bucket
            SELECT 
                bucket,
                SUM(song_count) as bucket_total
            FROM bucketed
            GROUP BY bucket
        ),
        gapfilled AS (
            -- Use time_bucket_gapfill for continuous time series (Grafana needs this)
            SELECT 
                time_bucket_gapfill($3::INTERVAL, bucket, $1::TIMESTAMPTZ, $2::TIMESTAMPTZ) as gf_bucket,
                interpolate(bucket_total) as gf_value
            FROM bucket_totals
        )
        SELECT 
            COALESCE(g.gf_bucket, bt.bucket) as bucket,
            COALESCE(g.gf_value, bt.bucket_total, (SELECT total FROM baseline_total)) as value
        FROM gapfilled g
        FULL OUTER JOIN bucket_totals bt ON g.gf_bucket = bt.bucket
        ORDER BY bucket ASC
        "#,
    )
    .bind(start)
    .bind(end)
    .bind(format!("{} seconds", interval_secs))
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
    let end = params.to.unwrap_or_else(|| OffsetDateTime::now_utc());

    let interval_secs = calculate_bucket_interval(&start, &end);

    let points = sqlx::query_as::<_, TimeSeriesPoint>(
        r#"
        WITH baseline AS (
            -- Count users seen before the time range
            SELECT COUNT(DISTINCT user_id)::FLOAT8 as initial_count
            FROM telemetry
            WHERE time < $1
        ),
        first_seen_per_user AS (
            -- Find when each user first appeared
            SELECT 
                user_id,
                MIN(time) as first_seen
            FROM telemetry
            GROUP BY user_id
        ),
        bucketed_users AS (
            -- Bucket users by their first seen time
            SELECT 
                time_bucket($3::INTERVAL, first_seen) as bucket,
                COUNT(*)::FLOAT8 as new_users
            FROM first_seen_per_user
            WHERE first_seen >= $1 AND first_seen <= $2
            GROUP BY bucket
        ),
        cumulative AS (
            -- Calculate running total using TimescaleDB's ordered aggregation
            SELECT 
                bucket,
                (SELECT initial_count FROM baseline) + 
                SUM(new_users) OVER (ORDER BY bucket ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
            FROM bucketed_users
        ),
        gapfilled AS (
            -- Gapfill for continuous time series
            SELECT 
                time_bucket_gapfill($3::INTERVAL, bucket, $1::TIMESTAMPTZ, $2::TIMESTAMPTZ) as gf_bucket,
                interpolate(value) as gf_value
            FROM cumulative
        )
        SELECT 
            COALESCE(g.gf_bucket, c.bucket) as bucket,
            COALESCE(g.gf_value, c.value, (SELECT initial_count FROM baseline)) as value
        FROM gapfilled g
        FULL OUTER JOIN cumulative c ON g.gf_bucket = c.bucket
        ORDER BY bucket ASC
        "#,
    )
    .bind(start)
    .bind(end)
    .bind(format!("{} seconds", interval_secs))
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
