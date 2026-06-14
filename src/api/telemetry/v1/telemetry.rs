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
    db,
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
    debug!(user_id = %payload.user_id, "receiving telemetry");

    match db::telemetry::insert_submission(&pool, &payload).await {
        Ok(_) => axum::http::StatusCode::OK,
        Err(e) => {
            error!("telemetry insert error: {}", e);
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn resolve_time_range(
    pool: &PgPool,
    from: Option<OffsetDateTime>,
    to: Option<OffsetDateTime>,
) -> Result<(OffsetDateTime, OffsetDateTime), axum::http::StatusCode> {
    let end = to.unwrap_or_else(OffsetDateTime::now_utc);
    let start = match from {
        Some(t) => t,
        None => {
            let min = db::telemetry::earliest_time(pool).await.map_err(|e| {
                error!("min time query error: {}", e);
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            })?;
            min.unwrap_or(end)
        }
    };
    Ok((start, end))
}

async fn get_songs_over_time(
    State(pool): State<PgPool>,
    Query(params): Query<StatsQuery>,
) -> Result<Json<Vec<TimeSeriesPoint>>, axum::http::StatusCode> {
    let (start, end) = resolve_time_range(&pool, params.from, params.to).await?;

    let interval = format!("{} seconds", calculate_bucket_interval(&start, &end));

    let points = db::telemetry::songs_over_time(&pool, start, end, interval)
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
    let (start, end) = resolve_time_range(&pool, params.from, params.to).await?;

    let interval = format!("{} seconds", calculate_bucket_interval(&start, &end));

    let points = db::telemetry::users_over_time(&pool, start, end, interval)
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
    let stats = db::telemetry::os_distribution(&pool).await.map_err(|e| {
        error!("os stats error: {}", e);
        axum::http::StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(stats))
}

async fn get_version_distribution(
    State(pool): State<PgPool>,
    Query(_): Query<StatsQuery>,
) -> Result<Json<Vec<DistributionPoint>>, axum::http::StatusCode> {
    let stats = db::telemetry::version_distribution(&pool)
        .await
        .map_err(|e| {
            error!("version stats error: {}", e);
            axum::http::StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(Json(stats))
}

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
