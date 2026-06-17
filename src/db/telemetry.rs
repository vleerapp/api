use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::models::telemetry::{DistributionPoint, TelemetrySubmission, TimeSeriesPoint};

pub async fn insert_submission(
    pool: &PgPool,
    payload: &TelemetrySubmission,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO telemetry (user_id, app_version, os, song_count, time)
        VALUES ($1, $2, $3, $4, NOW())
        "#,
    )
    .bind(payload.user_id)
    .bind(&payload.app_version)
    .bind(payload.os.as_str())
    .bind(payload.song_count)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn daily_submission_count(pool: &PgPool, user_id: Uuid) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COUNT(*)::BIGINT FROM telemetry WHERE user_id = $1 AND time >= date_trunc('day', NOW())",
    )
    .bind(user_id)
    .fetch_one(pool)
    .await
}

pub struct LastSubmission {
    pub song_count: i64,
    pub os: String,
}

pub async fn last_submission(
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Option<LastSubmission>, sqlx::Error> {
    sqlx::query_as!(
        LastSubmission,
        "SELECT song_count, os FROM telemetry WHERE user_id = $1 ORDER BY time DESC LIMIT 1",
        user_id
    )
    .fetch_optional(pool)
    .await
}

pub async fn earliest_time(pool: &PgPool) -> Result<Option<OffsetDateTime>, sqlx::Error> {
    sqlx::query_scalar("SELECT MIN(time) FROM telemetry")
        .fetch_one(pool)
        .await
}

pub async fn songs_over_time(
    pool: &PgPool,
    start: OffsetDateTime,
    end: OffsetDateTime,
    interval: String,
) -> Result<Vec<TimeSeriesPoint>, sqlx::Error> {
    sqlx::query_as::<_, TimeSeriesPoint>(
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
        -- Get all telemetry data in range ordered by time
        ordered_telemetry AS (
            SELECT
                time,
                user_id,
                song_count::FLOAT8 as song_count,
                time_bucket($3::INTERVAL, time) as bucket
            FROM telemetry
            WHERE time >= $1 AND time <= $2
            ORDER BY user_id, time
        ),
        -- Calculate deltas from previous row or baseline
        deltas AS (
            SELECT
                bucket,
                user_id,
                song_count,
                song_count - COALESCE(
                    LAG(song_count) OVER (PARTITION BY user_id ORDER BY time),
                    (SELECT b.last_val FROM baseline b WHERE b.user_id = ordered_telemetry.user_id),
                    0
                ) as delta
            FROM ordered_telemetry
        ),
        -- Sum all deltas per bucket (this gives us the change in total songs)
        bucket_changes AS (
            SELECT
                bucket,
                SUM(delta) as bucket_delta
            FROM deltas
            GROUP BY bucket
        ),
        -- Calculate cumulative total
        cumulative AS (
            SELECT
                bucket,
                (SELECT total FROM baseline_total) +
                SUM(bucket_delta) OVER (ORDER BY bucket ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
            FROM bucket_changes
        ),
        gapfilled AS (
            SELECT
                time_bucket_gapfill($3::INTERVAL, bucket, $1::TIMESTAMPTZ, $2::TIMESTAMPTZ) as gf_bucket,
                interpolate(value) as gf_value
            FROM cumulative
        ),
        all_points AS (
            SELECT
                COALESCE(g.gf_bucket, c.bucket) as bucket,
                COALESCE(g.gf_value, c.value, (SELECT total FROM baseline_total)) as value
            FROM gapfilled g
            FULL OUTER JOIN cumulative c ON g.gf_bucket = c.bucket
            WHERE COALESCE(g.gf_bucket, c.bucket) IS NOT NULL
        ),
        -- Only keep points where value changed from previous point
        changes_only AS (
            SELECT
                bucket,
                value,
                LAG(value) OVER (ORDER BY bucket) as prev_value
            FROM all_points
        )
        SELECT bucket, value FROM changes_only
        WHERE prev_value IS NULL OR value != prev_value
        ORDER BY bucket ASC
        "#,
    )
    .bind(start)
    .bind(end)
    .bind(interval)
    .fetch_all(pool)
    .await
}

pub async fn users_over_time(
    pool: &PgPool,
    start: OffsetDateTime,
    end: OffsetDateTime,
    interval: String,
) -> Result<Vec<TimeSeriesPoint>, sqlx::Error> {
    sqlx::query_as::<_, TimeSeriesPoint>(
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
    .bind(interval)
    .fetch_all(pool)
    .await
}

pub async fn os_distribution(pool: &PgPool) -> Result<Vec<DistributionPoint>, sqlx::Error> {
    sqlx::query_as::<_, DistributionPoint>(
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
    .fetch_all(pool)
    .await
}

pub async fn version_distribution(pool: &PgPool) -> Result<Vec<DistributionPoint>, sqlx::Error> {
    sqlx::query_as::<_, DistributionPoint>(
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
    .fetch_all(pool)
    .await
}
