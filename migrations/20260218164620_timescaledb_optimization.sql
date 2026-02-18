CREATE INDEX IF NOT EXISTS telemetry_time_user_idx ON telemetry (time DESC, user_id);
CREATE INDEX IF NOT EXISTS telemetry_user_time_latest_idx ON telemetry (user_id, time DESC) INCLUDE (song_count, app_version, os);
CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_hourly WITH (
    timescaledb.continuous,
    timescaledb.materialized_only = false
) AS
SELECT time_bucket('1 hour', time) as bucket,
    user_id,
    last(song_count, time) as song_count,
    last(app_version, time) as app_version,
    last(os, time) as os
FROM telemetry
GROUP BY bucket,
    user_id WITH NO DATA;
SELECT add_continuous_aggregate_policy(
        'telemetry_hourly',
        start_offset => INTERVAL '1 month',
        end_offset => INTERVAL '1 hour',
        schedule_interval => INTERVAL '1 hour'
    );
CREATE INDEX IF NOT EXISTS telemetry_hourly_bucket_user_idx ON telemetry_hourly (bucket DESC, user_id);
CREATE INDEX IF NOT EXISTS telemetry_hourly_user_bucket_idx ON telemetry_hourly (user_id, bucket DESC);
ALTER TABLE telemetry
SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'user_id'
    );
SELECT add_compression_policy('telemetry', INTERVAL '7 days');