CREATE TABLE IF NOT EXISTS telemetry (
  time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  user_id UUID NOT NULL,
  app_version TEXT NOT NULL,
  os TEXT NOT NULL,
  song_count BIGINT NOT NULL
);

SELECT create_hypertable('telemetry', 'time', if_not_exists => TRUE);