use futures::{TryStreamExt, stream};
use indicatif::{HumanCount, HumanDuration, ProgressBar, ProgressState, ProgressStyle};
use sqlx::{PgPool, Row};
use std::io::IsTerminal;
use time::OffsetDateTime;

const ROWS_PER_CHUNK: i64 = 2_000;
const MAX_BLOCK: i64 = u32::MAX as i64;
const SYNC_LAG: &str = "15 minutes";

pub struct Candidate {
    pub id: String,
    pub name: String,
    pub artist: String,
    pub album: String,
}

fn song_select() -> &'static str {
    "SELECT s.id, s.name,
        COALESCE((SELECT string_agg(DISTINCT a.name, ' ')
                  FROM song_artists sa JOIN artists a ON a.id = sa.artist_id
                  WHERE sa.song_id = s.id), ''),
        COALESCE((SELECT string_agg(DISTINCT al.name, ' ')
                  FROM song_albums sal JOIN albums al ON al.id = sal.album_id
                  WHERE sal.song_id = s.id), '')
     FROM songs s"
}

fn album_select() -> &'static str {
    "SELECT al.id, al.name,
        COALESCE((SELECT string_agg(DISTINCT a.name, ' ')
                  FROM artist_albums aa JOIN artists a ON a.id = aa.artist_id
                  WHERE aa.album_id = al.id), '')
     FROM albums al"
}

fn song_upsert(filter: &str) -> String {
    format!(
        "INSERT INTO search_songs (id, name, artist_name, album_name)
         {} WHERE {filter}
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name,
             artist_name = EXCLUDED.artist_name, album_name = EXCLUDED.album_name",
        song_select()
    )
}

fn album_upsert(filter: &str) -> String {
    format!(
        "INSERT INTO search_albums (id, name, artist_name)
         {} WHERE {filter}
         ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, artist_name = EXCLUDED.artist_name",
        album_select()
    )
}

fn match_clause(column: &str, param: usize) -> String {
    format!("({column} ||| ${param}::text OR {column} ||| ${param}::text::pdb.fuzzy(1, f, t))")
}

pub async fn candidates(
    pool: &PgPool,
    item_type: &str,
    name: &str,
    artist: Option<&str>,
    limit: i64,
) -> Result<Vec<Candidate>, sqlx::Error> {
    let (table, artist_col, album_col) = match item_type {
        "song" => ("search_songs", "artist_name", "album_name"),
        "album" => ("search_albums", "artist_name", "''::text"),
        _ => ("artists", "''::text", "''::text"),
    };
    let select = format!(
        "SELECT id, name, {artist_col} AS artist_name, {album_col} AS album_name FROM {table}"
    );
    let by_name = format!(
        "({select} WHERE {} ORDER BY pdb.score(id) DESC LIMIT $2)",
        match_clause("name", 1)
    );

    let artist = artist.filter(|_| artist_col == "artist_name");
    let sql = match artist {
        Some(_) => format!(
            "{by_name} UNION ({select} WHERE {} AND {} ORDER BY pdb.score(id) DESC LIMIT $2)",
            match_clause("name", 1),
            match_clause("artist_name", 3)
        ),
        None => by_name,
    };

    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(name).bind(limit);
    if let Some(a) = artist {
        query = query.bind(a);
    }

    Ok(query
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|r| Candidate {
            id: r.get(0),
            name: r.get(1),
            artist: r.get(2),
            album: r.get(3),
        })
        .collect())
}

pub async fn sync_recent(pool: &PgPool) -> Result<u64, sqlx::Error> {
    let mut tx = pool.begin().await?;

    let locked: bool =
        sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext('search_sync'))")
            .fetch_one(&mut *tx)
            .await?;
    if !locked {
        return Ok(0);
    }

    sqlx::raw_sql("SET LOCAL statement_timeout = 0; SET LOCAL client_min_messages = error")
        .execute(&mut *tx)
        .await?;

    let until: OffsetDateTime = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT now() - interval '{SYNC_LAG}'"
    )))
    .fetch_one(&mut *tx)
    .await?;

    let mut synced = 0;
    for (kind, sql) in [
        (
            "songs",
            song_upsert("s.date_added >= $1 AND s.date_added < $2"),
        ),
        (
            "albums",
            album_upsert("al.date_added >= $1 AND al.date_added < $2"),
        ),
    ] {
        let since: Option<OffsetDateTime> =
            sqlx::query_scalar("SELECT synced_until FROM search_sync_state WHERE item_type = $1")
                .bind(kind)
                .fetch_optional(&mut *tx)
                .await?;
        let Some(since) = since.filter(|s| *s < until) else {
            continue;
        };

        synced += sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(since)
            .bind(until)
            .execute(&mut *tx)
            .await?
            .rows_affected();

        sqlx::query("UPDATE search_sync_state SET synced_until = $2 WHERE item_type = $1")
            .bind(kind)
            .bind(until)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(synced)
}

const SETUP: &str = r#"
CREATE EXTENSION IF NOT EXISTS pg_search CASCADE;

CREATE TABLE IF NOT EXISTS search_songs (
    id text PRIMARY KEY REFERENCES songs(id) ON DELETE CASCADE,
    name text NOT NULL,
    artist_name text NOT NULL DEFAULT '',
    album_name text NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS search_albums (
    id text PRIMARY KEY REFERENCES albums(id) ON DELETE CASCADE,
    name text NOT NULL,
    artist_name text NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS search_sync_state (
    item_type text PRIMARY KEY,
    synced_until timestamptz NOT NULL
);
"#;

const DATE_ADDED_INDEXES: [&str; 2] = [
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS songs_date_added_idx ON songs (date_added)",
    "CREATE INDEX CONCURRENTLY IF NOT EXISTS albums_date_added_idx ON albums (date_added)",
];

const BM25_INDEXES: [&str; 3] = [
    "CREATE INDEX IF NOT EXISTS search_songs_bm25 ON search_songs
     USING bm25 (id, name, artist_name, album_name) WITH (key_field = 'id')",
    "CREATE INDEX IF NOT EXISTS search_albums_bm25 ON search_albums
     USING bm25 (id, name, artist_name) WITH (key_field = 'id')",
    "CREATE INDEX IF NOT EXISTS artists_bm25 ON artists
     USING bm25 (id, name) WITH (key_field = 'id')",
];

async fn approx_row_count(pool: &PgPool, table: &str) -> anyhow::Result<i64> {
    let count: i64 = sqlx::query_scalar(
        "SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE oid = $1::text::regclass",
    )
    .bind(table)
    .fetch_one(pool)
    .await?;
    Ok(count)
}

pub async fn backfill(pool: &PgPool, concurrency: usize) -> anyhow::Result<()> {
    tracing::info!("creating search tables");
    sqlx::raw_sql(SETUP).execute(pool).await?;

    let started: OffsetDateTime = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT now() - interval '{SYNC_LAG}'"
    )))
    .fetch_one(pool)
    .await?;

    for sql in DATE_ADDED_INDEXES {
        tracing::info!("{sql}");
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(pool)
            .await?;
    }

    let songs_done = approx_row_count(pool, "search_songs").await?;
    backfill_table(
        pool,
        "songs",
        &song_upsert(
            "s.ctid >= $1::text::tid AND s.ctid < $2::text::tid \
             AND NOT EXISTS (SELECT 1 FROM search_songs ss WHERE ss.id = s.id)",
        ),
        concurrency,
        songs_done,
    )
    .await?;
    let albums_done = approx_row_count(pool, "search_albums").await?;
    backfill_table(
        pool,
        "albums",
        &album_upsert(
            "al.ctid >= $1::text::tid AND al.ctid < $2::text::tid \
             AND NOT EXISTS (SELECT 1 FROM search_albums sa2 WHERE sa2.id = al.id)",
        ),
        concurrency,
        albums_done,
    )
    .await?;

    for sql in BM25_INDEXES {
        tracing::info!("{sql}");
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .execute(pool)
            .await?;
    }

    sqlx::query(
        "INSERT INTO search_sync_state (item_type, synced_until)
         VALUES ('songs', $1), ('albums', $1)
         ON CONFLICT (item_type) DO UPDATE SET synced_until = EXCLUDED.synced_until",
    )
    .bind(started)
    .execute(pool)
    .await?;

    tracing::info!("search backfill complete");
    Ok(())
}

async fn backfill_table(
    pool: &PgPool,
    table: &str,
    sql: &str,
    concurrency: usize,
    already_done: i64,
) -> anyhow::Result<()> {
    let (tuples, pages, blocks): (i64, i64, i64) = sqlx::query_as(
        "SELECT GREATEST(c.reltuples, 0)::bigint,
                GREATEST(c.relpages, 1)::bigint,
                pg_relation_size(c.oid) / current_setting('block_size')::bigint
         FROM pg_class c WHERE c.oid = $1::text::regclass",
    )
    .bind(table)
    .fetch_one(pool)
    .await?;

    let chunk_blocks = (ROWS_PER_CHUNK * pages / tuples.max(1)).clamp(1, 100_000);

    let estimated_boundary = already_done.max(0) * blocks / tuples.max(1);
    let start_block = (estimated_boundary - estimated_boundary / 5).clamp(0, blocks.max(1));

    let mut ranges: Vec<(i64, i64)> = (start_block..blocks.max(1))
        .step_by(chunk_blocks as usize)
        .map(|start| (start, start + chunk_blocks))
        .collect();
    if let Some(last) = ranges.last_mut() {
        last.1 = MAX_BLOCK;
    }

    tracing::info!(
        "{table}: backfilling ~{tuples} rows in {} chunks ({already_done} already done)",
        ranges.len()
    );

    let pb = ProgressBar::new(tuples.max(already_done).max(0) as u64);
    pb.set_position(already_done.max(0) as u64);

    pb.reset_eta();
    pb.set_style(
        ProgressStyle::default_bar()
            .template(&format!(
                "{table:<8}{{spinner:.green}} [{{bar:40.cyan/blue}}] {{human_pos}}/{{human_len}} ({{percent}}%) {{rate}} eta {{eta}}"
            ))?
            .with_key("rate", |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let _ = write!(w, "{}/s", HumanCount(state.per_sec() as u64));
            })
            .progress_chars("=>-"),
    );

    let reporter = (!std::io::stderr().is_terminal()).then(|| {
        let (pb, table) = (pb.clone(), table.to_string());
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                interval.tick().await;
                if pb.is_finished() {
                    break;
                }
                tracing::info!(
                    "{table}: {}/{} rows, {}/s, eta {}",
                    pb.position(),
                    pb.length().unwrap_or(0),
                    pb.per_sec() as u64,
                    HumanDuration(pb.eta()),
                );
            }
        })
    });

    let result = stream::iter(ranges.into_iter().map(Ok::<_, anyhow::Error>))
        .try_for_each_concurrent(concurrency, |(start, end)| {
            let pb = pb.clone();
            async move {
                let rows = sqlx::query(sqlx::AssertSqlSafe(sql.to_string()))
                    .bind(format!("({start},0)"))
                    .bind(format!("({end},0)"))
                    .execute(pool)
                    .await?
                    .rows_affected();
                pb.inc(rows);
                Ok(())
            }
        })
        .await;

    if let Some(reporter) = reporter {
        reporter.abort();
    }
    result?;
    let total = pb.position();
    pb.finish();
    tracing::info!(
        "{table}: {} new rows this run, {total} total",
        total.saturating_sub(already_done.max(0) as u64)
    );
    Ok(())
}
