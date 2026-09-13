use anyhow::{Context, Result, anyhow};
use futures::{TryStreamExt, stream};
use indicatif::{HumanCount, MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sqlx::{
    PgPool, Row,
    postgres::{PgPoolOptions, PgRow},
};
use std::{
    env,
    io::IsTerminal,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;

const ROWS_PER_CHUNK: i64 = 10_000;
const MAX_BLOCK: i64 = u32::MAX as i64;
const MAX_ATTEMPTS: u64 = 5;
const SMALL_TABLE_CONCURRENCY: usize = 8;

const SONGS_QUERY: &str = "SELECT s.id, s.name, s.duration,
        COALESCE((SELECT string_agg(DISTINCT a.name, ' ')
                  FROM song_artists sa JOIN artists a ON a.id = sa.artist_id
                  WHERE sa.song_id = s.id), '') AS artist_name,
        COALESCE((SELECT al.name
                  FROM song_albums sal JOIN albums al ON al.id = sal.album_id
                  WHERE sal.song_id = s.id
                  ORDER BY al.name LIMIT 1), '') AS album_name
    FROM songs s
    WHERE s.ctid >= $1::text::tid AND s.ctid < $2::text::tid";
const ARTISTS_QUERY: &str =
    "SELECT id, name FROM artists WHERE ctid >= $1::text::tid AND ctid < $2::text::tid";
const ALBUMS_QUERY: &str =
    "SELECT id, name, date FROM albums WHERE ctid >= $1::text::tid AND ctid < $2::text::tid";

struct Ctx {
    pool: PgPool,
    http: Client,
    base: String,
    mp: MultiProgress,
    http_permits: Semaphore,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter("info,sqlx=error")
        .with_writer(std::io::stderr)
        .init();

    let manticore_url =
        env::var("MANTICORE_URL").unwrap_or_else(|_| "http://localhost:9308".to_string());
    let scrape_db_url = env::var("SCRAPE_DATABASE_URL")?;
    let concurrency: usize = env::var("SYNC_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    let manticore_concurrency: usize = env::var("MANTICORE_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16);

    tracing::info!("connecting to scrape db");
    let pool = PgPoolOptions::new()
        .max_connections((concurrency + SMALL_TABLE_CONCURRENCY * 2 + 3) as u32)
        .acquire_timeout(Duration::from_secs(10))
        .after_connect(|conn, _| {
            Box::pin(async move {
                sqlx::query("SET max_parallel_workers_per_gather = 0")
                    .execute(&mut *conn)
                    .await?;
                sqlx::query("SET jit = off").execute(&mut *conn).await?;
                Ok(())
            })
        })
        .connect(&scrape_db_url)
        .await
        .map_err(|e| anyhow!("scrape db connect failed: {e}"))?;

    let ctx = Ctx {
        pool,
        http: Client::builder()
            .pool_idle_timeout(Duration::from_secs(5))
            .build()?,
        base: manticore_url.trim_end_matches('/').to_string(),
        mp: MultiProgress::new(),
        http_permits: Semaphore::new(manticore_concurrency),
    };

    tracing::info!("recreating music table");
    sql_ddl(&ctx.http, &ctx.base, "DROP TABLE IF EXISTS music").await?;
    sql_ddl(
        &ctx.http,
        &ctx.base,
        r#"CREATE TABLE music (
            doc_id string,
            name text,
            artist_name text,
            album_name text,
            item_type string,
            duration int,
            date string
        ) min_prefix_len='3' rt_mem_limit='2G'"#,
    )
    .await?;

    sql_ddl(&ctx.http, &ctx.base, "SET GLOBAL auto_optimize=0").await?;

    let tables = env::var("SYNC_TABLES").unwrap_or_else(|_| "songs,artists,albums".to_string());
    let ctx_ref = &ctx;
    let run = |table: &'static str, query: &'static str, to_doc: fn(&PgRow) -> Value, concurrency: usize| {
        let enabled = tables.split(',').any(|t| t.trim() == table);
        async move {
            match enabled {
                true => sync_table(ctx_ref, table, query, to_doc, concurrency).await,
                false => Ok(()),
            }
        }
    };
    let result = tokio::try_join!(
        run("songs", SONGS_QUERY, song_doc, concurrency),
        run("artists", ARTISTS_QUERY, artist_doc, SMALL_TABLE_CONCURRENCY),
        run("albums", ALBUMS_QUERY, album_doc, SMALL_TABLE_CONCURRENCY),
    );

    sql_ddl(&ctx.http, &ctx.base, "SET GLOBAL auto_optimize=1").await?;
    result?;

    ctx.mp.suspend(|| tracing::info!("optimizing music table"));
    sql_ddl(&ctx.http, &ctx.base, "OPTIMIZE TABLE music").await?;

    ctx.mp.suspend(|| tracing::info!("sync complete, optimize running in background"));
    Ok(())
}

fn song_doc(row: &PgRow) -> Value {
    json!({
        "doc_id": row.get::<String, _>("id"),
        "name": row.get::<String, _>("name"),
        "duration": row.get::<i64, _>("duration"),
        "artist_name": row.get::<String, _>("artist_name"),
        "album_name": row.get::<String, _>("album_name"),
        "item_type": "song"
    })
}

fn artist_doc(row: &PgRow) -> Value {
    json!({
        "doc_id": row.get::<String, _>("id"),
        "name": row.get::<String, _>("name"),
        "item_type": "artist"
    })
}

fn album_doc(row: &PgRow) -> Value {
    json!({
        "doc_id": row.get::<String, _>("id"),
        "name": row.get::<String, _>("name"),
        "date": row.get::<String, _>("date"),
        "item_type": "album"
    })
}

async fn sync_table(
    ctx: &Ctx,
    table: &str,
    query: &'static str,
    to_doc: fn(&PgRow) -> Value,
    concurrency: usize,
) -> Result<()> {
    let (tuples, pages, blocks): (i64, i64, i64) = sqlx::query_as(
        "SELECT GREATEST(c.reltuples, 0)::bigint,
                GREATEST(c.relpages, 1)::bigint,
                pg_relation_size(c.oid) / current_setting('block_size')::bigint
         FROM pg_class c WHERE c.oid = $1::text::regclass",
    )
    .bind(table)
    .fetch_one(&ctx.pool)
    .await?;

    let chunk_blocks = (ROWS_PER_CHUNK * pages / tuples.max(1)).clamp(1, 100_000);
    let mut ranges: Vec<(i64, i64)> = (0..blocks.max(1))
        .step_by(chunk_blocks as usize)
        .map(|start| (start, start + chunk_blocks))
        .collect();
    if let Some(last) = ranges.last_mut() {
        last.1 = MAX_BLOCK;
    }

    let pb = ctx.mp.add(ProgressBar::new(tuples as u64));
    pb.set_style(
        ProgressStyle::default_bar()
            .template(&format!(
                "{table:<8}{{spinner:.green}} [{{bar:40.cyan/blue}}] {{human_pos}}/{{human_len}} ({{percent}}%) {{rate}} {{eta}}"
            ))?
            .with_key("rate", |state: &ProgressState, w: &mut dyn std::fmt::Write| {
                let _ = write!(w, "{}/s", HumanCount(state.per_sec() as u64));
            })
            .progress_chars("=>-"),
    );

    let db_ms = Arc::new(AtomicU64::new(0));
    let http_ms = Arc::new(AtomicU64::new(0));
    let chunks = Arc::new(AtomicU64::new(0));
    let reporter = (!std::io::stderr().is_terminal()).then(|| {
        let (pb, db_ms, http_ms, chunks, table) =
            (pb.clone(), db_ms.clone(), http_ms.clone(), chunks.clone(), table.to_string());
        tokio::spawn(async move {
            let mut last = 0;
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
                let pos = pb.position();
                let n = chunks.load(Ordering::Relaxed).max(1);
                tracing::info!(
                    "{table}: {pos}/{} rate={}/s db_avg={}ms http_avg={}ms",
                    pb.length().unwrap_or(0),
                    (pos - last) / 15,
                    db_ms.load(Ordering::Relaxed) / n,
                    http_ms.load(Ordering::Relaxed) / n,
                );
                last = pos;
            }
        })
    });

    stream::iter(ranges.into_iter().map(Ok::<_, anyhow::Error>))
        .try_for_each_concurrent(concurrency, |(start, end)| {
            let pb = pb.clone();
            let (db_ms, http_ms, chunks) = (db_ms.clone(), http_ms.clone(), chunks.clone());
            async move {
                let rows = {
                    let t = Instant::now();
                    let rows = sqlx::query(query)
                        .bind(format!("({start},0)"))
                        .bind(format!("({end},0)"))
                        .fetch_all(&ctx.pool)
                        .await?;
                    db_ms.fetch_add(t.elapsed().as_millis() as u64, Ordering::Relaxed);
                    rows
                };
                if rows.is_empty() {
                    return Ok(());
                }

                let mut body = String::new();
                for row in &rows {
                    body.push_str(
                        &json!({ "insert": { "table": "music", "doc": to_doc(row) } }).to_string(),
                    );
                    body.push('\n');
                }

                let _permit = ctx.http_permits.acquire().await?;
                let t = Instant::now();
                send_bulk(&ctx.http, &ctx.base, body).await?;
                http_ms.fetch_add(t.elapsed().as_millis() as u64, Ordering::Relaxed);
                chunks.fetch_add(1, Ordering::Relaxed);
                pb.inc(rows.len() as u64);
                Ok(())
            }
        })
        .await?;

    if let Some(reporter) = reporter {
        reporter.abort();
    }
    pb.finish();
    ctx.mp
        .suspend(|| tracing::info!("{table}: {} synced", pb.position()));
    Ok(())
}

async fn send_bulk(http: &Client, base: &str, body: String) -> Result<()> {
    let mut attempt = 0;
    let resp = loop {
        attempt += 1;
        match http
            .post(format!("{base}/bulk"))
            .header("Content-Type", "application/x-ndjson")
            .body(body.clone())
            .send()
            .await
        {
            Ok(resp) if resp.status() != StatusCode::SERVICE_UNAVAILABLE || attempt >= MAX_ATTEMPTS => {
                break resp;
            }
            Err(e) if attempt >= MAX_ATTEMPTS => {
                return Err(e).context("manticore bulk request failed");
            }
            _ => tokio::time::sleep(Duration::from_secs(attempt)).await,
        }
    };

    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| anyhow!("failed to read bulk response: {e}"))?;

    if !status.is_success() {
        return Err(anyhow!("manticore bulk error {status}: {text}"));
    }

    let parsed: Value = serde_json::from_str(&text)
        .map_err(|e| anyhow!("failed to parse bulk response: {e}, body: {text}"))?;

    if parsed["errors"].as_bool().unwrap_or(false) {
        return Err(anyhow!("manticore bulk returned errors: {text}"));
    }

    Ok(())
}

async fn sql_ddl(http: &Client, base: &str, query: &str) -> Result<()> {
    let resp = http
        .post(format!("{base}/sql?mode=raw"))
        .form(&[("query", query)])
        .send()
        .await
        .map_err(|e| anyhow!("manticore request failed: {e}"))?;

    let status = resp.status();
    let body = resp
        .text()
        .await
        .map_err(|e| anyhow!("failed to read response: {e}"))?;

    if !status.is_success() {
        return Err(anyhow!("manticore DDL error {status}: {body}"));
    }

    let parsed: Value = serde_json::from_str(&body)
        .map_err(|e| anyhow!("failed to parse response: {e}, body: {body}"))?;

    if let Some(err) = parsed[0]["error"].as_str() {
        if !err.is_empty() {
            return Err(anyhow!("manticore sql error: {err}"));
        }
    }

    Ok(())
}
