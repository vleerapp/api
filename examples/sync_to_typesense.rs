use anyhow::{Result, anyhow};
use futures::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use serde_json::json;
use sqlx::{PgPool, Row};
use std::env;

const BATCH_SIZE: usize = 50000;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_writer(std::io::stderr)
        .init();

    let typesense_url =
        env::var("TYPESENSE_URL").unwrap_or_else(|_| "http://localhost:8108".to_string());
    let typesense_key = env::var("TYPESENSE_API_KEY").unwrap_or_else(|_| "vleer".to_string());
    let scrape_db_url = env::var("SCRAPE_DATABASE_URL")?;

    let pool = PgPool::connect(&scrape_db_url).await?;
    let http = Client::new();
    let base = typesense_url.trim_end_matches('/').to_string();

    tracing::info!("ensuring music collection exists");
    ensure_collection(&http, &base, &typesense_key).await?;

    let song_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM songs")
        .fetch_one(&pool)
        .await?;
    let artist_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM artists")
        .fetch_one(&pool)
        .await?;
    let album_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM albums")
        .fetch_one(&pool)
        .await?;

    tracing::info!(
        "total to sync: {} songs, {} artists, {} albums",
        song_count,
        artist_count,
        album_count
    );

    sync_songs(&pool, &http, &base, &typesense_key, song_count as u64).await?;
    sync_artists(&pool, &http, &base, &typesense_key, artist_count as u64).await?;
    sync_albums(&pool, &http, &base, &typesense_key, album_count as u64).await?;

    tracing::info!("sync complete");
    Ok(())
}

async fn ensure_collection(http: &Client, base: &str, api_key: &str) -> Result<()> {
    let schema = json!({
        "name": "music",
        "fields": [
            {"name": "name",        "type": "string"},
            {"name": "artist_name", "type": "string", "optional": true},
            {"name": "album_name",  "type": "string", "optional": true},
            {"name": "item_type",   "type": "string", "facet": true},
            {"name": "duration",    "type": "int32",  "optional": true},
            {"name": "date",        "type": "string", "optional": true}
        ]
    });

    let resp = http
        .post(format!("{base}/collections"))
        .header("X-TYPESENSE-API-KEY", api_key)
        .json(&schema)
        .send()
        .await
        .map_err(|e| anyhow!("typesense request failed: {e}"))?;

    // 409 = collection already exists
    if !resp.status().is_success() && resp.status().as_u16() != 409 {
        let body = resp.text().await.unwrap_or_default();
        return Err(anyhow!("failed to create collection: {body}"));
    }

    Ok(())
}

async fn sync_songs(
    pool: &PgPool,
    http: &Client,
    base: &str,
    api_key: &str,
    total: u64,
) -> Result<()> {
    let pb = progress_bar(total, "songs  ");

    let mut stream = sqlx::query(
        "SELECT DISTINCT ON (s.id) s.id, s.name, s.duration,
                a.name  AS artist_name,
                al.name AS album_name
         FROM songs s
         LEFT JOIN song_artists sa  ON sa.song_id  = s.id
         LEFT JOIN artists a        ON a.id         = sa.artist_id
         LEFT JOIN song_albums sal  ON sal.song_id  = s.id
         LEFT JOIN albums al        ON al.id         = sal.album_id
         ORDER BY s.id",
    )
    .fetch(pool);

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut synced = 0u64;
    let start = std::time::Instant::now();

    while let Some(row) = stream.try_next().await? {
        batch.push(json!({
            "id":          row.get::<String, _>("id"),
            "name":        row.get::<String, _>("name"),
            "duration":    row.get::<i64, _>("duration") as i32,
            "artist_name": row.get::<Option<String>, _>("artist_name").unwrap_or_default(),
            "album_name":  row.get::<Option<String>, _>("album_name").unwrap_or_default(),
            "item_type":   "song"
        }));

        if batch.len() >= BATCH_SIZE {
            import_batch(http, base, api_key, &batch).await?;
            synced += batch.len() as u64;
            pb.set_position(synced);
            batch.clear();
        }
    }

    if !batch.is_empty() {
        import_batch(http, base, api_key, &batch).await?;
        synced += batch.len() as u64;
        pb.set_position(synced);
    }

    finish_bar(pb, "songs", synced, start);
    Ok(())
}

async fn sync_artists(
    pool: &PgPool,
    http: &Client,
    base: &str,
    api_key: &str,
    total: u64,
) -> Result<()> {
    let pb = progress_bar(total, "artists");

    let mut stream = sqlx::query("SELECT id, name FROM artists").fetch(pool);

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut synced = 0u64;
    let start = std::time::Instant::now();

    while let Some(row) = stream.try_next().await? {
        batch.push(json!({
            "id":        row.get::<String, _>("id"),
            "name":      row.get::<String, _>("name"),
            "item_type": "artist"
        }));

        if batch.len() >= BATCH_SIZE {
            import_batch(http, base, api_key, &batch).await?;
            synced += batch.len() as u64;
            pb.set_position(synced);
            batch.clear();
        }
    }

    if !batch.is_empty() {
        import_batch(http, base, api_key, &batch).await?;
        synced += batch.len() as u64;
        pb.set_position(synced);
    }

    finish_bar(pb, "artists", synced, start);
    Ok(())
}

async fn sync_albums(
    pool: &PgPool,
    http: &Client,
    base: &str,
    api_key: &str,
    total: u64,
) -> Result<()> {
    let pb = progress_bar(total, "albums ");

    let mut stream = sqlx::query(
        "SELECT al.id, al.name, al.date,
                COALESCE(array_agg(DISTINCT a.name) FILTER (WHERE a.name IS NOT NULL), ARRAY[]::text[]) as artist_names
         FROM albums al
         LEFT JOIN artist_albums aa ON al.id = aa.album_id
         LEFT JOIN artists a ON aa.artist_id = a.id
         GROUP BY al.id, al.name, al.date",
    )
    .fetch(pool);

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut synced = 0u64;
    let start = std::time::Instant::now();

    while let Some(row) = stream.try_next().await? {
        let artist_names: Vec<String> = row.get("artist_names");
        batch.push(json!({
            "id":          row.get::<String, _>("id"),
            "name":        row.get::<String, _>("name"),
            "artist_name": artist_names.join(" "),
            "date":        row.get::<Option<String>, _>("date").unwrap_or_default(),
            "item_type":   "album"
        }));

        if batch.len() >= BATCH_SIZE {
            import_batch(http, base, api_key, &batch).await?;
            synced += batch.len() as u64;
            pb.set_position(synced);
            batch.clear();
        }
    }

    if !batch.is_empty() {
        import_batch(http, base, api_key, &batch).await?;
        synced += batch.len() as u64;
        pb.set_position(synced);
    }

    finish_bar(pb, "albums", synced, start);
    Ok(())
}

async fn import_batch(
    http: &Client,
    base: &str,
    api_key: &str,
    docs: &[serde_json::Value],
) -> Result<()> {
    let body: String = docs
        .iter()
        .map(|d| d.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    let resp = http
        .post(format!("{base}/collections/music/documents/import?action=upsert"))
        .header("X-TYPESENSE-API-KEY", api_key)
        .header("Content-Type", "text/plain")
        .body(body)
        .send()
        .await
        .map_err(|e| anyhow!("typesense import request failed: {e}"))?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| anyhow!("failed to read import response: {e}"))?;

    if !status.is_success() {
        return Err(anyhow!("typesense import error {status}: {text}"));
    }

    // Each line is a result JSON — check for any failures
    let failed: Vec<&str> = text
        .lines()
        .filter(|line| {
            serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|v| v["success"].as_bool())
                .map(|ok| !ok)
                .unwrap_or(false)
        })
        .collect();

    if !failed.is_empty() {
        return Err(anyhow!(
            "typesense import had {} failures: {}",
            failed.len(),
            failed[..failed.len().min(3)].join(", ")
        ));
    }

    Ok(())
}

fn progress_bar(total: u64, label: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(&format!(
                "{label} {{spinner:.green}} [{{bar:40.cyan/blue}}] {{pos}}/{{len}} ({{percent}}%) {{eta}}"
            ))
            .unwrap()
            .progress_chars("=>-"),
    );
    pb
}

fn finish_bar(pb: ProgressBar, label: &str, synced: u64, start: std::time::Instant) {
    pb.finish_and_clear();
    let elapsed = start.elapsed();
    let rate = if elapsed.as_secs() > 0 {
        synced / elapsed.as_secs()
    } else {
        synced
    };
    tracing::info!("{}: {} synced at {} docs/sec", label, synced, rate);
}
