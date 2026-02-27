use anyhow::Result;
use futures::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use serde_json::json;
use sqlx::{PgPool, Row};
use std::env;

const BATCH_SIZE: usize = 1000;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let manticore_url =
        env::var("MANTICORE_URL").unwrap_or_else(|_| "http://localhost:9308".to_string());
    let scrape_db_url = env::var("SCRAPE_DATABASE_URL")?;

    let pool = PgPool::connect(&scrape_db_url).await?;
    let client = Client::new();

    tracing::info!("creating music table");

    let create_sql = r#"
        CREATE TABLE IF NOT EXISTS music (
            doc_id string,
            name text,
            artist_name text,
            album_name text,
            item_type string,
            duration int,
            date string
        )
    "#;

    let resp = client
        .post(&format!("{}/sql", manticore_url))
        .form(&[("query", create_sql), ("mode", "raw")])
        .send()
        .await?;

    let text = resp.text().await?;
    tracing::info!("table creation response: {}", text);

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
        song_count, artist_count, album_count
    );

    sync_songs(&pool, &client, &manticore_url, song_count as u64).await?;
    sync_artists(&pool, &client, &manticore_url, artist_count as u64).await?;
    sync_albums(&pool, &client, &manticore_url, album_count as u64).await?;

    tracing::info!("sync complete");
    Ok(())
}

async fn sync_songs(pool: &PgPool, client: &Client, url: &str, total: u64) -> Result<()> {

    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("songs {spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {eta}")?
            .progress_chars("=>-"),
    );

    let mut stream = sqlx::query(
        "SELECT s.id, s.name, s.duration,
                COALESCE(array_agg(DISTINCT a.name) FILTER (WHERE a.name IS NOT NULL), ARRAY[]::text[]) as artist_names,
                COALESCE(array_agg(DISTINCT al.name) FILTER (WHERE al.name IS NOT NULL), ARRAY[]::text[]) as album_names
         FROM songs s
         LEFT JOIN song_artists sa ON s.id = sa.song_id
         LEFT JOIN artists a ON sa.artist_id = a.id
         LEFT JOIN song_albums sal ON s.id = sal.song_id
         LEFT JOIN albums al ON sal.album_id = al.id
         GROUP BY s.id, s.name, s.duration",
    )
    .fetch(pool);

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut synced = 0u64;
    let start = std::time::Instant::now();

    while let Some(row) = stream.try_next().await? {
        let artist_names: Vec<String> = row.get("artist_names");
        let album_names: Vec<String> = row.get("album_names");
        let artist_name = artist_names.join(" ");
        let album_name = album_names.first().cloned().unwrap_or_default();
        let id = row.get::<String, _>("id");

        batch.push(json!({
            "doc_id": &id,
            "name": row.get::<String, _>("name"),
            "duration": row.get::<i64, _>("duration"),
            "artist_name": artist_name,
            "album_name": album_name,
            "item_type": "song"
        }));

        if batch.len() >= BATCH_SIZE {
            if send_batch(client, url, "music", &batch).await.is_ok() {
                synced += batch.len() as u64;
            }
            pb.set_position(synced);
            batch.clear();
        }
    }

    if !batch.is_empty() {
        if send_batch(client, url, "music", &batch).await.is_ok() {
            synced += batch.len() as u64;
        }
        pb.set_position(synced);
    }

    pb.finish_and_clear();
    let elapsed = start.elapsed();
    let rate = if elapsed.as_secs() > 0 {
        synced / elapsed.as_secs()
    } else {
        synced
    };
    tracing::info!("songs: {} synced at {} docs/sec", synced, rate);
    Ok(())
}

async fn sync_artists(pool: &PgPool, client: &Client, url: &str, total: u64) -> Result<()> {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("artists {spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {eta}")?
            .progress_chars("=>-"),
    );

    let mut stream = sqlx::query("SELECT id, name FROM artists").fetch(pool);

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut synced = 0u64;
    let start = std::time::Instant::now();

    while let Some(row) = stream.try_next().await? {
        let id = row.get::<String, _>("id");

        batch.push(json!({
            "doc_id": &id,
            "name": row.get::<String, _>("name"),
            "item_type": "artist"
        }));

        if batch.len() >= BATCH_SIZE {
            if send_batch(client, url, "music", &batch).await.is_ok() {
                synced += batch.len() as u64;
            }
            pb.set_position(synced);
            batch.clear();
        }
    }

    if !batch.is_empty() {
        if send_batch(client, url, "music", &batch).await.is_ok() {
            synced += batch.len() as u64;
        }
        pb.set_position(synced);
    }

    pb.finish_and_clear();
    let elapsed = start.elapsed();
    let rate = if elapsed.as_secs() > 0 {
        synced / elapsed.as_secs()
    } else {
        synced
    };
    tracing::info!("artists: {} synced at {} docs/sec", synced, rate);
    Ok(())
}

async fn sync_albums(pool: &PgPool, client: &Client, url: &str, total: u64) -> Result<()> {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("albums  {spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {eta}")?
            .progress_chars("=>-"),
    );

    let mut stream = sqlx::query("SELECT id, name, date FROM albums").fetch(pool);

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut synced = 0u64;
    let start = std::time::Instant::now();

    while let Some(row) = stream.try_next().await? {
        let id = row.get::<String, _>("id");

        batch.push(json!({
            "doc_id": &id,
            "name": row.get::<String, _>("name"),
            "date": row.get::<String, _>("date"),
            "item_type": "album"
        }));

        if batch.len() >= BATCH_SIZE {
            if send_batch(client, url, "music", &batch).await.is_ok() {
                synced += batch.len() as u64;
            }
            pb.set_position(synced);
            batch.clear();
        }
    }

    if !batch.is_empty() {
        if send_batch(client, url, "music", &batch).await.is_ok() {
            synced += batch.len() as u64;
        }
        pb.set_position(synced);
    }

    pb.finish_and_clear();
    let elapsed = start.elapsed();
    let rate = if elapsed.as_secs() > 0 {
        synced / elapsed.as_secs()
    } else {
        synced
    };
    tracing::info!("albums: {} synced at {} docs/sec", synced, rate);
    Ok(())
}

async fn send_batch(
    client: &Client,
    url: &str,
    table: &str,
    docs: &[serde_json::Value],
) -> Result<()> {
    let mut body = String::new();
    for doc in docs {
        let line = serde_json::json!({
            "insert": {
                "table": table,
                "doc": {
                    "doc_id": doc["doc_id"].as_str().unwrap_or(""),
                    "name": doc["name"].as_str().unwrap_or(""),
                    "artist_name": doc["artist_name"].as_str().unwrap_or(""),
                    "album_name": doc["album_name"].as_str().unwrap_or(""),
                    "item_type": doc["item_type"].as_str().unwrap_or(""),
                    "duration": doc["duration"].as_i64().unwrap_or(0),
                    "date": doc["date"].as_str().unwrap_or("")
                }
            }
        });
        body.push_str(&line.to_string());
        body.push('\n');
    }

    client
        .post(&format!("{}/bulk", url))
        .header("Content-Type", "application/x-ndjson")
        .body(body)
        .send()
        .await?;

    Ok(())
}
