use anyhow::Result;
use elasticsearch::{
    BulkOperation, BulkParts, Elasticsearch,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
};
use serde_json::json;
use sqlx::{PgPool, Row};
use std::env;
use futures::TryStreamExt;

const BATCH_SIZE: usize = 5000;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let es_url = env::var("ELASTICSEARCH_URL")?;
    let scrape_db_url = env::var("SCRAPE_DATABASE_URL")?;

    let pool = PgPool::connect(&scrape_db_url).await?;

    let es_pool = SingleNodeConnectionPool::new(es_url.parse()?);
    let transport = TransportBuilder::new(es_pool).build()?;
    let client = Elasticsearch::new(transport);

    let index_exists = client
        .indices()
        .exists(elasticsearch::indices::IndicesExistsParts::Index(&["music"]))
        .send()
        .await?;
    
    if !index_exists.status_code().is_success() {
        println!("Creating 'music' index...");
        let mapping = json!({
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "music_analyzer": {
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding", "edge_ngram_filter"]
                        }
                    },
                    "filter": {
                        "edge_ngram_filter": {
                            "type": "edge_ngram",
                            "min_gram": 2,
                            "max_gram": 20
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "apple_music_id": {"type": "keyword"},
                    "name": {"type": "text", "analyzer": "music_analyzer"},
                    "artist_name": {"type": "text", "analyzer": "music_analyzer"},
                    "album_name": {"type": "text", "analyzer": "music_analyzer"},
                    "item_type": {"type": "keyword"},
                    "image": {"type": "keyword", "index": false}
                }
            }
        });

        let _ = client
            .indices()
            .create(elasticsearch::indices::IndicesCreateParts::Index("music"))
            .body(mapping)
            .send()
            .await?;
    }

    sync_songs(&pool, &client).await?;
    sync_artists(&pool, &client).await?;
    sync_albums(&pool, &client).await?;

    println!("\nSync complete");
    Ok(())
}

async fn sync_songs(pool: &PgPool, client: &Elasticsearch) -> Result<()> {
    println!("\nSyncing songs...");
    
    let mut stream = sqlx::query(
        "SELECT s.id, s.apple_music_id, s.name, s.duration, s.image, 
                COALESCE(array_agg(a.name) FILTER (WHERE a.name IS NOT NULL), ARRAY[]::text[]) as artist_names
         FROM songs s
         LEFT JOIN song_artists sa ON s.id = sa.song_id
         LEFT JOIN artists a ON sa.artist_id = a.id
         GROUP BY s.id, s.apple_music_id, s.name, s.duration, s.image"
    )
    .fetch(pool);

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut total = 0usize;
    let start = std::time::Instant::now();

    while let Some(row) = stream.try_next().await? {
        let artist_names: Vec<String> = row.get("artist_names");
        let artist_name = artist_names.first().cloned();
        
        let id = row.get::<String, _>("id");
        let mut doc = json!({
            "id": &id,
            "apple_music_id": row.get::<String, _>("apple_music_id"),
            "name": row.get::<String, _>("name"),
            "image": row.get::<String, _>("image"),
            "duration": row.get::<i64, _>("duration"),
            "item_type": "song"
        });
        
        if let Some(artist) = artist_name {
            doc["artist_name"] = json!(artist);
        }
        
        batch.push(BulkOperation::index(doc).id(&id).into());

        if batch.len() >= BATCH_SIZE {
            total += send_bulk(client, &batch).await?;
            batch.clear();
            
            let elapsed = start.elapsed().as_secs();
            let rate = if elapsed > 0 { total / elapsed as usize } else { 0 };
            println!("  Songs: {} ({} docs/sec)", total, rate);
        }
    }

    if !batch.is_empty() {
        total += send_bulk(client, &batch).await?;
    }

    let elapsed = start.elapsed().as_secs();
    let rate = if elapsed > 0 { total / elapsed as usize } else { 0 };
    println!("  Total songs: {} ({} docs/sec, {}s elapsed)", total, rate, elapsed);
    Ok(())
}

async fn sync_artists(pool: &PgPool, client: &Elasticsearch) -> Result<()> {
    println!("\nSyncing artists...");
    
    let mut stream = sqlx::query(
        "SELECT id, apple_music_id, name, image FROM artists"
    )
    .fetch(pool);

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut total = 0usize;
    let start = std::time::Instant::now();

    while let Some(row) = stream.try_next().await? {
        let id = row.get::<String, _>("id");
        let doc = json!({
            "id": &id,
            "apple_music_id": row.get::<String, _>("apple_music_id"),
            "name": row.get::<String, _>("name"),
            "image": row.get::<String, _>("image"),
            "item_type": "artist"
        });
        
        batch.push(BulkOperation::index(doc).id(&id).into());

        if batch.len() >= BATCH_SIZE {
            total += send_bulk(client, &batch).await?;
            batch.clear();
            
            let elapsed = start.elapsed().as_secs();
            let rate = if elapsed > 0 { total / elapsed as usize } else { 0 };
            println!("  Artists: {} ({} docs/sec)", total, rate);
        }
    }

    if !batch.is_empty() {
        total += send_bulk(client, &batch).await?;
    }

    let elapsed = start.elapsed().as_secs();
    let rate = if elapsed > 0 { total / elapsed as usize } else { 0 };
    println!("  Total artists: {} ({} docs/sec, {}s elapsed)", total, rate, elapsed);
    Ok(())
}

async fn sync_albums(pool: &PgPool, client: &Elasticsearch) -> Result<()> {
    println!("\nSyncing albums...");
    
    let mut stream = sqlx::query(
        "SELECT id, apple_music_id, name, image, date FROM albums"
    )
    .fetch(pool);

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut total = 0usize;
    let start = std::time::Instant::now();

    while let Some(row) = stream.try_next().await? {
        let id = row.get::<String, _>("id");
        let doc = json!({
            "id": &id,
            "apple_music_id": row.get::<String, _>("apple_music_id"),
            "name": row.get::<String, _>("name"),
            "image": row.get::<String, _>("image"),
            "date": row.get::<String, _>("date"),
            "item_type": "album"
        });
        
        batch.push(BulkOperation::index(doc).id(&id).into());

        if batch.len() >= BATCH_SIZE {
            total += send_bulk(client, &batch).await?;
            batch.clear();
            
            let elapsed = start.elapsed().as_secs();
            let rate = if elapsed > 0 { total / elapsed as usize } else { 0 };
            println!("  Albums: {} ({} docs/sec)", total, rate);
        }
    }

    if !batch.is_empty() {
        total += send_bulk(client, &batch).await?;
    }

    let elapsed = start.elapsed().as_secs();
    let rate = if elapsed > 0 { total / elapsed as usize } else { 0 };
    println!("  Total albums: {} ({} docs/sec, {}s elapsed)", total, rate, elapsed);
    Ok(())
}

async fn send_bulk(client: &Elasticsearch, batch: &[BulkOperation<serde_json::Value>]) -> Result<usize> {
    let response = client
        .bulk(BulkParts::Index("music"))
        .body(batch.to_vec())
        .send()
        .await?;
    
    let json = response.json::<serde_json::Value>().await?;
    let items = json["items"].as_array().map(|a| a.len()).unwrap_or(0);
    
    if json["errors"].as_bool().unwrap_or(false) {
        eprintln!("  Warning: Bulk errors occurred");
    }
    
    Ok(items)
}
