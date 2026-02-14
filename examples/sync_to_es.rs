use anyhow::Result;
use elasticsearch::{
    Elasticsearch, IndexParts,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
};
use serde_json::json;
use sqlx::{PgPool, Row};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let es_url = env::var("ELASTICSEARCH_URL")?;
    let scrape_db_url = env::var("SCRAPE_DATABASE_URL")?;

    let pool = PgPool::connect(&scrape_db_url).await?;

    let es_pool = SingleNodeConnectionPool::new(es_url.parse()?);
    let transport = TransportBuilder::new(es_pool).build()?;
    let client = Elasticsearch::new(transport);

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
                "name": {"type": "text", "analyzer": "music_analyzer"},
                "artist_name": {"type": "text", "analyzer": "music_analyzer"},
                "album_name": {"type": "text", "analyzer": "music_analyzer"},
                "item_type": {"type": "keyword"},
                "artwork_url": {"type": "keyword", "index": false}
            }
        }
    });

    let _ = client
        .indices()
        .create(elasticsearch::indices::IndicesCreateParts::Index("music"))
        .body(mapping)
        .send()
        .await;

    println!("syncing songs");
    let songs = sqlx::query(
        "SELECT apple_music_id, name, duration_seconds, artwork_url FROM songs LIMIT 10000"
    )
    .fetch_all(&pool)
    .await?;

    for song in songs {
        let doc = json!({
            "apple_music_id": song.get::<String, _>("apple_music_id"),
            "name": song.get::<String, _>("name"),
            "artwork_url": song.get::<String, _>("artwork_url"),
            "duration_seconds": song.get::<i64, _>("duration_seconds"),
            "item_type": "song"
        });

        let doc_id = format!("song_{}", song.get::<String, _>("apple_music_id"));
        let _ = client
            .index(IndexParts::IndexId("music", &doc_id))
            .body(doc)
            .send()
            .await;
    }

    println!("syncing artists");
    let artists = sqlx::query("SELECT apple_music_id, name, artwork_url FROM artists LIMIT 5000")
        .fetch_all(&pool)
        .await?;

    for artist in artists {
        let doc = json!({
            "apple_music_id": artist.get::<String, _>("apple_music_id"),
            "name": artist.get::<String, _>("name"),
            "artwork_url": artist.get::<String, _>("artwork_url"),
            "item_type": "artist"
        });

        let doc_id = format!("artist_{}", artist.get::<String, _>("apple_music_id"));
        let _ = client
            .index(IndexParts::IndexId("music", &doc_id))
            .body(doc)
            .send()
            .await;
    }

    println!("syncing albums");
    let albums = sqlx::query(
        "SELECT apple_music_id, name, artwork_url, release_date FROM albums LIMIT 5000"
    )
    .fetch_all(&pool)
    .await?;

    for album in albums {
        let doc = json!({
            "apple_music_id": album.get::<String, _>("apple_music_id"),
            "name": album.get::<String, _>("name"),
            "artwork_url": album.get::<String, _>("artwork_url"),
            "release_date": album.get::<String, _>("release_date"),
            "item_type": "album"
        });

        let doc_id = format!("album_{}", album.get::<String, _>("apple_music_id"));
        let _ = client
            .index(IndexParts::IndexId("music", &doc_id))
            .body(doc)
            .send()
            .await;
    }

    println!("sync complete");
    Ok(())
}
