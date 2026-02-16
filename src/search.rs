use anyhow::Result;
use elasticsearch::{
    Elasticsearch, SearchParts, GetParts,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{PgPool, Row};

use crate::models::metadata::{Album, Artist, Song, SearchResultItem};

#[derive(Clone)]
pub struct SearchClient {
    client: Elasticsearch,
    index_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedSearchResult {
    pub items: Vec<SearchResultItem>,
    pub total: i64,
}

impl SearchClient {
    pub fn new(es_url: &str) -> Result<Self> {
        let pool = SingleNodeConnectionPool::new(es_url.parse()?);
        let transport = TransportBuilder::new(pool).build()?;
        let client = Elasticsearch::new(transport);

        Ok(Self {
            client,
            index_name: "music".to_string(),
        })
    }

    pub async fn create_index(&self) -> Result<()> {
        let exists = self
            .client
            .indices()
            .exists(elasticsearch::indices::IndicesExistsParts::Index(&[
                &self.index_name
            ]))
            .send()
            .await?;

        if exists.status_code().is_success() {
            tracing::info!("Index '{}' already exists", self.index_name);
            return Ok(());
        }

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
                    "name": {
                        "type": "text",
                        "analyzer": "music_analyzer",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "artist_name": {"type": "text", "analyzer": "music_analyzer"},
                    "item_type": {"type": "keyword"},
                    "artwork_url": {"type": "keyword", "index": false},
                    "duration_seconds": {"type": "integer", "index": false},
                    "release_date": {"type": "keyword", "index": false}
                }
            }
        });

        self.client
            .indices()
            .create(elasticsearch::indices::IndicesCreateParts::Index(
                &self.index_name,
            ))
            .body(mapping)
            .send()
            .await?;

        tracing::info!("Created index '{}'", self.index_name);
        Ok(())
    }

    pub async fn search_advanced(
        &self,
        pool: &PgPool,
        query: &str,
        item_type: Option<&str>,
        artist_filter: Option<&str>,
        album_filter: Option<&str>,
        isrc_filter: Option<&str>,
        upc_filter: Option<&str>,
        limit: i32,
        offset: i32,
    ) -> Result<AdvancedSearchResult> {
        let must_clauses = vec![json!({
            "multi_match": {
                "query": query,
                "fields": ["name^2", "artist_name"],
                "type": "best_fields",
                "fuzziness": "AUTO",
                "prefix_length": 2
            }
        })];

        let mut filter_clauses = vec![];
        if let Some(t) = item_type {
            filter_clauses.push(json!({"term": {"item_type": t}}));
        }

        let search_body = json!({
            "query": {
                "bool": {
                    "must": must_clauses,
                    "filter": filter_clauses
                }
            },
            "size": limit,
            "from": offset,
            "_source": ["id", "item_type"]
        });

        let response_body = self
            .client
            .search(SearchParts::Index(&[&self.index_name]))
            .body(search_body)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        let hits = &response_body["hits"]["hits"];
        let total = response_body["hits"]["total"]["value"]
            .as_i64()
            .unwrap_or(0);

        let mut items = Vec::new();
        
        for hit in hits.as_array().unwrap_or(&vec![]) {
            let id = hit["_source"]["id"].as_str().unwrap_or("");
            let item_type = hit["_source"]["item_type"].as_str().unwrap_or("");
            
            if id.is_empty() || item_type.is_empty() {
                tracing::warn!("Empty id or item_type in ES hit");
                continue;
            }
            
            match item_type {
                "song" => {
                    if let Ok(Some(song)) = self.fetch_song_details(pool, id).await {
                        if let Some(artist) = artist_filter {
                            if !song.artist.to_lowercase().contains(&artist.to_lowercase()) {
                                continue;
                            }
                        }
                        if let Some(album) = album_filter {
                            if !song.album.to_lowercase().contains(&album.to_lowercase()) {
                                continue;
                            }
                        }
                        if let Some(isrc) = isrc_filter {
                            if song.isrc.to_lowercase() != isrc.to_lowercase() {
                                continue;
                            }
                        }
                        items.push(SearchResultItem::Song(song));
                    }
                }
                "artist" => {
                    if let Ok(Some(artist)) = self.fetch_artist_details(pool, id).await {
                        if let Some(artist_name) = artist_filter {
                            if !artist.name.to_lowercase().contains(&artist_name.to_lowercase()) {
                                continue;
                            }
                        }
                        items.push(SearchResultItem::Artist(artist));
                    }
                }
                "album" => {
                    if let Ok(Some(album)) = self.fetch_album_details(pool, id).await {
                        if let Some(artist_name) = artist_filter {
                            if !album.artist.to_lowercase().contains(&artist_name.to_lowercase()) {
                                continue;
                            }
                        }
                        if let Some(upc) = upc_filter {
                            if album.upc.to_lowercase() != upc.to_lowercase() {
                                continue;
                            }
                        }
                        items.push(SearchResultItem::Album(album));
                    }
                }
                _ => tracing::warn!("Unknown item_type: {}", item_type),
            }
        }

        Ok(AdvancedSearchResult {
            items,
            total,
        })
    }

    pub async fn get_song_by_id(&self, pool: &PgPool, id: &str) -> Result<Option<Song>> {
        let response = self
            .client
            .get(GetParts::IndexId(&self.index_name, id))
            .send()
            .await?;

        if response.status_code() == 404 {
            return Ok(None);
        }

        let body = response.json::<serde_json::Value>().await?;
        let item_type = body["_source"]["item_type"].as_str().unwrap_or("");

        if item_type != "song" {
            return Ok(None);
        }

        self.fetch_song_details(pool, id).await
    }

    pub async fn get_artist_by_id(&self, pool: &PgPool, id: &str) -> Result<Option<Artist>> {
        let response = self
            .client
            .get(GetParts::IndexId(&self.index_name, id))
            .send()
            .await?;

        if response.status_code() == 404 {
            return Ok(None);
        }

        let body = response.json::<serde_json::Value>().await?;
        let item_type = body["_source"]["item_type"].as_str().unwrap_or("");

        if item_type != "artist" {
            return Ok(None);
        }

        self.fetch_artist_details(pool, id).await
    }

    pub async fn get_album_by_id(&self, pool: &PgPool, id: &str) -> Result<Option<Album>> {
        let response = self
            .client
            .get(GetParts::IndexId(&self.index_name, id))
            .send()
            .await?;

        if response.status_code() == 404 {
            return Ok(None);
        }

        let body = response.json::<serde_json::Value>().await?;
        let item_type = body["_source"]["item_type"].as_str().unwrap_or("");

        if item_type != "album" {
            return Ok(None);
        }

        self.fetch_album_details(pool, id).await
    }

    async fn fetch_song_details(&self, pool: &PgPool, id: &str) -> Result<Option<Song>> {
        let row = sqlx::query(
            r#"SELECT s.id, s.name, s.image, s.duration, 
                      s.disc_number, s.track_number, s.isrc, s.date,
                      string_agg(DISTINCT a.name, ', ') as artist_names,
                      string_agg(DISTINCT al.name, ', ') as album_names
               FROM songs s
               LEFT JOIN song_artists sa ON s.id = sa.song_id
               LEFT JOIN artists a ON sa.artist_id = a.id
               LEFT JOIN song_albums sal ON s.id = sal.song_id
               LEFT JOIN albums al ON sal.album_id = al.id
               WHERE s.id = $1
               GROUP BY s.id, s.name, s.image, s.duration,
                        s.disc_number, s.track_number, s.isrc, s.date"#
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        match row {
            Some(r) => {
                let artist: String = r.get("artist_names");
                let album: String = r.get("album_names");
                
                if artist.is_empty() || album.is_empty() {
                    return Ok(None);
                }

                Ok(Some(Song {
                    id: r.get("id"),
                    name: r.get("name"),
                    artist,
                    album,
                    image: r.get("image"),
                    disc_number: r.get::<Option<i32>, _>("disc_number").unwrap_or(1),
                    track_number: r.get::<Option<i32>, _>("track_number").unwrap_or(1),
                    duration: r.get::<Option<i32>, _>("duration").unwrap_or(0),
                    isrc: r.get::<Option<String>, _>("isrc").unwrap_or_default(),
                    date: r.get::<Option<String>, _>("date").unwrap_or_default(),
                }))
            }
            None => Ok(None),
        }
    }

    async fn fetch_artist_details(&self, pool: &PgPool, id: &str) -> Result<Option<Artist>> {
        let row = sqlx::query(
            "SELECT id, name, image FROM artists WHERE id = $1"
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        match row {
            Some(r) => Ok(Some(Artist {
                id: r.get("id"),
                name: r.get("name"),
                image: r.get("image"),
            })),
            None => Ok(None),
        }
    }

    async fn fetch_album_details(&self, pool: &PgPool, id: &str) -> Result<Option<Album>> {
        let row = sqlx::query(
            r#"SELECT al.id, al.name, al.image, al.date, 
                      al.track_count, al.upc, al.label,
                      string_agg(DISTINCT a.name, ', ') as artist_names
               FROM albums al
               LEFT JOIN artist_albums aa ON al.id = aa.album_id
               LEFT JOIN artists a ON aa.artist_id = a.id
               WHERE al.id = $1
               GROUP BY al.id, al.name, al.image, al.date,
                        al.track_count, al.upc, al.label"#
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        match row {
            Some(r) => {
                let artist_name: String = r.get("artist_names");
                
                if artist_name.is_empty() {
                    return Ok(None);
                }

                Ok(Some(Album {
                    id: r.get("id"),
                    name: r.get("name"),
                    artist: artist_name,
                    image: r.get("image"),
                    date: r.get::<Option<String>, _>("date").unwrap_or_default(),
                    track_count: r.get::<Option<i32>, _>("track_count").unwrap_or(0),
                    upc: r.get::<Option<String>, _>("upc").unwrap_or_default(),
                    label: r.get("label"),
                }))
            }
            None => Ok(None),
        }
    }

    pub async fn count(&self) -> Result<i64> {
        let response = self
            .client
            .count(elasticsearch::CountParts::Index(&[&self.index_name]))
            .send()
            .await?;

        let body = response.json::<serde_json::Value>().await?;
        Ok(body["count"].as_i64().unwrap_or(0))
    }
}
