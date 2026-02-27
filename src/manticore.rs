use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::collections::HashMap;

use crate::models::metadata::{Album, Artist, SearchResultItem, Song};

#[derive(Clone)]
pub struct SearchClient {
    client: reqwest::Client,
    url: String,
    index_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedSearchResult {
    pub items: Vec<SearchResultItem>,
    pub total: i64,
}

impl SearchClient {
    pub fn new(manticore_url: &str) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::new(),
            url: manticore_url.to_string(),
            index_name: "music".to_string(),
        })
    }

    async fn sql(&self, query: &str) -> Result<serde_json::Value> {
        let response = self
            .client
            .post(&format!("{}/sql", self.url))
            .form(&[("query", query), ("mode", "json")])
            .send()
            .await?
            .json()
            .await?;
        Ok(response)
    }

    pub async fn create_index(&self) -> Result<()> {
        let create_sql = format!(
            r#"CREATE TABLE IF NOT EXISTS {} (
                doc_id string,
                name text,
                artist_name text,
                album_name text,
                item_type string,
                duration int,
                date string
            )"#,
            self.index_name
        );

        let response = self
            .client
            .post(&format!("{}/sql", self.url))
            .form(&[("query", &create_sql), ("mode", &"raw".to_string())])
            .send()
            .await?
            .text()
            .await?;

        tracing::info!("create table {} response: {}", self.index_name, response);
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
        let clean = |s: &str| {
            s.replace('\'', " ")
                .replace('"', " ")
                .replace('\\', " ")
                .replace('@', " ")
                .replace('!', " ")
                .replace('^', " ")
        };

        let mut match_expr = clean(query);
        if let Some(a) = artist_filter {
            match_expr.push_str(&format!(" @artist_name {}", clean(a)));
        }
        if let Some(a) = album_filter {
            match_expr.push_str(&format!(" @album_name {}", clean(a)));
        }

        if let Some(t) = item_type {
            // Single type query — use offset/limit directly
            let sql = format!(
                "SELECT doc_id FROM {} WHERE MATCH('{}') AND item_type='{}' LIMIT {}, {}",
                self.index_name, match_expr, t, offset, limit
            );
            let total_sql = format!(
                "SELECT COUNT(*) as cnt FROM {} WHERE MATCH('{}') AND item_type='{}'",
                self.index_name, match_expr, t
            );

            let (response, total_response) =
                tokio::try_join!(self.sql(&sql), self.sql(&total_sql))?;

            let empty_vec: Vec<serde_json::Value> = vec![];
            let hits = response["hits"]["hits"].as_array().unwrap_or(&empty_vec);
            let total = total_response["hits"]["hits"]
                .get(0)
                .and_then(|h| h["_source"]["cnt"].as_i64())
                .unwrap_or(0);

            let ids: Vec<String> = hits
                .iter()
                .filter_map(|h| h["_source"]["doc_id"].as_str().map(|s| s.to_string()))
                .collect();

            let mut items = Vec::new();
            match t {
                "song" => {
                    let map = self.fetch_songs_batch(pool, &ids).await?;
                    for id in &ids {
                        if let Some(song) = map.get(id) {
                            if let Some(isrc) = isrc_filter {
                                if song.isrc.to_lowercase() != isrc.to_lowercase() {
                                    continue;
                                }
                            }
                            items.push(SearchResultItem::Song(song.clone()));
                        }
                    }
                }
                "artist" => {
                    let map = self.fetch_artists_batch(pool, &ids).await?;
                    for id in &ids {
                        if let Some(artist) = map.get(id) {
                            items.push(SearchResultItem::Artist(artist.clone()));
                        }
                    }
                }
                "album" => {
                    let map = self.fetch_albums_batch(pool, &ids).await?;
                    for id in &ids {
                        if let Some(album) = map.get(id) {
                            if let Some(upc) = upc_filter {
                                if album.upc.to_lowercase() != upc.to_lowercase() {
                                    continue;
                                }
                            }
                            items.push(SearchResultItem::Album(album.clone()));
                        }
                    }
                }
                _ => {}
            }

            return Ok(AdvancedSearchResult { items, total });
        }

        // No type filter — default to songs only
        let sql = format!(
            "SELECT doc_id FROM {} WHERE MATCH('{}') AND item_type='song' LIMIT {}, {}",
            self.index_name, match_expr, offset, limit
        );
        let total_sql = format!(
            "SELECT COUNT(*) as cnt FROM {} WHERE MATCH('{}') AND item_type='song'",
            self.index_name, match_expr
        );

        let (response, total_response) = tokio::try_join!(self.sql(&sql), self.sql(&total_sql))?;

        let empty_vec: Vec<serde_json::Value> = vec![];
        let hits = response["hits"]["hits"].as_array().unwrap_or(&empty_vec);
        let total = total_response["hits"]["hits"]
            .get(0)
            .and_then(|h| h["_source"]["cnt"].as_i64())
            .unwrap_or(0);

        let ids: Vec<String> = hits
            .iter()
            .filter_map(|h| h["_source"]["doc_id"].as_str().map(|s| s.to_string()))
            .collect();

        let map = self.fetch_songs_batch(pool, &ids).await?;
        let mut items = Vec::new();
        for id in &ids {
            if let Some(song) = map.get(id) {
                if let Some(isrc) = isrc_filter {
                    if song.isrc.to_lowercase() != isrc.to_lowercase() {
                        continue;
                    }
                }
                items.push(SearchResultItem::Song(song.clone()));
            }
        }

        Ok(AdvancedSearchResult { items, total })
    }

    pub async fn get_song_by_id(&self, pool: &PgPool, id: &str) -> Result<Option<Song>> {
        let sql = format!(
            "SELECT item_type FROM {} WHERE doc_id='{}'",
            self.index_name, id
        );
        let response = self.sql(&sql).await?;
        let empty_vec: Vec<serde_json::Value> = vec![];
        let hits = response["hits"]["hits"].as_array().unwrap_or(&empty_vec);

        if hits.is_empty() {
            return Ok(None);
        }

        if hits[0]["_source"]["item_type"].as_str() != Some("song") {
            return Ok(None);
        }

        self.fetch_song_details(pool, id).await
    }

    pub async fn get_artist_by_id(&self, pool: &PgPool, id: &str) -> Result<Option<Artist>> {
        let sql = format!(
            "SELECT item_type FROM {} WHERE doc_id='{}'",
            self.index_name, id
        );
        let response = self.sql(&sql).await?;
        let empty_vec: Vec<serde_json::Value> = vec![];
        let hits = response["hits"]["hits"].as_array().unwrap_or(&empty_vec);

        if hits.is_empty() {
            return Ok(None);
        }

        if hits[0]["_source"]["item_type"].as_str() != Some("artist") {
            return Ok(None);
        }

        self.fetch_artist_details(pool, id).await
    }

    pub async fn get_album_by_id(&self, pool: &PgPool, id: &str) -> Result<Option<Album>> {
        let sql = format!(
            "SELECT item_type FROM {} WHERE doc_id='{}'",
            self.index_name, id
        );
        let response = self.sql(&sql).await?;
        let empty_vec: Vec<serde_json::Value> = vec![];
        let hits = response["hits"]["hits"].as_array().unwrap_or(&empty_vec);

        if hits.is_empty() {
            return Ok(None);
        }

        if hits[0]["_source"]["item_type"].as_str() != Some("album") {
            return Ok(None);
        }

        self.fetch_album_details(pool, id).await
    }

    async fn fetch_songs_batch(
        &self,
        pool: &PgPool,
        ids: &[String],
    ) -> Result<HashMap<String, Song>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = sqlx::query(
            r#"SELECT s.id, s.name, s.image, s.duration,
                      s.disc_number, s.track_number, s.isrc, s.date,
                      string_agg(DISTINCT a.name, ', ') as artist_names,
                      string_agg(DISTINCT al.name, ', ') as album_names
               FROM songs s
               LEFT JOIN song_artists sa ON s.id = sa.song_id
               LEFT JOIN artists a ON sa.artist_id = a.id
               LEFT JOIN song_albums sal ON s.id = sal.song_id
               LEFT JOIN albums al ON sal.album_id = al.id
               WHERE s.id = ANY($1)
               GROUP BY s.id, s.name, s.image, s.duration,
                        s.disc_number, s.track_number, s.isrc, s.date"#,
        )
        .bind(ids)
        .fetch_all(pool)
        .await?;

        let mut map = HashMap::new();
        for r in rows {
            let artist: String = r.get::<Option<String>, _>("artist_names").unwrap_or_default();
            let album: String = r.get::<Option<String>, _>("album_names").unwrap_or_default();
            if artist.is_empty() || album.is_empty() {
                continue;
            }
            let id: String = r.get("id");
            map.insert(id.clone(), Song {
                id,
                name: r.get("name"),
                artist,
                album,
                image: r.get("image"),
                disc_number: r.get::<i64, _>("disc_number") as i32,
                track_number: r.get::<i64, _>("track_number") as i32,
                duration: r.get::<i64, _>("duration") as i32,
                isrc: r.get("isrc"),
                date: r.get("date"),
            });
        }
        Ok(map)
    }

    async fn fetch_artists_batch(
        &self,
        pool: &PgPool,
        ids: &[String],
    ) -> Result<HashMap<String, Artist>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = sqlx::query("SELECT id, name, image FROM artists WHERE id = ANY($1)")
            .bind(ids)
            .fetch_all(pool)
            .await?;

        let mut map = HashMap::new();
        for r in rows {
            let id: String = r.get("id");
            map.insert(id.clone(), Artist {
                id,
                name: r.get("name"),
                image: r.get("image"),
            });
        }
        Ok(map)
    }

    async fn fetch_albums_batch(
        &self,
        pool: &PgPool,
        ids: &[String],
    ) -> Result<HashMap<String, Album>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = sqlx::query(
            r#"SELECT al.id, al.name, al.image, al.date,
                      al.track_count, al.upc, al.label,
                      string_agg(DISTINCT a.name, ', ') as artist_names
               FROM albums al
               LEFT JOIN artist_albums aa ON al.id = aa.album_id
               LEFT JOIN artists a ON aa.artist_id = a.id
               WHERE al.id = ANY($1)
               GROUP BY al.id, al.name, al.image, al.date,
                        al.track_count, al.upc, al.label"#,
        )
        .bind(ids)
        .fetch_all(pool)
        .await?;

        let mut map = HashMap::new();
        for r in rows {
            let artist_name: String = r.get::<Option<String>, _>("artist_names").unwrap_or_default();
            if artist_name.is_empty() {
                continue;
            }
            let id: String = r.get("id");
            map.insert(id.clone(), Album {
                id,
                name: r.get("name"),
                artist: artist_name,
                image: r.get("image"),
                date: r.get::<Option<String>, _>("date").unwrap_or_default(),
                track_count: r.get::<i64, _>("track_count") as i32,
                upc: r.get("upc"),
                label: r.get::<Option<String>, _>("label"),
            });
        }
        Ok(map)
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
                        s.disc_number, s.track_number, s.isrc, s.date"#,
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        match row {
            Some(r) => {
                let artist: String = r.get::<Option<String>, _>("artist_names").unwrap_or_default();
                let album: String = r.get::<Option<String>, _>("album_names").unwrap_or_default();

                if artist.is_empty() || album.is_empty() {
                    return Ok(None);
                }

                Ok(Some(Song {
                    id: r.get("id"),
                    name: r.get("name"),
                    artist,
                    album,
                    image: r.get("image"),
                    disc_number: r.get::<i64, _>("disc_number") as i32,
                    track_number: r.get::<i64, _>("track_number") as i32,
                    duration: r.get::<i64, _>("duration") as i32,
                    isrc: r.get("isrc"),
                    date: r.get("date"),
                }))
            }
            None => Ok(None),
        }
    }

    async fn fetch_artist_details(&self, pool: &PgPool, id: &str) -> Result<Option<Artist>> {
        let row = sqlx::query("SELECT id, name, image FROM artists WHERE id = $1")
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
                        al.track_count, al.upc, al.label"#,
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        match row {
            Some(r) => {
                let artist_name: String = r.get::<Option<String>, _>("artist_names").unwrap_or_default();

                if artist_name.is_empty() {
                    return Ok(None);
                }

                Ok(Some(Album {
                    id: r.get("id"),
                    name: r.get("name"),
                    artist: artist_name,
                    image: r.get("image"),
                    date: r.get::<Option<String>, _>("date").unwrap_or_default(),
                    track_count: r.get::<i64, _>("track_count") as i32,
                    upc: r.get("upc"),
                    label: r.get::<Option<String>, _>("label"),
                }))
            }
            None => Ok(None),
        }
    }

    pub async fn count(&self) -> Result<i64> {
        let sql = format!("SELECT COUNT(*) as cnt FROM {}", self.index_name);
        let response = self.sql(&sql).await?;
        let empty_vec: Vec<serde_json::Value> = vec![];
        let hits = response["hits"]["hits"].as_array().unwrap_or(&empty_vec);

        if hits.is_empty() {
            return Ok(0);
        }

        Ok(hits[0]["_source"]["cnt"].as_i64().unwrap_or(0))
    }
}
