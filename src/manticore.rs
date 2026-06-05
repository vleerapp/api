use anyhow::{Result, anyhow};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::collections::HashMap;

use crate::models::metadata::{Album, AlbumRef, Artist, ArtistRef, SearchResultItem, Song, SongSummary};

pub struct SearchClient {
    http: Client,
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
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .map_err(|e| anyhow!("failed to build http client: {e}"))?;
        Ok(Self {
            http,
            url: manticore_url.trim_end_matches('/').to_string(),
            index_name: "music".to_string(),
        })
    }

    async fn sql(&self, query: &str) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/sql", self.url))
            .form(&[("query", query)])
            .send()
            .await
            .map_err(|e| anyhow!("manticore request failed: {e}"))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| anyhow!("failed to read manticore response: {e}"))?;

        if !status.is_success() {
            return Err(anyhow!("manticore error {status}: {body}"));
        }

        serde_json::from_str(&body)
            .map_err(|e| anyhow!("failed to parse manticore response: {e}, body: {body}"))
    }

    async fn sql_raw(&self, query: &str) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/sql?mode=raw", self.url))
            .form(&[("query", query)])
            .send()
            .await
            .map_err(|e| anyhow!("manticore request failed: {e}"))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| anyhow!("failed to read manticore response: {e}"))?;

        if !status.is_success() {
            return Err(anyhow!("manticore error {status}: {body}"));
        }

        let parsed: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| anyhow!("failed to parse manticore response: {e}, body: {body}"))?;

        if let Some(err) = parsed[0]["error"].as_str() {
            if !err.is_empty() {
                return Err(anyhow!("manticore sql error: {err}"));
            }
        }

        Ok(parsed)
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
            ) min_prefix_len='3'"#,
            self.index_name
        );

        let response = self.sql_raw(&create_sql).await?;
        tracing::info!("create table {} response: {}", self.index_name, response);
        Ok(())
    }

    fn result_name(item: &SearchResultItem) -> &str {
        match item {
            SearchResultItem::Song(s) => &s.name,
            SearchResultItem::Artist(a) => &a.name,
            SearchResultItem::Album(a) => &a.name,
        }
    }

    fn rank_relevance(name: &str, query: &str) -> u8 {
        let name_lc = name.to_lowercase();
        let query_lc = query.to_lowercase();
        if name == query {
            0
        } else if name_lc == query_lc {
            1
        } else if name_lc.starts_with(&query_lc) {
            2
        } else {
            3
        }
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

        let cleaned_query = clean(query);
        let mut match_expr = if cleaned_query.ends_with('*') {
            cleaned_query
        } else {
            format!("{}*", cleaned_query)
        };
        if let Some(a) = artist_filter {
            match_expr.push_str(&format!(" @artist_name {}", clean(a)));
        }
        if let Some(a) = album_filter {
            match_expr.push_str(&format!(" @album_name {}", clean(a)));
        }

        if let Some(t) = item_type {
            let sql = format!(
                "SELECT doc_id FROM {} WHERE MATCH('{}') AND item_type='{}' LIMIT {}, {}",
                self.index_name, match_expr, t, offset, limit
            );

            let response = self.sql(&sql).await?;

            let empty_vec: Vec<serde_json::Value> = vec![];
            let hits = response["hits"]["hits"].as_array().unwrap_or(&empty_vec);
            let total = response["hits"]["total"].as_i64()
                .or_else(|| response["hits"]["total"]["value"].as_i64())
                .unwrap_or(hits.len() as i64);

            let ids: Vec<String> = {
                let mut seen = std::collections::HashSet::new();
                hits.iter()
                    .filter_map(|h| h["_source"]["doc_id"].as_str().map(|s| s.to_string()))
                    .filter(|id| seen.insert(id.clone()))
                    .collect()
            };

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

            items.sort_by_key(|item| Self::rank_relevance(Self::result_name(item), query));
            return Ok(AdvancedSearchResult { items, total });
        }

        let sql = format!(
            "SELECT doc_id FROM {} WHERE MATCH('{}') AND item_type='song' LIMIT {}, {}",
            self.index_name, match_expr, offset, limit
        );

        let response = self.sql(&sql).await?;

        let empty_vec: Vec<serde_json::Value> = vec![];
        let hits = response["hits"]["hits"].as_array().unwrap_or(&empty_vec);
        let total = response["hits"]["total"].as_i64()
            .or_else(|| response["hits"]["total"]["value"].as_i64())
            .unwrap_or(hits.len() as i64);

        let ids: Vec<String> = {
            let mut seen = std::collections::HashSet::new();
            hits.iter()
                .filter_map(|h| h["_source"]["doc_id"].as_str().map(|s| s.to_string()))
                .filter(|id| seen.insert(id.clone()))
                .collect()
        };

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

        items.sort_by_key(|item| Self::rank_relevance(Self::result_name(item), query));
        Ok(AdvancedSearchResult { items, total })
    }

    pub async fn get_song_by_id(&self, pool: &PgPool, id: &str) -> Result<Option<Song>> {
        self.fetch_song_details(pool, id).await
    }

    pub async fn get_artist_by_id(&self, pool: &PgPool, id: &str) -> Result<Option<Artist>> {
        self.fetch_artist_details(pool, id).await
    }

    pub async fn get_album_by_id(&self, pool: &PgPool, id: &str) -> Result<Option<Album>> {
        self.fetch_album_details(pool, id).await
    }

    async fn fetch_songs_batch(
        &self,
        pool: &PgPool,
        ids: &[String],
    ) -> Result<HashMap<String, SongSummary>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let rows = sqlx::query(
            r#"WITH artists_agg AS (
                    SELECT
                        sa.song_id,
                        json_agg(json_build_object('id', a.id, 'name', a.name) ORDER BY a.name) AS artists_json
                    FROM song_artists sa
                    JOIN artists a ON sa.artist_id = a.id
                    WHERE sa.song_id = ANY($1)
                    GROUP BY sa.song_id
                ),
                albums_agg AS (
                    SELECT
                        sal.song_id,
                        json_agg(json_build_object('id', al.id, 'name', al.name) ORDER BY al.name) AS albums_json
                    FROM song_albums sal
                    JOIN albums al ON sal.album_id = al.id
                    WHERE sal.song_id = ANY($1)
                    GROUP BY sal.song_id
                ),
                genres_agg AS (
                    SELECT
                        sg.song_id,
                        array_agg(g.name ORDER BY g.name) AS genres
                    FROM song_genres sg
                    JOIN genres g ON sg.genre_id = g.id
                    WHERE sg.song_id = ANY($1)
                    GROUP BY sg.song_id
                )
               SELECT s.id, s.name, s.image, s.duration,
                      s.disc_number, s.track_number, s.isrc, s.date,
                      artists_agg.artists_json,
                      albums_agg.albums_json,
                      COALESCE(genres_agg.genres, '{}') AS genres
               FROM songs s
               LEFT JOIN artists_agg ON artists_agg.song_id = s.id
               LEFT JOIN albums_agg ON albums_agg.song_id = s.id
               LEFT JOIN genres_agg ON genres_agg.song_id = s.id
               WHERE s.id = ANY($1)
            "#,
        )
        .bind(ids)
        .fetch_all(pool)
        .await?;

        let mut map = HashMap::new();
        for r in rows {
            let artists_json: Option<serde_json::Value> = r.get("artists_json");
            let albums_json: Option<serde_json::Value> = r.get("albums_json");
            let artists: Vec<ArtistRef> = match artists_json {
                Some(v) => serde_json::from_value(v).unwrap_or_default(),
                None => vec![],
            };
            let albums: Vec<AlbumRef> = match albums_json {
                Some(v) => serde_json::from_value(v).unwrap_or_default(),
                None => vec![],
            };
            if artists.is_empty() || albums.is_empty() {
                continue;
            }
            let genres: Vec<String> = r.get::<Vec<String>, _>("genres");
            let id: String = r.get("id");
            map.insert(
                id.clone(),
                SongSummary {
                    id,
                    name: r.get("name"),
                    artist: artists,
                    album: albums,
                    genres,
                    image: r.get("image"),
                    disc_number: r.get::<i64, _>("disc_number") as i32,
                    track_number: r.get::<i64, _>("track_number") as i32,
                    duration: r.get::<i64, _>("duration") as i32,
                    isrc: r.get("isrc"),
                    date: r.get("date"),
                },
            );
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
        let rows = sqlx::query(
            r#"SELECT a.id, a.name, a.image,
                      COALESCE(array_agg(DISTINCT g.name) FILTER (WHERE g.name IS NOT NULL), '{}') AS genres
               FROM artists a
               LEFT JOIN artist_genres ag ON ag.artist_id = a.id
               LEFT JOIN genres g ON g.id = ag.genre_id
               WHERE a.id = ANY($1)
               GROUP BY a.id, a.name, a.image"#,
        )
        .bind(ids)
        .fetch_all(pool)
        .await?;

        let mut map = HashMap::new();
        for r in rows {
            let id: String = r.get("id");
            map.insert(
                id.clone(),
                Artist {
                    id,
                    name: r.get("name"),
                    image: r.get("image"),
                    genres: r.get::<Vec<String>, _>("genres"),
                },
            );
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
            r#"WITH album_genres_agg AS (
                    SELECT
                        ag.album_id,
                        array_agg(g.name ORDER BY g.name) AS genres
                    FROM album_genres ag
                    JOIN genres g ON ag.genre_id = g.id
                    WHERE ag.album_id = ANY($1)
                    GROUP BY ag.album_id
                )
               SELECT al.id, al.name, al.image, al.date,
                      al.track_count, al.upc, al.label,
                      json_agg(json_build_object('id', a.id, 'name', a.name, 'image', a.image, 'genres', '[]'::json) ORDER BY a.name) as artists_json,
                      COALESCE(aga.genres, '{}') AS genres
               FROM albums al
               LEFT JOIN artist_albums aa ON al.id = aa.album_id
               LEFT JOIN artists a ON aa.artist_id = a.id
               LEFT JOIN album_genres_agg aga ON aga.album_id = al.id
               WHERE al.id = ANY($1)
               GROUP BY al.id, al.name, al.image, al.date,
                        al.track_count, al.upc, al.label, aga.genres"#,
        )
        .bind(ids)
        .fetch_all(pool)
        .await?;

        let mut map = HashMap::new();
        for r in rows {
            let artists_json: Option<serde_json::Value> = r.get("artists_json");
            let artists: Vec<Artist> = match artists_json {
                Some(v) => serde_json::from_value(v).unwrap_or_default(),
                None => vec![],
            };
            if artists.is_empty() {
                continue;
            }
            let id: String = r.get("id");
            map.insert(
                id.clone(),
                Album {
                    id,
                    name: r.get("name"),
                    artist: artists,
                    genres: r.get::<Vec<String>, _>("genres"),
                    image: r.get("image"),
                    date: r.get::<Option<String>, _>("date").unwrap_or_default(),
                    track_count: r.get::<i64, _>("track_count") as i32,
                    upc: r.get("upc"),
                    label: r.get::<Option<String>, _>("label"),
                },
            );
        }
        Ok(map)
    }

    async fn fetch_song_details(&self, pool: &PgPool, id: &str) -> Result<Option<Song>> {
        let row = sqlx::query(
            r#"WITH song_genres_agg AS (
                    SELECT
                        sg.song_id,
                        array_agg(g.name ORDER BY g.name) AS genres
                    FROM song_genres sg
                    JOIN genres g ON sg.genre_id = g.id
                    WHERE sg.song_id = $1
                    GROUP BY sg.song_id
                ),
                artist_genres_agg AS (
                    SELECT
                        ag.artist_id,
                        array_agg(g.name ORDER BY g.name) AS genres
                    FROM artist_genres ag
                    JOIN genres g ON ag.genre_id = g.id
                    WHERE ag.artist_id IN (
                        SELECT sa.artist_id FROM song_artists sa WHERE sa.song_id = $1
                    )
                    GROUP BY ag.artist_id
                ),
                artist_agg AS (
                    SELECT
                        sa.song_id,
                        json_agg(json_build_object(
                            'id', a.id,
                            'name', a.name,
                            'image', a.image,
                            'genres', COALESCE(to_json(aga.genres), '[]'::json)
                        ) ORDER BY a.name) AS artists_json
                    FROM song_artists sa
                    JOIN artists a ON sa.artist_id = a.id
                    LEFT JOIN artist_genres_agg aga ON aga.artist_id = a.id
                    WHERE sa.song_id = $1
                    GROUP BY sa.song_id
                ),
                album_artist_genres_agg AS (
                    SELECT
                        ag.artist_id,
                        array_agg(g.name ORDER BY g.name) AS genres
                    FROM artist_genres ag
                    JOIN genres g ON ag.genre_id = g.id
                    WHERE ag.artist_id IN (
                        SELECT aa.artist_id FROM artist_albums aa
                        WHERE aa.album_id IN (
                            SELECT sal.album_id FROM song_albums sal WHERE sal.song_id = $1
                        )
                    )
                    GROUP BY ag.artist_id
                ),
                album_artists_agg AS (
                    SELECT
                        aa.album_id,
                        json_agg(json_build_object(
                            'id', a.id,
                            'name', a.name,
                            'image', a.image,
                            'genres', COALESCE(to_json(aaga.genres), '[]'::json)
                        ) ORDER BY a.name) AS artists_json
                    FROM artist_albums aa
                    JOIN artists a ON aa.artist_id = a.id
                    LEFT JOIN album_artist_genres_agg aaga ON aaga.artist_id = a.id
                    WHERE aa.album_id IN (
                        SELECT sal.album_id FROM song_albums sal WHERE sal.song_id = $1
                    )
                    GROUP BY aa.album_id
                ),
                album_genres_agg AS (
                    SELECT
                        ag.album_id,
                        array_agg(g.name ORDER BY g.name) AS genres
                    FROM album_genres ag
                    JOIN genres g ON ag.genre_id = g.id
                    WHERE ag.album_id IN (
                        SELECT sal.album_id FROM song_albums sal WHERE sal.song_id = $1
                    )
                    GROUP BY ag.album_id
                ),
                album_agg AS (
                    SELECT
                        sal.song_id,
                        json_agg(json_build_object(
                            'id', al.id,
                            'name', al.name,
                            'artist', COALESCE(ala.artists_json, '[]'::json),
                            'genres', COALESCE(to_json(alga.genres), '[]'::json),
                            'image', al.image,
                            'date', al.date,
                            'track_count', al.track_count,
                            'upc', al.upc,
                            'label', al.label
                        ) ORDER BY al.name) AS albums_json
                    FROM song_albums sal
                    JOIN albums al ON sal.album_id = al.id
                    LEFT JOIN album_artists_agg ala ON ala.album_id = al.id
                    LEFT JOIN album_genres_agg alga ON alga.album_id = al.id
                    WHERE sal.song_id = $1
                    GROUP BY sal.song_id
                )
               SELECT s.id, s.name, s.image, s.duration,
                      s.disc_number, s.track_number, s.isrc, s.date,
                      artist_agg.artists_json,
                      album_agg.albums_json,
                      COALESCE(song_genres_agg.genres, '{}') AS genres
               FROM songs s
               LEFT JOIN artist_agg ON artist_agg.song_id = s.id
               LEFT JOIN album_agg ON album_agg.song_id = s.id
               LEFT JOIN song_genres_agg ON song_genres_agg.song_id = s.id
               WHERE s.id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        match row {
            Some(r) => {
                let artists_json: Option<serde_json::Value> = r.get("artists_json");
                let albums_json: Option<serde_json::Value> = r.get("albums_json");
                let artists: Vec<Artist> = match artists_json {
                    Some(v) => serde_json::from_value(v).unwrap_or_default(),
                    None => vec![],
                };
                let albums: Vec<Album> = match albums_json {
                    Some(v) => serde_json::from_value(v).unwrap_or_default(),
                    None => vec![],
                };

                if artists.is_empty() || albums.is_empty() {
                    return Ok(None);
                }

                Ok(Some(Song {
                    id: r.get("id"),
                    name: r.get("name"),
                    artist: artists,
                    album: albums,
                    genres: r.get::<Vec<String>, _>("genres"),
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
        let row = sqlx::query(
            r#"SELECT a.id, a.name, a.image,
                      COALESCE(array_agg(DISTINCT g.name) FILTER (WHERE g.name IS NOT NULL), '{}') AS genres
               FROM artists a
               LEFT JOIN artist_genres ag ON ag.artist_id = a.id
               LEFT JOIN genres g ON g.id = ag.genre_id
               WHERE a.id = $1
               GROUP BY a.id, a.name, a.image"#,
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        match row {
            Some(r) => Ok(Some(Artist {
                id: r.get("id"),
                name: r.get("name"),
                image: r.get("image"),
                genres: r.get::<Vec<String>, _>("genres"),
            })),
            None => Ok(None),
        }
    }

    async fn fetch_album_details(&self, pool: &PgPool, id: &str) -> Result<Option<Album>> {
        let row = sqlx::query(
            r#"WITH artist_genres_agg AS (
                    SELECT
                        ag.artist_id,
                        array_agg(g.name ORDER BY g.name) AS genres
                    FROM artist_genres ag
                    JOIN genres g ON ag.genre_id = g.id
                    WHERE ag.artist_id IN (
                        SELECT aa.artist_id FROM artist_albums aa WHERE aa.album_id = $1
                    )
                    GROUP BY ag.artist_id
                )
               SELECT al.id, al.name, al.image, al.date,
                      al.track_count, al.upc, al.label,
                      json_agg(json_build_object(
                          'id', a.id,
                          'name', a.name,
                          'image', a.image,
                          'genres', COALESCE(to_json(aga.genres), '[]'::json)
                      ) ORDER BY a.name) as artists_json,
                      COALESCE(array_agg(DISTINCT g2.name) FILTER (WHERE g2.name IS NOT NULL), '{}') AS genres
               FROM albums al
               LEFT JOIN artist_albums aa ON al.id = aa.album_id
               LEFT JOIN artists a ON aa.artist_id = a.id
               LEFT JOIN artist_genres_agg aga ON aga.artist_id = a.id
               LEFT JOIN album_genres alg ON alg.album_id = al.id
               LEFT JOIN genres g2 ON g2.id = alg.genre_id
               WHERE al.id = $1
               GROUP BY al.id, al.name, al.image, al.date,
                        al.track_count, al.upc, al.label"#,
        )
        .bind(id)
        .fetch_optional(pool)
        .await?;

        match row {
            Some(r) => {
                let artists_json: Option<serde_json::Value> = r.get("artists_json");
                let artists: Vec<Artist> = match artists_json {
                    Some(v) => serde_json::from_value(v).unwrap_or_default(),
                    None => vec![],
                };

                if artists.is_empty() {
                    return Ok(None);
                }

                Ok(Some(Album {
                    id: r.get("id"),
                    name: r.get("name"),
                    artist: artists,
                    genres: r.get::<Vec<String>, _>("genres"),
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
