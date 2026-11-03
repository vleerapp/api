use sqlx::{PgPool, Row};
use std::sync::Arc;

use crate::index::SearchIndex;

pub struct Candidate {
    pub id: String,
    pub name: String,
    pub artist: String,
    pub album: String,
    pub popularity_score: f64,
    pub duration: Option<i32>,
    pub album_tier: i32,
}

pub async fn candidates(
    index: &Arc<SearchIndex>,
    item_type: &str,
    name: &str,
    artist: Option<&str>,
    limit: i64,
) -> Result<Vec<Candidate>, sqlx::Error> {
    let hits = index
        .search(item_type, name, artist, limit.clamp(1, 1000) as usize)
        .await
        .map_err(|e| sqlx::Error::Io(std::io::Error::other(e.to_string())))?;
    Ok(hits
        .into_iter()
        .map(|h| Candidate {
            id: h.id,
            name: h.name,
            artist: h.artist,
            album: h.album,
            popularity_score: h.popularity,
            duration: None,
            album_tier: 0,
        })
        .collect())
}

#[derive(Default, Clone, Copy)]
pub struct Scope {
    pub artist: Option<i64>,
    pub album: Option<i64>,
}

impl Scope {
    pub fn is_empty(&self) -> bool {
        self.artist.is_none() && self.album.is_none()
    }
}

pub async fn scoped_candidates(
    pool: &PgPool,
    item_type: &str,
    scope: Scope,
) -> Result<Vec<Candidate>, sqlx::Error> {
    let query = match item_type {
        "song" => sqlx::query(
            "SELECT s.id, s.name, COALESCE(s.popularity, 0)::float8 AS popularity,
                    COALESCE((SELECT string_agg(al.name, ', ' ORDER BY al.name)
                              FROM song_albums sal JOIN albums al ON al.id = sal.album_id
                              WHERE sal.song_id = s.id), '') AS album
             FROM songs s
             WHERE ($1::bigint IS NULL OR s.id IN (
                       SELECT song_id FROM song_artists WHERE artist_id = $1))
               AND ($2::bigint IS NULL OR s.id IN (
                       SELECT song_id FROM song_albums WHERE album_id = $2))",
        )
        .bind(scope.artist)
        .bind(scope.album),
        "album" => sqlx::query(
            "SELECT al.id, al.name, COALESCE(al.popularity, 0)::float8 AS popularity,
                    ''::text AS album
             FROM albums al
             WHERE al.id IN (SELECT album_id FROM artist_albums WHERE artist_id = $1)",
        )
        .bind(scope.artist),
        _ => return Ok(Vec::new()),
    };
    let rows = query.persistent(false).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|r| Candidate {
            id: r.get::<i64, _>("id").to_string(),
            name: r.get("name"),
            artist: String::new(),
            album: r.get("album"),
            popularity_score: r.get("popularity"),
            duration: None,
            album_tier: 0,
        })
        .collect())
}

pub async fn enrich(pool: &PgPool, found: &mut [Candidate]) -> Result<(), sqlx::Error> {
    if found.is_empty() {
        return Ok(());
    }
    let ids: Vec<i64> = found.iter().filter_map(|c| c.id.parse().ok()).collect();
    let rows = sqlx::query(
        "SELECT s.id, s.duration, COALESCE((
             SELECT min(song_album_tier(al.name, al.track_count, EXISTS (
                 SELECT 1 FROM artist_albums aa
                 JOIN song_artists sar ON sar.artist_id = aa.artist_id AND sar.song_id = s.id
                 WHERE aa.album_id = al.id)))
             FROM song_albums sal JOIN albums al ON al.id = sal.album_id
             WHERE sal.song_id = s.id), 0) AS tier
         FROM songs s WHERE s.id = ANY($1)",
    )
    .persistent(false)
    .bind(&ids)
    .fetch_all(pool)
    .await?;
    let details: std::collections::HashMap<String, (i32, i32)> = rows
        .into_iter()
        .map(|r| {
            (
                r.get::<i64, _>(0).to_string(),
                (r.get::<i32, _>(1), r.get::<i32, _>(2)),
            )
        })
        .collect();
    for c in found {
        if let Some(&(duration, tier)) = details.get(&c.id) {
            c.duration = Some(duration);
            c.album_tier = tier;
        }
    }
    Ok(())
}
