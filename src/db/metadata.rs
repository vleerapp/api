use sqlx::{PgPool, Row};

use crate::models::metadata::{Album, Artist, Song};

pub async fn stats(pool: &PgPool) -> Result<(i64, i64, i64), sqlx::Error> {
    let rows = sqlx::query(
        "SELECT GREATEST(0, reltuples)::bigint AS estimate, relname
         FROM pg_class
         WHERE oid IN ('songs'::regclass, 'albums'::regclass, 'artists'::regclass)",
    )
    .fetch_all(pool)
    .await?;

    let mut songs = 0i64;
    let mut albums = 0i64;
    let mut artists = 0i64;
    for r in rows {
        let estimate: i64 = r.get("estimate");
        match r.get::<String, _>("relname").as_str() {
            "songs" => songs = estimate,
            "albums" => albums = estimate,
            "artists" => artists = estimate,
            _ => {}
        }
    }
    Ok((songs, albums, artists))
}

pub async fn song_ids_by_isrc(pool: &PgPool, isrcs: &[String]) -> Result<Vec<String>, sqlx::Error> {
    if isrcs.is_empty() {
        return Ok(Vec::new());
    }
    let upper: Vec<String> = isrcs.iter().map(|s| s.to_uppercase()).collect();
    let rows = sqlx::query("SELECT id FROM songs WHERE UPPER(isrc) = ANY($1) ORDER BY id")
        .bind(&upper)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|r| r.get::<String, _>("id")).collect())
}

pub async fn album_ids_by_upc(pool: &PgPool, upcs: &[String]) -> Result<Vec<String>, sqlx::Error> {
    if upcs.is_empty() {
        return Ok(Vec::new());
    }
    let rows = sqlx::query("SELECT id FROM albums WHERE upc = ANY($1) ORDER BY id")
        .bind(upcs)
        .fetch_all(pool)
        .await?;
    Ok(rows.into_iter().map(|r| r.get::<String, _>("id")).collect())
}

pub async fn get_song_by_id(pool: &PgPool, id: &str) -> Result<Option<Song>, sqlx::Error> {
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

    let Some(r) = row else { return Ok(None) };

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

pub async fn get_artist_by_id(pool: &PgPool, id: &str) -> Result<Option<Artist>, sqlx::Error> {
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

    Ok(row.map(|r| Artist {
        id: r.get("id"),
        name: r.get("name"),
        image: r.get("image"),
        genres: r.get::<Vec<String>, _>("genres"),
    }))
}

pub async fn get_album_by_id(pool: &PgPool, id: &str) -> Result<Option<Album>, sqlx::Error> {
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

    let Some(r) = row else { return Ok(None) };

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
