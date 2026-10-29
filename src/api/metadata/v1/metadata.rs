use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::PgPool;

use crate::api::metadata::v1::resource::{render_album, render_artist, render_song};
use crate::{db, search};

#[derive(Clone)]
pub struct SearchState {
    pub scrape_pool: PgPool,
    pub manticore: Option<std::sync::Arc<crate::manticore::Manticore>>,
}

const MAX_LOOKUP_VALUES: usize = 100;
const MATCH_CANDIDATES: i64 = 100;
const ENRICH_CANDIDATES: usize = 20;
const DEFAULT_SEARCH_LIMIT: i64 = 10;
const MAX_SEARCH_LIMIT: i64 = 50;

#[derive(Debug, Deserialize)]
pub struct CatalogQuery {
    pub ids: Option<String>,
    pub isrc: Option<String>,
    pub upc: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct IdentifyQuery {
    pub name: Option<String>,
    pub album: Option<String>,
    pub artist: Option<String>,
    pub duration: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub name: Option<String>,
    pub album: Option<String>,
    pub artist: Option<String>,
    pub duration: Option<i32>,
    pub limit: Option<i64>,
}

pub fn router() -> Router<SearchState> {
    Router::new()
        .route("/catalog", axum::routing::get(catalog_collection_handler))
        .route("/catalog/{id}", axum::routing::get(catalog_single_handler))
        .route("/identify/{type}", axum::routing::get(identify_handler))
        .route("/search/{type}", axum::routing::get(search_handler))
}

fn error_response(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({ "error": { "status": status.as_u16(), "message": message } })),
    )
}

fn classify_db_error(e: &sqlx::Error) -> Option<StatusCode> {
    match e {
        sqlx::Error::PoolTimedOut | sqlx::Error::Io(_) => Some(StatusCode::SERVICE_UNAVAILABLE),
        sqlx::Error::Database(d) => match d.code().as_deref() {
            Some("57014") => Some(StatusCode::GATEWAY_TIMEOUT),
            Some("42P01" | "42883" | "42704") => Some(StatusCode::SERVICE_UNAVAILABLE),
            _ => None,
        },
        _ => None,
    }
}

fn db_error_response(e: &sqlx::Error, context: &str, message: &str) -> (StatusCode, Json<Value>) {
    tracing::error!("{}: {}", context, e);
    let status = classify_db_error(e).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    error_response(status, message)
}

fn split_values(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn parse_id(raw: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = raw.trim().splitn(3, ':').collect();
    if parts.len() != 3 || parts[0] != "omm" {
        return None;
    }
    let item_type = parts[1];
    let id = parts[2];
    if !matches!(item_type, "song" | "album" | "artist") || !is_valid_omid(id) {
        return None;
    }
    Some((item_type.to_string(), id.to_string()))
}

fn is_valid_omid(id: &str) -> bool {
    id.len() <= 18 && id.parse::<i64>().is_ok_and(|n| n > 0) && id.bytes().all(|c| c.is_ascii_digit())
}

async fn fetch_resource(
    state: &SearchState,
    item_type: &str,
    id: &str,
) -> Result<Option<Value>, sqlx::Error> {
    Ok(match item_type {
        "song" => db::metadata::get_song_by_id(&state.scrape_pool, id)
            .await?
            .map(|s| render_song(&s)),
        "album" => db::metadata::get_album_by_id(&state.scrape_pool, id)
            .await?
            .map(|a| render_album(&a)),
        "artist" => db::metadata::get_artist_by_id(&state.scrape_pool, id)
            .await?
            .map(|a| render_artist(&a)),
        _ => None,
    })
}

async fn catalog_collection_handler(
    State(state): State<SearchState>,
    Query(params): Query<CatalogQuery>,
) -> impl IntoResponse {
    let ids = params.ids.as_deref().filter(|s| !s.is_empty());
    let isrc = params.isrc.as_deref().filter(|s| !s.is_empty());
    let upc = params.upc.as_deref().filter(|s| !s.is_empty());

    if [ids, isrc, upc]
        .iter()
        .any(|p| p.is_some_and(|s| s.len() > 10_000))
    {
        return error_response(StatusCode::BAD_REQUEST, "query parameter too long").into_response();
    }

    if [ids.is_some(), isrc.is_some(), upc.is_some()]
        .iter()
        .filter(|p| **p)
        .count()
        != 1
    {
        return error_response(
            StatusCode::BAD_REQUEST,
            "Provide exactly one of ids, isrc, or upc",
        )
        .into_response();
    }

    let resolved: Vec<(String, String)> = if let Some(ids) = ids {
        let raw_ids = split_values(ids);
        if raw_ids.len() > MAX_LOOKUP_VALUES {
            return error_response(StatusCode::BAD_REQUEST, "Maximum 100 lookup values allowed")
                .into_response();
        }
        raw_ids.iter().filter_map(|raw| parse_id(raw)).collect()
    } else if let Some(isrc) = isrc {
        let values = split_values(isrc);
        if values.len() > MAX_LOOKUP_VALUES {
            return error_response(StatusCode::BAD_REQUEST, "Maximum 100 lookup values allowed")
                .into_response();
        }
        match db::metadata::song_ids_by_isrc(&state.scrape_pool, &values).await {
            Ok(ids) => ids.into_iter().map(|id| ("song".to_string(), id)).collect(),
            Err(e) => {
                return db_error_response(&e, "lookup error", "Lookup failed").into_response();
            }
        }
    } else {
        let values = split_values(upc.unwrap());
        if values.len() > MAX_LOOKUP_VALUES {
            return error_response(StatusCode::BAD_REQUEST, "Maximum 100 lookup values allowed")
                .into_response();
        }
        match db::metadata::album_ids_by_upc(&state.scrape_pool, &values).await {
            Ok(ids) => ids
                .into_iter()
                .map(|id| ("album".to_string(), id))
                .collect(),
            Err(e) => {
                return db_error_response(&e, "lookup error", "Lookup failed").into_response();
            }
        }
    };

    let ids_of = |kind: &str| -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        resolved
            .iter()
            .filter(|(t, id)| t == kind && seen.insert(id.clone()))
            .map(|(_, id)| id.clone())
            .collect()
    };
    let song_ids = ids_of("song");
    let album_ids = ids_of("album");
    let artist_ids = ids_of("artist");

    let fetched = tokio::try_join!(
        db::metadata::get_songs_by_ids(&state.scrape_pool, &song_ids),
        db::metadata::get_albums_by_ids(&state.scrape_pool, &album_ids),
        db::metadata::get_artists_by_ids(&state.scrape_pool, &artist_ids),
    );
    let (songs, albums, artists) = match fetched {
        Ok(v) => v,
        Err(e) => {
            return db_error_response(&e, "lookup error", "Lookup failed").into_response();
        }
    };

    let songs: std::collections::HashMap<_, _> =
        songs.iter().map(|x| (x.id.as_str(), render_song(x))).collect();
    let albums: std::collections::HashMap<_, _> =
        albums.iter().map(|x| (x.id.as_str(), render_album(x))).collect();
    let artists: std::collections::HashMap<_, _> =
        artists.iter().map(|x| (x.id.as_str(), render_artist(x))).collect();

    let data: Vec<Value> = resolved
        .iter()
        .filter_map(|(t, id)| match t.as_str() {
            "song" => songs.get(id.as_str()),
            "album" => albums.get(id.as_str()),
            "artist" => artists.get(id.as_str()),
            _ => None,
        })
        .cloned()
        .collect();

    (StatusCode::OK, Json(json!({ "data": data }))).into_response()
}

async fn catalog_single_handler(
    State(state): State<SearchState>,
    Path(raw_id): Path<String>,
) -> impl IntoResponse {
    let Some((item_type, id)) = parse_id(&raw_id) else {
        return error_response(StatusCode::BAD_REQUEST, "Invalid id. Expected omm:TYPE:ID")
            .into_response();
    };

    match fetch_resource(&state, &item_type, &id).await {
        Ok(Some(resource)) => (StatusCode::OK, Json(json!({ "data": resource }))).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "Resource not found").into_response(),
        Err(e) => db_error_response(&e, "lookup error", "Lookup failed").into_response(),
    }
}

type MatchInputs<'a> = (&'a str, Option<&'a str>, Option<&'a str>);

fn match_inputs<'a>(
    item_type: &str,
    name: &'a Option<String>,
    artist: &'a Option<String>,
    album: &'a Option<String>,
) -> Result<MatchInputs<'a>, (StatusCode, Json<Value>)> {
    if !matches!(item_type, "song" | "album" | "artist") {
        return Err(error_response(StatusCode::BAD_REQUEST, "Invalid type"));
    }

    let Some(name) = name.as_deref().filter(|s| !s.is_empty()) else {
        return Err(error_response(StatusCode::BAD_REQUEST, "name is required"));
    };
    if name.len() > 256 {
        return Err(error_response(StatusCode::BAD_REQUEST, "name too long"));
    }

    let artist = artist.as_deref().filter(|s| !s.is_empty());
    let album = album.as_deref().filter(|s| !s.is_empty());
    if artist.is_some_and(|s| s.len() > 256) || album.is_some_and(|s| s.len() > 256) {
        return Err(error_response(
            StatusCode::BAD_REQUEST,
            "query parameter too long",
        ));
    }

    Ok(match item_type {
        "song" => (name, artist, album),
        "album" => (name, artist, None),
        _ => (name, None, None),
    })
}

async fn find_candidates(
    state: &SearchState,
    item_type: &str,
    (name, artist, album): MatchInputs<'_>,
    keep: usize,
) -> Result<Vec<db::search::Candidate>, Option<StatusCode>> {
    let engine = db::search::Engine {
        pool: &state.scrape_pool,
        manticore: state.manticore.as_deref(),
    };
    let mut candidates =
        match db::search::candidates(engine, item_type, name, artist, MATCH_CANDIDATES).await {
            Ok(result) => result,
            Err(e) => {
                let status = classify_db_error(&e);
                if status.is_some() {
                    tracing::error!("match error: {}", e);
                } else {
                    tracing::warn!("match query error, treating as no match: {}", e);
                }
                return Err(status);
            }
        };

    if item_type == "song" {
        candidates = search::shortlist(candidates, name, artist, album, keep);
        if let Err(e) = db::search::enrich(&state.scrape_pool, &mut candidates).await {
            let status = classify_db_error(&e);
            if status.is_some() {
                tracing::error!("enrich error: {}", e);
            } else {
                tracing::warn!("enrich error, treating as no match: {}", e);
            }
            return Err(status);
        }
    }

    Ok(candidates)
}

async fn identify_handler(
    State(state): State<SearchState>,
    Path(item_type): Path<String>,
    Query(params): Query<IdentifyQuery>,
) -> impl IntoResponse {
    let inputs = match match_inputs(&item_type, &params.name, &params.artist, &params.album) {
        Ok(inputs) => inputs,
        Err(e) => return e.into_response(),
    };
    let (name, artist, album) = inputs;

    let candidates = match find_candidates(&state, &item_type, inputs, ENRICH_CANDIDATES).await {
        Ok(candidates) => candidates,
        Err(None) => return error_response(StatusCode::NOT_FOUND, "No match found").into_response(),
        Err(Some(status)) => return error_response(status, "Match failed").into_response(),
    };

    let Some(matched) = search::best_match(
        &candidates,
        name,
        artist,
        album,
        params
            .duration
            .filter(|_| item_type == "song" && params.duration > Some(0)),
    ) else {
        return error_response(StatusCode::NOT_FOUND, "No match found").into_response();
    };
    let matched_id = matched.id.clone();

    let result = fetch_resource(&state, &item_type, &matched_id).await;

    match result {
        Ok(Some(resource)) => (StatusCode::OK, Json(json!({ "data": resource }))).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "No match found").into_response(),
        Err(e) => db_error_response(&e, "match error", "Match failed").into_response(),
    }
}

async fn search_handler(
    State(state): State<SearchState>,
    Path(item_type): Path<String>,
    Query(params): Query<SearchQuery>,
) -> impl IntoResponse {
    let inputs = match match_inputs(&item_type, &params.name, &params.artist, &params.album) {
        Ok(inputs) => inputs,
        Err(e) => return e.into_response(),
    };
    let (name, artist, album) = inputs;
    let limit = params.limit.unwrap_or(DEFAULT_SEARCH_LIMIT);
    if !(1..=MAX_SEARCH_LIMIT).contains(&limit) {
        return error_response(StatusCode::BAD_REQUEST, "limit must be between 1 and 50")
            .into_response();
    }

    let candidates = match find_candidates(&state, &item_type, inputs, ENRICH_CANDIDATES.max(limit as usize))
        .await
    {
        Ok(candidates) => candidates,
        Err(None) => return (StatusCode::OK, Json(json!({ "data": [] }))).into_response(),
        Err(Some(status)) => return error_response(status, "Search failed").into_response(),
    };

    let ranked = search::rank(
        &candidates,
        name,
        artist,
        album,
        params
            .duration
            .filter(|_| item_type == "song" && params.duration > Some(0)),
        limit as usize,
    );
    let ids: Vec<String> = ranked.iter().map(|c| c.id.clone()).collect();

    let rendered: Result<Vec<(String, Value)>, sqlx::Error> = match item_type.as_str() {
        "song" => db::metadata::get_songs_by_ids(&state.scrape_pool, &ids)
            .await
            .map(|v| v.iter().map(|x| (x.id.clone(), render_song(x))).collect()),
        "album" => db::metadata::get_albums_by_ids(&state.scrape_pool, &ids)
            .await
            .map(|v| v.iter().map(|x| (x.id.clone(), render_album(x))).collect()),
        _ => db::metadata::get_artists_by_ids(&state.scrape_pool, &ids)
            .await
            .map(|v| v.iter().map(|x| (x.id.clone(), render_artist(x))).collect()),
    };
    let rendered: std::collections::HashMap<String, Value> = match rendered {
        Ok(v) => v.into_iter().collect(),
        Err(e) => return db_error_response(&e, "search error", "Search failed").into_response(),
    };

    let data: Vec<&Value> = ids.iter().filter_map(|id| rendered.get(id)).collect();
    (StatusCode::OK, Json(json!({ "data": data }))).into_response()
}
