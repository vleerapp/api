use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::PgPool;
use std::sync::Arc;

use crate::api::metadata::v1::resource::{
    parse_includes, render_album, render_artist, render_song,
};
use crate::db;
use crate::manticore::SearchClient;

#[derive(Clone)]
pub struct SearchState {
    pub client: Arc<SearchClient>,
    pub scrape_pool: PgPool,
}

const MAX_LOOKUP_VALUES: usize = 100;
const MATCH_CANDIDATES: i32 = 50;

fn best_jw(candidate_joined: &str, query: &str) -> f64 {
    let q = query.to_lowercase();
    let c = candidate_joined.to_lowercase();
    if c.contains(q.as_str()) {
        return 1.0;
    }
    c.split_whitespace()
        .map(|part| strsim::jaro_winkler(part.trim_matches(','), q.as_str()))
        .fold(0.0_f64, f64::max)
}

fn score_candidate(
    cn: &str,
    ca: &str,
    cal: &str,
    qn: &str,
    qa: Option<&str>,
    qal: Option<&str>,
) -> f64 {
    let mut score = strsim::jaro_winkler(&cn.to_lowercase(), &qn.to_lowercase()) * 0.6;
    if let Some(a) = qa {
        score += best_jw(ca, a) * 0.3;
    }
    if let Some(a) = qal {
        score += best_jw(cal, a) * 0.1;
    }
    score
}

#[derive(Debug, Deserialize)]
pub struct IncludeQuery {
    pub include: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct LookupQuery {
    pub ids: Option<String>,
    pub isrc: Option<String>,
    pub upc: Option<String>,
    pub include: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MatchQuery {
    pub name: Option<String>,
    pub album: Option<String>,
    pub artist: Option<String>,
    pub include: Option<String>,
}

pub fn router() -> Router<SearchState> {
    Router::new()
        .route("/", axum::routing::get(stats_handler))
        .route("/lookup", axum::routing::get(lookup_collection_handler))
        .route("/lookup/{id}", axum::routing::get(lookup_single_handler))
        .route("/match/{type}", axum::routing::get(match_handler))
}

fn error_response(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({ "error": { "status": status.as_u16(), "message": message } })),
    )
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
    id.len() == 16
        && id
            .chars()
            .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase())
}

async fn stats_handler(State(state): State<SearchState>) -> impl IntoResponse {
    match db::metadata::stats(&state.scrape_pool).await {
        Ok((songs, albums, artists)) => (
            StatusCode::OK,
            Json(json!({
                "stats": { "songs": songs, "albums": albums, "artists": artists }
            })),
        ),
        Err(e) => {
            tracing::error!("stats error: {}", e);
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "Failed to load stats")
        }
    }
}

async fn fetch_resource(
    state: &SearchState,
    item_type: &str,
    id: &str,
    include: &std::collections::HashSet<String>,
) -> Result<Option<Value>, sqlx::Error> {
    Ok(match item_type {
        "song" => db::metadata::get_song_by_id(&state.scrape_pool, id)
            .await?
            .map(|s| render_song(&s, include)),
        "album" => db::metadata::get_album_by_id(&state.scrape_pool, id)
            .await?
            .map(|a| render_album(&a, include)),
        "artist" => db::metadata::get_artist_by_id(&state.scrape_pool, id)
            .await?
            .map(|a| render_artist(&a)),
        _ => None,
    })
}

async fn lookup_collection_handler(
    State(state): State<SearchState>,
    Query(params): Query<LookupQuery>,
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

    let include = parse_includes(&params.include);

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
                tracing::error!("lookup error: {}", e);
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "Lookup failed")
                    .into_response();
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
                tracing::error!("lookup error: {}", e);
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "Lookup failed")
                    .into_response();
            }
        }
    };

    let mut data: Vec<Value> = Vec::new();
    for (item_type, id) in resolved {
        match fetch_resource(&state, &item_type, &id, &include).await {
            Ok(Some(resource)) => data.push(resource),
            Ok(None) => {}
            Err(e) => {
                tracing::error!("lookup error: {}", e);
                return error_response(StatusCode::INTERNAL_SERVER_ERROR, "Lookup failed")
                    .into_response();
            }
        }
    }

    (StatusCode::OK, Json(json!({ "data": data }))).into_response()
}

async fn lookup_single_handler(
    State(state): State<SearchState>,
    Path(raw_id): Path<String>,
    Query(params): Query<IncludeQuery>,
) -> impl IntoResponse {
    let Some((item_type, id)) = parse_id(&raw_id) else {
        return error_response(StatusCode::BAD_REQUEST, "Invalid id. Expected omm:TYPE:ID")
            .into_response();
    };

    let include = parse_includes(&params.include);

    match fetch_resource(&state, &item_type, &id, &include).await {
        Ok(Some(resource)) => (StatusCode::OK, Json(json!({ "data": resource }))).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "Resource not found").into_response(),
        Err(e) => {
            tracing::error!("lookup error: {}", e);
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "Lookup failed").into_response()
        }
    }
}

async fn match_handler(
    State(state): State<SearchState>,
    Path(item_type): Path<String>,
    Query(params): Query<MatchQuery>,
) -> impl IntoResponse {
    if !matches!(item_type.as_str(), "song" | "album" | "artist") {
        return error_response(StatusCode::BAD_REQUEST, "Invalid type").into_response();
    }

    let name = params.name.as_deref().filter(|s| !s.is_empty());
    let Some(name) = name else {
        return error_response(StatusCode::BAD_REQUEST, "name is required").into_response();
    };
    if name.len() > 256 {
        return error_response(StatusCode::BAD_REQUEST, "name too long").into_response();
    }

    let artist = params.artist.as_deref().filter(|s| !s.is_empty());
    let album = params.album.as_deref().filter(|s| !s.is_empty());
    if artist.is_some_and(|s| s.len() > 256) || album.is_some_and(|s| s.len() > 256) {
        return error_response(StatusCode::BAD_REQUEST, "query parameter too long").into_response();
    }

    let (artist, album) = match item_type.as_str() {
        "song" => (artist, album),
        "album" => (artist, None),
        _ => (None, None),
    };

    let candidates = match state
        .client
        .search(&item_type, Some(name), artist, album, MATCH_CANDIDATES, 0)
        .await
    {
        Ok(result) => result,
        Err(e) => {
            tracing::error!("match error: {}", e);
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "Match failed")
                .into_response();
        }
    };

    let Some((matched_id, _, _, _)) =
        candidates
            .iter()
            .max_by(|(_, cn1, ca1, cal1), (_, cn2, ca2, cal2)| {
                score_candidate(cn1, ca1, cal1, name, artist, album)
                    .total_cmp(&score_candidate(cn2, ca2, cal2, name, artist, album))
            })
    else {
        return error_response(StatusCode::NOT_FOUND, "No match found").into_response();
    };
    let matched_id = matched_id.clone();

    let include = parse_includes(&params.include);

    let result = fetch_resource(&state, &item_type, &matched_id, &include).await;

    match result {
        Ok(Some(resource)) => (StatusCode::OK, Json(json!({ "data": resource }))).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "No match found").into_response(),
        Err(e) => {
            tracing::error!("match error: {}", e);
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "Match failed").into_response()
        }
    }
}
