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
    Fields, parse_includes, parse_set, render_album, render_artist, render_song,
};
use crate::db;
use crate::manticore::SearchClient;

#[derive(Clone)]
pub struct SearchState {
    pub client: Arc<SearchClient>,
    pub scrape_pool: PgPool,
}

const MAX_LOOKUP_VALUES: usize = 100;
const MATCH_CANDIDATES: i32 = 25;

fn name_rank(candidate: &str, query: &str) -> u8 {
    let c = candidate.trim().to_lowercase();
    let q = query.trim().to_lowercase();
    if candidate == query {
        0
    } else if c == q {
        1
    } else if c.starts_with(&q) {
        2
    } else if c.contains(&q) {
        3
    } else {
        4
    }
}

#[derive(Debug, Deserialize)]
pub struct FieldsQuery {
    pub include: Option<String>,
    #[serde(rename = "fields[song]")]
    pub fields_song: Option<String>,
    #[serde(rename = "fields[album]")]
    pub fields_album: Option<String>,
    #[serde(rename = "fields[artist]")]
    pub fields_artist: Option<String>,
}

impl FieldsQuery {
    fn fields(&self) -> Fields {
        Fields {
            song: parse_set(&self.fields_song),
            album: parse_set(&self.fields_album),
            artist: parse_set(&self.fields_artist),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct LookupQuery {
    pub ids: Option<String>,
    pub isrc: Option<String>,
    pub upc: Option<String>,
    pub include: Option<String>,
    #[serde(rename = "fields[song]")]
    pub fields_song: Option<String>,
    #[serde(rename = "fields[album]")]
    pub fields_album: Option<String>,
    #[serde(rename = "fields[artist]")]
    pub fields_artist: Option<String>,
}

impl LookupQuery {
    fn fields(&self) -> Fields {
        Fields {
            song: parse_set(&self.fields_song),
            album: parse_set(&self.fields_album),
            artist: parse_set(&self.fields_artist),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct MatchQuery {
    pub name: Option<String>,
    pub album: Option<String>,
    pub artist: Option<String>,
    pub include: Option<String>,
    #[serde(rename = "fields[song]")]
    pub fields_song: Option<String>,
    #[serde(rename = "fields[album]")]
    pub fields_album: Option<String>,
    #[serde(rename = "fields[artist]")]
    pub fields_artist: Option<String>,
}

impl MatchQuery {
    fn fields(&self) -> Fields {
        Fields {
            song: parse_set(&self.fields_song),
            album: parse_set(&self.fields_album),
            artist: parse_set(&self.fields_artist),
        }
    }
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
    fields: &Fields,
    include: &std::collections::HashSet<String>,
) -> Result<Option<Value>, sqlx::Error> {
    Ok(match item_type {
        "song" => db::metadata::get_song_by_id(&state.scrape_pool, id)
            .await?
            .map(|s| render_song(&s, fields, include)),
        "album" => db::metadata::get_album_by_id(&state.scrape_pool, id)
            .await?
            .map(|a| render_album(&a, fields, include)),
        "artist" => db::metadata::get_artist_by_id(&state.scrape_pool, id)
            .await?
            .map(|a| render_artist(&a, &fields.artist)),
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

    let fields = params.fields();
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
        match fetch_resource(&state, &item_type, &id, &fields, &include).await {
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
    Query(params): Query<FieldsQuery>,
) -> impl IntoResponse {
    let Some((item_type, id)) = parse_id(&raw_id) else {
        return error_response(StatusCode::BAD_REQUEST, "Invalid id. Expected omm:TYPE:ID")
            .into_response();
    };

    let fields = params.fields();
    let include = parse_includes(&params.include);

    match fetch_resource(&state, &item_type, &id, &fields, &include).await {
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

    let artist = params.artist.as_deref().filter(|s| !s.is_empty());
    let album = params.album.as_deref().filter(|s| !s.is_empty());

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

    let Some((matched_id, _)) = candidates
        .iter()
        .min_by_key(|(_, candidate_name)| name_rank(candidate_name, name))
    else {
        return error_response(StatusCode::NOT_FOUND, "No match found").into_response();
    };
    let matched_id = matched_id.clone();

    let fields = params.fields();
    let include = parse_includes(&params.include);

    match fetch_resource(&state, &item_type, &matched_id, &fields, &include).await {
        Ok(Some(resource)) => (StatusCode::OK, Json(json!({ "data": resource }))).into_response(),
        Ok(None) => error_response(StatusCode::NOT_FOUND, "No match found").into_response(),
        Err(e) => {
            tracing::error!("match error: {}", e);
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "Match failed").into_response()
        }
    }
}
