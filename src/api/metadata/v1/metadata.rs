use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;
use validator::Validate;

use crate::{manticore::SearchClient, models::metadata::SearchResponse};

#[derive(Clone)]
pub struct SearchState {
    pub client: Arc<SearchClient>,
    pub scrape_pool: PgPool,
}

#[derive(Debug, Deserialize, Validate)]
pub struct SearchQuery {
    pub q: String,
    #[serde(rename = "type")]
    pub item_type: Option<String>,
    pub artist: Option<String>,
    pub album: Option<String>,
    pub isrc: Option<String>,
    pub upc: Option<String>,
    #[serde(default = "default_limit")]
    #[validate(range(min = 1, max = 100))]
    pub limit: i32,
    #[serde(default)]
    #[validate(range(min = 0))]
    pub offset: i32,
}

#[derive(Debug, Deserialize)]
pub struct LookupQuery {
    pub id: String,
}

fn is_valid_omid(id: &str) -> bool {
    id.len() == 16
        && id
            .chars()
            .all(|c| c.is_ascii_digit() || (c >= 'a' && c <= 'z'))
}

fn default_limit() -> i32 {
    20
}

pub fn router() -> Router<SearchState> {
    Router::new()
        .route("/search", axum::routing::get(search_handler))
        .route("/lookup", axum::routing::get(lookup_handler))
}

async fn search_handler(
    State(state): State<SearchState>,
    Query(params): Query<SearchQuery>,
) -> impl IntoResponse {
    if params.limit < 1 || params.limit > 100 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Limit must be between 1 and 100" })),
        );
    }

    if params.offset < 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Offset must be non-negative" })),
        );
    }

    match state
        .client
        .search_advanced(
            &state.scrape_pool,
            &params.q,
            params.item_type.as_deref(),
            params.artist.as_deref(),
            params.album.as_deref(),
            params.isrc.as_deref(),
            params.upc.as_deref(),
            params.limit,
            params.offset,
        )
        .await
    {
        Ok(result) => {
            let response = SearchResponse {
                item_type: params
                    .item_type
                    .clone()
                    .unwrap_or_else(|| "song".to_string()),
                data: result.items,
                total: result.total,
                limit: params.limit,
                offset: params.offset,
            };
            (
                StatusCode::OK,
                Json(serde_json::to_value(response).unwrap()),
            )
        }
        Err(e) => {
            tracing::error!("search error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Search failed" })),
            )
        }
    }
}

async fn lookup_handler(
    State(state): State<SearchState>,
    Query(params): Query<LookupQuery>,
) -> impl IntoResponse {
    let ids: Vec<&str> = params.id.split(',').map(|s| s.trim()).collect();

    let mut results: Vec<serde_json::Value> = Vec::new();
    let mut errors: Vec<serde_json::Value> = Vec::new();

    for raw_id in ids {
        let parts: Vec<&str> = raw_id.splitn(3, ':').collect();
        if parts.len() != 3 || parts[0] != "omm" {
            errors.push(json!({ "id": raw_id, "error": "Invalid format. Expected omm:TYPE:ID" }));
            continue;
        }

        let item_type = parts[1];
        let id = parts[2];

        if !is_valid_omid(id) {
            errors.push(json!({ "id": raw_id, "error": "Invalid OMID format. Must be 16 lowercase alphanumeric characters." }));
            continue;
        }

        match item_type {
            "song" => match state.client.get_song_by_id(&state.scrape_pool, id).await {
                Ok(Some(song)) => results.push(serde_json::to_value(song).unwrap()),
                Ok(None) => errors.push(json!({ "id": raw_id, "error": "Not found" })),
                Err(e) => {
                    tracing::error!("lookup error: {}", e);
                    errors.push(json!({ "id": raw_id, "error": "Lookup failed" }));
                }
            },
            "artist" => match state.client.get_artist_by_id(&state.scrape_pool, id).await {
                Ok(Some(artist)) => results.push(serde_json::to_value(artist).unwrap()),
                Ok(None) => errors.push(json!({ "id": raw_id, "error": "Not found" })),
                Err(e) => {
                    tracing::error!("lookup error: {}", e);
                    errors.push(json!({ "id": raw_id, "error": "Lookup failed" }));
                }
            },
            "album" => match state.client.get_album_by_id(&state.scrape_pool, id).await {
                Ok(Some(album)) => results.push(serde_json::to_value(album).unwrap()),
                Ok(None) => errors.push(json!({ "id": raw_id, "error": "Not found" })),
                Err(e) => {
                    tracing::error!("lookup error: {}", e);
                    errors.push(json!({ "id": raw_id, "error": "Lookup failed" }));
                }
            },
            _ => {
                errors.push(json!({ "id": raw_id, "error": "Unknown type. Must be song, artist, or album" }));
            }
        }
    }

    let status = if results.is_empty() && !errors.is_empty() {
        StatusCode::NOT_FOUND
    } else {
        StatusCode::OK
    };

    (status, Json(json!({ "data": results, "errors": errors })))
}
