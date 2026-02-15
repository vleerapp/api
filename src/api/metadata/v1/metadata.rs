use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;
use validator::Validate;

use crate::{elasticsearch::SearchClient, models::metadata::SearchResponse};

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

fn default_limit() -> i32 {
    20
}

pub fn router() -> Router<SearchState> {
    Router::new()
        .route("/search", axum::routing::get(search_handler))
        .route("/song/{id}", axum::routing::get(get_song_handler))
        .route("/artist/{id}", axum::routing::get(get_artist_handler))
        .route("/album/{id}", axum::routing::get(get_album_handler))
}

fn is_valid_omid(id: &str) -> bool {
    id.len() == 16
        && id
            .chars()
            .all(|c| c.is_ascii_digit() || (c >= 'a' && c <= 'z'))
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
            tracing::error!("Search error: {}", e);
            let response = json!({
                "error": "Search failed",
                "message": e.to_string(),
            });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
        }
    }
}

async fn get_song_handler(
    State(state): State<SearchState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if !is_valid_omid(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({ "error": "Invalid OMID format. Must be 16 lowercase alphanumeric characters." }),
            ),
        );
    }

    match state.client.get_song_by_id(&state.scrape_pool, &id).await {
        Ok(Some(song)) => (StatusCode::OK, Json(serde_json::to_value(song).unwrap())),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Song not found" })),
        ),
        Err(e) => {
            tracing::error!("Get song error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to fetch song" })),
            )
        }
    }
}

async fn get_artist_handler(
    State(state): State<SearchState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if !is_valid_omid(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({ "error": "Invalid OMID format. Must be 16 lowercase alphanumeric characters." }),
            ),
        );
    }

    match state.client.get_artist_by_id(&state.scrape_pool, &id).await {
        Ok(Some(artist)) => (StatusCode::OK, Json(serde_json::to_value(artist).unwrap())),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Artist not found" })),
        ),
        Err(e) => {
            tracing::error!("Get artist error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to fetch artist" })),
            )
        }
    }
}

async fn get_album_handler(
    State(state): State<SearchState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    if !is_valid_omid(&id) {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({ "error": "Invalid OMID format. Must be 16 lowercase alphanumeric characters." }),
            ),
        );
    }

    match state.client.get_album_by_id(&state.scrape_pool, &id).await {
        Ok(Some(album)) => (StatusCode::OK, Json(serde_json::to_value(album).unwrap())),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": "Album not found" })),
        ),
        Err(e) => {
            tracing::error!("Get album error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to fetch album" })),
            )
        }
    }
}
