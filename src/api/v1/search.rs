use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::json;
use std::sync::Arc;

use crate::search::{SearchClient, SearchQuery};

pub fn router() -> Router<Arc<SearchClient>> {
    Router::new().route("/", axum::routing::get(search_handler))
}

async fn search_handler(
    State(client): State<Arc<SearchClient>>,
    Query(params): Query<SearchQuery>,
) -> impl IntoResponse {
    match client
        .search(&params.q, params.limit, params.item_type.as_deref())
        .await
    {
        Ok(result) => {
            let response = json!({
                "success": true,
                "data": result,
            });
            (StatusCode::OK, Json(response))
        }
        Err(e) => {
            tracing::error!("Search error: {}", e);
            let response = json!({
                "success": false,
                "error": "Search failed",
                "message": e.to_string(),
            });
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response))
        }
    }
}
