use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use reqwest::Client;
use serde_json::{Map, Value, json};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::timeout;

const REPO: &str = "vleerapp/vleer";
const S3_BASE: &str = "https://vleer-releases.objects.eplg.cloud";
const PLATFORMS: [(&str, &str); 4] = [
    ("macos", "aarch64.dmg"),
    ("windows", "x86_64.msi"),
    ("linux", "x86_64.AppImage"),
    ("linux-aarch64", "aarch64.AppImage"),
];
const HEAD_TIMEOUT: Duration = Duration::from_secs(5);
const GITHUB_TTL: Duration = Duration::from_secs(60);

type CacheEntry = (String, Value, Map<String, Value>);

struct CacheData {
    entry: Option<CacheEntry>,
    last_checked: Option<Instant>,
}

#[derive(Clone)]
struct UpdateState {
    client: Client,
    cache: Arc<RwLock<CacheData>>,
}

pub fn router() -> Router {
    let state = UpdateState {
        client: Client::new(),
        cache: Arc::new(RwLock::new(CacheData {
            entry: None,
            last_checked: None,
        })),
    };
    Router::new()
        .route("/check", get(update_handler))
        .with_state(state)
}

fn error_response(status: StatusCode, message: &str) -> (StatusCode, Json<Value>) {
    (
        status,
        Json(json!({ "error": { "status": status.as_u16(), "message": message } })),
    )
}

async fn fetch_platforms(client: Client, version: &str) -> Map<String, Value> {
    let handles: Vec<_> = PLATFORMS
        .iter()
        .map(|(key, suffix)| {
            let client = client.clone();
            let url = format!("{S3_BASE}/Vleer-{version}-{suffix}");
            let key = key.to_string();
            tokio::spawn(async move {
                let result = timeout(HEAD_TIMEOUT, client.head(&url).send()).await;
                (key, url, result)
            })
        })
        .collect();

    let mut platforms = Map::new();
    for handle in handles {
        let Ok((key, url, result)) = handle.await else {
            continue;
        };
        let resp = match result {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                tracing::warn!("update: head request failed for {}: {}", url, e);
                continue;
            }
            Err(_) => {
                tracing::warn!("update: head request timed out for {}", url);
                continue;
            }
        };
        if !resp.status().is_success() {
            continue;
        }
        let size = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        let etag = resp
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        platforms.insert(key, json!({ "url": url, "size": size, "etag": etag }));
    }
    platforms
}

async fn update_handler(State(state): State<UpdateState>) -> impl IntoResponse {
    let client = state.client.clone();

    {
        let cache = state.cache.read().await;
        if cache
            .last_checked
            .is_some_and(|t| t.elapsed() < GITHUB_TTL)
        {
            return match &cache.entry {
                Some((_, release, platforms)) => serve_response(release, platforms),
                None => error_response(StatusCode::NOT_FOUND, "No releases found")
                    .into_response(),
            };
        }
    }

    let etag = state
        .cache
        .read()
        .await
        .entry
        .as_ref()
        .map(|(e, _, _)| e.clone());

    let mut req = client
        .get(format!(
            "https://api.github.com/repos/{REPO}/releases/latest"
        ))
        .header("User-Agent", "vleer-api")
        .header("Accept", "application/vnd.github+json");

    if let Some(ref e) = etag {
        req = req.header("If-None-Match", e);
    }

    match req.send().await {
        Ok(resp) if resp.status() == reqwest::StatusCode::NOT_MODIFIED => {
            let mut cache = state.cache.write().await;
            cache.last_checked = Some(Instant::now());
            let (_, release, platforms) = cache.entry.as_ref().unwrap();
            serve_response(release, platforms)
        }
        Ok(resp) if resp.status().is_success() => {
            let new_etag = resp
                .headers()
                .get(reqwest::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            let release: Value = match resp.json().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!("update: failed to parse release: {}", e);
                    state.cache.write().await.last_checked = Some(Instant::now());
                    return error_response(
                        StatusCode::BAD_GATEWAY,
                        "Failed to parse release metadata",
                    )
                    .into_response();
                }
            };

            let tag = release.get("tag_name").and_then(Value::as_str).unwrap_or("");
            let version = tag.strip_prefix('v').unwrap_or(tag);
            if version.is_empty() {
                state.cache.write().await.last_checked = Some(Instant::now());
                return error_response(StatusCode::BAD_GATEWAY, "Release missing version")
                    .into_response();
            }

            let platforms = fetch_platforms(client.clone(), version).await;

            let mut cache = state.cache.write().await;
            cache.last_checked = Some(Instant::now());
            if let Some(etag) = new_etag {
                cache.entry = Some((etag, release.clone(), platforms.clone()));
            }

            serve_response(&release, &platforms)
        }
        Ok(resp) if resp.status() == reqwest::StatusCode::NOT_FOUND => {
            state.cache.write().await.last_checked = Some(Instant::now());
            error_response(StatusCode::NOT_FOUND, "No releases found").into_response()
        }
        Ok(resp) => {
            tracing::error!("update: github returned {}", resp.status());
            let mut cache = state.cache.write().await;
            cache.last_checked = Some(Instant::now());
            match &cache.entry {
                Some((_, release, platforms)) => serve_response(release, platforms),
                None => error_response(StatusCode::BAD_GATEWAY, "Failed to fetch release")
                    .into_response(),
            }
        }
        Err(e) => {
            tracing::error!("update: github request failed: {}", e);
            let mut cache = state.cache.write().await;
            cache.last_checked = Some(Instant::now());
            match &cache.entry {
                Some((_, release, platforms)) => serve_response(release, platforms),
                None => error_response(StatusCode::BAD_GATEWAY, "Failed to fetch release")
                    .into_response(),
            }
        }
    }
}

fn serve_response(release: &Value, platforms: &Map<String, Value>) -> axum::response::Response {
    let tag = release
        .get("tag_name")
        .and_then(Value::as_str)
        .unwrap_or("");
    let version = tag.strip_prefix('v').unwrap_or(tag);

    let pub_date = release
        .get("published_at")
        .and_then(Value::as_str)
        .and_then(|s| s.split('T').next())
        .unwrap_or("")
        .to_string();

    let notes_url = release
        .get("html_url")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();

    if platforms.is_empty() {
        return error_response(StatusCode::NOT_FOUND, "No release assets available")
            .into_response();
    }

    (
        StatusCode::OK,
        Json(json!({
            "version": version,
            "pub_date": pub_date,
            "notes_url": notes_url,
            "platforms": platforms,
        })),
    )
        .into_response()
}
