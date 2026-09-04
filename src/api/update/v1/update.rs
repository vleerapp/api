use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use regex::Regex;
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::timeout;

static SEMVER_RE: OnceLock<Regex> = OnceLock::new();
static SEMVER_PRERELEASE_RE: OnceLock<Regex> = OnceLock::new();
static CONTENTS_RE: OnceLock<Regex> = OnceLock::new();
static KEY_RE: OnceLock<Regex> = OnceLock::new();
static SIZE_RE: OnceLock<Regex> = OnceLock::new();
static MODIFIED_RE: OnceLock<Regex> = OnceLock::new();

fn is_semver(v: &str) -> bool {
    SEMVER_RE
        .get_or_init(|| Regex::new(r"^\d+\.\d+\.\d+$").unwrap())
        .is_match(v)
}

fn is_semver_prerelease(v: &str) -> bool {
    SEMVER_PRERELEASE_RE
        .get_or_init(|| {
            Regex::new(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$").unwrap()
        })
        .is_match(v)
}

const REPO: &str = "vleerapp/vleer";
const S3_BASE: &str = "https://vleer-releases.objects.eplg.cloud";
const RELEASE_PREFIX: &str = "release";
const NIGHTLY_PREFIX: &str = "nightly";
const NIGHTLY_NOTES_URL: &str = "https://github.com/vleerapp/vleer/actions/workflows/nightly.yml";
const PLATFORMS: [(&str, &str); 6] = [
    ("macos-aarch64", "aarch64.dmg"),
    ("macos-x86_64", "x86_64.dmg"),
    ("windows-x86_64", "x86_64.msi"),
    ("windows-aarch64", "aarch64.msi"),
    ("linux-x86_64", "x86_64.tar.gz"),
    ("linux-aarch64", "aarch64.tar.gz"),
];
const ALIASES: [(&str, &str); 3] = [
    ("macos", "macos-aarch64"),
    ("windows", "windows-x86_64"),
    ("linux", "linux-x86_64"),
];
const S3_TIMEOUT: Duration = Duration::from_secs(5);
const CACHE_TTL: Duration = Duration::from_secs(60);
const MAX_VERSION_LEN: usize = 64;
const MAX_LISTING_BYTES: usize = 1_000_000;

type CacheEntry = (String, Value, Map<String, Value>);

struct NightlyEntry {
    version: String,
    pub_date: String,
    platforms: Map<String, Value>,
}

struct NightlyAsset {
    version: String,
    platform: &'static str,
    url: String,
    size: Option<u64>,
    last_modified: Option<String>,
}

#[derive(Default)]
struct CacheData {
    stable: Option<CacheEntry>,
    stable_checked: Option<Instant>,
    nightly: Option<NightlyEntry>,
    nightly_checked: Option<Instant>,
}

#[derive(Deserialize)]
struct CheckQuery {
    nightly: Option<bool>,
}

#[derive(Clone)]
struct UpdateState {
    client: Client,
    cache: Arc<RwLock<CacheData>>,
}

pub fn router() -> Router {
    let state = UpdateState {
        client: Client::new(),
        cache: Arc::new(RwLock::new(CacheData::default())),
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

async fn fetch_platforms(client: Client, prefix: &str, version: &str) -> Map<String, Value> {
    let handles: Vec<_> = PLATFORMS
        .iter()
        .map(|(key, suffix)| {
            let client = client.clone();
            let url = format!("{S3_BASE}/{prefix}/Vleer-{version}-{suffix}");
            let key = key.to_string();
            tokio::spawn(async move {
                let result = timeout(S3_TIMEOUT, client.head(&url).send()).await;
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
        platforms.insert(key, json!({ "url": url, "size": size }));
    }

    apply_aliases(&mut platforms);
    platforms
}

fn apply_aliases(platforms: &mut Map<String, Value>) {
    for (alias, target) in ALIASES {
        if let Some(entry) = platforms.get(target).cloned() {
            platforms.insert(alias.to_string(), entry);
        }
    }
}

async fn update_handler(
    State(state): State<UpdateState>,
    Query(query): Query<CheckQuery>,
) -> impl IntoResponse {
    if query.nightly.unwrap_or(false) {
        nightly_response(state).await
    } else {
        stable_response(state).await
    }
}

async fn stable_response(state: UpdateState) -> Response {
    let client = state.client.clone();

    {
        let cache = state.cache.read().await;
        if cache
            .stable_checked
            .is_some_and(|t| t.elapsed() < CACHE_TTL)
        {
            return match &cache.stable {
                Some((_, release, platforms)) => serve_release(release, platforms),
                None => error_response(StatusCode::NOT_FOUND, "No releases found").into_response(),
            };
        }
    }

    let etag = state
        .cache
        .read()
        .await
        .stable
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
            cache.stable_checked = Some(Instant::now());
            match &cache.stable {
                Some((_, release, platforms)) => serve_release(release, platforms),
                None => error_response(StatusCode::NOT_FOUND, "No releases found").into_response(),
            }
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
                    state.cache.write().await.stable_checked = Some(Instant::now());
                    return error_response(
                        StatusCode::BAD_GATEWAY,
                        "Failed to parse release metadata",
                    )
                    .into_response();
                }
            };

            let tag = release
                .get("tag_name")
                .and_then(Value::as_str)
                .unwrap_or("");
            let version = tag.strip_prefix('v').unwrap_or(tag);
            if !is_semver(version) {
                state.cache.write().await.stable_checked = Some(Instant::now());
                return error_response(StatusCode::BAD_GATEWAY, "Release missing valid version")
                    .into_response();
            }

            let platforms = fetch_platforms(client.clone(), RELEASE_PREFIX, version).await;

            let mut cache = state.cache.write().await;
            cache.stable_checked = Some(Instant::now());
            if let Some(etag) = new_etag {
                cache.stable = Some((etag, release.clone(), platforms.clone()));
            }

            serve_release(&release, &platforms)
        }
        Ok(resp) if resp.status() == reqwest::StatusCode::NOT_FOUND => {
            state.cache.write().await.stable_checked = Some(Instant::now());
            error_response(StatusCode::NOT_FOUND, "No releases found").into_response()
        }
        Ok(resp) => {
            tracing::error!("update: github returned {}", resp.status());
            stale_stable(&state).await
        }
        Err(e) => {
            tracing::error!("update: github request failed: {}", e);
            stale_stable(&state).await
        }
    }
}

async fn stale_stable(state: &UpdateState) -> Response {
    let mut cache = state.cache.write().await;
    cache.stable_checked = Some(Instant::now());
    match &cache.stable {
        Some((_, release, platforms)) => serve_release(release, platforms),
        None => error_response(StatusCode::BAD_GATEWAY, "Failed to fetch release").into_response(),
    }
}

async fn nightly_response(state: UpdateState) -> Response {
    let client = state.client.clone();

    {
        let cache = state.cache.read().await;
        if cache
            .nightly_checked
            .is_some_and(|t| t.elapsed() < CACHE_TTL)
        {
            return match &cache.nightly {
                Some(entry) => serve_nightly(entry),
                None => {
                    error_response(StatusCode::NOT_FOUND, "No nightly builds found").into_response()
                }
            };
        }
    }

    let url = format!("{S3_BASE}/?list-type=2&prefix={NIGHTLY_PREFIX}/");
    let resp = match timeout(S3_TIMEOUT, client.get(&url).send()).await {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => {
            tracing::error!("update: nightly listing request failed: {}", e);
            return stale_nightly(&state, "Failed to list nightly builds").await;
        }
        Err(_) => {
            tracing::error!("update: nightly listing request timed out");
            return stale_nightly(&state, "Failed to list nightly builds").await;
        }
    };

    if !resp.status().is_success() {
        tracing::error!("update: nightly listing returned {}", resp.status());
        return stale_nightly(&state, "Failed to list nightly builds").await;
    }

    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("update: failed to read nightly listing: {}", e);
            return stale_nightly(&state, "Failed to list nightly builds").await;
        }
    };

    if body.len() > MAX_LISTING_BYTES {
        tracing::error!("update: nightly listing too large ({} bytes)", body.len());
        return stale_nightly(&state, "Failed to list nightly builds").await;
    }

    let Some(entry) = nightly_entry_from_listing(&body) else {
        state.cache.write().await.nightly_checked = Some(Instant::now());
        return error_response(StatusCode::NOT_FOUND, "No nightly builds found").into_response();
    };

    let response = serve_nightly(&entry);

    let mut cache = state.cache.write().await;
    cache.nightly_checked = Some(Instant::now());
    cache.nightly = Some(entry);

    response
}

async fn stale_nightly(state: &UpdateState, message: &str) -> Response {
    let mut cache = state.cache.write().await;
    cache.nightly_checked = Some(Instant::now());
    match &cache.nightly {
        Some(entry) => serve_nightly(entry),
        None => error_response(StatusCode::BAD_GATEWAY, message).into_response(),
    }
}

fn pub_date_from_version(version: &str) -> Option<String> {
    let (_, prerelease) = version.split_once('-')?;
    let stamp = prerelease
        .split('.')
        .find(|s| s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit()))?;
    Some(format!(
        "{}-{}-{}",
        &stamp[0..4],
        &stamp[4..6],
        &stamp[6..8]
    ))
}

fn field(block: &str, cell: &'static OnceLock<Regex>, pattern: &str) -> Option<String> {
    cell.get_or_init(|| Regex::new(pattern).unwrap())
        .captures(block)
        .map(|c| c[1].to_string())
}

fn version_rank(version: &str) -> Vec<u64> {
    version
        .split(|c: char| !c.is_ascii_digit())
        .filter(|s| !s.is_empty())
        .filter_map(|s| s.parse::<u64>().ok())
        .collect()
}

fn nightly_entry_from_listing(xml: &str) -> Option<NightlyEntry> {
    let contents =
        CONTENTS_RE.get_or_init(|| Regex::new(r"(?s)<Contents>(.*?)</Contents>").unwrap());

    let mut assets: Vec<NightlyAsset> = Vec::new();

    for block in contents.captures_iter(xml) {
        let block = &block[1];
        let Some(key) = field(block, &KEY_RE, r"<Key>([^<]*)</Key>") else {
            continue;
        };
        let Some(rest) = key.strip_prefix(&format!("{NIGHTLY_PREFIX}/Vleer-")) else {
            continue;
        };
        for (platform, suffix) in PLATFORMS {
            let Some(version) = rest.strip_suffix(&format!("-{suffix}")) else {
                continue;
            };
            if version.len() > MAX_VERSION_LEN || !is_semver_prerelease(version) {
                continue;
            }
            assets.push(NightlyAsset {
                version: version.to_string(),
                platform,
                url: format!("{S3_BASE}/{key}"),
                size: field(block, &SIZE_RE, r"<Size>(\d+)</Size>").and_then(|s| s.parse().ok()),
                last_modified: field(block, &MODIFIED_RE, r"<LastModified>([^<]*)</LastModified>"),
            });
            break;
        }
    }

    let newest = assets
        .iter()
        .map(|a| a.version.clone())
        .max_by_key(|v| version_rank(v))?;

    let mut platforms = Map::new();
    let mut modified: Option<String> = None;
    for asset in assets.iter().filter(|a| a.version == newest) {
        platforms.insert(
            asset.platform.to_string(),
            json!({ "url": asset.url, "size": asset.size }),
        );
        if asset.last_modified.as_deref() > modified.as_deref() {
            modified.clone_from(&asset.last_modified);
        }
    }
    apply_aliases(&mut platforms);

    let pub_date = pub_date_from_version(&newest)
        .or_else(|| {
            modified
                .as_deref()
                .and_then(|m| m.split('T').next())
                .map(str::to_string)
        })
        .unwrap_or_default();

    Some(NightlyEntry {
        version: newest,
        pub_date,
        platforms,
    })
}

fn serve_release(release: &Value, platforms: &Map<String, Value>) -> Response {
    let tag = release
        .get("tag_name")
        .and_then(Value::as_str)
        .unwrap_or("");
    let version = tag.strip_prefix('v').unwrap_or(tag);

    let pub_date = release
        .get("published_at")
        .and_then(Value::as_str)
        .and_then(|s| s.split('T').next())
        .unwrap_or("");

    let notes_url = release
        .get("html_url")
        .and_then(Value::as_str)
        .unwrap_or("");

    serve_response(version, pub_date, notes_url, platforms)
}

fn serve_nightly(entry: &NightlyEntry) -> Response {
    serve_response(
        &entry.version,
        &entry.pub_date,
        NIGHTLY_NOTES_URL,
        &entry.platforms,
    )
}

fn serve_response(
    version: &str,
    pub_date: &str,
    notes_url: &str,
    platforms: &Map<String, Value>,
) -> Response {
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
