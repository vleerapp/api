use axum::{
    Json, Router,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use regex::Regex;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tokio::time::timeout;

static SEMVER_RE: OnceLock<Regex> = OnceLock::new();
static CONTENTS_RE: OnceLock<Regex> = OnceLock::new();
static KEY_RE: OnceLock<Regex> = OnceLock::new();

const S3_BASE: &str = "https://vleer-releases.objects.eplg.cloud";
const RELEASE_PREFIX: &str = "release";
const OSES: [(&str, &str); 3] = [("macos", "dmg"), ("windows", "msi"), ("linux", "AppImage")];
const ARCHES: [&str; 2] = ["aarch64", "x86_64"];
const S3_TIMEOUT: Duration = Duration::from_secs(5);
const CACHE_TTL: Duration = Duration::from_secs(60);
const MAX_VERSION_LEN: usize = 64;
const MAX_LISTING_BYTES: usize = 1_000_000;

struct Asset {
    version: String,
    os: &'static str,
    arch: &'static str,
    url: String,
}

struct ReleaseEntry {
    assets: Vec<Asset>,
}

#[derive(Deserialize)]
struct ReleaseQuery {
    os: String,
    arch: String,
}

#[derive(Clone)]
struct ReleasesState {
    client: Client,
    cache: Arc<RwLock<Cache>>,
}

#[derive(Default)]
struct Cache {
    entry: Option<Arc<ReleaseEntry>>,
    checked: Option<Instant>,
}

pub fn router() -> Router {
    let state = ReleasesState {
        client: Client::new(),
        cache: Arc::new(RwLock::new(Cache::default())),
    };
    Router::new()
        .route("/v1", get(releases_handler))
        .route("/v1/", get(releases_handler))
        .with_state(state)
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({ "error": { "status": status.as_u16(), "message": message } })),
    )
        .into_response()
}

fn is_semver(v: &str) -> bool {
    SEMVER_RE
        .get_or_init(|| Regex::new(r"^\d+\.\d+\.\d+$").unwrap())
        .is_match(v)
}

fn field(block: &str, cell: &'static OnceLock<Regex>, pattern: &str) -> Option<String> {
    cell.get_or_init(|| Regex::new(pattern).unwrap())
        .captures(block)
        .map(|c| c[1].to_string())
}

fn version_rank(version: &str) -> Vec<u64> {
    version
        .split('.')
        .filter_map(|s| s.parse::<u64>().ok())
        .collect()
}

fn normalize_os(os: &str) -> Option<(&'static str, &'static str)> {
    let os = os.to_ascii_lowercase();
    let os = match os.as_str() {
        "mac" | "osx" | "darwin" => "macos",
        "win" => "windows",
        other => other,
    };
    OSES.into_iter().find(|(name, _)| *name == os)
}

fn normalize_arch(arch: &str) -> Option<&'static str> {
    let arch = arch.to_ascii_lowercase();
    let arch = match arch.as_str() {
        "arm64" | "arm" => "aarch64",
        "x64" | "amd64" | "x86-64" => "x86_64",
        other => other,
    };
    ARCHES.into_iter().find(|a| *a == arch)
}

async fn releases_handler(
    State(state): State<ReleasesState>,
    Query(query): Query<ReleaseQuery>,
) -> Response {
    let Some((os, _)) = normalize_os(&query.os) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "Unsupported os, expected one of: macos, windows, linux",
        );
    };
    let Some(arch) = normalize_arch(&query.arch) else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "Unsupported arch, expected one of: aarch64, x86_64",
        );
    };

    let entry = match latest_release(&state).await {
        Ok(entry) => entry,
        Err(message) => return error_response(StatusCode::BAD_GATEWAY, &message),
    };

    let Some(asset) = entry.assets.iter().find(|a| a.os == os && a.arch == arch) else {
        return error_response(
            StatusCode::NOT_FOUND,
            "No release asset for this os and arch",
        );
    };

    let Ok(location) = header::HeaderValue::from_str(&asset.url) else {
        tracing::error!("releases: invalid asset url {}", asset.url);
        return error_response(StatusCode::BAD_GATEWAY, "Invalid release asset url");
    };

    (
        StatusCode::FOUND,
        [
            (header::LOCATION, location),
            (
                header::CACHE_CONTROL,
                header::HeaderValue::from_static("no-store"),
            ),
        ],
    )
        .into_response()
}

async fn latest_release(state: &ReleasesState) -> Result<Arc<ReleaseEntry>, String> {
    {
        let cache = state.cache.read().await;
        if cache.checked.is_some_and(|t| t.elapsed() < CACHE_TTL) {
            return cache
                .entry
                .clone()
                .ok_or_else(|| "No releases found".to_string());
        }
    }

    let url = format!("{S3_BASE}/?list-type=2&prefix={RELEASE_PREFIX}/");
    let listing = match timeout(S3_TIMEOUT, state.client.get(&url).send()).await {
        Ok(Ok(resp)) if resp.status().is_success() => resp.text().await.map_err(|e| {
            tracing::error!("releases: failed to read listing: {}", e);
            "Failed to list releases".to_string()
        }),
        Ok(Ok(resp)) => {
            tracing::error!("releases: listing returned {}", resp.status());
            Err("Failed to list releases".to_string())
        }
        Ok(Err(e)) => {
            tracing::error!("releases: listing request failed: {}", e);
            Err("Failed to list releases".to_string())
        }
        Err(_) => {
            tracing::error!("releases: listing request timed out");
            Err("Failed to list releases".to_string())
        }
    };

    let body = match listing {
        Ok(body) if body.len() > MAX_LISTING_BYTES => {
            tracing::error!("releases: listing too large ({} bytes)", body.len());
            return stale(state, "Failed to list releases").await;
        }
        Ok(body) => body,
        Err(message) => return stale(state, &message).await,
    };

    let mut cache = state.cache.write().await;
    cache.checked = Some(Instant::now());
    cache.entry = entry_from_listing(&body).map(Arc::new);
    cache
        .entry
        .clone()
        .ok_or_else(|| "No releases found".to_string())
}

async fn stale(state: &ReleasesState, message: &str) -> Result<Arc<ReleaseEntry>, String> {
    let mut cache = state.cache.write().await;
    cache.checked = Some(Instant::now());
    cache.entry.clone().ok_or_else(|| message.to_string())
}

fn entry_from_listing(xml: &str) -> Option<ReleaseEntry> {
    let contents =
        CONTENTS_RE.get_or_init(|| Regex::new(r"(?s)<Contents>(.*?)</Contents>").unwrap());

    let mut assets: Vec<Asset> = Vec::new();

    for block in contents.captures_iter(xml) {
        let block = &block[1];
        let Some(key) = field(block, &KEY_RE, r"<Key>([^<]*)</Key>") else {
            continue;
        };
        let Some(rest) = key.strip_prefix(&format!("{RELEASE_PREFIX}/Vleer-")) else {
            continue;
        };
        for (os, extension) in OSES {
            let Some(rest) = rest.strip_suffix(&format!(".{extension}")) else {
                continue;
            };
            let Some(arch) = ARCHES
                .into_iter()
                .find(|a| rest.ends_with(&format!("-{a}")))
            else {
                continue;
            };
            let version = &rest[..rest.len() - arch.len() - 1];
            if version.len() > MAX_VERSION_LEN || !is_semver(version) {
                continue;
            }
            assets.push(Asset {
                version: version.to_string(),
                os,
                arch,
                url: format!("{S3_BASE}/{key}"),
            });
            break;
        }
    }

    let newest = assets
        .iter()
        .map(|a| a.version.clone())
        .max_by_key(|v| version_rank(v))?;

    assets.retain(|a| a.version == newest);

    Some(ReleaseEntry { assets })
}
