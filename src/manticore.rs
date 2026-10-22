use anyhow::{Result, anyhow};
use reqwest::Client;
use serde_json::Value;
use std::time::Duration;
use tokio::sync::Semaphore;

const MAX_CONCURRENT_SEARCHES: usize = 16;
const QUEUE_TIMEOUT: Duration = Duration::from_secs(3);
const HTTP_TIMEOUT: Duration = Duration::from_secs(8);
const MAX_QUERY_TIME_MS: u32 = 5_000;

pub struct Manticore {
    http: Client,
    url: String,
    permits: Semaphore,
}

pub struct Hit {
    pub id: String,
    pub name: String,
    pub artist: String,
    pub album: String,
    pub popularity: f64,
}

impl Manticore {
    pub fn new(url: &str) -> Result<Self> {
        let http = Client::builder()
            .timeout(HTTP_TIMEOUT)
            .connect_timeout(Duration::from_secs(2))
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .map_err(|e| anyhow!("failed to build manticore client: {e}"))?;
        Ok(Self {
            http,
            url: url.trim_end_matches('/').to_string(),
            permits: Semaphore::new(MAX_CONCURRENT_SEARCHES),
        })
    }

    async fn rows(&self, sql: &str) -> Result<Vec<Value>> {
        let _permit = tokio::time::timeout(QUEUE_TIMEOUT, self.permits.acquire())
            .await
            .map_err(|_| anyhow!("manticore queue timeout"))??;
        let resp = self
            .http
            .post(format!("{}/sql?mode=raw", self.url))
            .form(&[("query", sql)])
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await?;
        if !status.is_success() {
            return Err(anyhow!("manticore {status}: {text}"));
        }
        let parsed: Value = serde_json::from_str(&text)?;
        let first = parsed
            .get(0)
            .ok_or_else(|| anyhow!("manticore empty response: {text}"))?;
        if let Some(err) = first["error"].as_str().filter(|e| !e.is_empty()) {
            return Err(anyhow!("manticore error: {err}"));
        }
        Ok(first["data"].as_array().cloned().unwrap_or_default())
    }

    pub async fn search(
        &self,
        table: &str,
        expression: &str,
        fuzzy: bool,
        limit: i64,
    ) -> Result<Vec<Hit>> {
        let options = if fuzzy {
            format!("ranker=sph04, fuzzy=1, distance=1, max_query_time={MAX_QUERY_TIME_MS}")
        } else {
            format!("ranker=sph04, max_query_time={MAX_QUERY_TIME_MS}")
        };
        let sql = format!(
            "SELECT doc_id, name, artist_names, album_name, popularity FROM {table} \
             WHERE MATCH('{}') ORDER BY weight() DESC, popularity DESC LIMIT {} OPTION {options}",
            escape(expression),
            limit.clamp(1, 1000),
        );
        let rows = self.rows(&sql).await?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                Some(Hit {
                    id: r["doc_id"].as_str()?.to_string(),
                    name: r["name"].as_str().unwrap_or_default().to_string(),
                    artist: r["artist_names"].as_str().unwrap_or_default().to_string(),
                    album: r["album_name"].as_str().unwrap_or_default().to_string(),
                    popularity: r["popularity"].as_f64().unwrap_or(0.0),
                })
            })
            .collect())
    }

    pub async fn exists(&self, table: &str, expression: &str) -> Result<bool> {
        let sql = format!(
            "SELECT doc_id FROM {table} WHERE MATCH('{}') LIMIT 1",
            escape(expression)
        );
        Ok(!self.rows(&sql).await?.is_empty())
    }
}

fn escape(text: &str) -> String {
    text.replace('\\', "\\\\").replace('\'', "\\'")
}

pub fn words(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
        .map(str::to_lowercase)
        .collect()
}
