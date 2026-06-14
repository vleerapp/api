use anyhow::{Result, anyhow};
use reqwest::Client;

pub struct SearchClient {
    http: Client,
    url: String,
    index_name: String,
}

impl SearchClient {
    pub fn new(manticore_url: &str) -> Result<Self> {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .map_err(|e| anyhow!("failed to build http client: {e}"))?;
        Ok(Self {
            http,
            url: manticore_url.trim_end_matches('/').to_string(),
            index_name: "music".to_string(),
        })
    }

    async fn sql(&self, query: &str) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/sql", self.url))
            .form(&[("query", query)])
            .send()
            .await
            .map_err(|e| anyhow!("manticore request failed: {e}"))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| anyhow!("failed to read manticore response: {e}"))?;

        if !status.is_success() {
            return Err(anyhow!("manticore error {status}: {body}"));
        }

        serde_json::from_str(&body)
            .map_err(|e| anyhow!("failed to parse manticore response: {e}, body: {body}"))
    }

    async fn sql_raw(&self, query: &str) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/sql?mode=raw", self.url))
            .form(&[("query", query)])
            .send()
            .await
            .map_err(|e| anyhow!("manticore request failed: {e}"))?;

        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| anyhow!("failed to read manticore response: {e}"))?;

        if !status.is_success() {
            return Err(anyhow!("manticore error {status}: {body}"));
        }

        let parsed: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| anyhow!("failed to parse manticore response: {e}, body: {body}"))?;

        if let Some(err) = parsed[0]["error"].as_str()
            && !err.is_empty()
        {
            return Err(anyhow!("manticore sql error: {err}"));
        }

        Ok(parsed)
    }

    async fn search_json(&self, body: serde_json::Value) -> Result<serde_json::Value> {
        let resp = self
            .http
            .post(format!("{}/search", self.url))
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow!("manticore request failed: {e}"))?;

        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| anyhow!("failed to read manticore response: {e}"))?;

        if !status.is_success() {
            return Err(anyhow!("manticore error {status}: {text}"));
        }

        serde_json::from_str(&text)
            .map_err(|e| anyhow!("failed to parse manticore response: {e}, body: {text}"))
    }

    pub async fn create_index(&self) -> Result<()> {
        let create_sql = format!(
            r#"CREATE TABLE IF NOT EXISTS {} (
                doc_id string,
                name text,
                artist_name text,
                album_name text,
                item_type string,
                duration int,
                date string
            ) min_prefix_len='3'"#,
            self.index_name
        );

        let response = self.sql_raw(&create_sql).await?;
        tracing::info!("create table {} response: {}", self.index_name, response);
        Ok(())
    }

    pub async fn search(
        &self,
        item_type: &str,
        name: Option<&str>,
        artist: Option<&str>,
        album: Option<&str>,
        limit: i32,
        offset: i32,
    ) -> Result<Vec<(String, String)>> {
        let mut must: Vec<serde_json::Value> =
            vec![serde_json::json!({ "equals": { "item_type": item_type } })];
        if let Some(n) = name {
            must.push(serde_json::json!({ "match": { "name": n } }));
        }
        if let Some(a) = artist {
            must.push(serde_json::json!({ "match": { "artist_name": a } }));
        }
        if let Some(a) = album {
            must.push(serde_json::json!({ "match": { "album_name": a } }));
        }

        let body = serde_json::json!({
            "index": self.index_name,
            "query": { "bool": { "must": must } },
            "source": ["doc_id", "name"],
            "limit": limit,
            "offset": offset,
        });

        let response = self.search_json(body).await?;

        let empty_vec: Vec<serde_json::Value> = vec![];
        let hits = response["hits"]["hits"].as_array().unwrap_or(&empty_vec);

        let mut seen = std::collections::HashSet::new();
        let candidates: Vec<(String, String)> = hits
            .iter()
            .filter_map(|h| {
                let id = h["_source"]["doc_id"].as_str()?.to_string();
                let name = h["_source"]["name"].as_str().unwrap_or("").to_string();
                Some((id, name))
            })
            .filter(|(id, _)| seen.insert(id.clone()))
            .collect();

        Ok(candidates)
    }

    pub async fn count(&self) -> Result<i64> {
        let sql = format!("SELECT COUNT(*) as cnt FROM {}", self.index_name);
        let response = self.sql(&sql).await?;
        let empty_vec: Vec<serde_json::Value> = vec![];
        let hits = response["hits"]["hits"].as_array().unwrap_or(&empty_vec);

        if hits.is_empty() {
            return Ok(0);
        }

        Ok(hits[0]["_source"]["cnt"].as_i64().unwrap_or(0))
    }
}
