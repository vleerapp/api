use anyhow::Result;
use elasticsearch::{
    Elasticsearch, SearchParts,
    http::transport::{SingleNodeConnectionPool, TransportBuilder},
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone)]
pub struct SearchClient {
    client: Elasticsearch,
    index_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchableItem {
    pub id: String,
    pub apple_music_id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artist_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub album_name: Option<String>,
    pub artwork_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub release_date: Option<String>,
    pub item_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub items: Vec<SearchableItem>,
    pub total: i64,
    pub took_ms: i64,
}

#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub q: String,
    #[serde(default = "default_limit")]
    pub limit: i64,
    #[serde(default)]
    pub item_type: Option<String>,
}

fn default_limit() -> i64 {
    20
}

impl SearchClient {
    pub fn new(es_url: &str) -> Result<Self> {
        let pool = SingleNodeConnectionPool::new(es_url.parse()?);
        let transport = TransportBuilder::new(pool).build()?;
        let client = Elasticsearch::new(transport);

        Ok(Self {
            client,
            index_name: "music".to_string(),
        })
    }

    pub async fn create_index(&self) -> Result<()> {
        let exists = self
            .client
            .indices()
            .exists(elasticsearch::indices::IndicesExistsParts::Index(&[
                &self.index_name
            ]))
            .send()
            .await?;

        if exists.status_code().is_success() {
            tracing::info!("Index '{}' already exists", self.index_name);
            return Ok(());
        }

        let mapping = json!({
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 0,
                "analysis": {
                    "analyzer": {
                        "music_analyzer": {
                            "tokenizer": "standard",
                            "filter": ["lowercase", "asciifolding", "edge_ngram_filter"]
                        }
                    },
                    "filter": {
                        "edge_ngram_filter": {
                            "type": "edge_ngram",
                            "min_gram": 2,
                            "max_gram": 20
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "name": {
                        "type": "text",
                        "analyzer": "music_analyzer",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "artist_name": {"type": "text", "analyzer": "music_analyzer"},
                    "album_name": {"type": "text", "analyzer": "music_analyzer"},
                    "item_type": {"type": "keyword"},
                    "apple_music_id": {"type": "keyword"},
                    "artwork_url": {"type": "keyword", "index": false},
                    "duration_seconds": {"type": "integer", "index": false},
                    "release_date": {"type": "keyword", "index": false}
                }
            }
        });

        self.client
            .indices()
            .create(elasticsearch::indices::IndicesCreateParts::Index(
                &self.index_name,
            ))
            .body(mapping)
            .send()
            .await?;

        tracing::info!("Created index '{}'", self.index_name);
        Ok(())
    }

    pub async fn search(
        &self,
        query: &str,
        limit: i64,
        item_type: Option<&str>,
    ) -> Result<SearchResult> {
        let mut search_query = json!({
            "multi_match": {
                "query": query,
                "fields": ["name^3", "artist_name^2", "album_name"],
                "type": "best_fields",
                "fuzziness": "AUTO",
                "prefix_length": 2
            }
        });

        if let Some(t) = item_type {
            search_query = json!({
                "bool": {
                    "must": [search_query],
                    "filter": [{"term": {"item_type": t}}]
                }
            });
        }

        let search_body = json!({
            "query": search_query,
            "size": limit,
            "highlight": {
                "fields": {
                    "name": {},
                    "artist_name": {},
                    "album_name": {}
                }
            }
        });

        let response_body = self
            .client
            .search(SearchParts::Index(&[&self.index_name]))
            .body(search_body)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        let took = response_body["took"].as_i64().unwrap_or(0);
        let hits = &response_body["hits"]["hits"];
        let total = response_body["hits"]["total"]["value"]
            .as_i64()
            .unwrap_or(0);

        let items: Vec<SearchableItem> = hits
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|hit| serde_json::from_value(hit["_source"].clone()).ok())
            .collect();

        Ok(SearchResult {
            items,
            total,
            took_ms: took,
        })
    }

    pub async fn count(&self) -> Result<i64> {
        let response = self
            .client
            .count(elasticsearch::CountParts::Index(&[&self.index_name]))
            .send()
            .await?;

        let body = response.json::<serde_json::Value>().await?;
        Ok(body["count"].as_i64().unwrap_or(0))
    }
}
