use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Song {
    pub id: String,
    pub name: String,
    pub artist: String,
    pub album: String,
    pub image: String,
    #[serde(rename = "disc_number")]
    pub disc_number: i32,
    #[serde(rename = "track_number")]
    pub track_number: i32,
    pub duration: i32,
    pub isrc: String,
    pub date: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artist {
    pub id: String,
    pub name: String,
    pub image: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Album {
    pub id: String,
    pub name: String,
    pub artist: String,
    pub image: String,
    pub date: String,
    #[serde(rename = "track_count")]
    pub track_count: i32,
    pub upc: String,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SearchResultItem {
    Song(Song),
    Artist(Artist),
    Album(Album),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResponse {
    pub data: Vec<SearchResultItem>,
    pub total: i64,
    pub limit: i32,
    pub offset: i32,
}
