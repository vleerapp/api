use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Song {
    pub id: String,
    pub name: String,
    pub album: String,
    pub artist: String,
    pub cover: String,
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
    pub cover: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Album {
    pub id: String,
    #[serde(rename = "artist_name")]
    pub artist_name: String,
    pub name: String,
    #[serde(rename = "artwork_url")]
    pub artwork_url: String,
    #[serde(rename = "release_date")]
    pub release_date: String,
    #[serde(rename = "track_count")]
    pub track_count: i32,
    pub upc: String,
    #[serde(rename = "record_label")]
    pub record_label: Option<String>,
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
