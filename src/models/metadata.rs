use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Artist {
    pub id: String,
    pub name: String,
    pub image: String,
    pub genres: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Song {
    pub id: String,
    pub name: String,
    pub artist: Vec<Artist>,
    pub album: Vec<Album>,
    pub genres: Vec<String>,
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
pub struct Album {
    pub id: String,
    pub name: String,
    pub artist: Vec<Artist>,
    pub genres: Vec<String>,
    pub image: String,
    pub date: String,
    #[serde(rename = "track_count")]
    pub track_count: i32,
    pub upc: String,
    pub label: Option<String>,
}
