use serde_json::{Map, Value, json};

use crate::models::metadata::{Album, Artist, Song};

fn put_str(map: &mut Map<String, Value>, key: &str, val: &str) {
    if !val.is_empty() {
        map.insert(key.to_string(), json!(val));
    }
}

fn put_int(map: &mut Map<String, Value>, key: &str, val: i64) {
    if val > 0 {
        map.insert(key.to_string(), json!(val));
    }
}

fn put_genres(map: &mut Map<String, Value>, genres: &[String]) {
    if !genres.is_empty() {
        map.insert("genres".to_string(), json!(genres));
    }
}

fn rel_list(items: Vec<Value>) -> Value {
    json!({ "data": items })
}

fn artist_names(artists: &[Artist]) -> Vec<String> {
    artists.iter().map(|x| x.name.clone()).collect()
}

pub fn render_artist(a: &Artist) -> Value {
    let mut attrs = Map::new();
    attrs.insert("name".to_string(), json!(a.name));
    put_str(&mut attrs, "artworkUrl", &a.image);
    put_genres(&mut attrs, &a.genres);
    json!({
        "id": format!("omm:artist:{}", a.id),
        "type": "artist",
        "attributes": Value::Object(attrs),
    })
}

pub fn render_album(a: &Album) -> Value {
    render_album_inner(a, true)
}

fn render_album_inner(a: &Album, with_rels: bool) -> Value {
    let mut attrs = Map::new();
    attrs.insert("name".to_string(), json!(a.name));
    attrs.insert("trackCount".to_string(), json!(a.track_count as i64));
    attrs.insert("artistNames".to_string(), json!(artist_names(&a.artist)));
    put_str(&mut attrs, "artworkUrl", &a.image);
    put_str(&mut attrs, "upc", a.upc.as_deref().unwrap_or_default());
    put_str(&mut attrs, "label", a.label.as_deref().unwrap_or_default());
    put_genres(&mut attrs, &a.genres);
    put_str(&mut attrs, "releaseDate", &a.date);

    let mut resource = Map::new();
    resource.insert("id".to_string(), json!(format!("omm:album:{}", a.id)));
    resource.insert("type".to_string(), json!("album"));
    resource.insert("attributes".to_string(), Value::Object(attrs));

    if with_rels {
        let items = a.artist.iter().map(render_artist).collect();
        let mut rels = Map::new();
        rels.insert("artists".to_string(), rel_list(items));
        resource.insert("relationships".to_string(), Value::Object(rels));
    }
    Value::Object(resource)
}

pub fn render_song(s: &Song) -> Value {
    let album_name = s.album.first().map(|x| x.name.clone()).unwrap_or_default();

    let mut attrs = Map::new();
    attrs.insert("name".to_string(), json!(s.name));
    put_str(&mut attrs, "albumName", &album_name);
    attrs.insert("artistNames".to_string(), json!(artist_names(&s.artist)));
    put_str(&mut attrs, "isrc", s.isrc.as_deref().unwrap_or_default());
    put_str(&mut attrs, "artworkUrl", &s.image);
    put_int(&mut attrs, "trackNumber", s.track_number as i64);
    put_int(&mut attrs, "discNumber", s.disc_number as i64);
    put_genres(&mut attrs, &s.genres);
    put_str(&mut attrs, "releaseDate", &s.date);
    if s.duration > 0 {
        attrs.insert("durationMs".to_string(), json!(s.duration));
    }

    let mut resource = Map::new();
    resource.insert("id".to_string(), json!(format!("omm:song:{}", s.id)));
    resource.insert("type".to_string(), json!("song"));
    resource.insert("attributes".to_string(), Value::Object(attrs));

    let mut rels = Map::new();
    rels.insert(
        "albums".to_string(),
        rel_list(s.album.iter().map(|x| render_album_inner(x, false)).collect()),
    );
    rels.insert(
        "artists".to_string(),
        rel_list(s.artist.iter().map(render_artist).collect()),
    );
    resource.insert("relationships".to_string(), Value::Object(rels));
    Value::Object(resource)
}
