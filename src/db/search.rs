use sqlx::{PgPool, Row};

use crate::manticore::{Manticore, words};

#[derive(Clone, Copy)]
pub struct Engine<'a> {
    pub pool: &'a PgPool,
    pub manticore: Option<&'a Manticore>,
}

fn engine_error(e: anyhow::Error) -> sqlx::Error {
    sqlx::Error::Io(std::io::Error::other(e.to_string()))
}

pub struct Candidate {
    pub id: String,
    pub name: String,
    pub artist: String,
    pub album: String,
    pub popularity_score: f64,
    pub duration: Option<i32>,
    pub album_tier: i32,
}

#[derive(Clone, Copy, PartialEq)]
enum Scope {
    Hot,
    Full,
}

#[derive(Clone, Copy)]
enum Mode {
    Fast,
    ExactAll,
    ExactAny,
    FuzzyName,
    FuzzyArtist,
    FuzzyBoth,
}

fn clause(column: &str, param: usize, op: &str, fuzzy: bool) -> String {
    if fuzzy {
        format!("{column} {op} ${param}::text::pdb.fuzzy(1, f, t)")
    } else {
        format!("{column} {op} ${param}::text")
    }
}

const FUZZY_MIN_LEN: usize = 4;
const FUZZY_MAX_TERMS: usize = 3;

fn fuzzy_terms(text: &str) -> String {
    let words: Vec<&str> = text.split_whitespace().collect();
    let mut long: Vec<(usize, usize)> = words
        .iter()
        .enumerate()
        .map(|(i, w)| (i, w.chars().filter(|c| c.is_alphanumeric()).count()))
        .filter(|&(_, n)| n >= FUZZY_MIN_LEN)
        .collect();
    if long.is_empty() {
        return text.to_string();
    }
    long.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    long.truncate(FUZZY_MAX_TERMS);
    long.sort_by_key(|&(i, _)| i);
    long.iter()
        .map(|&(i, _)| words[i])
        .collect::<Vec<_>>()
        .join(" ")
}

fn artist_matches(found: &[Candidate], wanted: Option<&str>) -> bool {
    wanted.is_none_or(|w| found.iter().any(|c| c.artist.to_lowercase().contains(w)))
}

pub async fn candidates(
    engine: Engine<'_>,
    item_type: &str,
    name: &str,
    artist: Option<&str>,
    limit: i64,
) -> Result<Vec<Candidate>, sqlx::Error> {
    find(engine, item_type, name, artist, limit).await
}

async fn find(
    engine: Engine<'_>,
    item_type: &str,
    name: &str,
    artist: Option<&str>,
    limit: i64,
) -> Result<Vec<Candidate>, sqlx::Error> {
    let mut known = None;
    if item_type == "artist" {
        return candidates_in(
            engine,
            Scope::Full,
            false,
            &mut known,
            item_type,
            name,
            artist,
            limit,
        )
        .await;
    }
    let wanted = artist.map(str::to_lowercase);
    let hot = candidates_in(
        engine,
        Scope::Hot,
        false,
        &mut known,
        item_type,
        name,
        artist,
        limit,
    )
    .await?;
    if !hot.is_empty() && artist_matches(&hot, wanted.as_deref()) {
        return Ok(hot);
    }
    let strict = !hot.is_empty();
    let full = candidates_in(
        engine,
        Scope::Full,
        strict,
        &mut known,
        item_type,
        name,
        artist,
        limit,
    )
    .await?;
    if full.is_empty() || (!hot.is_empty() && !artist_matches(&full, wanted.as_deref())) {
        return Ok(hot);
    }
    Ok(full)
}

#[allow(clippy::too_many_arguments)]
async fn candidates_in(
    engine: Engine<'_>,
    scope: Scope,
    strict: bool,
    artist_known: &mut Option<bool>,
    item_type: &str,
    name: &str,
    artist: Option<&str>,
    limit: i64,
) -> Result<Vec<Candidate>, sqlx::Error> {
    let wanted_artist = artist.map(str::to_lowercase);
    let mut fallback = Vec::new();
    let mut fast = Vec::new();

    if wanted_artist.is_some() && item_type != "artist" {
        let found = run(engine, scope, item_type, name, artist, limit, Mode::Fast).await?;
        let wanted = wanted_artist.as_deref().unwrap_or_default();
        if found
            .iter()
            .any(|c| c.artist.to_lowercase().contains(wanted))
        {
            return Ok(found);
        }
        fast = found;
    }

    let single_word = name.split_whitespace().nth(1).is_none();
    for mode in [Mode::ExactAll, Mode::ExactAny] {
        if single_word && matches!(mode, Mode::ExactAny) {
            continue;
        }
        let found = if matches!(mode, Mode::ExactAll) && wanted_artist.is_some() {
            let mut merged = run(engine, scope, item_type, name, None, limit, mode).await?;
            let seen: std::collections::HashSet<String> =
                merged.iter().map(|c| c.id.clone()).collect();
            merged.extend(
                std::mem::take(&mut fast)
                    .into_iter()
                    .filter(|c| !seen.contains(&c.id)),
            );
            merged.sort_by(|a, b| a.id.cmp(&b.id));
            merged
        } else {
            run(engine, scope, item_type, name, artist, limit, mode).await?
        };
        if found.is_empty() {
            continue;
        }
        let artist_matched = wanted_artist
            .as_ref()
            .is_none_or(|a| found.iter().any(|c| c.artist.to_lowercase().contains(a)));
        if artist_matched {
            return Ok(found);
        }
        if fallback.is_empty() {
            fallback = found;
        }
    }

    let artist_exists = match (*artist_known, artist) {
        (Some(known), _) => known,
        (None, Some(a)) if item_type != "artist" => {
            let exists = match engine.manticore {
                Some(m) => {
                    let w = words(a);
                    !w.is_empty()
                        && m.exists("search_artists", &format!("@name ({})", w.join(" ")))
                            .await
                            .map_err(engine_error)?
                }
                None => sqlx::query_scalar::<_, i32>(
                    "SELECT 1 FROM artists WHERE name &&& $1::text LIMIT 1",
                )
                .persistent(false)
                .bind(a)
                .fetch_optional(engine.pool)
                .await?
                .is_some(),
            };
            *artist_known = Some(exists);
            exists
        }
        _ => false,
    };
    let fuzzy_modes: &[Mode] = if engine.manticore.is_some() {
        if strict && !artist_exists {
            &[]
        } else {
            &[Mode::FuzzyBoth]
        }
    } else if strict {
        if artist_exists {
            &[Mode::FuzzyName]
        } else {
            &[]
        }
    } else if artist_exists {
        &[Mode::FuzzyName, Mode::FuzzyBoth]
    } else if artist.is_some() && item_type != "artist" && !fallback.is_empty() {
        &[Mode::FuzzyArtist, Mode::FuzzyBoth]
    } else {
        &[Mode::FuzzyBoth]
    };
    for &mode in fuzzy_modes {
        let found = run(engine, scope, item_type, name, artist, limit, mode).await?;
        if !found.is_empty() {
            return Ok(found);
        }
    }

    Ok(fallback)
}

async fn run(
    engine: Engine<'_>,
    scope: Scope,
    item_type: &str,
    name: &str,
    artist: Option<&str>,
    limit: i64,
    mode: Mode,
) -> Result<Vec<Candidate>, sqlx::Error> {
    if let Some(m) = engine.manticore {
        return run_manticore(m, scope, item_type, name, artist, limit, mode).await;
    }
    let pool = engine.pool;
    let (table, artist_col, album_col, pop_col) = match item_type {
        "song" if scope == Scope::Hot => (
            "search_songs_hot",
            "artist_names",
            "album_name",
            "popularity",
        ),
        "song" => ("search_songs", "artist_names", "album_name", "popularity"),
        "album" if scope == Scope::Hot => (
            "search_albums_hot",
            "artist_names",
            "''::text",
            "popularity",
        ),
        "album" => ("search_albums", "artist_names", "''::text", "popularity"),
        _ => ("artists", "''::text", "''::text", "popularity_score"),
    };
    let artist = artist.filter(|_| artist_col == "artist_names");

    let (name_clause, artist_clause) = match mode {
        Mode::Fast | Mode::ExactAll => (
            clause("name", 1, "&&&", false),
            clause("artist_names", 3, "&&&", false),
        ),
        Mode::ExactAny => (
            clause("name", 1, "|||", false),
            clause("artist_names", 3, "&&&", false),
        ),
        Mode::FuzzyName => (
            clause("name", 1, "&&&", true),
            clause("artist_names", 3, "&&&", false),
        ),
        Mode::FuzzyArtist => (
            clause("name", 1, "&&&", false),
            clause("artist_names", 3, "&&&", true),
        ),
        Mode::FuzzyBoth => (
            clause("name", 1, "&&&", true),
            clause("artist_names", 3, "&&&", true),
        ),
    };

    let select = format!(
        "SELECT id, name, {artist_col} AS artist_name, {album_col} AS album_name, \
         COALESCE({pop_col}, 0)::float8 AS pop FROM {table}"
    );
    let order = format!("ORDER BY pdb.score(id) DESC, {pop_col} DESC LIMIT $2");

    let sql = match artist {
        Some(_) => format!("{select} WHERE {name_clause} AND {artist_clause} {order}"),
        None => format!("{select} WHERE {name_clause} {order}"),
    };

    let (name_fuzzy, artist_fuzzy) = match mode {
        Mode::FuzzyName => (true, false),
        Mode::FuzzyArtist => (false, true),
        Mode::FuzzyBoth => (true, true),
        _ => (false, false),
    };
    let name_arg = if name_fuzzy {
        fuzzy_terms(name)
    } else {
        name.to_string()
    };
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
        .persistent(false)
        .bind(name_arg)
        .bind(limit);
    if let Some(a) = artist {
        query = query.bind(if artist_fuzzy {
            fuzzy_terms(a)
        } else {
            a.to_string()
        });
    }

    let found: Vec<Candidate> = query
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|r| Candidate {
            id: r.get(0),
            name: r.get(1),
            artist: r.get(2),
            album: r.get(3),
            popularity_score: r.get(4),
            duration: None,
            album_tier: 0,
        })
        .collect();

    Ok(found)
}

async fn run_manticore(
    manticore: &Manticore,
    scope: Scope,
    item_type: &str,
    name: &str,
    artist: Option<&str>,
    limit: i64,
    mode: Mode,
) -> Result<Vec<Candidate>, sqlx::Error> {
    let table = match item_type {
        "song" if scope == Scope::Hot => "search_songs_hot",
        "song" => "search_songs",
        "album" if scope == Scope::Hot => "search_albums_hot",
        "album" => "search_albums",
        _ => "search_artists",
    };
    let artist = artist.filter(|_| item_type != "artist");
    let fuzzy = matches!(mode, Mode::FuzzyName | Mode::FuzzyArtist | Mode::FuzzyBoth);

    let expression = if fuzzy {
        let mut terms = words(&fuzzy_terms(name));
        if let Some(a) = artist {
            terms.extend(words(&fuzzy_terms(a)));
        }
        terms.join(" ")
    } else {
        let name_words = words(name);
        if name_words.is_empty() {
            return Ok(Vec::new());
        }
        let joiner = if matches!(mode, Mode::ExactAny) {
            " | "
        } else {
            " "
        };
        let mut expression = format!("@name ({})", name_words.join(joiner));
        if let Some(a) = artist {
            let artist_words = words(a);
            if !artist_words.is_empty() {
                expression.push_str(&format!(" @artist_names ({})", artist_words.join(" ")));
            }
        }
        expression
    };
    if expression.trim().is_empty() {
        return Ok(Vec::new());
    }

    let hits = manticore
        .search(table, &expression, fuzzy, limit)
        .await
        .map_err(engine_error)?;
    Ok(hits
        .into_iter()
        .map(|h| Candidate {
            id: h.id,
            name: h.name,
            artist: h.artist,
            album: h.album,
            popularity_score: h.popularity,
            duration: None,
            album_tier: 0,
        })
        .collect())
}

pub async fn enrich(pool: &PgPool, found: &mut [Candidate]) -> Result<(), sqlx::Error> {
    if found.is_empty() {
        return Ok(());
    }
    let ids: Vec<String> = found.iter().map(|c| c.id.clone()).collect();
    let rows = sqlx::query(
        "SELECT s.id, s.duration, COALESCE((
             SELECT min(song_album_tier(al.name, al.track_count, EXISTS (
                 SELECT 1 FROM artist_albums aa
                 JOIN song_artists sar ON sar.artist_id = aa.artist_id AND sar.song_id = s.id
                 WHERE aa.album_id = al.id)))
             FROM song_albums sal JOIN albums al ON al.id = sal.album_id
             WHERE sal.song_id = s.id), 0) AS tier
         FROM songs s WHERE s.id = ANY($1)",
    )
    .persistent(false)
    .bind(&ids)
    .fetch_all(pool)
    .await?;
    let details: std::collections::HashMap<String, (i32, i32)> = rows
        .into_iter()
        .map(|r| {
            (
                r.get::<String, _>(0),
                (r.get::<i32, _>(1), r.get::<i32, _>(2)),
            )
        })
        .collect();
    for c in found {
        if let Some(&(duration, tier)) = details.get(&c.id) {
            c.duration = Some(duration);
            c.album_tier = tier;
        }
    }
    Ok(())
}
