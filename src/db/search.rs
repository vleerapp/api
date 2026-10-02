use sqlx::{PgPool, Row};

pub struct Candidate {
    pub id: String,
    pub name: String,
    pub artist: String,
    pub album: String,
    pub popularity_score: i64,
    pub duration: Option<i32>,
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

pub async fn candidates(
    pool: &PgPool,
    item_type: &str,
    name: &str,
    artist: Option<&str>,
    limit: i64,
) -> Result<Vec<Candidate>, sqlx::Error> {
    let wanted_artist = artist.map(str::to_lowercase);
    let mut fallback = Vec::new();

    if wanted_artist.is_some() && item_type != "artist" {
        let found = run(pool, item_type, name, artist, limit, Mode::Fast).await?;
        let wanted = wanted_artist.as_deref().unwrap_or_default();
        if found
            .iter()
            .any(|c| c.artist.to_lowercase().contains(wanted))
        {
            return Ok(found);
        }
    }

    let single_word = name.split_whitespace().nth(1).is_none();
    for mode in [Mode::ExactAll, Mode::ExactAny] {
        if single_word && matches!(mode, Mode::ExactAny) {
            continue;
        }
        let found = run(pool, item_type, name, artist, limit, mode).await?;
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

    let artist_exists = match artist {
        Some(a) if item_type != "artist" => {
            sqlx::query_scalar::<_, i32>("SELECT 1 FROM artists WHERE name &&& $1::text LIMIT 1")
                .persistent(false)
                .bind(a)
                .fetch_optional(pool)
                .await?
                .is_some()
        }
        _ => false,
    };
    let fuzzy_modes: &[Mode] = if artist_exists {
        &[Mode::FuzzyName, Mode::FuzzyBoth]
    } else if artist.is_some() && item_type != "artist" && !fallback.is_empty() {
        &[Mode::FuzzyArtist, Mode::FuzzyBoth]
    } else {
        &[Mode::FuzzyBoth]
    };
    for &mode in fuzzy_modes {
        let found = run(pool, item_type, name, artist, limit, mode).await?;
        if !found.is_empty() {
            return Ok(found);
        }
    }

    Ok(fallback)
}

async fn run(
    pool: &PgPool,
    item_type: &str,
    name: &str,
    artist: Option<&str>,
    limit: i64,
    mode: Mode,
) -> Result<Vec<Candidate>, sqlx::Error> {
    let (table, artist_col, album_col, pop_col) = match item_type {
        "song" => ("search_songs", "artist_names", "album_name", "popularity"),
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
         COALESCE({pop_col}, 0)::bigint AS pop FROM {table}"
    );
    let order = format!("ORDER BY pdb.score(id) DESC, {pop_col} DESC LIMIT $2");
    let order_pop = format!("ORDER BY {pop_col} DESC, pdb.score(id) DESC LIMIT $2");
    let exact = matches!(mode, Mode::Fast | Mode::ExactAll | Mode::ExactAny);

    let sql = match (artist, mode) {
        (Some(_), Mode::Fast | Mode::ExactAny) => format!(
            "({select} WHERE {name_clause} AND {artist_clause} {order}) UNION \
             ({select} WHERE {name_clause} AND {artist_clause} {order_pop})"
        ),
        (Some(_), Mode::FuzzyName | Mode::FuzzyArtist | Mode::FuzzyBoth) => {
            format!("{select} WHERE {name_clause} AND {artist_clause} {order}")
        }
        (Some(_), _) => {
            let mut arms = vec![
                format!("({select} WHERE {name_clause} {order})"),
                format!("({select} WHERE {name_clause} AND {artist_clause} {order})"),
            ];
            if exact {
                arms.push(format!("({select} WHERE {name_clause} {order_pop})"));
                arms.push(format!(
                    "({select} WHERE {name_clause} AND {artist_clause} {order_pop})"
                ));
            }
            arms.join(" UNION ")
        }
        (None, _) if exact => format!(
            "({select} WHERE {name_clause} {order}) UNION ({select} WHERE {name_clause} {order_pop})"
        ),
        (None, _) => format!("{select} WHERE {name_clause} {order}"),
    };

    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql))
        .persistent(false)
        .bind(name)
        .bind(limit);
    if let Some(a) = artist {
        query = query.bind(a);
    }

    let mut found: Vec<Candidate> = query
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
        })
        .collect();

    if item_type == "song" && !found.is_empty() {
        let ids: Vec<String> = found.iter().map(|c| c.id.clone()).collect();
        let rows = sqlx::query("SELECT id, duration FROM songs WHERE id = ANY($1)")
            .persistent(false)
            .bind(&ids)
            .fetch_all(pool)
            .await?;
        let durations: std::collections::HashMap<String, i32> = rows
            .into_iter()
            .map(|r| (r.get::<String, _>(0), r.get::<i32, _>(1)))
            .collect();
        for c in &mut found {
            c.duration = durations.get(&c.id).copied();
        }
    }

    Ok(found)
}
