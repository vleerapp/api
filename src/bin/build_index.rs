#[path = "../index.rs"]
#[allow(dead_code)]
mod index;

use anyhow::{Context, Result, bail};
use clap::Parser;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, ConnectOptions, PgPool, Row};
use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Instant;
use tantivy::schema::Field;
use tantivy::{Index, IndexWriter, TantivyDocument};
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const OUT_DIR: &str = "index";

struct Spec {
    kind: &'static str,
    artist_links: Option<&'static str>,
    album_links: Option<&'static str>,
}

const ARTIST: Spec = Spec {
    kind: "artist",
    artist_links: None,
    album_links: None,
};
const ALBUM: Spec = Spec {
    kind: "album",
    artist_links: Some("SELECT album_id, artist_id FROM artist_albums WHERE album_id = ANY($1)"),
    album_links: None,
};
const SONG: Spec = Spec {
    kind: "song",
    artist_links: Some("SELECT song_id, artist_id FROM song_artists WHERE song_id = ANY($1)"),
    album_links: Some("SELECT song_id, album_id FROM song_albums WHERE song_id = ANY($1)"),
};

#[derive(Parser)]
struct Args {
    #[arg(
        long,
        env = "SCRAPE_DATABASE_URL",
        default_value = "postgres://postgres:postgres@localhost:5432/apple_music_scrape"
    )]
    db_url: String,

    #[arg(long, default_value_t = 400)]
    chunk: i64,

    #[arg(long, default_value_t = 8)]
    workers: u32,

    #[arg(long, default_value_t = 4)]
    index_threads: usize,

    #[arg(long, default_value_t = 2048)]
    heap_mb: usize,
}

struct Names {
    base: i64,
    loc: Vec<(u64, u32)>,
    data: Vec<u8>,
}

impl Names {
    fn get(&self, id: i64) -> Option<&str> {
        let i = usize::try_from(id - self.base).ok()?;
        let &(start, len) = self.loc.get(i)?;
        if len == 0 {
            return None;
        }
        let start = start as usize;
        std::str::from_utf8(&self.data[start..start + len as usize]).ok()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    let args = Args::parse();
    let opts = args
        .db_url
        .parse::<PgConnectOptions>()
        .context("parse database url")?
        .disable_statement_logging();
    let pool = PgPoolOptions::new()
        .max_connections(args.workers)
        .connect_with(opts)
        .await
        .context("connect to scrape database")?;
    fs::create_dir_all(OUT_DIR)?;

    build(&pool, &args, &ARTIST, None, None).await?;
    let artists = Arc::new(load_names(&pool, &args, "artists").await?);
    build(&pool, &args, &ALBUM, Some(artists.clone()), None).await?;
    let albums = Arc::new(load_names(&pool, &args, "albums").await?);
    build(&pool, &args, &SONG, Some(artists), Some(albums)).await?;
    Ok(())
}

fn tid(block: i64) -> String {
    format!("({block},0)")
}

async fn block_count(pool: &PgPool, table: &str) -> Result<i64> {
    let (blocks,): (i64,) = sqlx::query_as(
        "SELECT (pg_relation_size($1::text::regclass) / current_setting('block_size')::bigint)::bigint",
    )
    .bind(table)
    .fetch_one(pool)
    .await?;
    if blocks == 0 {
        bail!("{table} is empty");
    }
    Ok(blocks)
}

async fn for_each_chunk<F, Fut>(blocks: i64, chunk: i64, workers: u32, f: F) -> Result<()>
where
    F: Fn(i64, i64) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<()>> + Send + 'static,
{
    let next = Arc::new(AtomicI64::new(0));
    let mut tasks = Vec::new();
    for _ in 0..workers {
        let (f, next) = (f.clone(), next.clone());
        tasks.push(tokio::spawn(async move {
            loop {
                let from = next.fetch_add(chunk, Ordering::Relaxed);
                if from >= blocks {
                    return Ok::<(), anyhow::Error>(());
                }
                f(from, from + chunk).await?;
            }
        }));
    }
    drop(f);
    for t in tasks {
        t.await??;
    }
    Ok(())
}

async fn load_names(pool: &PgPool, args: &Args, table: &'static str) -> Result<Names> {
    let (lo, hi): (Option<i64>, Option<i64>) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT min(id), max(id) FROM {table}"
    )))
    .fetch_one(pool)
    .await?;
    let (Some(lo), Some(hi)) = (lo, hi) else {
        bail!("{table} is empty");
    };
    let blocks = block_count(pool, table).await?;
    info!("{table}: loading names, ids {lo}..={hi}");

    let started = Instant::now();
    let sql = Arc::new(format!(
        "SELECT id, name FROM {table} WHERE ctid >= $1::text::tid AND ctid < $2::text::tid"
    ));
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<(i64, String)>>(args.workers as usize * 2);
    let producer = {
        let pool = pool.clone();
        tokio::spawn(for_each_chunk(
            blocks,
            args.chunk,
            args.workers,
            move |from, to| {
                let (pool, sql, tx) = (pool.clone(), sql.clone(), tx.clone());
                async move {
                    let rows = sqlx::query(AssertSqlSafe(sql.as_str()))
                        .bind(tid(from))
                        .bind(tid(to))
                        .fetch_all(&pool)
                        .await?;
                    let batch = rows
                        .iter()
                        .map(|r| (r.get::<i64, _>(0), r.get::<String, _>(1)))
                        .collect();
                    tx.send(batch).await?;
                    Ok(())
                }
            },
        ))
    };

    let mut names = Names {
        base: lo,
        loc: vec![(0, 0); (hi - lo + 1) as usize],
        data: Vec::new(),
    };
    let mut count = 0u64;
    while let Some(batch) = rx.recv().await {
        for (id, name) in batch {
            if let Some(slot) = names.loc.get_mut((id - lo) as usize) {
                *slot = (names.data.len() as u64, name.len() as u32);
                names.data.extend_from_slice(name.as_bytes());
                count += 1;
            }
        }
    }
    producer.await??;
    info!(
        "{table}: {count} names ({} MB) in {:.0}s",
        names.data.len() / 1_000_000,
        started.elapsed().as_secs_f64()
    );
    Ok(names)
}

async fn links(
    pool: &PgPool,
    sql: Option<&'static str>,
    ids: &[i64],
) -> Result<HashMap<i64, Vec<i64>>> {
    let mut out: HashMap<i64, Vec<i64>> = HashMap::new();
    let Some(sql) = sql else {
        return Ok(out);
    };
    let rows = sqlx::query(sql)
        .persistent(false)
        .bind(ids)
        .fetch_all(pool)
        .await?;
    for row in rows {
        out.entry(row.get(0)).or_default().push(row.get(1));
    }
    Ok(out)
}

fn joined(links: &HashMap<i64, Vec<i64>>, id: i64, names: Option<&Names>) -> String {
    let (Some(ids), Some(names)) = (links.get(&id), names) else {
        return String::new();
    };
    let mut list: Vec<&str> = ids.iter().filter_map(|&i| names.get(i)).collect();
    list.sort_unstable();
    list.dedup();
    list.join(" ")
}

async fn build(
    pool: &PgPool,
    args: &Args,
    spec: &'static Spec,
    artists: Option<Arc<Names>>,
    albums: Option<Arc<Names>>,
) -> Result<()> {
    let kind = spec.kind;
    let out = PathBuf::from(OUT_DIR);
    let table = format!("{kind}s");
    let blocks = block_count(pool, &table).await?;
    info!("{table}: {blocks} blocks");
    let rows_sql = Arc::new(format!(
        "SELECT id, name, popularity::float8 FROM {table} WHERE ctid >= $1::text::tid AND ctid < $2::text::tid"
    ));

    let staging = out.join(format!("{table}.new"));
    if staging.exists() {
        fs::remove_dir_all(&staging)?;
    }
    fs::create_dir_all(&staging)?;
    let (schema, fields) = index::build_schema();
    let idx = Index::create_in_dir(&staging, schema)?;
    index::register(&idx);
    let writer: Arc<IndexWriter> =
        Arc::new(idx.writer_with_num_threads(args.index_threads, args.heap_mb * 1024 * 1024)?);

    let done = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let chunk = args.chunk;
    {
        let (pool, writer, done, table) =
            (pool.clone(), writer.clone(), done.clone(), table.clone());
        for_each_chunk(blocks, chunk, args.workers, move |from, to| {
            let (pool, writer, done, table) =
                (pool.clone(), writer.clone(), done.clone(), table.clone());
            let (rows_sql, artists, albums) = (rows_sql.clone(), artists.clone(), albums.clone());
            async move {
                let rows = sqlx::query(AssertSqlSafe(rows_sql.as_str()))
                    .persistent(false)
                    .bind(tid(from))
                    .bind(tid(to))
                    .fetch_all(&pool)
                    .await?;
                let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
                let (artist_links, album_links) = tokio::try_join!(
                    links(&pool, spec.artist_links, &ids),
                    links(&pool, spec.album_links, &ids)
                )?;
                let count = rows.len() as u64;
                let w = writer.clone();
                tokio::task::spawn_blocking(move || -> Result<()> {
                    for row in rows {
                        let id: i64 = row.get(0);
                        let artist = joined(&artist_links, id, artists.as_deref());
                        let album = joined(&album_links, id, albums.as_deref());
                        let mut popularity: f64 = row.get(2);
                        if spec.kind == "song" && (artist.is_empty() || album.is_empty()) {
                            popularity = 0.0;
                        }
                        let mut doc = TantivyDocument::default();
                        doc.add_u64(fields.id, id as u64);
                        add_text(&mut doc, fields.name, row.get(1));
                        add_text(&mut doc, fields.artist, artist);
                        add_text(&mut doc, fields.album, album);
                        doc.add_f64(fields.popularity, popularity);
                        w.add_document(doc)?;
                    }
                    Ok(())
                })
                .await??;
                let total = done.fetch_add(count, Ordering::Relaxed) + count;
                if (from / chunk) % 50 == 0 {
                    let secs = started.elapsed().as_secs_f64().max(1.0);
                    info!(
                        "{table}: {total} docs, {:.0} docs/s, at block {from}/{blocks}",
                        total as f64 / secs
                    );
                }
                Ok(())
            }
        })
        .await?;
    }

    let mut writer = Arc::try_unwrap(writer).map_err(|_| anyhow::anyhow!("writer still shared"))?;
    writer.commit()?;
    writer.wait_merging_threads()?;
    info!("{table}: committed, building wordlists");

    let idx = Index::open_in_dir(&staging)?;
    index::register(&idx);
    for (label, field) in [("name", fields.name), ("artist", fields.artist)] {
        if kind == "artist" && label == "artist" {
            continue;
        }
        let path = out.join(format!("{table}.{label}.fst.new"));
        let kept = index::write_words(&idx, field, &path)?;
        info!("{table}: {label} wordlist {kept} words");
    }

    let live = out.join(&table);
    if live.exists() {
        fs::remove_dir_all(&live)?;
    }
    fs::rename(&staging, &live)?;
    for label in ["name", "artist"] {
        let new = out.join(format!("{table}.{label}.fst.new"));
        if new.exists() {
            fs::rename(&new, index::words_path(&out, kind, label))?;
        }
    }
    info!("{table}: done in {:.0}s", started.elapsed().as_secs_f64());
    Ok(())
}

fn add_text(doc: &mut TantivyDocument, field: Field, text: String) {
    if !text.is_empty() {
        doc.add_text(field, text);
    }
}
