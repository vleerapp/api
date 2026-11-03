use anyhow::{Context, Result, anyhow};
use fst::Automaton;
use fst::automaton::{Levenshtein, Str};
use fst::{IntoStreamer, Map, MapBuilder, Streamer};
use memmap2::Mmap;
use std::cmp::Reverse;
use std::collections::HashMap;
use std::collections::{BinaryHeap, HashSet};
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, STORED, Schema, TextFieldIndexing, TextOptions, Value,
};
use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, RemoveLongFilter, TextAnalyzer, Token, TokenStream, Tokenizer,
};
use tantivy::{DocAddress, Index, IndexReader, ReloadPolicy, Score, TantivyDocument, Term};

const MIN_WORD_FREQ: u64 = 1;
pub const KINDS: [&str; 3] = ["song", "album", "artist"];
pub const TOKENIZER: &str = "vleer";

const MAX_TOKEN_LEN: usize = 40;
const CORRECTION_MIN_LEN: usize = 3;
const POPULARITY_WEIGHT: f32 = 4.0;
const MAX_CANDIDATES: usize = 16;
const MAX_TRIES: usize = 24;
const MAX_NAME_TERMS: usize = 5;
const MAX_ARTIST_TERMS: usize = 3;
const SEARCH_BUDGET: Duration = Duration::from_millis(25);
const MAX_VERIFIED: usize = 6;
const NAME_ONLY_LISTS: usize = 2;
const HUGE_DF: u64 = 150_000;
const CACHE_ENTRIES: usize = 4096;
const SUSPECT_FREQ: u64 = 1000;
const NEIGHBOR_RATIO: u64 = 20;
const MIN_NEIGHBOR_FREQ: u64 = 50;

pub fn kind_dir(root: &Path, kind: &str) -> PathBuf {
    root.join(format!("{kind}s"))
}

pub fn words_path(root: &Path, kind: &str, field: &str) -> PathBuf {
    root.join(format!("{kind}s.{field}.fst"))
}

#[derive(Clone, Copy)]
pub struct Fields {
    pub id: Field,
    pub name: Field,
    pub artist: Field,
    pub album: Field,
    pub popularity: Field,
}

pub fn build_schema() -> (Schema, Fields) {
    let mut builder = Schema::builder();
    let text = |stored: bool| {
        let opts = TextOptions::default().set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer(TOKENIZER)
                .set_index_option(IndexRecordOption::WithFreqs),
        );
        if stored { opts.set_stored() } else { opts }
    };
    let id = builder.add_u64_field("id", STORED | tantivy::schema::FAST);
    let name = builder.add_text_field("name", text(true));
    let artist = builder.add_text_field("artist", text(true));
    let album = builder.add_text_field("album", text(true));
    let popularity = builder.add_f64_field("popularity", STORED | tantivy::schema::FAST);
    (
        builder.build(),
        Fields {
            id,
            name,
            artist,
            album,
            popularity,
        },
    )
}

fn is_cjk(c: char) -> bool {
    matches!(c as u32,
        0x3040..=0x30FF | 0x3400..=0x4DBF | 0x4E00..=0x9FFF | 0xAC00..=0xD7AF
        | 0xF900..=0xFAFF | 0x20000..=0x2FA1F)
}

#[derive(Clone)]
pub struct WordTokenizer;

pub struct WordStream {
    tokens: Vec<Token>,
    at: usize,
}

impl Tokenizer for WordTokenizer {
    type TokenStream<'a> = WordStream;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> WordStream {
        let mut tokens: Vec<Token> = Vec::new();
        let mut start: Option<usize> = None;
        let push = |from: usize, to: usize, tokens: &mut Vec<Token>| {
            tokens.push(Token {
                offset_from: from,
                offset_to: to,
                position: tokens.len(),
                text: text[from..to].to_string(),
                position_length: 1,
            });
        };
        for (i, c) in text.char_indices() {
            if is_cjk(c) {
                if let Some(s) = start.take() {
                    push(s, i, &mut tokens);
                }
                push(i, i + c.len_utf8(), &mut tokens);
            } else if c.is_alphanumeric() {
                start.get_or_insert(i);
            } else if let Some(s) = start.take() {
                push(s, i, &mut tokens);
            }
        }
        if let Some(s) = start {
            push(s, text.len(), &mut tokens);
        }
        WordStream { tokens, at: 0 }
    }
}

impl TokenStream for WordStream {
    fn advance(&mut self) -> bool {
        if self.at < self.tokens.len() {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn token(&self) -> &Token {
        &self.tokens[self.at - 1]
    }

    fn token_mut(&mut self) -> &mut Token {
        &mut self.tokens[self.at - 1]
    }
}

pub fn analyzer() -> TextAnalyzer {
    TextAnalyzer::builder(WordTokenizer)
        .filter(RemoveLongFilter::limit(MAX_TOKEN_LEN))
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter)
        .build()
}

pub fn register(index: &Index) {
    index.tokenizers().register(TOKENIZER, analyzer());
}

pub fn terms(analyzer: &mut TextAnalyzer, text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut stream = analyzer.token_stream(text);
    while stream.advance() {
        out.push(stream.token().text.clone());
    }
    out
}

pub struct WordList {
    map: Map<Mmap>,
}

impl WordList {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let mmap = unsafe { Mmap::map(&file)? };
        Ok(Self {
            map: Map::new(mmap)?,
        })
    }

    fn freq(&self, word: &str) -> Option<u64> {
        self.map.get(word)
    }

    pub fn rarity(&self, word: &str) -> u64 {
        self.freq(word).unwrap_or(0)
    }

    pub fn merge_adjacent(&self, words: Vec<String>) -> Vec<String> {
        let mut out: Vec<String> = Vec::with_capacity(words.len());
        let mut i = 0;
        while i < words.len() {
            if i + 1 < words.len() {
                let joined = format!("{}{}", words[i], words[i + 1]);
                let unknown = self.freq(&words[i]).is_none() || self.freq(&words[i + 1]).is_none();
                if unknown && self.freq(&joined).is_some() {
                    out.push(joined);
                    i += 2;
                    continue;
                }
            }
            out.push(words[i].clone());
            i += 1;
        }
        out
    }

    pub fn is_trivial(&self, word: &str) -> bool {
        contraction(word).is_none()
            && (word.chars().count() < CORRECTION_MIN_LEN
                || self.freq(word).is_some_and(|f| f >= SUSPECT_FREQ))
    }

    pub fn candidates(&self, word: &str) -> Vec<Candidate> {
        let mut out = vec![Candidate {
            terms: vec![word.to_string()],
            cost: 0.0,
        }];
        if let Some(parts) = contraction(word) {
            out.push(Candidate {
                terms: parts,
                cost: 0.5,
            });
        }
        let chars: Vec<char> = word.chars().collect();
        let known = self.freq(word);
        if chars.len() < CORRECTION_MIN_LEN || known.is_some_and(|f| f >= SUSPECT_FREQ) {
            return out;
        }
        let min_freq = known.map_or(0, |f| (f * NEIGHBOR_RATIO).max(MIN_NEIGHBOR_FREQ));

        let mut found: Vec<Candidate> = Vec::new();
        let push = |found: &mut Vec<Candidate>, term: String, distance: u32, freq: u64| {
            if freq >= min_freq
                && term != word
                && !found
                    .iter()
                    .any(|c| c.terms[0] == term && c.terms.len() == 1)
            {
                found.push(Candidate {
                    terms: vec![term],
                    cost: 4.0 * distance as f32 - 0.2 * (freq as f32).ln(),
                });
            }
        };

        for i in 0..chars.len() - 1 {
            if chars[i] == chars[i + 1] {
                continue;
            }
            let mut swapped = chars.clone();
            swapped.swap(i, i + 1);
            let swapped: String = swapped.into_iter().collect();
            if let Some(freq) = self.freq(&swapped) {
                push(&mut found, swapped, 1, freq);
            }
        }

        let max_distance = if known.is_none() && chars.len() >= 5 {
            2
        } else {
            1
        };
        for distance in 1..=max_distance {
            if distance == 2 && found.len() >= 3 {
                break;
            }
            let Ok(lev) = Levenshtein::new(word, distance) else {
                continue;
            };
            let mut near: Vec<(String, u64)> = Vec::new();
            if distance == 1 {
                let mut stream = self.map.search(lev).into_stream();
                while let Some((key, freq)) = stream.next() {
                    if let Ok(term) = std::str::from_utf8(key) {
                        near.push((term.to_string(), freq));
                    }
                }
            } else {
                let first: String = chars[..1].iter().collect();
                let narrowed = lev.intersection(Str::new(&first).starts_with());
                let mut stream = self.map.search(narrowed).into_stream();
                while let Some((key, freq)) = stream.next() {
                    if let Ok(term) = std::str::from_utf8(key) {
                        near.push((term.to_string(), freq));
                    }
                }
            }
            near.sort_by_key(|a| Reverse(a.1));
            for (term, freq) in near.into_iter().take(MAX_CANDIDATES) {
                push(&mut found, term, distance, freq);
            }
        }

        if known.is_none() && chars.len() >= 4 {
            for i in 2..=chars.len() - 2 {
                let left: String = chars[..i].iter().collect();
                let right: String = chars[i..].iter().collect();
                if let (Some(l), Some(r)) = (self.freq(&left), self.freq(&right)) {
                    found.push(Candidate {
                        terms: vec![left, right],
                        cost: 3.0 - 0.1 * (l.min(r) as f32).ln(),
                    });
                }
            }
        }

        found.sort_by(|a, b| a.cost.total_cmp(&b.cost));
        found.truncate(MAX_CANDIDATES);
        out.extend(found);
        out
    }
}

const CONTRACTIONS: [(&str, &str, &str); 30] = [
    ("dont", "don", "t"),
    ("cant", "can", "t"),
    ("wont", "won", "t"),
    ("isnt", "isn", "t"),
    ("arent", "aren", "t"),
    ("wasnt", "wasn", "t"),
    ("werent", "weren", "t"),
    ("didnt", "didn", "t"),
    ("doesnt", "doesn", "t"),
    ("couldnt", "couldn", "t"),
    ("wouldnt", "wouldn", "t"),
    ("shouldnt", "shouldn", "t"),
    ("havent", "haven", "t"),
    ("hasnt", "hasn", "t"),
    ("hadnt", "hadn", "t"),
    ("aint", "ain", "t"),
    ("im", "i", "m"),
    ("ive", "i", "ve"),
    ("ill", "i", "ll"),
    ("youre", "you", "re"),
    ("youll", "you", "ll"),
    ("youve", "you", "ve"),
    ("theyre", "they", "re"),
    ("weve", "we", "ve"),
    ("thats", "that", "s"),
    ("whats", "what", "s"),
    ("theres", "there", "s"),
    ("heres", "here", "s"),
    ("lets", "let", "s"),
    ("shes", "she", "s"),
];

fn contraction(word: &str) -> Option<Vec<String>> {
    CONTRACTIONS
        .iter()
        .find(|(plain, _, _)| *plain == word)
        .map(|(_, a, b)| vec![a.to_string(), b.to_string()])
}

#[derive(Clone)]
pub struct Candidate {
    pub terms: Vec<String>,
    pub cost: f32,
}

fn best_first(lists: &[Vec<Candidate>], max: usize) -> Vec<(f32, Vec<usize>)> {
    if lists.is_empty() || lists.iter().any(|l| l.is_empty()) {
        return Vec::new();
    }
    let cost =
        |idx: &[usize]| -> f32 { idx.iter().enumerate().map(|(g, &i)| lists[g][i].cost).sum() };
    let key = |c: f32| (c * 1000.0) as i64;
    let start = vec![0; lists.len()];
    let mut heap = BinaryHeap::new();
    let mut seen = HashSet::new();
    heap.push(Reverse((key(cost(&start)), start.clone())));
    seen.insert(start);
    let mut out = Vec::new();
    while let Some(Reverse((_, idx))) = heap.pop() {
        out.push((cost(&idx), idx.clone()));
        if out.len() >= max {
            break;
        }
        for g in 0..lists.len() {
            if idx[g] + 1 < lists[g].len() {
                let mut next = idx.clone();
                next[g] += 1;
                if seen.insert(next.clone()) {
                    heap.push(Reverse((key(cost(&next)), next)));
                }
            }
        }
    }
    out
}

#[derive(Clone)]
pub struct Hit {
    pub id: String,
    pub name: String,
    pub artist: String,
    pub album: String,
    pub popularity: f64,
}

type Popularity = Arc<HashMap<tantivy::index::SegmentId, tantivy::columnar::Column<f64>>>;

struct Kind {
    reader: IndexReader,
    popularity: Popularity,
    fields: Fields,
    name_words: Option<WordList>,
    artist_words: Option<WordList>,
}

impl Kind {
    fn new(
        reader: IndexReader,
        fields: Fields,
        name_words: Option<WordList>,
        artist_words: Option<WordList>,
    ) -> Result<Self> {
        let mut columns = HashMap::new();
        for segment in reader.searcher().segment_readers() {
            columns.insert(
                segment.segment_id(),
                segment.fast_fields().f64("popularity")?,
            );
        }
        Ok(Self {
            popularity: Arc::new(columns),
            reader,
            fields,
            name_words,
            artist_words,
        })
    }
}

pub struct SearchIndex {
    kinds: HashMap<&'static str, Kind>,
    cache: Mutex<HashMap<String, Arc<Vec<Hit>>>>,
}

impl SearchIndex {
    pub fn open(root: &Path) -> Result<Self> {
        let mut kinds = HashMap::new();
        for kind in KINDS {
            let dir = kind_dir(root, kind);
            let index = Index::open_in_dir(&dir)
                .with_context(|| format!("open index {}", dir.display()))?;
            register(&index);
            let (_, fields) = build_schema();
            let reader = index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?;
            let words = |field: &str| WordList::open(&words_path(root, kind, field)).ok();
            kinds.insert(
                kind,
                Kind::new(reader, fields, words("name"), words("artist"))?,
            );
        }
        Ok(Self {
            kinds,
            cache: Mutex::new(HashMap::new()),
        })
    }

    pub async fn search(
        self: &Arc<Self>,
        kind: &str,
        name: &str,
        artist: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Hit>> {
        let this = Arc::clone(self);
        let kind = kind.to_string();
        let name = name.to_string();
        let artist = artist.map(str::to_string);
        tokio::task::spawn_blocking(move || {
            this.search_blocking(&kind, &name, artist.as_deref(), limit)
        })
        .await?
    }

    pub fn sample(&self, kind: &str, n: usize, mut seed: u64) -> Result<Vec<Hit>> {
        let kind = self
            .kinds
            .get(kind)
            .ok_or_else(|| anyhow!("unknown kind {kind}"))?;
        let searcher = kind.reader.searcher();
        let segments = searcher.segment_readers();
        let total: u64 = segments.iter().map(|s| s.max_doc() as u64).sum();
        let mut out = Vec::with_capacity(n);
        if total == 0 {
            return Ok(out);
        }
        seed |= 1;
        while out.len() < n {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let mut pick = seed % total;
            for (i, segment) in segments.iter().enumerate() {
                let max = segment.max_doc() as u64;
                if pick < max {
                    let doc: TantivyDocument =
                        searcher.doc(DocAddress::new(i as u32, pick as u32))?;
                    out.push(read_hit(&kind.fields, &doc));
                    break;
                }
                pick -= max;
            }
        }
        Ok(out)
    }

    pub fn num_docs(&self, kind: &str) -> u64 {
        self.kinds
            .get(kind)
            .map(|k| k.reader.searcher().num_docs())
            .unwrap_or(0)
    }

    pub fn search_blocking(
        &self,
        kind: &str,
        name: &str,
        artist: Option<&str>,
        limit: usize,
    ) -> Result<Vec<Hit>> {
        let kind_name = kind;
        let kind = self
            .kinds
            .get(kind)
            .ok_or_else(|| anyhow!("unknown kind {kind}"))?;
        let mut analyzer = analyzer();
        let full_name = terms(&mut analyzer, name);
        if full_name.is_empty() {
            return Ok(Vec::new());
        }
        let artist_terms = match artist {
            Some(a) if kind_name != "artist" => terms(&mut analyzer, a),
            _ => Vec::new(),
        };
        let artist_terms = if is_placeholder(&artist_terms) {
            Vec::new()
        } else {
            artist_terms
        };

        let cache_key = format!(
            "{kind_name}\u{1}{}\u{1}{}\u{1}{limit}",
            full_name.join(" "),
            artist_terms.join(" ")
        );
        if let Ok(cache) = self.cache.lock()
            && let Some(hits) = cache.get(&cache_key)
        {
            return Ok(hits.as_ref().clone());
        }

        let searcher = kind.reader.searcher();
        let mut any_huge = false;
        let mut lists = collect(
            kind,
            &searcher,
            &full_name,
            &artist_terms,
            limit,
            &mut any_huge,
        )?;
        let core_name = terms(&mut analyzer, &strip_noise(name));
        if !core_name.is_empty() && core_name != full_name {
            lists.extend(collect(
                kind,
                &searcher,
                &core_name,
                &artist_terms,
                limit,
                &mut any_huge,
            )?);
        }

        let mut hits = Vec::new();
        for address in interleave(lists, limit) {
            hits.push(read_hit(&kind.fields, &searcher.doc(address)?));
        }

        if any_huge && let Ok(mut cache) = self.cache.lock() {
            if cache.len() >= CACHE_ENTRIES {
                cache.clear();
            }
            cache.insert(cache_key, Arc::new(hits.clone()));
        }
        Ok(hits)
    }
}

const PLACEHOLDER_ARTISTS: [&str; 12] = [
    "various artists",
    "various artist",
    "various",
    "va",
    "unknown artist",
    "unknown",
    "verschiedene interpreten",
    "verschiedene",
    "diverse",
    "divers",
    "artistes divers",
    "varios artistas",
];

fn is_placeholder(artist_terms: &[String]) -> bool {
    let joined = artist_terms.join(" ");
    PLACEHOLDER_ARTISTS.contains(&joined.as_str())
}

pub fn strip_noise(title: &str) -> String {
    let mut out = String::with_capacity(title.len());
    let mut depth = 0;
    for c in title.chars() {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' if depth > 0 => depth -= 1,
            _ if depth == 0 => out.push(c),
            _ => {}
        }
    }
    let cut = out.split(" - ").next().unwrap_or(&out);
    cut.trim().to_string()
}

fn rarest(words: &Option<WordList>, raw: Vec<String>, max: usize) -> Vec<String> {
    let raw = match words {
        Some(w) => w.merge_adjacent(raw),
        None => raw,
    };
    if raw.len() <= max {
        return raw;
    }
    let Some(w) = words else {
        return raw.into_iter().take(max).collect();
    };
    let mut order: Vec<usize> = (0..raw.len()).collect();
    order.sort_by_key(|&i| w.rarity(&raw[i]));
    order.truncate(max);
    order.sort_unstable();
    order.into_iter().map(|i| raw[i].clone()).collect()
}

fn collect(
    kind: &Kind,
    searcher: &tantivy::Searcher,
    name_terms: &[String],
    artist_terms: &[String],
    limit: usize,
    any_huge: &mut bool,
) -> Result<Vec<Vec<Ranked>>> {
    let deadline = Instant::now() + SEARCH_BUDGET;
    let raw_name = rarest(&kind.name_words, name_terms.to_vec(), MAX_NAME_TERMS);
    let raw_artist = rarest(&kind.artist_words, artist_terms.to_vec(), MAX_ARTIST_TERMS);
    let (name_groups, artist_groups) = std::thread::scope(|scope| {
        let names = fan_out(scope, &kind.name_words, &raw_name);
        let artists = fan_out(scope, &kind.artist_words, &raw_artist);
        (collect_groups(names), collect_groups(artists))
    });
    let name_count = name_groups.len();
    let joint: Vec<Vec<Candidate>> = name_groups.iter().chain(&artist_groups).cloned().collect();

    let query = |name_terms: &[String],
                 artist_terms: &[String],
                 name_occur: Occur,
                 artist_occur: Occur|
     -> Box<dyn Query> {
        let mut parts: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        for t in name_terms {
            parts.push((name_occur, term_query(kind.fields.name, t)));
        }
        for t in artist_terms {
            parts.push((artist_occur, term_query(kind.fields.artist, t)));
        }
        Box::new(BooleanQuery::new(parts))
    };
    let flatten = |groups: &[Vec<Candidate>], combo: &[usize]| -> Vec<String> {
        combo
            .iter()
            .enumerate()
            .flat_map(|(g, &i)| groups[g][i].terms.clone())
            .collect()
    };

    let suspicious = joint.iter().any(|g| g.len() > 1);
    let artist_suspicious = joint[name_count..].iter().any(|g| g.len() > 1);
    let df = |field: Field, terms: &[String]| -> u64 {
        terms
            .iter()
            .map(|t| {
                searcher
                    .doc_freq(&Term::from_field_text(field, t))
                    .unwrap_or(0)
            })
            .min()
            .unwrap_or(u64::MAX)
    };

    let mut found: Vec<Vec<Ranked>> = Vec::new();
    for (cost, combo) in best_first(&joint, MAX_TRIES) {
        if !found.is_empty() && Instant::now() > deadline {
            break;
        }
        let n = flatten(&joint[..name_count], &combo[..name_count]);
        let a = flatten(&joint[name_count..], &combo[name_count..]);
        let smallest = df(kind.fields.name, &n).min(df(kind.fields.artist, &a));
        if smallest == 0 {
            continue;
        }
        let huge = smallest > HUGE_DF;
        if huge && !found.is_empty() {
            continue;
        }
        *any_huge |= huge;
        let q = query(&n, &a, Occur::Must, Occur::Must);
        let hits = scored(searcher, &kind.popularity, &*q, limit)?;
        if hits.is_empty() {
            continue;
        }
        found.push(hits);
        if (cost == 0.0 && !suspicious) || found.len() >= MAX_VERIFIED {
            break;
        }
    }

    let top = |groups: &[Vec<Candidate>]| -> Vec<String> {
        groups
            .iter()
            .flat_map(|g| g.iter().take(2).flat_map(|c| c.terms.clone()))
            .collect()
    };
    let artist_hint = top(&artist_groups);
    if !artist_hint.is_empty() && (found.is_empty() || artist_suspicious) {
        let mut extra = 0;
        for (_, combo) in best_first(&joint[..name_count], MAX_TRIES) {
            if !found.is_empty() && Instant::now() > deadline {
                break;
            }
            let n = flatten(&joint[..name_count], &combo);
            let smallest = df(kind.fields.name, &n);
            if smallest == 0 || (smallest > HUGE_DF && (extra > 0 || !found.is_empty())) {
                continue;
            }
            *any_huge |= smallest > HUGE_DF;
            let q = query(&n, &artist_hint, Occur::Must, Occur::Should);
            let hits = scored(searcher, &kind.popularity, &*q, limit)?;
            if hits.is_empty() {
                continue;
            }
            found.push(hits);
            extra += 1;
            if extra >= NAME_ONLY_LISTS {
                break;
            }
        }
    }

    if found.is_empty() {
        let name_hint = top(&joint[..name_count]);
        if name_hint.len() + artist_hint.len() > 1 {
            let q = query(&name_hint, &artist_hint, Occur::Should, Occur::Should);
            found.push(wand(searcher, &kind.popularity, &*q, limit, 3)?);
        }
    }
    Ok(found)
}

type Ranked = (f32, DocAddress);

type Pending<'scope> =
    Result<Vec<Candidate>, std::thread::ScopedJoinHandle<'scope, Vec<Candidate>>>;

fn fan_out<'scope, 'env>(
    scope: &'scope std::thread::Scope<'scope, 'env>,
    words: &'env Option<WordList>,
    raw: &'env [String],
) -> Vec<Pending<'scope>> {
    raw.iter()
        .map(|t| match words {
            Some(w) if !w.is_trivial(t) => Err(scope.spawn(move || w.candidates(t))),
            _ => Ok(vec![Candidate {
                terms: vec![t.clone()],
                cost: 0.0,
            }]),
        })
        .collect()
}

fn collect_groups(list: Vec<Pending<'_>>) -> Vec<Vec<Candidate>> {
    list.into_iter()
        .map(|pending| match pending {
            Ok(done) => done,
            Err(handle) => handle.join().unwrap_or_default(),
        })
        .collect()
}

fn interleave(mut lists: Vec<Vec<Ranked>>, limit: usize) -> Vec<DocAddress> {
    for list in &mut lists {
        list.sort_by(|a, b| b.0.total_cmp(&a.0));
    }
    let limit = limit.max(1);
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let mut row = 0;
    while out.len() < limit && lists.iter().any(|l| row < l.len()) {
        for list in &lists {
            if let Some(&(_, address)) = list.get(row)
                && out.len() < limit
                && seen.insert(address)
            {
                out.push(address);
            }
        }
        row += 1;
    }
    out
}

fn scored(
    searcher: &tantivy::Searcher,
    popularity: &Popularity,
    query: &dyn Query,
    limit: usize,
) -> Result<Vec<Ranked>> {
    let columns = Arc::clone(popularity);
    let collector =
        TopDocs::with_limit(limit.max(1)).tweak_score(move |segment: &tantivy::SegmentReader| {
            let column = columns.get(&segment.segment_id()).cloned();
            move |doc: u32, score: Score| {
                let p = column
                    .as_ref()
                    .and_then(|c| c.first(doc))
                    .unwrap_or(0.0)
                    .clamp(0.0, 1.0) as f32;
                score * (1.0 + POPULARITY_WEIGHT * p)
            }
        });
    let top: Vec<(Score, DocAddress)> = searcher.search(query, &collector)?;
    Ok(top)
}

fn wand(
    searcher: &tantivy::Searcher,
    popularity: &Popularity,
    query: &dyn Query,
    limit: usize,
    pool: usize,
) -> Result<Vec<Ranked>> {
    let top: Vec<(Score, DocAddress)> = searcher.search(
        query,
        &TopDocs::with_limit(limit.max(1) * pool).order_by_score(),
    )?;
    let mut ranked: Vec<Ranked> = top
        .into_iter()
        .map(|(score, address)| {
            let segment = searcher.segment_reader(address.segment_ord);
            let p = popularity
                .get(&segment.segment_id())
                .and_then(|c| c.first(address.doc_id))
                .unwrap_or(0.0)
                .clamp(0.0, 1.0) as f32;
            (score * (1.0 + POPULARITY_WEIGHT * p), address)
        })
        .collect();
    ranked.sort_by(|a, b| b.0.total_cmp(&a.0));
    ranked.truncate(limit.max(1));
    Ok(ranked)
}

fn term_query(field: Field, text: &str) -> Box<dyn Query> {
    Box::new(TermQuery::new(
        Term::from_field_text(field, text),
        IndexRecordOption::WithFreqs,
    ))
}

fn read_hit(fields: &Fields, doc: &TantivyDocument) -> Hit {
    let text = |f: Field| {
        doc.get_first(f)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string()
    };
    Hit {
        id: doc
            .get_first(fields.id)
            .and_then(|v| v.as_u64())
            .unwrap_or_default()
            .to_string(),
        name: text(fields.name),
        artist: text(fields.artist),
        album: text(fields.album),
        popularity: doc
            .get_first(fields.popularity)
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0),
    }
}

pub fn write_words(idx: &Index, field: Field, path: &Path) -> Result<u64> {
    let searcher = idx.reader()?.searcher();
    let mut dicts = Vec::new();
    for segment in searcher.segment_readers() {
        dicts.push(segment.inverted_index(field)?);
    }
    let mut streams = Vec::new();
    for dict in &dicts {
        streams.push(dict.terms().stream()?);
    }

    let mut heap = BinaryHeap::new();
    for (i, s) in streams.iter_mut().enumerate() {
        if s.advance() {
            heap.push(Reverse((s.key().to_vec(), i)));
        }
    }
    let mut builder = MapBuilder::new(BufWriter::new(File::create(path)?))?;
    let mut kept = 0;
    while let Some(Reverse((key, i))) = heap.pop() {
        let mut freq = streams[i].value().doc_freq as u64;
        if streams[i].advance() {
            heap.push(Reverse((streams[i].key().to_vec(), i)));
        }
        while heap.peek().is_some_and(|Reverse((k, _))| *k == key) {
            let Reverse((_, j)) = heap.pop().unwrap();
            freq += streams[j].value().doc_freq as u64;
            if streams[j].advance() {
                heap.push(Reverse((streams[j].key().to_vec(), j)));
            }
        }
        if freq >= MIN_WORD_FREQ {
            builder.insert(&key, freq)?;
            kept += 1;
        }
    }
    builder.finish()?;
    Ok(kept)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tantivy::doc;

    fn build(dir: &Path, rows: &[(u64, &str, &str, &str, f64)]) {
        fs::create_dir_all(kind_dir(dir, "song")).unwrap();
        let (schema, f) = build_schema();
        let idx = Index::create_in_dir(kind_dir(dir, "song"), schema).unwrap();
        register(&idx);
        let mut w: tantivy::IndexWriter = idx.writer_with_num_threads(1, 15_000_000).unwrap();
        for &(id, name, artist, album, pop) in rows {
            w.add_document(doc!(f.id => id, f.name => name, f.artist => artist, f.album => album, f.popularity => pop)).unwrap();
        }
        w.commit().unwrap();
        write_words(&idx, f.name, &words_path(dir, "song", "name")).unwrap();
        write_words(&idx, f.artist, &words_path(dir, "song", "artist")).unwrap();
    }

    fn open_song_only(dir: &Path) -> SearchIndex {
        let index = Index::open_in_dir(kind_dir(dir, "song")).unwrap();
        register(&index);
        let (_, fields) = build_schema();
        let reader = index.reader().unwrap();
        let words = |f: &str| WordList::open(&words_path(dir, "song", f)).ok();
        let mut kinds = HashMap::new();
        kinds.insert(
            "song",
            Kind::new(reader, fields, words("name"), words("artist")).unwrap(),
        );
        SearchIndex {
            kinds,
            cache: Mutex::new(HashMap::new()),
        }
    }

    #[test]
    fn finds_with_typos_and_artist() {
        let dir = std::env::temp_dir().join(format!("vleer-index-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        build(
            &dir,
            &[
                (1, "Bohemian Rhapsody", "Queen", "A Night at the Opera", 0.9),
                (2, "Bohemian Rhapsody", "Panic! at the Disco", "Live", 0.2),
                (
                    3,
                    "Bohemian Like You",
                    "The Dandy Warhols",
                    "Thirteen Tales",
                    0.5,
                ),
                (4, "Café del Mar", "Energy 52", "Cafe", 0.1),
                (5, "Bohemian Rhapsody", "Queen", "Greatest Hits", 0.8),
                (6, "Bohemian Rhapsody", "Queen", "Greatest Hits II", 0.8),
            ],
        );
        let idx = open_song_only(&dir);

        let hits = idx
            .search_blocking("song", "Bohemian Rhapsody", Some("Queen"), 10)
            .unwrap();
        assert_eq!(hits[0].artist, "Queen");
        assert!(hits.iter().all(|h| h.artist == "Queen"));

        let hits = idx
            .search_blocking("song", "bohemain rapsody", Some("quen"), 10)
            .unwrap();
        assert_eq!(hits[0].name, "Bohemian Rhapsody");
        assert_eq!(hits[0].artist, "Queen");

        let hits = idx
            .search_blocking("song", "cafe del mar", None, 10)
            .unwrap();
        assert_eq!(hits[0].id, "4");

        let hits = idx
            .search_blocking("song", "bohemian", Some("nobody"), 10)
            .unwrap();
        assert!(!hits.is_empty());

        assert!(
            idx.search_blocking("song", "zzzzqqqq", None, 10)
                .unwrap()
                .is_empty()
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    #[ignore]
    fn bench_synthetic() {
        let dir = std::env::temp_dir().join(format!("vleer-bench-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(kind_dir(&dir, "song")).unwrap();
        let (schema, f) = build_schema();
        let idx = Index::create_in_dir(kind_dir(&dir, "song"), schema).unwrap();
        register(&idx);
        let mut w: tantivy::IndexWriter = idx.writer_with_num_threads(4, 1_000_000_000).unwrap();
        let mut x: u64 = 88172645463325252;
        let mut rnd = move || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let vocab: Vec<String> = (0..300_000).map(|i| format!("w{}x{}", i, i % 97)).collect();
        let pick = |r: u64| {
            let z = (r % 1_000_000) as f64 / 1_000_000.0;
            (z * z * z * 300_000.0) as usize
        };
        let n: u64 = std::env::var("BENCH_DOCS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5_000_000);
        for id in 0..n {
            let name = format!(
                "{} {} {}",
                vocab[pick(rnd())],
                vocab[pick(rnd())],
                vocab[pick(rnd())]
            );
            let artist = format!("{} {}", vocab[pick(rnd())], vocab[pick(rnd())]);
            let album = format!("{} {}", vocab[pick(rnd())], vocab[pick(rnd())]);
            w.add_document(doc!(f.id => id, f.name => name, f.artist => artist, f.album => album, f.popularity => (rnd() % 100) as f64 / 100.0)).unwrap();
        }
        w.commit().unwrap();
        w.wait_merging_threads().unwrap();
        write_words(&idx, f.name, &words_path(&dir, "song", "name")).unwrap();
        write_words(&idx, f.artist, &words_path(&dir, "song", "artist")).unwrap();
        let du = std::process::Command::new("du")
            .arg("-sb")
            .arg(&dir)
            .output()
            .unwrap();
        eprintln!("size bytes: {}", String::from_utf8_lossy(&du.stdout));
        let s = open_song_only(&dir);
        let queries = [
            (vocab[0].to_string(), None),
            (format!("{} {}", vocab[0], vocab[1]), Some(vocab[2].clone())),
            (
                format!("{} {}", vocab[5000], vocab[70000]),
                Some(vocab[3000].clone()),
            ),
            (format!("{}q {}", vocab[10], vocab[200]), None),
            ("zzzz nothing".to_string(), None),
        ];
        for (q, a) in &queries {
            let t = std::time::Instant::now();
            let hits = s.search_blocking("song", q, a.as_deref(), 100).unwrap();
            eprintln!(
                "{:>6.1}ms {:>3} hits  q={q:?} artist={a:?}",
                t.elapsed().as_secs_f64() * 1000.0,
                hits.len()
            );
        }
        let _ = fs::remove_dir_all(&dir);
    }
}
