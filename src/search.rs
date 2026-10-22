use nucleo_matcher::{
    Config, Matcher, Utf32Str,
    pattern::{AtomKind, CaseMatching, Normalization, Pattern},
};

use crate::db::search::Candidate;

const NAME_WEIGHT: f64 = 0.6;
const ARTIST_WEIGHT: f64 = 0.3;
const ALBUM_WEIGHT: f64 = 0.1;
const POPULARITY_WEIGHT: f64 = 0.15;
const DURATION_WEIGHT: f64 = 0.3;
const ALBUM_TIER_PENALTY: f64 = 0.05;
pub const DURATION_TOLERANCE_MS: i32 = 50;

struct Field {
    query: String,
    pattern: Pattern,
    max: u32,
}

impl Field {
    fn new(query: &str, matcher: &mut Matcher, buf: &mut Vec<char>) -> Self {
        let pattern = Pattern::new(
            query,
            CaseMatching::Ignore,
            Normalization::Smart,
            AtomKind::Fuzzy,
        );
        let max = pattern
            .score(Utf32Str::new(query, buf), matcher)
            .unwrap_or(0);
        Self {
            query: query.to_lowercase(),
            pattern,
            max,
        }
    }

    fn score(
        &self,
        haystack: &str,
        whole: bool,
        matcher: &mut Matcher,
        buf: &mut Vec<char>,
    ) -> f64 {
        let lower = haystack.to_lowercase();
        let jw = if whole {
            strsim::jaro_winkler(&lower, &self.query)
        } else if lower.contains(self.query.as_str()) {
            1.0
        } else {
            let words: Vec<&str> = lower
                .split_whitespace()
                .map(|part| part.trim_matches(','))
                .collect();
            let n = self.query.split_whitespace().count().max(1);
            if words.len() <= n {
                strsim::jaro_winkler(&words.join(" "), &self.query)
            } else {
                words
                    .windows(n)
                    .map(|w| strsim::jaro_winkler(&w.join(" "), &self.query))
                    .fold(0.0, f64::max)
            }
        };

        if self.max == 0 {
            return jw;
        }
        let fuzzy = self
            .pattern
            .score(Utf32Str::new(haystack, buf), matcher)
            .map_or(0.0, |s| (s as f64 / self.max as f64).min(1.0));

        (jw + fuzzy) / 2.0
    }
}

struct Scorer {
    matcher: Matcher,
    buf: Vec<char>,
    name: Field,
    artist: Option<Field>,
    album: Option<Field>,
    max_pop: f64,
}

impl Scorer {
    fn new(
        candidates: &[Candidate],
        name: &str,
        artist: Option<&str>,
        album: Option<&str>,
    ) -> Self {
        let mut matcher = Matcher::new(Config::DEFAULT);
        let mut buf = Vec::new();
        let name = Field::new(name, &mut matcher, &mut buf);
        let artist = artist.map(|a| Field::new(a, &mut matcher, &mut buf));
        let album = album.map(|a| Field::new(a, &mut matcher, &mut buf));
        let max_pop = candidates
            .iter()
            .map(|c| c.popularity_score * 100.0)
            .fold(1.0, f64::max);
        Self {
            matcher,
            buf,
            name,
            artist,
            album,
            max_pop,
        }
    }

    fn base(&mut self, c: &Candidate) -> f64 {
        let mut score = self
            .name
            .score(&c.name, true, &mut self.matcher, &mut self.buf)
            * NAME_WEIGHT;
        if let Some(f) = &self.artist {
            score += f.score(&c.artist, false, &mut self.matcher, &mut self.buf) * ARTIST_WEIGHT;
        }
        if let Some(f) = &self.album {
            score += f.score(&c.album, false, &mut self.matcher, &mut self.buf) * ALBUM_WEIGHT;
        }
        let pop = ((c.popularity_score * 100.0 + 1.0).ln() / (self.max_pop + 1.0).ln()).min(1.0);
        score + pop * POPULARITY_WEIGHT
    }
}

pub fn shortlist(
    candidates: Vec<Candidate>,
    name: &str,
    artist: Option<&str>,
    album: Option<&str>,
    keep: usize,
) -> Vec<Candidate> {
    if candidates.len() <= keep {
        return candidates;
    }
    let mut scorer = Scorer::new(&candidates, name, artist, album);
    let mut scored: Vec<(f64, Candidate)> = candidates
        .into_iter()
        .map(|c| (scorer.base(&c), c))
        .collect();
    scored.sort_by(|a, b| b.0.total_cmp(&a.0));
    scored.truncate(keep);
    scored.into_iter().map(|(_, c)| c).collect()
}

pub fn best_match<'a>(
    candidates: &'a [Candidate],
    name: &str,
    artist: Option<&str>,
    album: Option<&str>,
    duration: Option<i32>,
) -> Option<&'a Candidate> {
    let mut scorer = Scorer::new(candidates, name, artist, album);

    candidates
        .iter()
        .map(|c| {
            let mut score = scorer.base(c);
            score -= c.album_tier.clamp(0, 8) as f64 * ALBUM_TIER_PENALTY;
            if let (Some(want), Some(have)) = (duration, c.duration) {
                let diff = (want - have).abs();
                if diff <= DURATION_TOLERANCE_MS {
                    score += (1.0 - diff as f64 / (DURATION_TOLERANCE_MS as f64 + 1.0))
                        * DURATION_WEIGHT;
                }
            }
            (c, score)
        })
        .max_by(|(_, a), (_, b)| a.total_cmp(b))
        .map(|(c, _)| c)
}
