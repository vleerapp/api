use nucleo_matcher::{
    Config, Matcher, Utf32Str,
    pattern::{AtomKind, CaseMatching, Normalization, Pattern},
};

use crate::db::search::Candidate;

const NAME_WEIGHT: f64 = 0.6;
const ARTIST_WEIGHT: f64 = 0.3;
const ALBUM_WEIGHT: f64 = 0.1;

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
            lower
                .split_whitespace()
                .map(|part| strsim::jaro_winkler(part.trim_matches(','), &self.query))
                .fold(0.0, f64::max)
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

pub fn best_match<'a>(
    candidates: &'a [Candidate],
    name: &str,
    artist: Option<&str>,
    album: Option<&str>,
) -> Option<&'a Candidate> {
    let mut matcher = Matcher::new(Config::DEFAULT);
    let mut buf = Vec::new();

    let name = Field::new(name, &mut matcher, &mut buf);
    let artist = artist.map(|a| Field::new(a, &mut matcher, &mut buf));
    let album = album.map(|a| Field::new(a, &mut matcher, &mut buf));

    candidates
        .iter()
        .map(|c| {
            let mut score = name.score(&c.name, true, &mut matcher, &mut buf) * NAME_WEIGHT;
            if let Some(f) = &artist {
                score += f.score(&c.artist, false, &mut matcher, &mut buf) * ARTIST_WEIGHT;
            }
            if let Some(f) = &album {
                score += f.score(&c.album, false, &mut matcher, &mut buf) * ALBUM_WEIGHT;
            }
            (c, score)
        })
        .max_by(|(_, a), (_, b)| a.total_cmp(b))
        .map(|(c, _)| c)
}
