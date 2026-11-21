//! How the index analyzes message text and how a search ranks it.
//!
//! Every relevance choice lives here: the analyzers, the text fields they
//! fill, the match query and the age rescore. The rest of
//! [`super`] only filters and pages.
//!
//! **Fields.** `content` holds whole words and their parts; `content.stem`
//! holds light English stems; `content.prefix` holds word prefixes for
//! [`SearchMode::Prefix`]. The two subfields keep term frequencies but no
//! positions, because no phrase query reads them.
//!
//! **Analysis**, the same splitting for every field:
//! - the `standard` tokenizer splits words on Unicode word boundaries (UAX
//!   #29), so markdown punctuation (quotes, backticks, `**`, link brackets,
//!   fences) never sticks to a word;
//! - `word_delimiter_graph` keeps each token whole and adds its parts, split
//!   at `.`, `_`, `-`, `:` and case changes, so `docs.example.org`,
//!   `config.prod.yaml`, `buzz_search` and `getUserName` match whole and by
//!   part. It does not split letters from digits, so a hex id stays one token
//!   instead of dozens of fragments;
//! - `lowercase`, then `asciifolding`, so accented and plain spellings meet.
//!
//! Queries use the same chain without the word-delimiter: a query token
//! matches either a whole indexed token or one of its parts.
//!
//! **Prefixes** ([`prefix_mapping`] and [`prefix_query`], the only two places
//! to change to use `index_prefixes` instead): edge n-grams of
//! [`MIN_PREFIX`] to [`MAX_PREFIX`] characters, of tokens up to
//! [`LONG_TOKEN`] characters. Longer tokens (`nostr:` URIs, keys, hashes, ids)
//! get none: nobody types them a letter at a time, and each would add dozens
//! of prefixes.
//!
//! **Query match (Q)**, BM25 with its length normalization on:
//! - each query word must match in one of three ways, scored by the best:
//!   exactly, by its stem ([`STEM_WEIGHT`]), or with a typo ([`TYPO_WEIGHT`]);
//!   in prefix mode the last word may instead be a prefix
//!   ([`PREFIX_WEIGHT`]) and is never corrected;
//! - the words are OR-ed with a minimum ([`MIN_WORDS_MATCHED`]);
//! - an exact phrase ([`PHRASE_WEIGHT`]) and the words close together
//!   ([`PROXIMITY_WEIGHT`]) add a bonus, scored for the top
//!   [`RESCORE_WINDOW`] hits by the words alone. Phrase matching reads the
//!   positions of every message holding all the words, so in the main query
//!   it cost 55 ms for two common words on a million messages, against 22 ms
//!   in the window, and grows with the history.
//!
//! Typo matching is one clause, [`typo_clause`]: dropping it, for example for
//! a separate spell checker, leaves the rest as it is.
//!
//! **Age (T)** = `0.2 + 0.8 × 0.5^(age / 30 days)`. A rescore multiplies Q by
//! T for the top [`RESCORE_WINDOW`] hits by Q (per shard), and only for them,
//! which bounds the rescore's cost. Matching
//! still skips documents that cannot reach the top, so search cost does not
//! grow with every match a common word has. The trade-off: a weak match
//! outside the window cannot be lifted by recency.
//!
//! **Serverless.** Built-in analysis only (no ICU plugin), `rescore` with a
//! `function_score` `exp` decay, and no scripts. Each still has to be checked
//! on a real Serverless collection: `word_delimiter_graph`, `edge_ngram`,
//! `length`, the `light_english` stemmer, `rescore`, and `function_score`
//! decay inside it.

use serde_json::{json, Value};

use crate::SearchMode;

/// Shortest prefix indexed for prefix mode.
const MIN_PREFIX: u32 = 2;
/// Longest prefix indexed: a longer partial word matches through its first
/// [`MAX_PREFIX`] characters.
const MAX_PREFIX: u32 = 20;
/// Tokens longer than this get no prefixes.
const LONG_TOKEN: u32 = 24;

/// How many of the query words a message must match: all of one or two,
/// three quarters (rounded down) of more.
const MIN_WORDS_MATCHED: &str = "2<75%";
/// Query words past this many are ignored, which bounds a query's clauses.
const MAX_WORDS: usize = 32;
/// Weight of a word matched only through its stem.
const STEM_WEIGHT: f64 = 0.4;
/// Weight of a word matched only with a typo.
const TYPO_WEIGHT: f64 = 0.3;
/// Weight of a partial last word matched as a prefix.
const PREFIX_WEIGHT: f64 = 0.5;
/// Bonus weight of the query found as an exact phrase.
const PHRASE_WEIGHT: f64 = 2.0;
/// Bonus weight of the query words found within [`PROXIMITY_SLOP`] moves of
/// each other.
const PROXIMITY_WEIGHT: f64 = 1.0;
const PROXIMITY_SLOP: u32 = 4;

/// How many of the best hits by Q the age factor reorders.
pub(super) const RESCORE_WINDOW: i64 = 500;
/// T never falls below this, so old exact matches survive.
const AGE_FLOOR: f64 = 0.2;
/// Age at which the decaying part of T has halved.
const AGE_HALF_LIFE_DAYS: f64 = 30.0;

/// The index's `analysis` settings.
pub(super) fn analysis() -> Value {
    json!({
        "filter": {
            "word_parts": {
                "type": "word_delimiter_graph",
                "preserve_original": true,
                "split_on_numerics": false
            },
            "short_tokens": { "type": "length", "max": LONG_TOKEN },
            "prefixes": { "type": "edge_ngram", "min_gram": MIN_PREFIX, "max_gram": MAX_PREFIX },
            "light_stem": { "type": "stemmer", "language": "light_english" }
        },
        "analyzer": {
            "message": analyzer(&["word_parts", "lowercase", "asciifolding"]),
            "message_search": analyzer(&["lowercase", "asciifolding"]),
            "message_stem": analyzer(&["word_parts", "lowercase", "asciifolding", "light_stem"]),
            "message_stem_search": analyzer(&["lowercase", "asciifolding", "light_stem"]),
            "message_prefix": analyzer(&[
                "word_parts", "lowercase", "asciifolding", "short_tokens", "prefixes"
            ])
        }
    })
}

fn analyzer(filters: &[&str]) -> Value {
    json!({ "type": "custom", "tokenizer": "standard", "filter": filters })
}

/// The mapping of `content`, the message text.
pub(super) fn content_mapping() -> Value {
    json!({
        "type": "text",
        "analyzer": "message",
        "search_analyzer": "message_search",
        "fields": {
            "stem": {
                "type": "text",
                "analyzer": "message_stem",
                "search_analyzer": "message_stem_search",
                "index_options": "freqs"
            },
            "prefix": prefix_mapping()
        }
    })
}

/// The prefix field: edge n-grams, searched with whole query tokens.
fn prefix_mapping() -> Value {
    json!({
        "type": "text",
        "analyzer": "message_prefix",
        "search_analyzer": "message_search",
        "index_options": "freqs"
    })
}

/// `word` as a prefix of an indexed word.
fn prefix_query(word: &str) -> Value {
    json!({ "match": { "content.prefix": {
        "query": word, "operator": "and", "boost": PREFIX_WEIGHT
    } } })
}

/// `word` with up to `AUTO` edits (none for one or two characters, one up to
/// five, then two), its first letter exact.
fn typo_clause(word: &str) -> Value {
    json!({ "match": { "content": {
        "query": word,
        "operator": "and",
        "fuzziness": "AUTO",
        "prefix_length": 1,
        "boost": TYPO_WEIGHT
    } } })
}

/// A search's text: what matches, and the phrase bonus for a
/// [`rescore`].
pub(super) struct TextQuery {
    /// The words: which messages match, and their Q before any bonus.
    pub(super) matched: Value,
    /// The phrase and proximity bonus, for queries of two words or more.
    bonus: Option<Value>,
}

/// The text query for `text` in `mode`, or `None` if it has no words.
pub(super) fn text_query(text: &str, mode: SearchMode) -> Option<TextQuery> {
    // A punctuation-only word analyzes to nothing and would count against
    // the minimum without ever matching.
    let words: Vec<&str> = text
        .split_whitespace()
        .filter(|word| word.chars().any(char::is_alphanumeric))
        .take(MAX_WORDS)
        .collect();
    let (last, completed) = words.split_last()?;
    let mut clauses: Vec<Value> = completed.iter().map(|word| word_clause(word)).collect();
    clauses.push(match mode {
        SearchMode::FullText => word_clause(last),
        SearchMode::Prefix => json!({ "dis_max": { "queries": [
            exact_clause(last),
            stem_clause(last),
            prefix_query(last),
        ] } }),
    });
    let matched = json!({ "bool": {
        "should": clauses,
        "minimum_should_match": MIN_WORDS_MATCHED
    } });
    if words.len() < 2 {
        return Some(TextQuery {
            matched,
            bonus: None,
        });
    }
    let phrase = words.join(" ");
    let phrase_query = match mode {
        SearchMode::FullText => "match_phrase",
        SearchMode::Prefix => "match_phrase_prefix",
    };
    let bonus = |slop: u32, boost: f64| json!({ phrase_query: { "content": { "query": phrase, "slop": slop, "boost": boost } } });
    Some(TextQuery {
        matched,
        bonus: Some(json!({ "bool": {
            "should": [bonus(0, PHRASE_WEIGHT), bonus(PROXIMITY_SLOP, PROXIMITY_WEIGHT)]
        } })),
    })
}

/// One query word, matched exactly, by stem or with a typo, scored by the
/// best of the three.
fn word_clause(word: &str) -> Value {
    json!({ "dis_max": { "queries": [
        exact_clause(word),
        stem_clause(word),
        typo_clause(word),
    ] } })
}

fn exact_clause(word: &str) -> Value {
    json!({ "match": { "content": { "query": word, "operator": "and" } } })
}

fn stem_clause(word: &str) -> Value {
    json!({ "match": { "content.stem": {
        "query": word, "operator": "and", "boost": STEM_WEIGHT
    } } })
}

/// The rescores of a relevance search over the top [`RESCORE_WINDOW`] hits:
/// add `text`'s phrase bonus to their Q, then multiply it by the age factor
/// at `now` (Unix seconds); see [`age_factor`].
pub(super) fn rescore(text: &TextQuery, now: i64) -> Value {
    let bonus = text.bonus.as_ref().map(|bonus| {
        json!({
            "window_size": RESCORE_WINDOW,
            "query": {
                "rescore_query": bonus,
                "query_weight": 1.0,
                "rescore_query_weight": 1.0,
                "score_mode": "total"
            }
        })
    });
    Value::Array(bonus.into_iter().chain([age_rescore(now)]).collect())
}

fn age_rescore(now: i64) -> Value {
    json!({
        "window_size": RESCORE_WINDOW,
        "query": {
            "rescore_query": { "function_score": {
                "query": { "match_all": {} },
                "functions": [
                    {
                        "exp": { "created_at": {
                            "origin": now.to_string(),
                            "scale": format!("{AGE_HALF_LIFE_DAYS}d"),
                            "decay": 0.5
                        } },
                        "weight": 1.0 - AGE_FLOOR
                    },
                    { "weight": AGE_FLOOR }
                ],
                "score_mode": "sum",
                "boost_mode": "replace"
            } },
            "query_weight": 1.0,
            "rescore_query_weight": 1.0,
            "score_mode": "multiply"
        }
    })
}

/// T for a message created at `created_at`, at `now` (both Unix seconds):
/// what [`rescore`] multiplies by. A message from the future (clock
/// skew) decays as if as old.
pub(super) fn age_factor(created_at: i64, now: i64) -> f64 {
    let age_days = (now - created_at).unsigned_abs() as f64 / 86_400.0;
    AGE_FLOOR + (1.0 - AGE_FLOOR) * 0.5_f64.powf(age_days / AGE_HALF_LIFE_DAYS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn age_factor_halves_its_decaying_part_every_thirty_days() {
        let now = 1_800_000_000;
        let day = 86_400;
        for (age, expected) in [(0, 1.0), (30 * day, 0.6), (60 * day, 0.4)] {
            assert!((age_factor(now - age, now) - expected).abs() < 1e-9);
        }
        assert!((age_factor(now - 3650 * day, now) - AGE_FLOOR).abs() < 1e-9);
    }

    #[test]
    fn punctuation_only_queries_have_no_words() {
        assert!(text_query(" ' : & | ", SearchMode::FullText).is_none());
        assert!(text_query("alpha ' bet", SearchMode::Prefix).is_some());
    }
}
