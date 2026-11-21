//! Which engine answers a message search, chosen per community by the
//! `search.backend` flag, and the shadow comparison between the two.
//!
//! | Flag value | Setting | Who serves | OpenSearch |
//! |---|---|---|---|
//! | 0 (default) | [`BackendSetting::Postgres`] | Postgres | not queried |
//! | 1 | [`BackendSetting::Shadow`] | Postgres | queried off the response path and compared |
//! | 2 | [`BackendSetting::OpenSearch`] | OpenSearch | its failures fail the search |
//!
//! Any other value means Postgres and counts in
//! `buzz_search_backend_flag_invalid_total`. The flag is read on every search, in
//! [`Selection::setting`] alone, so a provider that changes values while the
//! relay runs (LaunchDarkly) switches a community without a restart, and
//! changing the flag's scope (per relay instead of per community) is a change
//! there. Without an OpenSearch address the relay builds a
//! [`crate::SearchService`] with no OpenSearch backend and never reads it.
//!
//! Both search modes run on either backend. An OpenSearch failure at setting
//! 2 fails the search: there is no fallback to Postgres.
//!
//! **Shadow.** Only first pages are compared. The OpenSearch query starts
//! before the Postgres one, on its own task with its own deadline
//! ([`SHADOW_DEADLINE`]) and at most [`SHADOW_MAX_IN_FLIGHT`] at once; a search that finds every slot taken
//! skips the comparison (`reason="shadow_busy"`). Postgres' result is handed
//! to the task when it is ready and returned to the caller at once, so a slow
//! or failing OpenSearch never delays or fails the response.
//!
//! **Metrics**, all without query text:
//! - `buzz_search_duration_seconds{backend, role}` (histogram): search
//!   latency, `role` = `primary` (served the caller) or `shadow`;
//! - `buzz_search_requests_total{backend, role, outcome}`: `outcome` is
//!   `hits`, `zero_results` (a first page with no hits), `empty_page` (a
//!   later page with none), `error` (including partly failed OpenSearch
//!   searches) or `timeout` (a shadow search past its deadline);
//! - `buzz_search_shadow_overlap_ratio` (histogram): shared share of the top
//!   [`OVERLAP_TOP`] hits, recorded when either backend found something;
//! - `buzz_search_opensearch_skipped_total{setting, reason}`.
//!
//! Each shadow comparison also logs one `search shadow comparison` line with
//! the community, page, hit counts, overlap, latencies and outcomes.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use buzz_core::CommunityId;
use buzz_feature_flags::{EvaluationContext, FlagEvaluator, IntegerFlag};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::info;

use crate::opensearch::SearchIndex;
use crate::{SearchError, SearchQuery, SearchResult};

/// Integer flag choosing the search backend per community; see the module
/// docs. Environment form: `BUZZ_FEATURE_FLAG_SEARCH_BACKEND`.
pub const SEARCH_BACKEND_FLAG: IntegerFlag = IntegerFlag::new("search.backend", 0);

/// Longest a shadow OpenSearch query may run before it is abandoned and
/// counted as a timeout.
pub const SHADOW_DEADLINE: Duration = Duration::from_secs(2);

/// Most shadow queries in flight at once, so a slow cluster cannot pile up
/// requests behind live traffic.
pub const SHADOW_MAX_IN_FLIGHT: usize = 32;

/// How many top hits the shadow overlap compares.
pub const OVERLAP_TOP: usize = 10;

/// What a community's `search.backend` value asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendSetting {
    /// Postgres serves; OpenSearch is not queried (0, and unknown values).
    Postgres,
    /// Postgres serves; OpenSearch runs alongside and is compared (1).
    Shadow,
    /// OpenSearch serves (2).
    OpenSearch,
}

impl BackendSetting {
    /// The setting a flag value selects. Unknown values select Postgres.
    pub fn from_flag_value(value: i64) -> Self {
        match value {
            0 => Self::Postgres,
            1 => Self::Shadow,
            2 => Self::OpenSearch,
            _ => {
                metrics::counter!("buzz_search_backend_flag_invalid_total").increment(1);
                Self::Postgres
            }
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Shadow => "shadow",
            Self::OpenSearch => "opensearch",
        }
    }
}

/// How the setting is chosen.
#[derive(Clone)]
pub(crate) enum Selection {
    /// Always this setting (tests and tools).
    Fixed(BackendSetting),
    /// [`SEARCH_BACKEND_FLAG`], evaluated for the searched community.
    Flag(Arc<dyn FlagEvaluator>),
}

impl std::fmt::Debug for Selection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fixed(setting) => f.debug_tuple("Fixed").field(setting).finish(),
            Self::Flag(_) => f.write_str("Flag"),
        }
    }
}

impl Selection {
    /// The one place the switch is read.
    fn setting(&self, community: CommunityId) -> BackendSetting {
        match self {
            Self::Fixed(setting) => *setting,
            Self::Flag(flags) => BackendSetting::from_flag_value(flags.evaluate_int(
                SEARCH_BACKEND_FLAG,
                &EvaluationContext::for_community(community),
            )),
        }
    }
}

/// The OpenSearch side of a [`crate::SearchService`].
#[derive(Debug, Clone)]
pub(crate) struct OpenSearchBackend {
    index: SearchIndex,
    selection: Selection,
    shadow_deadline: Duration,
    shadow_slots: Arc<Semaphore>,
}

/// Which engine a search runs on, as reported in metrics.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Engine {
    Postgres,
    OpenSearch,
}

impl Engine {
    fn label(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::OpenSearch => "opensearch",
        }
    }
}

/// What the routed search should do.
pub(crate) enum Route<'a> {
    /// Serve from Postgres.
    Postgres,
    /// Serve from this OpenSearch index.
    OpenSearch(&'a SearchIndex),
    /// Serve from Postgres and hand its result to the shadow comparison.
    Shadow(oneshot::Sender<Option<Vec<[u8; 32]>>>),
}

impl OpenSearchBackend {
    pub(crate) fn new(index: SearchIndex, selection: Selection) -> Self {
        Self {
            index,
            selection,
            shadow_deadline: SHADOW_DEADLINE,
            shadow_slots: Arc::new(Semaphore::new(SHADOW_MAX_IN_FLIGHT)),
        }
    }

    pub(crate) fn set_shadow_deadline(&mut self, deadline: Duration) {
        self.shadow_deadline = deadline;
    }

    /// The index serving `community`'s searches, if the setting is
    /// OpenSearch.
    pub(crate) fn serving(&self, community: CommunityId) -> Option<&SearchIndex> {
        (self.selection.setting(community) == BackendSetting::OpenSearch).then_some(&self.index)
    }

    /// Decide where `query` runs, starting the shadow search if it gets one.
    pub(crate) fn route(&self, query: &SearchQuery) -> Route<'_> {
        let setting = self.selection.setting(query.community);
        if setting == BackendSetting::Postgres {
            return Route::Postgres;
        }
        if setting == BackendSetting::OpenSearch {
            return Route::OpenSearch(&self.index);
        }
        // One comparison per search: later pages of the same query would
        // multiply the shadow's load without saying much more.
        if query.page > 1 {
            return Route::Postgres;
        }
        let Ok(permit) = Arc::clone(&self.shadow_slots).try_acquire_owned() else {
            skipped(setting, "shadow_busy");
            return Route::Postgres;
        };
        let (postgres_tx, postgres_rx) = oneshot::channel();
        tokio::spawn(shadow(
            self.index.clone(),
            query.clone(),
            self.shadow_deadline,
            postgres_rx,
            permit,
        ));
        Route::Shadow(postgres_tx)
    }
}

/// Run `search` on `engine` as the primary search, recording its metrics.
pub(crate) async fn primary(
    engine: Engine,
    query: &SearchQuery,
    search: impl std::future::Future<Output = Result<SearchResult, SearchError>>,
) -> Result<SearchResult, SearchError> {
    let started = Instant::now();
    let result = search.await;
    let outcome = match &result {
        Ok(result) => hits_outcome(query, result),
        Err(_) => "error",
    };
    record(engine, "primary", started.elapsed(), outcome);
    result
}

/// The shadow OpenSearch search: run it under its deadline, wait for
/// Postgres' hits (`None` if Postgres failed or the caller went away), then
/// record and log the comparison.
async fn shadow(
    index: SearchIndex,
    query: SearchQuery,
    deadline: Duration,
    postgres: oneshot::Receiver<Option<Vec<[u8; 32]>>>,
    _permit: OwnedSemaphorePermit,
) {
    let started = Instant::now();
    let outcome = tokio::time::timeout(deadline, index.search(&query)).await;
    let elapsed = started.elapsed();
    let (opensearch_outcome, opensearch_hits, error_kind) = match outcome {
        Ok(Ok(result)) => (hits_outcome(&query, &result), Some(top_ids(&result)), None),
        Ok(Err(error)) => ("error", None, Some(error.kind())),
        Err(_) => ("timeout", None, None),
    };

    let postgres_hits = postgres.await.ok().flatten();
    record(Engine::OpenSearch, "shadow", elapsed, opensearch_outcome);
    let overlap = match (&postgres_hits, &opensearch_hits) {
        (Some(postgres), Some(opensearch)) => overlap(postgres, opensearch),
        _ => None,
    };
    if let Some(overlap) = overlap {
        metrics::histogram!("buzz_search_shadow_overlap_ratio").record(overlap);
    }
    info!(
        community = %query.community,
        page = query.page,
        postgres_hits = postgres_hits.as_ref().map(Vec::len),
        opensearch_hits = opensearch_hits.as_ref().map(Vec::len),
        overlap,
        opensearch_ms = elapsed.as_millis() as u64,
        opensearch_outcome,
        error_kind,
        "search shadow comparison"
    );
}

/// The ids of a result's top [`OVERLAP_TOP`] hits, in order.
pub(crate) fn top_ids(result: &SearchResult) -> Vec<[u8; 32]> {
    result
        .hits
        .iter()
        .take(OVERLAP_TOP)
        .map(|hit| hit.event_id)
        .collect()
}

/// Shared ids over the larger top list; `None` when both are empty.
fn overlap(a: &[[u8; 32]], b: &[[u8; 32]]) -> Option<f64> {
    let larger = a.len().max(b.len());
    if larger == 0 {
        return None;
    }
    let a: HashSet<_> = a.iter().collect();
    let shared = b.iter().filter(|id| a.contains(id)).count();
    Some(shared as f64 / larger as f64)
}

fn hits_outcome(query: &SearchQuery, result: &SearchResult) -> &'static str {
    match (result.hits.is_empty(), query.page <= 1) {
        (false, _) => "hits",
        (true, true) => "zero_results",
        (true, false) => "empty_page",
    }
}

fn record(engine: Engine, role: &'static str, elapsed: Duration, outcome: &'static str) {
    metrics::histogram!(
        "buzz_search_duration_seconds",
        "backend" => engine.label(),
        "role" => role,
    )
    .record(elapsed.as_secs_f64());
    metrics::counter!(
        "buzz_search_requests_total",
        "backend" => engine.label(),
        "role" => role,
        "outcome" => outcome,
    )
    .increment(1);
}

fn skipped(setting: BackendSetting, reason: &'static str) {
    metrics::counter!(
        "buzz_search_opensearch_skipped_total",
        "setting" => setting.label(),
        "reason" => reason,
    )
    .increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_values_select_settings_and_unknown_values_mean_postgres() {
        for (value, setting) in [
            (0, BackendSetting::Postgres),
            (1, BackendSetting::Shadow),
            (2, BackendSetting::OpenSearch),
            (3, BackendSetting::Postgres),
            (-1, BackendSetting::Postgres),
        ] {
            assert_eq!(BackendSetting::from_flag_value(value), setting, "{value}");
        }
    }

    #[test]
    fn overlap_compares_the_larger_top_list() {
        let id = |n: u8| [n; 32];
        assert_eq!(overlap(&[], &[]), None);
        assert_eq!(overlap(&[id(1)], &[]), Some(0.0));
        assert_eq!(overlap(&[id(1), id(2)], &[id(2), id(1)]), Some(1.0));
        assert_eq!(
            overlap(&[id(1), id(2), id(3), id(4)], &[id(4), id(9)]),
            Some(0.25)
        );
    }
}
