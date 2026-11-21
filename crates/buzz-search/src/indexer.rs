//! The indexer: drains `search_outbox` into the OpenSearch index.
//!
//! One relay instance holds the indexer lease (a session advisory lock) at a
//! time; when it stops, its lock connection closes and another instance takes
//! over on its next attempt. Rows are claimed with `SKIP LOCKED`, so more
//! workers can run without a redesign.
//!
//! Each batch re-reads its events from Postgres, writes them (or removes the
//! documents of events that are gone) in one bulk request, and only then
//! deletes the rows it claimed. A failed request leaves the rows queued for
//! the next attempt. A batch the cluster rejects outright skips its community
//! for the rest of the pass, so one community cannot hold back the others.
//!
//! A batch holds its community's admission open for up to one request
//! timeout. Community deletion's quiesce step waits for that batch, and once
//! the community is quiescing no new batch for it can start.
//!
//! The leader also backfills events stored before the index existed (see
//! `buzz_db::search_backfill`): before each drain it queues their keys,
//! newest first, one batch per community in turn, but only while the outbox
//! holds fewer than the backfill bound. Live rows so wait behind at most that
//! many backfill rows, and Postgres reads one batch of history at a time. Only
//! the lease holder backfills, so a fleet queues each event once.

use std::time::{Duration, Instant};

use buzz_db::{search_backfill, search_outbox, Db, DbError};
use tracing::{debug, info, warn};

use crate::opensearch::SearchIndex;
use crate::SearchError;
use buzz_core::CommunityId;

/// Deployment-global advisory lock electing the indexing instance.
const INDEXER_LOCK_KEY: i64 = 0x4255_5A5A_5345_4152;

/// Outbox rows per batch, and so documents per bulk request.
const BATCH_SIZE: i64 = 500;

/// Backfill queues keys only while the outbox holds fewer rows than this.
const BACKFILL_OUTBOX_BOUND: i64 = 4 * BATCH_SIZE;

/// How often an idle leader checks the queue, and a follower the lease.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Longest wait after repeated failures.
const MAX_BACKOFF: Duration = Duration::from_secs(60);

/// Why a batch was not indexed. Its rows stay queued.
#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    /// Claiming, reading or completing the batch failed.
    #[error("outbox: {0}")]
    Outbox(#[from] DbError),
    /// The index write failed.
    #[error("index write: {}", .0.kind())]
    Index(#[from] SearchError),
}

/// Moves queued events from `search_outbox` into a [`SearchIndex`].
#[derive(Debug, Clone)]
pub struct Indexer {
    db: Db,
    index: SearchIndex,
    batch_size: i64,
    backfill_bound: i64,
}

impl Indexer {
    /// An indexer reading the outbox from `db` and writing `index`.
    pub fn new(db: Db, index: SearchIndex) -> Self {
        Self {
            db,
            index,
            batch_size: BATCH_SIZE,
            backfill_bound: BACKFILL_OUTBOX_BOUND,
        }
    }

    /// Claim at most `rows` outbox rows per bulk request, and backfill as
    /// many event keys per step (at least one).
    pub fn with_batch_size(mut self, rows: i64) -> Self {
        self.batch_size = rows.max(1);
        self
    }

    /// Backfill only while the outbox holds fewer than `rows` rows.
    pub fn with_backfill_bound(mut self, rows: i64) -> Self {
        self.backfill_bound = rows;
        self
    }

    /// Queue backfill batches, one community at a time in turn, while the
    /// outbox holds fewer rows than the backfill bound. Returns the number of
    /// event keys queued: zero when every community's backfill is complete
    /// or the outbox is full.
    ///
    /// Call it only on the lease holder, as [`Indexer::run`] does, so a
    /// fleet queues each stored event once.
    pub async fn backfill(&self) -> Result<u64, IndexerError> {
        let pool = self.db.pool();
        let mut queued = 0u64;
        while search_backfill::outbox_below(pool, self.backfill_bound).await? {
            let Some(step) = search_backfill::step(pool, self.batch_size, &[]).await? else {
                break;
            };
            let community = step.community.to_string();
            metrics::counter!("buzz_search_backfill_queued_total", "community" => community.clone())
                .increment(step.queued);
            if let Some(cursor) = step.cursor {
                metrics::gauge!("buzz_search_backfill_cursor_timestamp_seconds", "community" => community.clone())
                    .set(cursor.timestamp() as f64);
            }
            metrics::gauge!("buzz_search_backfill_complete", "community" => community)
                .set(if step.completed { 1.0 } else { 0.0 });
            if step.completed {
                info!(community = %step.community, queued = step.total_queued, "search backfill complete");
            } else {
                debug!(community = %step.community, queued = step.queued, cursor = ?step.cursor, "search backfill step");
            }
            queued += step.queued;
        }
        Ok(queued)
    }

    /// Index batches until none is left to claim. A community whose batch
    /// the cluster rejects is skipped for the rest of the pass and reported
    /// once the others are done; any other failure ends the pass.
    ///
    /// Ends with an info log of the documents written and removed and the
    /// elapsed time, for measuring throughput.
    pub async fn drain(&self) -> Result<(), IndexerError> {
        let mut pass = DrainPass::start();
        let result = self.drain_counting(&mut pass).await;
        pass.report();
        result
    }

    async fn drain_counting(&self, pass: &mut DrainPass) -> Result<(), IndexerError> {
        let mut skipped: Vec<CommunityId> = Vec::new();
        let mut rejection = None;
        while let Some(batch) =
            search_outbox::claim_batch(self.db.pool(), self.batch_size, &skipped).await?
        {
            let community = batch.community();
            match self
                .index
                .write(community, &batch.documents, &batch.removed)
                .await
            {
                Ok(()) => {
                    let (written, removed) = (batch.documents.len(), batch.removed.len());
                    batch.complete().await?;
                    pass.record(community, written, removed);
                }
                Err(error @ SearchError::Rejected(_)) => {
                    warn!(%community, error = %error.kind(), "search index rejected a batch; its rows stay queued");
                    skipped.push(community);
                    rejection.get_or_insert(error);
                }
                Err(error) => return Err(error.into()),
            }
        }
        rejection.map_or(Ok(()), |error| Err(error.into()))
    }

    /// One leader pass: make sure the index exists, top the outbox up with
    /// backfill, then drain. Checking the index every pass (one request when
    /// the alias exists) recreates an index that went away, and creating it
    /// here rather than at startup means an unreachable cluster never delays
    /// serving. Backfilling before draining keeps it going while a rejected
    /// batch's rows stay queued. Returns whether backfill queued anything, in
    /// which case the next pass should follow at once.
    async fn pass(&self) -> Result<bool, IndexerError> {
        self.index.ensure().await?;
        let backfilled = self.backfill().await?;
        self.drain().await?;
        Ok(backfilled > 0)
    }

    /// Hold the indexer lease and keep the index up to date, forever. Abort
    /// the task to stop; that closes the lease connection and lets another
    /// instance take over.
    pub async fn run(self) {
        // Spread lease attempts of instances that start together.
        tokio::time::sleep(POLL_INTERVAL.mul_f64(rand::random::<f64>())).await;
        let mut leader: Option<buzz_db::SessionLeader> = None;
        let mut failures = 0u32;
        loop {
            if let Some(guard) = leader.as_mut() {
                if !guard.is_live().await {
                    warn!("search indexer lease session lost; re-electing");
                    leader = None;
                }
            }
            if leader.is_none() {
                match self.db.try_lock_session_leader(INDEXER_LOCK_KEY).await {
                    Ok(Some(acquired)) => {
                        info!("acquired the search indexer lease");
                        leader = Some(acquired);
                    }
                    Ok(None) => {}
                    Err(error) => warn!(%error, "search indexer lease attempt failed"),
                }
            }
            let mut backfilling = false;
            if leader.is_some() {
                match self.pass().await {
                    Ok(more) => {
                        failures = 0;
                        backfilling = more;
                    }
                    Err(error) => {
                        failures = failures.saturating_add(1);
                        warn!(%error, failures, "search indexing failed; rows stay queued");
                    }
                }
            }
            if backfilling {
                // Next backfill batch now, after re-checking the lease.
                tokio::task::yield_now().await;
                continue;
            }
            tokio::time::sleep(backoff(failures)).await;
        }
    }
}

/// Documents one drain pass wrote and removed, for metrics and the
/// throughput log.
struct DrainPass {
    started: Instant,
    batches: u64,
    written: u64,
    removed: u64,
}

impl DrainPass {
    fn start() -> Self {
        Self {
            started: Instant::now(),
            batches: 0,
            written: 0,
            removed: 0,
        }
    }

    fn record(&mut self, community: CommunityId, written: usize, removed: usize) {
        let (written, removed) = (written as u64, removed as u64);
        let label = community.to_string();
        metrics::counter!("buzz_search_indexed_documents_total", "community" => label.clone(), "op" => "write")
            .increment(written);
        metrics::counter!("buzz_search_indexed_documents_total", "community" => label, "op" => "remove")
            .increment(removed);
        self.batches += 1;
        self.written += written;
        self.removed += removed;
    }

    fn report(&self) {
        if self.batches == 0 {
            return;
        }
        let elapsed = self.started.elapsed();
        let per_second = (self.written + self.removed) as f64 / elapsed.as_secs_f64().max(1e-3);
        info!(
            batches = self.batches,
            written = self.written,
            removed = self.removed,
            elapsed_ms = elapsed.as_millis() as u64,
            documents_per_second = per_second.round() as u64,
            "search indexer drained the outbox"
        );
    }
}

/// The wait before the next attempt: the poll interval, doubled per
/// consecutive failure up to [`MAX_BACKOFF`], with jitter after a failure.
fn backoff(failures: u32) -> Duration {
    if failures == 0 {
        return POLL_INTERVAL;
    }
    let ceiling = POLL_INTERVAL
        .saturating_mul(1 << failures.min(4))
        .min(MAX_BACKOFF);
    ceiling.mul_f64(0.5 + rand::random::<f64>() / 2.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_to_a_cap() {
        assert_eq!(backoff(0), POLL_INTERVAL);
        for failures in 1..10 {
            let wait = backoff(failures);
            assert!(wait <= MAX_BACKOFF, "{failures}: {wait:?}");
            assert!(wait >= POLL_INTERVAL, "{failures}: {wait:?}");
        }
        assert!(backoff(8) >= MAX_BACKOFF / 2);
    }
}
