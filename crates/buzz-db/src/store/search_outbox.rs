//! The search outbox: the durable queue between Postgres and the search
//! index.
//!
//! When [`enable`] has been called (the relay does so when it is configured
//! with an OpenSearch address), every write that changes what search should
//! return queues a `search_outbox` row in the same transaction, so a change
//! always has its index update queued and a rolled-back write never does. The
//! write makes no call to the search engine. Those writes are:
//!
//! - admitting a [`SEARCHABLE_KINDS`] event (through the event insert
//!   follow-up);
//! - admitting or deleting a message edit, which queues the message it edits;
//! - soft-deleting an event (NIP-09 and NIP-29 deletions, moderation);
//! - account deletion scrubbing or erasing an author's messages.
//!
//! A source-policy test in `tests/observability_source.rs` fails when a new
//! production `UPDATE events` or `DELETE FROM events` neither queues a row
//! nor is listed as never touching a searchable kind. Replaceable and
//! addressable kinds are never searchable, so replacing one never changes
//! search results. Purging a community's documents from the index is not
//! queued here: the purge deletes the community's rows, this queue included.
//!
//! The indexer drains the queue one community at a time with
//! [`claim_batch`]: it locks rows, re-reads the events they name (with an
//! edited message's current text), writes the index, and calls
//! [`SearchOutboxBatch::complete_except`] to delete exactly the rows it
//! claimed whose index writes succeeded. Because the indexer re-reads
//! `events`, the order and number of rows for one event don't matter, and a
//! change committed while its message's earlier row is being indexed keeps
//! its own row for the next batch. Only one batch per community runs at a
//! time, so workers never race to write two states of one message.

use std::sync::atomic::{AtomicBool, Ordering};

use buzz_core::CommunityId;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::{AdmittedTx, Result};

/// Event kinds the search index holds; no other kind is ever indexed. Stream
/// messages (9), their v2 and forum forms (40002, 40008), and workflow
/// messages (45001, 45003). These are also the kinds a message edit applies
/// to.
pub const SEARCHABLE_KINDS: [i32; 5] = [9, 40002, 40008, 45001, 45003];

/// A message edit: its content replaces the text of the message its first
/// valid `e` tag names, in the same channel.
const KIND_MESSAGE_EDIT: i32 = buzz_core::kind::KIND_STREAM_MESSAGE_EDIT as i32;

/// Communities a claim may pass over because their deletion fenced them
/// between candidate selection and admission, or another worker is
/// indexing a batch of theirs.
const CLAIM_COMMUNITY_CANDIDATES: usize = 8;

/// First key of the transaction advisory lock a batch holds on its
/// community; the second is a hash of the community id. Two-key advisory
/// locks never collide with single-key ones such as the indexer lease.
const CLAIM_LOCK_CLASS: i32 = 0x5345_4152;

static ENABLED: AtomicBool = AtomicBool::new(false);

/// Start writing outbox rows on admission, for the life of the process.
///
/// Process-wide because the outbox follows every event insert path. Call it
/// once at startup, before serving writes, when a search index is configured.
/// Fails, leaving the outbox off, if migrations have not created the table:
/// enabled without it, every searchable insert would fail.
pub async fn enable(pool: &PgPool) -> Result<()> {
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('public.search_outbox') IS NOT NULL")
        .fetch_one(pool)
        .await?;
    if !exists {
        return Err(crate::DbError::InvalidData(
            "the search_outbox table is missing; run migrations before enabling search indexing"
                .into(),
        ));
    }
    ENABLED.store(true, Ordering::Relaxed);
    Ok(())
}

/// Queue the index change for an event this admitted transaction inserted,
/// soft-deleted or otherwise changed: the event itself when it is
/// searchable, or the message it edits when it is a message edit.
///
/// Call while the event's row still exists, before any physical delete in the
/// same transaction.
pub(crate) async fn enqueue_in_transaction(
    tx: &mut AdmittedTx,
    event_id: &[u8],
    created_at: DateTime<Utc>,
    kind: i32,
) -> Result<()> {
    if !ENABLED.load(Ordering::Relaxed) {
        return Ok(());
    }
    if SEARCHABLE_KINDS.contains(&kind) {
        let community = tx.community();
        sqlx::query(
            "INSERT INTO search_outbox (community_id, event_id, event_created_at) \
             VALUES ($1, $2, $3)",
        )
        .bind(community.as_uuid())
        .bind(event_id)
        .bind(created_at)
        .execute(tx.conn())
        .await?;
    } else if kind == KIND_MESSAGE_EDIT {
        enqueue_events_in_transaction(tx, &[event_id.to_vec()], &[created_at]).await?;
    }
    Ok(())
}

/// [`enqueue_in_transaction`] for events named by key whose kinds the caller
/// does not hold. Events of other kinds are skipped.
pub(crate) async fn enqueue_events_in_transaction(
    tx: &mut AdmittedTx,
    event_ids: &[Vec<u8>],
    created_at: &[DateTime<Utc>],
) -> Result<()> {
    if !ENABLED.load(Ordering::Relaxed) || event_ids.is_empty() {
        return Ok(());
    }
    let community = tx.community();
    // An edit queues the message it targets in its own channel.
    let sql = format!(
        "INSERT INTO search_outbox (community_id, event_id, event_created_at) \
         SELECT $1, e.id, e.created_at \
         FROM unnest($2::bytea[], $3::timestamptz[]) AS k(id, created_at) \
         JOIN events e ON e.community_id = $1 AND e.created_at = k.created_at AND e.id = k.id \
         WHERE e.kind = ANY($4) \
         UNION ALL \
         SELECT $1, original.id, original.created_at \
         FROM unnest($2::bytea[], $3::timestamptz[]) AS k(id, created_at) \
         JOIN events edit \
           ON edit.community_id = $1 AND edit.created_at = k.created_at AND edit.id = k.id \
         JOIN events original \
           ON original.community_id = $1 AND original.channel_id = edit.channel_id \
          AND original.id = {target} \
         WHERE edit.kind = {KIND_MESSAGE_EDIT} AND original.kind = ANY($4)",
        target = crate::store::media::edit_target("edit"),
    );
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(community.as_uuid())
        .bind(event_ids)
        .bind(created_at)
        .bind(SEARCHABLE_KINDS.as_slice())
        .execute(tx.conn())
        .await?;
    Ok(())
}

/// The current state of a queued event, as the index should hold it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchDocument {
    /// Event id.
    pub event_id: [u8; 32],
    /// Event kind.
    pub kind: i32,
    /// Author public key.
    pub pubkey: [u8; 32],
    /// Channel, for channel-scoped events.
    pub channel_id: Option<Uuid>,
    /// Signed creation time.
    pub created_at: DateTime<Utc>,
    /// Message text.
    pub content: String,
    /// The channel's type (`stream`, `forum`, `dm`, ...), when it has one.
    pub channel_type: Option<String>,
    /// Whether the author is an agent with an owner.
    pub author_is_agent: bool,
    /// Root of the thread the event replies in, if it is a reply.
    pub thread_root: Option<[u8; 32]>,
    /// Hex public keys the event mentions.
    pub mentions: Vec<String>,
}

/// Rows claimed for one community, with the current state of their events.
///
/// Holds an admitted transaction, so the community cannot be fenced for
/// deletion while the batch is being indexed. Dropping the batch without
/// [`SearchOutboxBatch::complete`] rolls back and leaves the rows queued.
#[must_use = "dropping a batch leaves its rows queued; call complete()"]
pub struct SearchOutboxBatch {
    tx: AdmittedTx,
    /// Claimed rows and the events they name.
    rows: Vec<(Uuid, [u8; 32])>,
    /// Events to write to the index.
    pub documents: Vec<SearchDocument>,
    /// Queued events that are gone, deleted or not searchable: their
    /// documents must be removed from the index.
    pub removed: Vec<[u8; 32]>,
}

impl SearchOutboxBatch {
    /// The community every row in the batch belongs to.
    pub fn community(&self) -> CommunityId {
        self.tx.community()
    }

    /// Delete the claimed rows and commit. Call once the index holds the
    /// batch.
    pub async fn complete(self) -> Result<()> {
        self.complete_except(&[]).await
    }

    /// Delete the claimed rows except those naming an event in `retry`, and
    /// commit. Call once the index holds every other event of the batch; the
    /// kept rows are claimed again by a later batch.
    pub async fn complete_except(mut self, retry: &[[u8; 32]]) -> Result<()> {
        let community = self.tx.community();
        let done: Vec<Uuid> = self
            .rows
            .iter()
            .filter(|(_, event)| !retry.contains(event))
            .map(|(id, _)| *id)
            .collect();
        sqlx::query("DELETE FROM search_outbox WHERE community_id = $1 AND id = ANY($2)")
            .bind(community.as_uuid())
            .bind(&done)
            .execute(self.tx.conn())
            .await?;
        self.tx.commit().await
    }
}

/// Claim up to `limit` of the oldest queued rows of one community not in
/// `skip` and not being indexed by another worker, and read their events'
/// current state.
///
/// Returns `None` when no other active community has unclaimed rows.
pub async fn claim_batch(
    pool: &PgPool,
    limit: i64,
    skip: &[CommunityId],
) -> Result<Option<SearchOutboxBatch>> {
    let mut refused: Vec<Uuid> = skip.iter().map(|community| *community.as_uuid()).collect();
    for _ in 0..CLAIM_COMMUNITY_CANDIDATES {
        // Exclude fenced communities so they cannot occupy the queue head.
        let candidate: Option<Uuid> = sqlx::query_scalar(
            "SELECT o.community_id FROM search_outbox o \
             JOIN communities c ON c.id = o.community_id \
             WHERE c.deletion_state = 'active' AND c.deleted_at IS NULL \
               AND o.community_id <> ALL($1) \
             ORDER BY o.created_at LIMIT 1",
        )
        .bind(&refused)
        .fetch_optional(pool)
        .await?;
        let Some(id) = candidate else {
            return Ok(None);
        };
        let community = CommunityId::from_uuid(id);
        let Some(mut tx) = crate::begin_community_sweep_transaction(
            pool,
            community,
            crate::observability::WriterOperation::Maintenance,
        )
        .await?
        else {
            refused.push(id);
            continue;
        };
        // One batch per community at a time, so two workers never write
        // different states of one message in the wrong order. The lock ends
        // with the batch's transaction.
        let exclusive: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_xact_lock($1, hashtext($2::text))")
                .bind(CLAIM_LOCK_CLASS)
                .bind(id)
                .fetch_one(tx.conn())
                .await?;
        if !exclusive {
            refused.push(id);
            continue;
        }
        let rows = sqlx::query(
            "SELECT id, event_id, event_created_at FROM search_outbox \
             WHERE community_id = $1 \
             ORDER BY created_at \
             FOR UPDATE SKIP LOCKED \
             LIMIT $2",
        )
        .bind(community.as_uuid())
        .bind(limit)
        .fetch_all(tx.conn())
        .await?;
        if rows.is_empty() {
            // Another worker holds every row of this community, or its rows
            // were completed since the candidate read.
            refused.push(id);
            continue;
        }
        let mut claimed = Vec::with_capacity(rows.len());
        let mut queued = std::collections::BTreeMap::<[u8; 32], DateTime<Utc>>::new();
        for row in &rows {
            let event = bytes32(row.try_get("event_id")?)?;
            claimed.push((row.try_get("id")?, event));
            queued.insert(event, row.try_get("event_created_at")?);
        }
        let documents = read_documents(&mut tx, &queued).await?;
        for document in &documents {
            queued.remove(&document.event_id);
        }
        let removed = queued.into_keys().collect();
        return Ok(Some(SearchOutboxBatch {
            tx,
            rows: claimed,
            documents,
            removed,
        }));
    }
    Ok(None)
}

/// Read the current state of each live, searchable queued event, with the
/// text of its latest live edit in place of its own.
async fn read_documents(
    tx: &mut AdmittedTx,
    queued: &std::collections::BTreeMap<[u8; 32], DateTime<Utc>>,
) -> Result<Vec<SearchDocument>> {
    let community = tx.community();
    let ids: Vec<&[u8]> = queued.keys().map(|id| id.as_slice()).collect();
    let created: Vec<DateTime<Utc>> = queued.values().copied().collect();
    // The (created_at, id) pair is the events key, so each lookup prunes to
    // one partition. The latest edit is the one clients show (see
    // `media::latest_message_edit_sql`), read through the partial
    // `idx_events_media_edit_target`, whose predicate needs the literal kind.
    let sql = format!(
        "SELECT e.id, e.kind, e.pubkey, e.channel_id, e.created_at, \
                COALESCE(edit.content, e.content) AS content, \
                c.channel_type::text AS channel_type, \
                (u.agent_owner_pubkey IS NOT NULL) AS author_is_agent, \
                t.root_event_id, \
                ARRAY(SELECT m.pubkey_hex::text FROM event_mentions m \
                      WHERE m.community_id = e.community_id AND m.event_id = e.id \
                      ORDER BY m.pubkey_hex) AS mentions \
         FROM unnest($2::bytea[], $3::timestamptz[]) AS k(id, created_at) \
         JOIN events e \
           ON e.community_id = $1 AND e.created_at = k.created_at AND e.id = k.id \
         LEFT JOIN LATERAL ( \
             SELECT ed.content FROM events ed \
             WHERE ed.community_id = e.community_id AND ed.channel_id = e.channel_id \
               AND ed.kind = {KIND_MESSAGE_EDIT} AND ed.deleted_at IS NULL \
               AND {target} = e.id \
             ORDER BY ed.created_at DESC, ed.id DESC LIMIT 1 \
         ) edit ON true \
         LEFT JOIN channels c ON c.community_id = e.community_id AND c.id = e.channel_id \
         LEFT JOIN users u ON u.community_id = e.community_id AND u.pubkey = e.pubkey \
         LEFT JOIN thread_metadata t \
           ON t.community_id = e.community_id \
          AND t.event_created_at = e.created_at AND t.event_id = e.id \
         WHERE e.deleted_at IS NULL AND e.kind = ANY($4)",
        target = crate::store::media::edit_target("ed"),
    );
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(community.as_uuid())
        .bind(&ids)
        .bind(&created)
        .bind(SEARCHABLE_KINDS.as_slice())
        .fetch_all(tx.conn())
        .await?;
    let mut documents = Vec::with_capacity(rows.len());
    for row in rows {
        documents.push(SearchDocument {
            event_id: bytes32(row.try_get("id")?)?,
            kind: row.try_get("kind")?,
            pubkey: bytes32(row.try_get("pubkey")?)?,
            channel_id: row.try_get("channel_id")?,
            created_at: row.try_get("created_at")?,
            content: row.try_get("content")?,
            channel_type: row.try_get("channel_type")?,
            author_is_agent: row.try_get("author_is_agent")?,
            thread_root: row
                .try_get::<Option<Vec<u8>>, _>("root_event_id")?
                .map(bytes32)
                .transpose()?,
            mentions: row.try_get("mentions")?,
        });
    }
    Ok(documents)
}

/// Queued rows, for lag checks and tests.
pub async fn depth(pool: &PgPool) -> Result<i64> {
    Ok(sqlx::query_scalar("SELECT count(*) FROM search_outbox")
        .fetch_one(pool)
        .await?)
}

fn bytes32(bytes: Vec<u8>) -> Result<[u8; 32]> {
    <[u8; 32]>::try_from(bytes.as_slice())
        .map_err(|_| crate::DbError::InvalidData(format!("expected 32 bytes, got {}", bytes.len())))
}
#[cfg(test)]
mod tests {
    use super::*;

    /// Replacing a replaceable or addressable event soft-deletes its previous
    /// version without queuing a row. That is correct only while no such kind
    /// is searchable or a message edit; adding one needs the replacement paths
    /// to queue the superseded version too.
    #[test]
    fn searchable_kinds_are_regular_events() {
        for kind in SEARCHABLE_KINDS.into_iter().chain([KIND_MESSAGE_EDIT]) {
            let nostr_kind = nostr::Kind::from(kind as u16);
            assert!(
                !nostr_kind.is_replaceable()
                    && !nostr_kind.is_addressable()
                    && !nostr_kind.is_ephemeral(),
                "kind {kind} is replaceable, addressable or ephemeral"
            );
        }
    }
}
