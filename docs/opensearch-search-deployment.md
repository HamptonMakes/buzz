# OpenSearch message index deployment

The relay can keep an OpenSearch index of messages up to date. Searches still
run on Postgres full-text search; the index is built ahead of switching reads.

## How it works

- Admission writes a `search_outbox` row in the same transaction as each
  stored event of a searchable kind (9, 40002, 40008, 45001, 45003). A
  rolled-back write queues nothing, and admission never calls OpenSearch.
  Deletions (NIP-09, NIP-29 and moderation) and account-deletion scrubs queue
  a row for each removed message the same way. A message edit (kind 40003),
  or a deletion of one, queues the message it edits. Account deletion
  (`buzz-admin user-deletions`) needs no OpenSearch setting: it queues
  whenever `search_outbox` or `search_backfill` holds a row, which is once
  an indexing relay has run against the database. To stop indexing for
  good, unset `BUZZ_OPENSEARCH_URL` everywhere, then empty both tables.
- One relay instance holds the indexer lease (a Postgres session advisory
  lock). Instances that start together spread their lease attempts with
  jitter, and followers check the lease every 2.5 to 5 seconds. The lease
  holder runs `BUZZ_SEARCH_INDEXER_WORKERS` workers. Each claims a batch of
  one community's rows, re-reads those events, writes or removes their
  documents in one bulk request, and deletes the claimed rows whose items
  succeeded. So at most that many bulk requests are in flight. Workers can
  index one community together: its queue is split into 64 partitions by
  event id, and a batch claims only rows of partitions no other batch holds,
  so two states of one message are never written out of order. Each worker
  holds a Postgres transaction, from the relay's main pool
  (`BUZZ_DB_POOL_SIZE`), for the length of its bulk request, so leave the
  pool room for the worker count on top of serving traffic. A message is indexed with the text
  of its latest live edit; a deleted or scrubbed message has its document
  removed. A change committed while its message's earlier row is being
  indexed keeps its own row and is indexed afterwards.
- Failures delay indexing and never drop a row. A failed request or item
  keeps its rows queued and skips its community for the rest of the pass;
  a failed pass retries with jittered backoff capped at 60 seconds. Items
  the cluster refuses for good (HTTP 400 or 413, for example a term too long
  for the mapping) are logged, counted and retried with each later batch of
  their community, but do not back the indexer off or stop the rest of the
  community indexing. Only a batch of nothing but refused items skips its
  community for the pass, so a community with a full batch of them is stuck
  until they are fixed: watch `buzz_search_index_rejected_total`. Throttling (HTTP 429, which Serverless returns while
  it scales) backs off the same way without counting as a failure.
- Every item is written with an external version: the next value of the
  `search_index_version` sequence, read while the batch holds its
  partitions. A later batch of the same message reads a later state and,
  since the sequence never goes back on one primary, a larger version, so
  a bulk request the cluster applies late (after a timeout or a crash)
  cannot undo a newer write. The sequence starts above the clock-based
  versions earlier builds wrote. A delete's version is kept for the index's
  `gc_deletes` (60 seconds by default). Promoting an asynchronous replica
  that had not seen the latest values can hand some of them out again:
  items whose document holds a version from before the failover then fail
  with a conflict and keep their rows queued, and each retry draws a new
  value, so they index once the sequence passes it. That shows up as
  queued rows and `buzz_search_index_version_conflicts_total`, not as lost
  writes; the counter should otherwise stay at zero.
- The lease holder also backfills messages stored before the outbox was on.
  For each community it queues the keys of searchable messages in batches
  of 500, newest first, and records how far it got in `search_backfill`
  in the same transaction, so a restart resumes below the last batch.
  Communities take turns one batch each. It queues only while the outbox
  holds fewer than 2,000 rows, or two batches per worker (1,000 rows a
  worker) when that is more, so live messages wait behind at most that many
  backfill rows: 2,000 for up to two workers, 8,000 at eight. Backfill goes through the same outbox and indexer as
  live writes, and queuing an indexed message again only rewrites its
  document. A community's backfill is complete when a batch comes back
  short; new communities backfill once, cheaply. To backfill a community
  again, delete its `search_backfill` row (or all rows for every
  community); the leader starts over from the newest message.
- The index (`buzz-messages`) is created by the leader on its first attempt,
  so an unreachable cluster never delays startup. Writes go through the alias
  and never create an index, so a deleted index is recreated with its mapping
  on the next attempt. Documents hold the message
  text in an indexed field only; `_source` does not store it.
- Community deletion fences and purges `search_outbox` and `search_backfill`
  with the community's other rows. It does not yet remove the community's documents from the
  index. Every search filters on the community, and a deleted community's id
  is never reused, so those documents never match a search, but their words
  stay in the index.

## Settings

| Variable | Meaning |
| --- | --- |
| `BUZZ_OPENSEARCH_URL` | HTTP(S) cluster URL without credentials, query or fragment. Unset disables the outbox and indexer. |
| `BUZZ_OPENSEARCH_USERNAME`, `BUZZ_OPENSEARCH_PASSWORD` | Optional basic auth. Set both or neither. |
| `BUZZ_SEARCH_INDEXER_WORKERS` | Indexer workers, and so bulk requests and Postgres connections held in flight, on the lease holder: 1 to 32, default 1. Workers share one community's queue by partition, so a single large community indexes faster too. With more than one, the backfill keeps two batches per worker queued, so live rows can wait behind that many backfill rows. Backfill is fed from the lease holder alone whatever the count. |

## Rollout order

1. Deploy a release that knows the `search_outbox` and `search_backfill`
   tables as pending deletion-scoped tables, before the release with
   migrations `0106` to `0108`. That keeps community deletion working in a
   mixed fleet and after a rollback.
2. Deploy the release with the migrations. Without `BUZZ_OPENSEARCH_URL`
   nothing is queued and nothing changes.
3. Set `BUZZ_OPENSEARCH_URL` on every relay instance. Each instance queues
   only the messages it admits itself, so an instance without the setting
   leaves gaps. Startup fails if migrations `0106` and `0108` have not run. The backfill
   starts as soon as the first instance holds the lease and covers every
   message stored before that. If instances without the setting admitted
   messages after it started, run `DELETE FROM search_backfill` once the
   whole fleet has the setting to backfill again.

If you stream Postgres changes through logical replication, keep
`search_outbox` and `search_backfill` out of the publication: their rows are
short-lived queue entries and progress markers, not data.

## Metrics and throughput

| Metric | Labels | Meaning |
| --- | --- | --- |
| `buzz_search_backfill_queued_total` | `community` | Message keys backfill queued. |
| `buzz_search_backfill_cursor_timestamp_seconds` | `community` | Creation time of the oldest message queued so far; everything newer is queued. |
| `buzz_search_backfill_complete` | `community` | 1 once the community's backfill is complete. |
| `buzz_search_indexed_documents_total` | `community`, `op` (`write`, `remove`) | Documents the indexer wrote or removed. Its rate is the indexing rate. |
| `buzz_search_index_rejected_total` | `community` | Bulk items the cluster refused for good. Their rows stay queued; a steady rate needs a look at the documents or mapping. |
| `buzz_search_index_version_conflicts_total` | `community` | Bulk items refused because the index held a newer version, which means the version sequence handed out values again, as after promoting an asynchronous replica. Their rows stay queued until the sequence passes that version. Should stay at zero. |
| `buzz_search_indexer_throttled_total` | | Passes the cluster throttled. |

Each drain pass that indexed anything ends with an info log,
`search indexer drained the outbox`, with `workers`, `batches`, `written`,
`removed`, `failed`, `elapsed_ms` and `documents_per_second`. To measure throughput locally, store
messages with `BUZZ_OPENSEARCH_URL` unset, then start the relay with it set:
the backfill passes log their rates as they catch up.

## Local run

```sh
docker compose --profile search up -d opensearch
BUZZ_OPENSEARCH_URL=http://localhost:9200 just relay
curl -s 'localhost:9200/buzz-messages/_search?pretty'
```

Watch the queue drain with
`psql -c 'SELECT count(*) FROM search_outbox'` and the backfill progress with
`psql -c 'SELECT * FROM search_backfill'`.
