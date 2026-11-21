//! Message search through the OpenSearch backend.
//!
//! The shared [`search_suite`] cases run against OpenSearch, then sync tests
//! write through the database layer's normal paths and wait for the indexer.
//! Needs Postgres (run through `scripts/postgres-test-run.sh -p buzz-search
//! --tests`) and an OpenSearch cluster at `BUZZ_TEST_OPENSEARCH_URL`
//! (default `http://localhost:9200`; `docker compose up -d opensearch`).

mod search_suite;

use search_suite::{search_suite_tests, Backend};

// Not run here, with the reason:
// - kind0_* and artifact_*: kinds 0 and 45010 are not searchable kinds;
// - prefix_*: prefix mode is reworked with prefix analysis (issue 06).
search_suite_tests! {
    Backend::OpenSearch;
    #[ignore = "requires Postgres and OpenSearch"]
    fn search_finds_event_in_same_community,
    fn search_does_not_return_other_community_events,
    fn authors_filter_restricts_results,
    fn channel_scope_restricts_results,
    fn channel_scope_of_thousands_of_channels,
    fn empty_query_returns_empty_result_no_roundtrip,
    fn since_until_filters,
    fn pagination_works,
    fn newest_sort_pages_by_time_then_id_within_scope,
    fn channel_less_only_excludes_per_channel_events,
    fn nul_bytes_in_query_are_sanitized,
    fn enormous_page_number_is_clamped,
    fn very_long_query_is_bounded_before_pg_parse,
    fn excluded_kinds_are_storage_level_unsearchable,
    fn author_only_kinds_are_storage_level_unsearchable,
    fn p_gated_persistent_kinds_have_storage_null_tsvector,
    fn deleted_events_are_excluded,
}

use std::time::Duration;

use buzz_core::CommunityId;
use buzz_db::channel::{ChannelType, ChannelVisibility};
use buzz_db::event::ThreadMetadataParams;
use buzz_db::user_deletion::content_batches::ContentBatchOptions;
use buzz_db::user_deletion::ExecutionMode;
use buzz_db::{search_outbox, Db, DbConfig, DeletionTarget};
use buzz_search::indexer::Indexer;
use buzz_search::opensearch::{OpenSearchConfig, SearchIndex};
use buzz_search::{ChannelScope, SearchMode, SearchQuery, SearchService, SearchSort};
use nostr::{Event, EventBuilder, Keys, Kind, Tag, Timestamp};
use opensearch::http::transport::Transport;
use opensearch::indices::{IndicesCreateParts, IndicesExistsParts};
use opensearch::{GetParts, OpenSearch};
use uuid::Uuid;

const TEST_OPENSEARCH_URL: &str = "http://localhost:9200";

fn opensearch_url() -> String {
    std::env::var("BUZZ_TEST_OPENSEARCH_URL").unwrap_or_else(|_| TEST_OPENSEARCH_URL.into())
}

/// A database layer with the search outbox on, as the relay runs it when
/// OpenSearch is configured.
async fn sync_db() -> Db {
    let db = unsynced_db().await;
    search_outbox::enable(db.pool())
        .await
        .expect("enable outbox");
    db
}

/// A database layer as a relay without OpenSearch runs it: admission queues
/// nothing.
async fn unsynced_db() -> Db {
    let (pool, _) = search_suite::setup().await;
    pool.close().await;
    Db::new(&DbConfig {
        database_url: std::env::var("BUZZ_TEST_DATABASE_URL")
            .unwrap_or_else(|_| search_suite::TEST_DB_URL.into()),
        ..DbConfig::default()
    })
    .await
    .expect("connect database layer")
}

/// A new index of the test's own, and its index-name prefix.
async fn sync_index() -> (SearchIndex, String) {
    let prefix = format!("test-{}-", Uuid::new_v4().simple());
    let index = SearchIndex::new(&OpenSearchConfig {
        url: opensearch_url(),
        basic_auth: None,
        index_prefix: prefix.clone(),
    })
    .expect("OpenSearch client");
    index.ensure().await.expect("create index");
    (index, prefix)
}

/// A message signed `offset` seconds from now.
fn message_at(keys: &Keys, content: &str, offset: i64) -> Event {
    EventBuilder::new(Kind::Custom(9), content)
        .custom_created_at(Timestamp::from(
            (Timestamp::now().as_secs() as i64 + offset) as u64,
        ))
        .sign_with_keys(keys)
        .expect("sign")
}

fn message(keys: &Keys, content: &str) -> Event {
    EventBuilder::new(Kind::Custom(9), content)
        .sign_with_keys(keys)
        .expect("sign")
}

/// Wait until the indexer has drained the outbox, then make its writes
/// visible to search.
async fn wait_until_indexed(db: &Db, index: &SearchIndex) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while search_outbox::depth(db.pool()).await.expect("outbox depth") > 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the indexer drains the outbox");
    index.refresh().await.expect("refresh");
}

async fn find(service: &SearchService, community: CommunityId, q: &str) -> Vec<[u8; 32]> {
    service
        .search(&SearchQuery {
            community,
            q: q.into(),
            channel_scope: ChannelScope::Any,
            kinds: None,
            authors: None,
            since: None,
            until: None,
            page: 1,
            per_page: 10,
            mode: SearchMode::FullText,
            sort: SearchSort::Newest,
        })
        .await
        .expect("search")
        .hits
        .into_iter()
        .map(|hit| hit.event_id)
        .collect()
}

/// An admitted message becomes searchable once the lease-holding indexer
/// catches up; a rolled-back one never does, and its text is not stored in
/// the document source.
#[tokio::test]
#[ignore = "requires Postgres and OpenSearch"]
async fn admitted_messages_become_searchable() {
    let db = sync_db().await;
    let (index, prefix) = sync_index().await;
    let community = search_suite::mk_community(db.pool(), "sync.example").await;
    let keys = Keys::generate();
    let indexer = tokio::spawn(Indexer::new(db.clone(), index.clone()).run());

    let kept = message(&keys, "syncword kept");
    db.insert_event(community, &kept, None)
        .await
        .expect("insert");
    let mut tx = db
        .begin_event_write_transaction(community)
        .await
        .expect("begin");
    buzz_db::event::insert_event_in_transaction(
        &mut tx,
        &message(&keys, "syncword rolled back"),
        None,
    )
    .await
    .expect("insert");
    tx.rollback().await.expect("rollback");

    wait_until_indexed(&db, &index).await;
    let service = SearchService::with_opensearch(db.pool().clone(), index.clone());
    assert_eq!(
        find(&service, community, "syncword").await,
        vec![*kept.id.as_bytes()]
    );

    let client = OpenSearch::new(Transport::single_node(&opensearch_url()).expect("transport"));
    let document: serde_json::Value = client
        .get(GetParts::IndexId(
            &format!("{prefix}messages"),
            &format!("{}:{}", community.as_uuid(), kept.id.to_hex()),
        ))
        .send()
        .await
        .expect("get document")
        .json()
        .await
        .expect("document body");
    assert_eq!(document["found"], true);
    assert_eq!(document["_source"]["kind"], 9);
    assert!(
        document["_source"].get("content").is_none(),
        "message text must not be stored: {document}"
    );

    indexer.abort();
    index.delete().await.expect("delete index");
}

/// Stopping the instance holding the indexer lease lets another take over,
/// and rows queued meanwhile are indexed.
#[tokio::test]
#[ignore = "requires Postgres and OpenSearch"]
async fn the_indexer_lease_hands_over() {
    let db = sync_db().await;
    let (index, _) = sync_index().await;
    let community = search_suite::mk_community(db.pool(), "lease.example").await;
    let keys = Keys::generate();
    let service = SearchService::with_opensearch(db.pool().clone(), index.clone());

    let first = tokio::spawn(Indexer::new(db.clone(), index.clone()).run());
    let before = message(&keys, "leaseword before");
    db.insert_event(community, &before, None)
        .await
        .expect("insert");
    wait_until_indexed(&db, &index).await;

    // The first instance holds the lease, so the second only waits.
    let second = tokio::spawn(Indexer::new(db.clone(), index.clone()).run());
    first.abort();
    let _ = first.await;
    let after = message(&keys, "leaseword after");
    db.insert_event(community, &after, None)
        .await
        .expect("insert");
    wait_until_indexed(&db, &index).await;
    assert_eq!(
        find(&service, community, "leaseword").await,
        vec![*after.id.as_bytes(), *before.id.as_bytes()]
    );

    second.abort();
    index.delete().await.expect("delete index");
}

/// Admission never calls OpenSearch: with it unreachable, messages are
/// stored and their rows stay queued until indexing succeeds.
#[tokio::test]
#[ignore = "requires Postgres and OpenSearch"]
async fn admission_queues_while_opensearch_is_unreachable() {
    let db = sync_db().await;
    let community = search_suite::mk_community(db.pool(), "down.example").await;
    let unreachable = SearchIndex::new(&OpenSearchConfig {
        url: "http://127.0.0.1:9".into(),
        basic_auth: None,
        index_prefix: "unreachable-".into(),
    })
    .expect("client");
    db.insert_event(community, &message(&Keys::generate(), "downword"), None)
        .await
        .expect("a message is stored while OpenSearch is down");

    Indexer::new(db.clone(), unreachable)
        .drain()
        .await
        .expect_err("indexing fails while OpenSearch is down");
    assert_eq!(search_outbox::depth(db.pool()).await.expect("depth"), 1);

    let (index, _) = sync_index().await;
    Indexer::new(db.clone(), index.clone())
        .drain()
        .await
        .expect("indexing succeeds once OpenSearch is back");
    index.refresh().await.expect("refresh");
    let service = SearchService::with_opensearch(db.pool().clone(), index.clone());
    assert_eq!(find(&service, community, "downword").await.len(), 1);
    index.delete().await.expect("delete index");
}

fn raw_client() -> OpenSearch {
    OpenSearch::new(Transport::single_node(&opensearch_url()).expect("transport"))
}

/// A batch the cluster rejects leaves its rows queued and its community
/// skipped, while other communities are still indexed in the same pass.
#[tokio::test]
#[ignore = "requires Postgres and OpenSearch"]
async fn a_rejected_community_does_not_hold_back_others() {
    let db = sync_db().await;
    // An index whose `content` is a keyword: a term longer than 32766 bytes
    // is rejected per document with HTTP 400.
    let prefix = format!("test-{}-", Uuid::new_v4().simple());
    let response = raw_client()
        .indices()
        .create(IndicesCreateParts::Index(&format!("{prefix}messages-v1")))
        .body(serde_json::json!({
            "mappings": { "properties": { "content": { "type": "keyword" } } },
            "aliases": { format!("{prefix}messages"): {} }
        }))
        .send()
        .await
        .expect("create index");
    assert!(response.status_code().is_success());
    let index = SearchIndex::new(&OpenSearchConfig {
        url: opensearch_url(),
        basic_auth: None,
        index_prefix: prefix,
    })
    .expect("client");

    let poisoned = search_suite::mk_community(db.pool(), "poisoned.example").await;
    let healthy = search_suite::mk_community(db.pool(), "healthy.example").await;
    let keys = Keys::generate();
    db.insert_event(poisoned, &message(&keys, &"x".repeat(40_000)), None)
        .await
        .expect("insert oversized term");
    db.insert_event(healthy, &message(&keys, "fine"), None)
        .await
        .expect("insert");

    let error = Indexer::new(db.clone(), index.clone())
        .drain()
        .await
        .expect_err("the pass reports the rejected batch");
    assert!(error.to_string().contains("rejected"), "{error}");
    let queued: Vec<Uuid> = sqlx::query_scalar("SELECT community_id FROM search_outbox")
        .fetch_all(db.pool())
        .await
        .expect("queued rows");
    assert_eq!(queued, vec![*poisoned.as_uuid()]);
    index.delete().await.expect("delete index");
}

/// Writes go through the alias and never create an index themselves, so a
/// deleted index cannot come back with a dynamic mapping; `ensure` restores
/// it with its alias.
#[tokio::test]
#[ignore = "requires Postgres and OpenSearch"]
async fn writes_never_create_an_index() {
    let db = sync_db().await;
    let (index, prefix) = sync_index().await;
    index.delete().await.expect("delete index");
    let community = search_suite::mk_community(db.pool(), "gone.example").await;
    db.insert_event(community, &message(&Keys::generate(), "orphan"), None)
        .await
        .expect("insert");

    let indexer = Indexer::new(db.clone(), index.clone());
    indexer
        .drain()
        .await
        .expect_err("a write without the index fails");
    let exists = raw_client()
        .indices()
        .exists(IndicesExistsParts::Index(&[&format!(
            "{prefix}messages-v1"
        )]))
        .send()
        .await
        .expect("exists");
    assert_eq!(exists.status_code().as_u16(), 404, "no index was created");
    assert_eq!(search_outbox::depth(db.pool()).await.expect("depth"), 1);

    index.ensure().await.expect("recreate index");
    indexer.drain().await.expect("indexing resumes");
    index.refresh().await.expect("refresh");
    let service = SearchService::with_opensearch(db.pool().clone(), index.clone());
    assert_eq!(find(&service, community, "orphan").await.len(), 1);
    index.delete().await.expect("delete index");
}

fn signed_at(event: &Event) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(event.created_at.as_secs() as i64, 0).expect("timestamp")
}

/// A channel of `community` created by `owner`.
async fn channel(db: &Db, community: CommunityId, owner: &Keys) -> Uuid {
    db.ensure_user(community, owner.public_key().as_bytes())
        .await
        .expect("ensure user");
    db.create_channel(
        community,
        &format!("sync-{}", Uuid::new_v4().simple()),
        ChannelType::Stream,
        ChannelVisibility::Open,
        None,
        owner.public_key().as_bytes(),
        None,
    )
    .await
    .expect("create channel")
    .id
}

/// A message in `channel`, signed `offset` seconds from now.
fn channel_message(
    keys: &Keys,
    channel: Uuid,
    kind: u16,
    content: &str,
    tags: &[&[&str]],
    offset: i64,
) -> Event {
    let mut all = vec![Tag::parse(["h", &channel.to_string()]).expect("h tag")];
    all.extend(
        tags.iter()
            .map(|tag| Tag::parse(tag.iter().copied()).expect("tag")),
    );
    EventBuilder::new(Kind::Custom(kind), content)
        .tags(all)
        .custom_created_at(Timestamp::from(
            (Timestamp::now().as_secs() as i64 + offset) as u64,
        ))
        .sign_with_keys(keys)
        .expect("sign")
}

/// Delete `target` with a NIP-09 deletion, as the relay stores one.
async fn delete(db: &Db, community: CommunityId, keys: &Keys, channel: Uuid, target: &Event) {
    let deletion = channel_message(keys, channel, 5, "", &[&["e", &target.id.to_hex()]], 2);
    let (_, _, deleted, _) = db
        .insert_deletion_event(
            community,
            &deletion,
            Some(channel),
            None,
            &[DeletionTarget {
                event_id: target.id.as_bytes(),
                created_at: signed_at(target),
                parent_event_id: None,
                root_event_id: None,
            }],
            &Keys::generate(),
        )
        .await
        .expect("insert deletion");
    assert_eq!(deleted, vec![true]);
}

/// A deleted message no longer matches once the indexer catches up.
#[tokio::test]
#[ignore = "requires Postgres and OpenSearch"]
async fn a_deleted_message_no_longer_matches() {
    let db = sync_db().await;
    let (index, _) = sync_index().await;
    let community = search_suite::mk_community(db.pool(), "delete.example").await;
    let keys = Keys::generate();
    let channel = channel(&db, community, &keys).await;
    let service = SearchService::with_opensearch(db.pool().clone(), index.clone());
    let indexer = tokio::spawn(Indexer::new(db.clone(), index.clone()).run());

    let doomed = channel_message(&keys, channel, 9, "deleteword doomed", &[], 0);
    let kept = channel_message(&keys, channel, 9, "deleteword kept", &[], 0);
    for event in [&doomed, &kept] {
        db.insert_event(community, event, Some(channel))
            .await
            .expect("insert");
    }
    wait_until_indexed(&db, &index).await;
    assert_eq!(find(&service, community, "deleteword").await.len(), 2);

    delete(&db, community, &keys, channel, &doomed).await;
    wait_until_indexed(&db, &index).await;
    assert_eq!(
        find(&service, community, "deleteword").await,
        vec![*kept.id.as_bytes()]
    );

    indexer.abort();
    index.delete().await.expect("delete index");
}

/// An edited message matches its new text and not its old text; deleting
/// the edit brings the old text back.
#[tokio::test]
#[ignore = "requires Postgres and OpenSearch"]
async fn an_edited_message_matches_its_current_text() {
    let db = sync_db().await;
    let (index, _) = sync_index().await;
    let community = search_suite::mk_community(db.pool(), "edit.example").await;
    let keys = Keys::generate();
    let channel = channel(&db, community, &keys).await;
    let service = SearchService::with_opensearch(db.pool().clone(), index.clone());
    let indexer = tokio::spawn(Indexer::new(db.clone(), index.clone()).run());

    let original = channel_message(&keys, channel, 9, "oldword typo", &[], 0);
    db.insert_event(community, &original, Some(channel))
        .await
        .expect("insert");
    wait_until_indexed(&db, &index).await;
    assert_eq!(
        find(&service, community, "oldword").await,
        vec![*original.id.as_bytes()]
    );

    let target = original.id.to_hex();
    let first = channel_message(&keys, channel, 40003, "midword", &[&["e", &target]], 1);
    let latest = channel_message(
        &keys,
        channel,
        40003,
        "newword fixed",
        &[&["e", &target]],
        2,
    );
    for edit in [&latest, &first] {
        db.insert_event(community, edit, Some(channel))
            .await
            .expect("insert edit");
    }
    wait_until_indexed(&db, &index).await;
    assert!(find(&service, community, "oldword").await.is_empty());
    assert!(find(&service, community, "midword").await.is_empty());
    assert_eq!(
        find(&service, community, "newword").await,
        vec![*original.id.as_bytes()]
    );

    delete(&db, community, &keys, channel, &latest).await;
    delete(&db, community, &keys, channel, &first).await;
    wait_until_indexed(&db, &index).await;
    assert!(find(&service, community, "newword").await.is_empty());
    assert_eq!(
        find(&service, community, "oldword").await,
        vec![*original.id.as_bytes()]
    );

    indexer.abort();
    index.delete().await.expect("delete index");
}

/// Account deletion scrubs a message another person replied to and erases
/// the rest; neither matches its text afterwards, and the reply still does.
#[tokio::test]
#[ignore = "requires Postgres and OpenSearch"]
async fn scrubbed_and_erased_messages_no_longer_match() {
    let db = sync_db().await;
    let (index, _) = sync_index().await;
    let community = search_suite::mk_community(db.pool(), "scrub.example").await;
    let leaving = Keys::generate();
    let staying = Keys::generate();
    let channel = channel(&db, community, &leaving).await;
    db.ensure_user(community, staying.public_key().as_bytes())
        .await
        .expect("ensure user");
    let service = SearchService::with_opensearch(db.pool().clone(), index.clone());
    let indexer = tokio::spawn(Indexer::new(db.clone(), index.clone()).run());

    let root = channel_message(&leaving, channel, 9, "scrubword referenced", &[], -2);
    let erased = channel_message(&leaving, channel, 9, "scrubword alone", &[], -1);
    for event in [&root, &erased] {
        db.insert_event(community, event, Some(channel))
            .await
            .expect("insert");
    }
    let root_hex = root.id.to_hex();
    let reply = channel_message(
        &staying,
        channel,
        9,
        "replyword",
        &[
            &["e", &root_hex, "", "root"],
            &["e", &root_hex, "", "reply"],
        ],
        0,
    );
    db.insert_event_with_thread_metadata(
        community,
        &reply,
        Some(channel),
        Some(ThreadMetadataParams {
            event_id: reply.id.as_bytes(),
            event_created_at: signed_at(&reply),
            channel_id: channel,
            parent_event_id: Some(root.id.as_bytes()),
            parent_event_created_at: Some(signed_at(&root)),
            root_event_id: Some(root.id.as_bytes()),
            root_event_created_at: Some(signed_at(&root)),
            depth: 1,
            broadcast: false,
        }),
    )
    .await
    .expect("insert reply");
    wait_until_indexed(&db, &index).await;
    assert_eq!(find(&service, community, "scrubword").await.len(), 2);

    let summary = db
        .user_deletion_store()
        .run_message_batches(
            &ContentBatchOptions {
                community: *community.as_uuid(),
                pubkey: leaving.public_key().to_hex(),
                relay_pubkey: None,
                batch_size: 100,
                max_items: 1000,
            },
            ExecutionMode::Delete,
            None,
            &mut Vec::new(),
        )
        .await
        .expect("delete the account's messages");
    assert_eq!(
        summary.scrubbed_events, 1,
        "the replied-to root is scrubbed"
    );
    wait_until_indexed(&db, &index).await;
    assert!(find(&service, community, "scrubword").await.is_empty());
    assert_eq!(
        find(&service, community, "replyword").await,
        vec![*reply.id.as_bytes()]
    );

    indexer.abort();
    index.delete().await.expect("delete index");
}

/// Keys queued in the outbox, newest event first.
async fn queued_events(db: &Db) -> Vec<[u8; 32]> {
    sqlx::query_scalar::<_, Vec<u8>>(
        "SELECT event_id FROM search_outbox ORDER BY event_created_at DESC, event_id DESC",
    )
    .fetch_all(db.pool())
    .await
    .expect("queued rows")
    .into_iter()
    .map(|id| id.try_into().expect("32-byte id"))
    .collect()
}

/// Index what is queued and make it visible to search.
async fn drain(indexer: &Indexer, index: &SearchIndex) {
    indexer.drain().await.expect("drain");
    index.refresh().await.expect("refresh");
}

/// Messages stored before OpenSearch was configured are backfilled newest
/// first, one bounded batch at a time. A restart resumes below the last
/// batch instead of starting over, live messages are indexed while the
/// backfill runs, and backfilling again creates no duplicate documents.
#[tokio::test]
#[ignore = "requires Postgres and OpenSearch"]
async fn stored_messages_are_backfilled_newest_first_and_resume() {
    let db = unsynced_db().await;
    let (index, prefix) = sync_index().await;
    let community = search_suite::mk_community(db.pool(), "backfill.example").await;
    // Another community, with nothing to backfill, takes its turns too.
    search_suite::mk_community(db.pool(), "bystander.example").await;
    let keys = Keys::generate();
    // Oldest first: stored[4] is the newest.
    let stored: Vec<Event> = (0..5)
        .map(|n| message_at(&keys, &format!("oldword {n}"), n - 10))
        .collect();
    for event in &stored {
        db.insert_event(community, event, None)
            .await
            .expect("insert");
    }
    let id = |event: &Event| *event.id.as_bytes();
    assert_eq!(search_outbox::depth(db.pool()).await.expect("depth"), 0);

    search_outbox::enable(db.pool())
        .await
        .expect("enable outbox");
    let service = SearchService::with_opensearch(db.pool().clone(), index.clone());
    // Two keys per batch, and no more queued than one batch at a time.
    let indexer = || {
        Indexer::new(db.clone(), index.clone())
            .with_batch_size(2)
            .with_backfill_bound(2)
    };

    // The first batch is the two newest messages.
    let first = indexer();
    assert_eq!(first.backfill().await.expect("backfill"), 2);
    assert_eq!(
        queued_events(&db).await,
        vec![id(&stored[4]), id(&stored[3])]
    );
    assert_eq!(
        first.backfill().await.expect("backfill"),
        0,
        "nothing more is queued while the outbox is at its bound"
    );
    drain(&first, &index).await;
    assert_eq!(
        find(&service, community, "oldword").await,
        vec![id(&stored[4]), id(&stored[3])]
    );

    // Stop here. A new indexer, as after a restart, continues below the
    // first batch.
    drop(first);
    let restarted = indexer();
    assert_eq!(restarted.backfill().await.expect("backfill"), 2);
    assert_eq!(
        queued_events(&db).await,
        vec![id(&stored[2]), id(&stored[1])]
    );

    // A live message admitted mid-backfill is indexed by the next drain.
    let live = message(&keys, "oldword live");
    db.insert_event(community, &live, None)
        .await
        .expect("insert live");
    drain(&restarted, &index).await;
    let completed: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT completed_at FROM search_backfill WHERE community_id = $1")
            .bind(community.as_uuid())
            .fetch_one(db.pool())
            .await
            .expect("marker");
    assert!(completed.is_none(), "the backfill is still running");
    assert_eq!(
        find(&service, community, "oldword").await,
        vec![
            id(&live),
            id(&stored[4]),
            id(&stored[3]),
            id(&stored[2]),
            id(&stored[1])
        ]
    );

    // The last batch is short, which completes the community.
    assert_eq!(restarted.backfill().await.expect("backfill"), 1);
    drain(&restarted, &index).await;
    assert_eq!(restarted.backfill().await.expect("backfill"), 0);
    let everything = vec![
        id(&live),
        id(&stored[4]),
        id(&stored[3]),
        id(&stored[2]),
        id(&stored[1]),
        id(&stored[0]),
    ];
    assert_eq!(find(&service, community, "oldword").await, everything);

    // Backfill everything again: the documents are rewritten, not doubled.
    sqlx::query("DELETE FROM search_backfill")
        .execute(db.pool())
        .await
        .expect("restart backfill");
    while restarted.backfill().await.expect("backfill") > 0 {
        drain(&restarted, &index).await;
    }
    drain(&restarted, &index).await;
    assert_eq!(find(&service, community, "oldword").await, everything);
    let count: serde_json::Value = raw_client()
        .count(opensearch::CountParts::Index(&[&format!(
            "{prefix}messages"
        )]))
        .send()
        .await
        .expect("count")
        .json()
        .await
        .expect("count body");
    assert_eq!(count["count"], 6, "{count}");
    index.delete().await.expect("delete index");
}

/// The lease holder starts backfilling on its own once OpenSearch is
/// configured.
#[tokio::test]
#[ignore = "requires Postgres and OpenSearch"]
async fn the_indexer_backfills_on_its_own() {
    let db = unsynced_db().await;
    let (index, _) = sync_index().await;
    let community = search_suite::mk_community(db.pool(), "auto.example").await;
    let keys = Keys::generate();
    for n in 0..5 {
        db.insert_event(
            community,
            &message_at(&keys, &format!("autoword {n}"), -n),
            None,
        )
        .await
        .expect("insert");
    }
    search_outbox::enable(db.pool())
        .await
        .expect("enable outbox");
    let indexer = tokio::spawn(
        Indexer::new(db.clone(), index.clone())
            .with_batch_size(2)
            .with_backfill_bound(2)
            .run(),
    );
    let service = SearchService::with_opensearch(db.pool().clone(), index.clone());
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            index.refresh().await.expect("refresh");
            if find(&service, community, "autoword").await.len() == 5 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("every stored message becomes searchable");
    indexer.abort();
    index.delete().await.expect("delete index");
}
