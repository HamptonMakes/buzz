-- The external version the search indexer writes with each batch (see
-- crates/buzz-db/src/store/search_outbox.rs). A sequence never hands out a
-- smaller value than one it already handed out on the same primary, so a
-- batch's version is larger than that of every batch before it. It starts
-- above the database clock in microseconds, which earlier versions used, so
-- an index written with those keeps accepting writes.
CREATE SEQUENCE search_index_version AS BIGINT START WITH 10000000000000000;
