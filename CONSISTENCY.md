# Consistency

A fresh SQL request runs at a committed index. Raft elects up-to-date replicas, persists term and vote before granting votes, and commits an index by counting a majority for an entry in the leader's term. AppendEntries replies follow durable processing. The state machine advances in index order and stores each SQL outcome in the same durable transaction as its mutation and applied checkpoint.

## Reads and retries

A fresh SELECT has a fresh request ID and uses the same quorum-committed path as a write. Reads are linearizable without follower reads, leases, or role-based shortcuts. Status information is an observation of one replica. A node may still report itself as leader during a minority partition. It can replay an already committed outcome but needs a quorum to confirm a fresh statement.

`POST /sql` accepts a valid `request_id` and one `sql` string of at most 16,384 UTF-8 bytes. A successful response includes `ok`, `request_id`, and committed `index`, plus `affected` or `columns` and `rows`.

| HTTP status | Meaning |
| --- | --- |
| 200 | The statement executed at a committed index |
| 400 | The request envelope is invalid |
| 409 | The request ID was used with different SQL |
| 422 | The statement committed and returned a SQL error |
| 503 | This replica could not confirm the operation |

A connection error or 503 leaves the outcome uncertain. Retry with the original ID and identical SQL to retrieve the original result or complete the pending operation. SQL errors are durable outcomes. Deduplication records have no expiration in this release and survive compaction and leadership changes.

## Batched results and imports

The optional `page` envelope contains an `offset` and `size`. A SELECT response includes `next_offset` when paginated. Use the same request ID and query for every page. Paging slices a single materialized, committed result, so all pages report the same index and concurrent writes cannot change later pages. A different request ID starts a new read. Independent SELECTs with manually incremented SQL OFFSET values can see different database versions.

The client and exports use paginated responses. Individual result pages do not hold a live read transaction or cursor on the server. Legacy migrated request outcomes can exceed the 16 MiB limit on new results. CSV import commits each batch independently. Retry the import with identical input, ordering, batch size, and request prefix to reuse committed batches.

## Operational boundaries

The supported failure model is crash/restart and communication failure among trusted peers on local POSIX storage that honors fsync and atomic rename. Remain within a fixed cluster configuration. An unavailable majority prevents new confirmations. Integrity failures are surfaced or stop the process. Recover lost durable state from a trusted backup using a coordinated cluster recovery procedure. Reusing a node identity with empty durable state is outside the supported failure model. Maintenance requests serialize against local application and operate only on applied committed state.
