# Storage format

A replica directory has a `lock`, a `raft/` journal, and a `relational/` journal. The durable `NATIVE` marker prevents fallback to the old image after native initialization. Imported replicas can also contain the preserved `raft.json`. A journal root has a `CURRENT` file with one newline-terminated `gen-<32 lowercase hex digits>` directory name. Generations have contiguous numbered segments named `00000000000000000001.seg`, starting at 1. Raft generations anchored by snapshots include an immutable `snapshot.bin`.

## Frame

Multi-byte integers in frame headers and payloads are big-endian.

| Bytes | Field |
| --- | --- |
| 4 | ASCII magic `RDB2` |
| 1 | Frame kind |
| 8 | Payload length |
| 4 | CRC32 of payload |
| 4 | CRC32 of preceding header bytes |
| Variable | Payload |

A frame may occupy its own segment when it exceeds the rotation threshold. Frame payloads are capped at 128 MiB. Rotation fsyncs the previous segment, then creates and fsyncs the new file and its directory entry. Recovery permits a physically short frame at the active segment's end. Full-frame CRC errors, short archived frames, missing interior segments, and missing generation pointers for existing data are integrity errors. Verification of a stopped replica reports a torn active frame before any restart repair.

## Raft frames

| Kind | Payload |
| --- | --- |
| 1 | JSON array containing node identity and optional snapshot metadata |
| 2 | JSON array containing current term, voted-for node or null, and commit index |
| 3 | First absolute index (u64), followed by one or more entry bodies |

A kind-3 entry body contains u64 term, one byte of flags (0 for native, 1 for imported SQL), u32 request-ID length, and u32 SQL length, followed by ASCII request ID and UTF-8 SQL. Both lengths are zero for a no-op. Entry indices are consecutive from the first index. Applying an edit replaces the suffix beginning at that index. Replay validates increasing hard state, nondecreasing entry terms, and the immutability of the committed prefix. The base entry comes from snapshot metadata or the index-zero sentinel. A new generation contains one metadata frame, hard state, any retained suffix, and a final commit marker.

## Relational frames

| Kind | Payload |
| --- | --- |
| 16 | Empty transaction start |
| 17 | u32 key length, key bytes, value bytes |
| 18 | u32 key length and key bytes (tombstone) |
| 19 | Empty transaction end |

An end frame commits the staged keys. Rotation may sync individual mutation records while a statement is in progress. Replay ignores staged records without an end marker, including abandoned transactions in prior segments. A subsequent start supersedes any abandoned staging. Values are checked during replay and on lookup. The live key directory stores offsets, not row values.

| Key bytes | Value |
| --- | --- |
| `S` + ASCII table name | JSON primary-column name and ordered schema |
| `R` + ASCII table name + NUL + typed primary key | JSON array of values in schema order |
| `D` + ASCII request ID | JSON SQL, original index, and response, or NUL + u64 legacy chunk count |
| `C` + request ID + NUL + u64 chunk number | A slice of a large imported outcome |
| `M\0applied` | u64 application checkpoint |

Primary key tags are `i` followed by i64, `b` followed by one byte (0 or 1), and `s` followed by UTF-8 text. The row key is unambiguous when text contains embedded NULs. A portable snapshot is one complete relational transaction containing PUT frames, ordered as in the source key directory. Snapshot metadata carries its last included index and term, length, and SHA-256 digest.

## Publication

The writer fsyncs files and the new generation directory, writes and fsyncs `CURRENT.tmp`, fsyncs the journal root, renames the pointer over `CURRENT`, then fsyncs the root again. Cleanup of abandoned or obsolete generations follows a successful publication or validation on reopen. During snapshot installation, `raft/CURRENT` is authoritative before `relational/CURRENT` switches. A checkpoint behind the snapshot on restart triggers restoration. A checkpoint ahead of Raft's durable commit is an error.
