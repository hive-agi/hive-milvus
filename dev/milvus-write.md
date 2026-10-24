# Acknowledged add receipts (2026-10-24)

## Safety and diagnosis

Live nREPL 7910 was inspected read-only (configuration and store class names).
The clojure_eval bridge refuses with an existing-session error; the direct
nREPL socket client was used instead. No tracing, redefinition, reload, tests,
or writes were performed in that JVM. Live configuration confirms milvus-clj
HTTP transport, database `default`. Read-only cluster pod inspection confirms
running image `milvusdb/milvus:v2.5.6` (pod milvus-78bcf65f85-hq6sb).

Pinned milvus-clj 0.2.5 sends exactly one REST POST for an upsert, no flush.
Warm collection setup issues no calls. Liveness can add a collections/has POST
when the 5-second probe cache expires. Reads are not strong in this HTTP client:
the consistency option is discarded. The scratch run actually needed 2–3 get
requests per freshly added entry, not just one.

Milvus v2.5.6's CollectionDataReq has dbName, collectionName, partitionName and
data, no partial-update/merge option (upstream source:
https://github.com/milvus-io/milvus/blob/v2.5.6/internal/distributed/proxy/httpserver/request_v2.go).
Do not send scalar-only replacement upserts or cache old vectors: either can
lose content/vector updates. Metadata update remains read-vector + full upsert.

## Change

`hive-milvus.store.write/add-entry-returning!` returns an acknowledged receipt
`{:id id :entry normalized-entry}` decoded from the exact sent record with the
same schema/record->entry function as get. Generated fields, JSON coercion,
empty/default values survive; vectors never escape. Ordinary add still returns
an id. Guard/refusal and transient-failure results remain unchanged.

Optional `hive-spi.memory.receipt/IMemoryStoreAddReceipt` is implemented when
that SPI namespace is available. Older SPI remains supported. A caller must
use the capability only on the actual outer store, never unwrap decorators.

## Isolated benchmark

Separate JVM, -Xmx1500m, scratch `cx_write_1792820578747`, 2560-dimensional
stub vector, 1 KiB content. One warmup per arm, then AB/BA interleaving, n=5
per arm. Timed composition: duplicate lookup + embed/add + canonical get with
production's six-attempt/40ms-linear retry policy, versus duplicate lookup +
embed/add receipt. No KG edges requested. Setup excluded equally. Each receipt
was compared to a later actual fetch outside the timed/request-count window.
No with-redefs; a protocol-decorating client records requests in this JVM only.

| Measured item | Baseline | Receipt |
| --- | ---: | ---: |
| Mean total ms | 1037.459 | 450.275 |
| Dedupe query mean ms | 180.188 | 193.357 |
| Upsert mean ms | 294.263 | 255.942 |
| Get mean ms | 181.848 | none |
| Requests across 5 adds | 5 query + 5 upsert + 13 get | 5 query + 5 upsert |
| Requests per add | 4.6 | 2 |

56.6% lower mean, 2.30x faster in this no-KG composition, not an end-to-end
tool or embedding-provider latency claim. The upsert itself is one REST RTT
(~186–363ms here), not several hidden SDK writes. Backoff and visibility polls
explain much of the add+read wall time. Scratch collection dropped in finally.
Initial failed run also dropped its scratch collection; no samples were taken
from it.

## Deployment limitation

Live outer store chain is ShapeLayer -> VaultLayer -> CipherLayer ->
VisibilityLayer -> store-metrics reify. These cloak decorators must opt into
receipts by applying exactly their normal read transforms before a live add
can skip readback. The consumer MUST fall back otherwise. This branch does
not claim the scratch improvement is active in that live decorated chain.
KG writes can change the entry after the add receipt, and therefore require
normal fetch unless the final write supplies a fresh canonical receipt.

## Validation

Isolated focused tests (fake client, no network, no with-redefs):
- Legacy SPI: 8 tests / 41 assertions, 0 failures.
- New receipt protocol loaded: 9 tests / 43 assertions, 0 failures/errors.
- Receipt equality covers generated/provided ids and timestamps, JSON maps
  and JSON strings, defaults, counts, tags, project scope; oversize refusal.
- Scratch receipt equality is asserted against actual Milvus get.

Reproduction: start `clojure -J-Xmx1500m -M:dev -m nrepl.cmdline` in this
worktree, require write-bench, call `(write-bench/run! private-config-file)`.
The config file must stay private. Stop that JVM after use.
