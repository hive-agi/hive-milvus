# REST scalar indexes: diagnosis, scratch measurement, rollout

## Read-only live diagnosis

2026-10-24: attempted clojure_eval on 7910; bridge refused with existing
`port-7910` session error. Used a direct nREPL socket for local, read-only
expressions instead. No definitions, reloads, tests, redefs, tracers or var-root
changes in that JVM. Read the singleton class/config (token omitted), index
memo, and REST collection list/stats/index list/describe. No real index writes.

- Client: `milvus_clj.transport.http.HttpClient`, database `default`.
- `hive_mcp_memory_2560d`: 741,650 rows; only `embedding_idx`, HNSW/COSINE,
  Finished, 740,964 indexed rows at describe time. No scalar indexes.
- `hive_mcp_memory_1024d`: 9,429 rows; only `embedding_idx`, HNSW/COSINE,
  Finished. No scalar indexes.
- `hive_mcp_presets_4096d`: 2 rows, only embedding index.
- Important correction to the original finding: legacy `hive_mcp_memory`
  (5,792 rows) already has `embedding`, `type_idx`, `project_id_idx`.
- Process index memo nevertheless contained `hive_mcp_memory_2560d`.

`query-entries` builds server predicates for type equality, project_id equality
or IN, tags LIKE `%tag%`, excluded tags NOT LIKE, created/updated greater-than,
and default `(expires == "" or expires > now)`. Duration is NOT a server-side
filter in this implementation. `get-entries` uses primary-key get by distinct
ids and has no type/tag/duration predicate. Content-hash and expiry-specific
operations are separate from query-entries.

Only type/project_id are indexed by this patch (the existing declared policy).
Serialized tags are VarChar, not arrays: infix LIKE is not made fast by these
indexes. No schema migration or promises about tag performance.

## Implementation

The pinned milvus-clj 0.2.5 HTTP extras don't implement create-index. A small
hive-milvus adapter uses the supported v2 REST indexes/list, /describe, /create
endpoints, checked HTTP status and envelope code, token auth, database, bounded
connect/request timeouts. It avoids modifying or shadowing the dependency.
This adapter can move upstream when the client publishes scalar extras.

Probe actual field names, preserve existing indexes (even custom names/types),
create missing `type_idx`/`project_id_idx` as INVERTED, no metricType for scalar.
Only memoize when BOTH report Finished; pending builds are re-probed, failed
builds throw into WARN + retry/no memo. Creates returning errors are no longer
misrepresented as success. The memo guard serializes installation within this
process; server definitions make subsequent processes idempotent. Invalidation
of the collection-load memo also invalidates its index memo. gRPC failures are
likewise warned/not memoized, though its legacy extras lack describe and may
retry already-existing indexes; the field-aware idempotence is the REST path.

## Reproduction and results

Isolated JVM from this worktree, never the live JVM:

```
clojure -J-Xmx1200m -Sdeps '{:deps {org.slf4j/slf4j-api {:mvn/version "2.0.20"}}}' -M:dev -m nrepl.cmdline --bind 127.0.0.1 --port 0
```

Require scalar-index-bench; call `(scalar-index-bench/run! private-json-config)`.
The config is a private JSON rendering of the ordinary flat milvus-clj config;
never commit it. gRPC is used ONLY for scratch flush so rows are sealed/indexable.
The production reads and index creation use HTTP. Both collection names are
fresh `cx_scalar_<timestamp>_a/b`, dropped in finally. No existing name accepted.

Final run `cx_scalar_1792817287093`: two identical 50,000-row collections,
1 KiB content each, 32d vectors, 97 scopes, 10% decisions, two tags, medium
duration, blank expiry. This uses the allowed 50k fallback, NOT the live 741k
scale, and reduces vector dimensions to bound resource use on the shared
server. Each collection verified rowCount=50,000. A has embedding only; B also
has INVERTED type/project_id. Waited for Finished, **no release/reload**.
One warmup per case/arm, then eight samples/arm, alternating AB/BA. Actual
`entries/query-entries`, default full-content projection, limit 100. Scratch-only
embedder injection routes exclusively to scratch. Auto-index ensure is excluded
in BOTH arms by populating the isolated JVM's memo for those scratch names.

| Query (all include default expiry predicate) | Before mean ms | After mean ms | Before median ms | After median ms |
| --- | ---: | ---: | ---: | ---: |
| decision + project_0 (52 hits) | 546.41 | 387.52 | 275.65 | 274.44 |
| tags bench, exclude hidden, project IN [0,1] | 586.66 | 635.49 | 385.51 | 474.18 |
| note + created > 2025-01-01 | 506.17 | 504.40 | 375.25 | 370.58 |
| tag scope:project:project_0 only | 497.01 | 488.23 | 381.01 | 382.31 |

**No robust latency win demonstrated at 50k.** The apparent 29% mean scope/type
improvement is tail-driven; medians are essentially equal. Latency drifted from
~190ms to >1s in both arms on the shared server. No speedup claim or extrapolation
to the live 741k collection is warranted. Index existence/build success is proven,
not that missing indexes explain all reported latency.

Submission/probe/create pass took 1,444ms; two pending polls then Finished
(~4 seconds from submission through final polling). Both 50k rows fully indexed.
A prior raw scalar-query run (same 50k shape, no warmup, explicit scratch
release/load) also showed noisy results: mean scope/type 256.96 -> 331.68ms,
tags/exclude 257.67 -> 236.52ms, type/time 328.58 -> 265.78ms, tags-only
344.13 -> 269.02ms. Its explicit scratch reload took 588ms. It is NOT the rollout
path and NOT evidence a reload is required.

All four scratch collections from both runs were dropped. Finished definitions
were re-ensured successfully on scratch without recreation.

## Rollout: exactly what this code requests

At next fresh startup, existing ensure-collection!/query-entries paths will
probe each collection they touch. The 2560d and 1024d collections will receive
missing type_idx/project_id_idx INVERTED definitions; presets receives them if
that collection is ensured. The legacy collection's existing indexes are kept.
No content/vector rewrite, flush, release, index replacement/drop, or extra
load is requested by this patch. Existing startup load-collection behavior is
unchanged. No live state was changed during development.

Index submission is synchronous HTTP (up to configured 30s per request), but
server builds are asynchronous. The caller does not wait for Finished: the next
ensure re-probes until both are Finished, then the process memo skips RPCs.
A single process-local monitor serializes ensure passes; it does not lock
Milvus data. During a slow pass other index ensures can wait on that monitor.
Each failed pass warns and retries at next ensure; persistent failures can add
latency/log volume. Queries remain allowed when index setup fails.

**Live build duration and server lock/resource impact are unknown without a
live rollout.** The 50k scratch builds finished in ~4s online with no observed
query errors, but this does not bound 741k build time or assert zero internal
Milvus locking/load impact. Expect index-node CPU, disk and eventual query-node
memory work; schedule and observe. Finished is the server's build state, not
proof each already-loaded segment immediately uses the new index. This patch
deliberately does NOT release/reload real collections to force adoption. If the
coordinator wants a reload after observing usage, it is a separate disruptive
operation requiring its own decision.

A hot reload into the existing process alone is insufficient: defonce memo is
already falsely populated. Prefer fresh process startup; do not mutate live
vars to force rollout. Rollback of application code does not remove indexes
already submitted; index deletion would be a separate explicit admin action.

## Tests

Seven focused tests, 18 assertions, zero failures/errors in isolated JVM; no
with-redefs. Includes actual local HTTP server auth/database/envelope check,
preserve existing custom-name field index, partial failure retry, failed build,
pending-build re-probe without recreation, Finished idempotence, transport error.
`git diff --check` passes. No tests run in the live JVM.
