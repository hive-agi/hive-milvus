# Milvus update latency experiment

## Diagnosis

Read-only inspection of the live JVM on port 7910 used local expressions only:
no definitions, reloads, instrumentation, tests or writes. The clojure_eval bridge
failed before evaluation with an existing-session error, so a direct nREPL socket
client was used. All benchmark and test evaluation used a separate JVM.

The live client is milvus-clj's HTTP transport. Collection
`hive_mcp_memory_2560d` reports Bounded consistency and only an embedding index.
The process's scalar-index memo says indexed, but HTTP `-create-index` is
unimplemented: index failures are swallowed by ensure-scalar-indexes!. This
explains why the memo is not evidence of actual scalar indexes. No live index
was created in this experiment.

The write implementation sends one REST upsert and does not flush. HTTP ignores
the requested consistency-level on reads; changing Strong to Bounded in the store
would not fix this transport. Explicit raw REST Strong/Bounded one-row queries
both took 180–198 ms. Server is Milvus 2.5.6; its user config only overrides auth,
session lease and etcd timeout, not flush or consistency settings.

Live read-only samples (three per path):

| Path | milliseconds |
| --- | --- |
| get-entry-by-id | 179.7, 190.8, 179.5 |
| scalar vector read (2560 dimensions) | 544.5, 545.1, 370.6 |
| scalar query 100 full-content rows | 1999.6, 1976.3, 1076.9 |

Metadata updates already read the row with its vector, merge, and upsert the
whole record. General update-entry! separately collected the entry and then
read its vector: an avoidable extra round trip, with an additional race between
those two snapshots. This change collects one entry/vector snapshot for general
updates, leaving the public result vector-free. Unchanged input retains the
vector; changed input or target still embeds. Ordinary relocation keeps its
narrow read, so no-op moves do not transfer unused vectors. No caching or change
to consistency semantics is introduced.

## Reproduction

Start an isolated JVM from this checkout:

```
clojure -J-Xmx1500m -M:dev -m nrepl.cmdline --bind 127.0.0.1 --port 0
```

Load dev/latency_bench.clj and evaluate `(latency-bench/run! connection-file)`.
The private EDN connection file contains the normal flat milvus-clj config; do
not commit credentials. The script creates a uniquely named `cx_latency_*`
collection with the memory schema and 2560-dimensional vectors, then drops it
in finally. It never writes existing collections. The embedding adapter used by
the smoke check routes exclusively to that scratch collection.

Two warmups, then eight samples per arm, interleaved AB/BA. Before composes the
old get + vector query + upsert; after composes get-with-vector + local keep +
upsert. Both use production helpers; routing/index setup and embedding-provider
latency are excluded equally. Input: 1 KiB content, 2560 floats. This is not a
claim about full tool latency or add-entry!/update-metadata! speedups.

Final run, scratch `cx_latency_1792816342192`:

| Stage mean, ms | Before | After |
| --- | ---: | ---: |
| Collect | 182.094 | 184.980 |
| Keep vector | 184.254 | 0.040 |
| Upsert | 191.359 | 185.691 |
| Total | 557.708 | 370.711 |

**33.5% lower mean, 1.50x faster**, eight samples per arm. Two earlier runs
returned 652.6 → 420.9 ms and 602.8 → 400.1 ms (eight per arm each).
Initial scratch upsert: 364.7 ms. Unchanged metadata helper (five samples):
913.8, 557.6, 540.3, 544.3, 369.0 ms; it remains a full-vector read/upsert.
All scratch collections were dropped; the isolated JVM was stopped.

## Validation

- update-snapshot-test + keep-vector-test: 17 tests, 36 assertions, zero failures
  or errors, including 200 generated equal-text cases. No with-redefs.
- Scratch round trip preserves vector and final metadata.
- Real relocate-update smoke on scratch preserves id despite rename attempt,
  does not expose the vector, performs zero embeds for metadata-only changes,
  and embeds once when content changes. The new content/vector survive reread.
- git diff --check passes.

## Remaining work

- Implement REST scalar-index management in milvus-clj and stop marking failed
  installations indexed. Validate index build benefit on representative data.
- Honor consistencyLevel in HTTP independently of this latency change; the
  existing strong-read documentation currently overpromises HTTP behavior.
- Metadata-only writes still transfer the full vector. Server 2.5.6 and the
  current client surface need capability validation before partial-upsert use.
- Add-entry! embeds then upserts; return-value contracts currently expose only
  id. Avoiding its immediate read-back needs an explicit receipt/return-entry
  capability across decorators, not an unbounded backend cache.
