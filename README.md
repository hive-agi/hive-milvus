# hive-milvus

Milvus vector database addon for [hive-mcp](https://github.com/hive-agi/hive-mcp).

Part of the [hive-agi](https://github.com/hive-agi) ecosystem.

## What

Standalone addon implementing `IMemoryStore` + `IAddon` for Milvus. Plugs into hive-mcp via classpath discovery (META-INF/hive-addons) -- hive-mcp stays backend-agnostic.

## Architecture

```
milvus-clj           <- Java SDK wrapper (gRPC, schema, index)
hive-milvus          <- THIS: IMemoryStore + IAddon + META-INF manifest
hive-mcp             <- Protocols only (IMemoryStore, IAddon). No milvus knowledge.
```

## Usage

### As dependency

```clojure
;; deps.edn
io.github.hive-agi/hive-milvus {:git/tag "v0.1.0" :git/sha "..."}
```

### Auto-discovery (addon)

Add to `local.deps.edn` or classpath. hive-mcp discovers `META-INF/hive-addons/milvus.edn` and registers the addon automatically.

Config via env vars:
```bash
export MILVUS_HOST=milvus.milvus.svc.cluster.local
export MILVUS_PORT=19530
```

### Manual registration

```clojure
(require '[hive-milvus.addon :as milvus-addon])
(require '[hive-mcp.addons.core :as addons])

(addons/register-addon! (milvus-addon/create-addon))
(addons/init-addon! "hive.milvus"
  {:host "localhost" :port 19530})
```

## Protocols Implemented

| Protocol | Methods |
|----------|---------|
| `IMemoryStore` | connect!, disconnect!, CRUD, query, search, expiration, duplicates |
| `IMemoryStoreWithAnalytics` | log-access!, record-feedback!, helpfulness-ratio |
| `IMemoryStoreWithStaleness` | update-staleness!, get-stale-entries, propagate-staleness! |
| `IAddon` | addon-id, initialize!, shutdown!, health, capabilities |

## Tests

```bash
# Requires running Milvus (port-forward or local)
clj -A:test
```

- **Contract tests** -- backend-agnostic suite from hive-mcp (22 tests)
- **Mutation tests** -- verifies tests catch tag/type/filter/staleness bugs
- **Property tests** -- roundtrip, totality, invariant, metamorphic
- **Golden tests** -- characterization snapshots for conversions + filters

## Dependencies

- [milvus-clj](https://github.com/hive-agi/milvus-clj) -- Milvus Java SDK wrapper
- [hive-mcp](https://github.com/hive-agi/hive-mcp) -- Protocol interfaces only
- [hive-test](https://github.com/hive-agi/hive-test) (test-only) -- property, mutation, golden macros

## License

AGPL-3.0-or-later
