# Chroma → Milvus Incremental Sync

## What was accomplished (2026-04-06)

### Infrastructure
- **Cloudflare tunnel routes** already configured in `k8s-agi/contexts/home-lab/cloudflared/configmap.yaml`:
  - `milvus.hive-mcp.com` → `tcp://milvus.milvus.svc.cluster.local:19530`
  - `ollama.hive-mcp.com` → `http://ollama.ollama.svc.cluster.local:11434`
- **hive-mcp startup script** (`dotfiles/.local/bin/blw/hive-mcp`) updated with CF tunnel + env vars
- **CF Access auth required** — browser login prompt on first `cloudflared access tcp` use

### Migration tooling
- Added `sync!` function to `hive-milvus/src/hive_milvus/migrate.clj`
  - Reads all entries from fresh Chroma SQLite backup
  - Queries Milvus for existing IDs via `query-scalar` (paginated)
  - Only embeds + upserts the **delta** (new entries not in Milvus)
  - Much faster than full `run-all!` with `reset-cursor? true`
- Added `:sync` alias to `hive-milvus/deps.edn`

### KG performance fixes
- Added indexes on `:kg-edge/from`, `/to`, `/relation`, `/scope` (`norms/kg/004-kg-edge-indexes.edn`)
- `count-edges` and `edge-stats` now use aggregate queries (no full table load)
- Lazy `concat` → eager `into` in `get-node-context` and related functions

### Known issue: CF tunnel doesn't work for Milvus gRPC
- `cloudflared access tcp` wraps TCP-over-WebSocket — Milvus gRPC times out (`DEADLINE_EXCEEDED`)
- **Workaround**: use `kubectl port-forward` for sync operations
- Long-term fix: run hive-mcp inside k8s (no tunnel needed)

## How to sync tomorrow

### 1. Take a fresh Chroma SQLite backup

```bash
docker cp hive-mcp-chroma:/data/chroma.sqlite3 ~/backups/chroma/chroma-$(date +%Y%m%d).sqlite3
```

### 2. Start Milvus port-forward

```bash
kubectl port-forward -n milvus svc/milvus 19530:19530 &
```

### 3. Run incremental sync

From an **isolated temp dir** (avoids deps.edn merge conflicts):

```bash
cd /tmp/hive-sync  # already has deps.edn with absolute paths

CHROMA_SQLITE_PATH=~/backups/chroma/chroma-YYYYMMDD.sqlite3 \
MILVUS_HOST=localhost \
MILVUS_PORT=19530 \
OLLAMA_HOST=localhost:11434 \
clj -M -e '(do (require (quote [hive-milvus.migrate :as m]))
               (println (pr-str (m/sync! {:collections ["hive-mcp-memory" "hive-mcp-plans"]}))))'
```

The `/tmp/hive-sync/deps.edn` contains:

```edn
{:paths ["/home/leibniz/PP/hive/hive-milvus/src"]
 :deps {org.clojure/clojure       {:mvn/version "1.12.1"}
        org.clojure/data.json     {:mvn/version "2.5.1"}
        com.taoensso/timbre       {:mvn/version "6.8.0"}
        org.xerial/sqlite-jdbc    {:mvn/version "3.49.1.0"}
        org.slf4j/slf4j-simple    {:mvn/version "2.0.17"}
        io.github.hive-agi/milvus-clj {:local/root "/home/leibniz/PP/hive/milvus-clj"}
        io.github.hive-agi/hive-di {:local/root "/home/leibniz/PP/hive/hive-di"}
        io.github.hive-agi/hive-dsl {:local/root "/home/leibniz/PP/hive/hive-dsl"}}}
```

### 4. Kill port-forward when done

```bash
pkill -f "kubectl port-forward.*milvus"
```

## Expected output

```
Syncing hive-mcp-memory → hive_mcp_memory
Connected to Milvus at localhost:19530
Delta: 359 new of 64443 total
Batch 1/8 (50 entries)
...
Sync complete: {:collections 2, :results [{:collection "hive-mcp-memory", :status :synced, :new 359, :inserted 359, :skipped 0} ...]}
```

## Once hive-mcp runs on k8s

All of this goes away — hive-mcp connects directly to `milvus.milvus.svc:19530` cluster-internal. No tunnels, no port-forwards, no sync. See kanban epic `20260405225319-07677763`.
