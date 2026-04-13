#!/usr/bin/env bash
# Chroma → Milvus delta sync
# Reads all entries from Chroma SQLite, diffs against Milvus, embeds+upserts missing.
# Safe to re-run — only syncs what's missing.
#
# Prerequisites:
#   - Milvus port-forward alive (kubectl port-forward svc/milvus 19530:19530)
#   - Ollama running (embeddings)
#   - Chroma SQLite backup at CHROMA_SQLITE_PATH
#
# Usage:
#   ./sync-chroma.sh                    # foreground
#   ./sync-chroma.sh --background       # nohup, logs to ~/backups/chroma/sync.log

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_FILE="${HOME}/backups/chroma/sync.log"

# Defaults (override via env)
: "${MILVUS_HOST:=localhost}"
: "${MILVUS_PORT:=19530}"
: "${OLLAMA_HOST:=localhost:11434}"
: "${CHROMA_SQLITE_PATH:=${HOME}/backups/chroma/chroma-20260406.sqlite3}"
: "${COLLECTIONS:=hive-mcp-memory}"

export MILVUS_HOST MILVUS_PORT OLLAMA_HOST CHROMA_SQLITE_PATH

# Verify prerequisites
if ! bash -c "echo >/dev/tcp/${MILVUS_HOST}/${MILVUS_PORT}" 2>/dev/null; then
  echo "ERROR: Milvus not reachable at ${MILVUS_HOST}:${MILVUS_PORT}"
  echo "Start port-forward:"
  echo "  kubectl --context admin@blw -n milvus port-forward svc/milvus 19530:19530 &"
  exit 1
fi

if ! bash -c "echo >/dev/tcp/localhost/11434" 2>/dev/null; then
  echo "ERROR: Ollama not reachable at localhost:11434"
  echo "Start tunnel:"
  echo "  cloudflared access tcp --hostname ollama.hive-mcp.com --url localhost:11434 &"
  exit 1
fi

if [ ! -f "${CHROMA_SQLITE_PATH}" ]; then
  echo "ERROR: Chroma SQLite not found at ${CHROMA_SQLITE_PATH}"
  exit 1
fi

# Build collections array for Clojure
IFS=',' read -ra COLS <<< "${COLLECTIONS}"
CLJ_COLS=$(printf '"%s" ' "${COLS[@]}")

CLJ_CMD="(do
  (require (quote [hive-milvus.migrate :as m]))
  (let [result (m/sync! {:collections [${CLJ_COLS}]})]
    (prn result)
    (shutdown-agents)))"

# IMPORTANT: run from /tmp to avoid deps.edn merge with hive-milvus project
CLJ_DEPS="{
  :paths [\"${SCRIPT_DIR}/src\"]
  :deps {
    org.clojure/clojure       {:mvn/version \"1.12.1\"}
    org.clojure/data.json     {:mvn/version \"2.5.1\"}
    com.taoensso/timbre       {:mvn/version \"6.8.0\"}
    io.github.hive-agi/milvus-clj {:local/root \"${SCRIPT_DIR}/../milvus-clj\"}
    io.github.hive-agi/hive-di    {:local/root \"${SCRIPT_DIR}/../hive-di\"}
    io.github.hive-agi/hive-dsl   {:local/root \"${SCRIPT_DIR}/../hive-dsl\"}
    org.xerial/sqlite-jdbc    {:mvn/version \"3.49.1.0\"}
    org.slf4j/slf4j-simple    {:mvn/version \"2.0.17\"}
  }}"

run_sync() {
  cd /tmp
  exec clj -Sforce -Sdeps "${CLJ_DEPS}" -M -e "${CLJ_CMD}"
}

if [ "${1:-}" = "--background" ]; then
  echo "Starting sync in background..."
  echo "Log: ${LOG_FILE}"
  nohup bash -c "$(declare -f run_sync); run_sync" > "${LOG_FILE}" 2>&1 &
  echo "PID: $!"
  echo "Monitor: tail -f ${LOG_FILE}"
else
  echo "Syncing ${COLLECTIONS} from ${CHROMA_SQLITE_PATH}"
  echo "Milvus: ${MILVUS_HOST}:${MILVUS_PORT} | Ollama: ${OLLAMA_HOST}"
  echo "---"
  run_sync
fi
