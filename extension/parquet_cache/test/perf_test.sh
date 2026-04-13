#!/bin/bash
#
# Performance test: demonstrates latency reduction from persistent parquet metadata caching.
#
# Starts a slow HTTP server with configurable per-request latency, then compares query
# execution with and without the persistent cache across process restarts.
#
# Usage: ./extension/parquet_cache/test/perf_test.sh [latency_ms]
#
set -uo pipefail

LATENCY_MS="${1:-75}"
PORT=9798
DUCKDB="./build/release/duckdb"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CACHE_DB="/tmp/perf_test_parquet_cache.duckdb"

if [ ! -f "$DUCKDB" ]; then
    echo "Error: $DUCKDB not found. Run 'make' first."
    exit 1
fi

cleanup() {
    if [ -n "${SERVER_PID:-}" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    rm -f "${CACHE_DB}"*
}
trap cleanup EXIT

echo "Starting slow HTTP server (port=$PORT, latency=${LATENCY_MS}ms)..."
python3 "$SCRIPT_DIR/slow_http_server.py" "$PORT" "$LATENCY_MS" &
SERVER_PID=$!
sleep 1

if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "Error: HTTP server failed to start"
    exit 1
fi

FILE_URL="http://127.0.0.1:${PORT}/data/parquet-testing/cache/large_test.parquet"

echo ""
echo "Simulated per-request latency: ${LATENCY_MS}ms"
echo "Each HTTP request (HEAD or GET) adds ${LATENCY_MS}ms to wall-clock time."
echo ""
echo "================================================================"
echo "Test 1: Cold read — NO persistent cache"
echo "        (new process, no cached metadata, all HTTP requests needed)"
echo "================================================================"
echo ""

$DUCKDB -noheader <<SQL 2>/dev/null
LOAD httpfs;
CALL enable_logging('HTTP');

.timer on
SELECT count(*) FROM read_parquet('${FILE_URL}') WHERE i = 42;
.timer off

SELECT '  ' || request.type || ' requests: ' || COUNT(*)
FROM duckdb_logs_parsed('HTTP')
GROUP BY request.type
ORDER BY request.type;

SELECT '  Total HTTP requests: ' || COUNT(*) FROM duckdb_logs_parsed('HTTP');
SQL

echo ""
echo "================================================================"
echo "Test 2: Cold read — WITH persistent cache (populating)"
echo "        (new process, cache is empty, populates on first read)"
echo "================================================================"
echo ""

rm -f "${CACHE_DB}"*

$DUCKDB -noheader <<SQL 2>/dev/null
LOAD httpfs;
SET parquet_persistent_cache_path='${CACHE_DB}';
CALL enable_logging('HTTP');

.timer on
SELECT count(*) FROM read_parquet('${FILE_URL}') WHERE i = 42;
.timer off

SELECT '  ' || request.type || ' requests: ' || COUNT(*)
FROM duckdb_logs_parsed('HTTP')
GROUP BY request.type
ORDER BY request.type;

SELECT '  Total HTTP requests: ' || COUNT(*) FROM duckdb_logs_parsed('HTTP');
SQL

echo ""
echo "================================================================"
echo "Test 3: Warm read — persistent cache hit (after process restart)"
echo "        (new process, metadata served from local disk cache)"
echo "================================================================"
echo ""

$DUCKDB -noheader <<SQL 2>/dev/null
LOAD httpfs;
SET parquet_persistent_cache_path='${CACHE_DB}';
CALL enable_logging('HTTP');

.timer on
SELECT count(*) FROM read_parquet('${FILE_URL}') WHERE i = 42;
.timer off

SELECT '  ' || request.type || ' requests: ' || COUNT(*)
FROM duckdb_logs_parsed('HTTP')
GROUP BY request.type
ORDER BY request.type;

SELECT '  Total HTTP requests: ' || COUNT(*) FROM duckdb_logs_parsed('HTTP');
SQL

echo ""
echo "================================================================"
echo "Test 4: Warm read — persistent cache hit + TTL=-1 (skip validation)"
echo "        (new process, metadata from disk, no ETag/timestamp check)"
echo "================================================================"
echo ""

$DUCKDB -noheader <<SQL 2>/dev/null
LOAD httpfs;
SET parquet_persistent_cache_path='${CACHE_DB}';
SET parquet_persistent_cache_ttl=-1;
CALL enable_logging('HTTP');

.timer on
SELECT count(*) FROM read_parquet('${FILE_URL}') WHERE i = 42;
.timer off

SELECT '  ' || request.type || ' requests: ' || COUNT(*)
FROM duckdb_logs_parsed('HTTP')
GROUP BY request.type
ORDER BY request.type;

SELECT '  Total HTTP requests: ' || COUNT(*) FROM duckdb_logs_parsed('HTTP');
SQL

echo ""
echo "================================================================"
echo "Summary"
echo "================================================================"
echo ""
echo "At ${LATENCY_MS}ms per HTTP request:"
echo "  Test 1 (no cache):     footer metadata fetched via HTTP every time"
echo "  Test 2 (populate):     same as Test 1, but metadata saved to disk"
echo "  Test 3 (cache hit):    footer from local disk, saves metadata round trips"
echo "  Test 4 (cache+TTL):    footer from local disk, ETag check also skipped"
echo ""
echo "Note: 1 HEAD request is always required to open the HTTP file handle"
echo "(determines file size for range requests). This is inherent to HTTP"
echo "file access and cannot be eliminated without protocol-level changes."
echo "The persistent cache eliminates the footer/metadata round trips."
