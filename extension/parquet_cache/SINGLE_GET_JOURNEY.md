# From 4 Requests to 1: Optimizing Remote Parquet Point Lookups

## The Problem

A single-row lookup on a remote parquet file over S3/HTTP requires multiple HTTP round trips before returning data. Each round trip adds latency — at 75ms per request (typical for S3 cross-region), metadata overhead dominates query time.

**Baseline**: A point lookup (`SELECT * FROM read_parquet('s3://...') WHERE id = '...'`) on a 1GB sorted parquet file with 392 row groups requires:

| # | Request | Purpose | Latency |
|---|---------|---------|--------:|
| 1 | HEAD | Open file, get size | 75ms |
| 2 | GET (262KB) | Read footer from end of file | 75ms |
| 3 | GET (252KB) | Read `id` column chunk (filter evaluation) | 75ms |
| 4 | GET (5.1MB) | Read `payload` column chunk (result data) | 75ms |
| | **Total** | | **337ms** |

Only request #4 carries the data the user asked for. Requests #1-#3 are metadata overhead.

## The Journey

### Step 1: Persistent Metadata Cache (commit c7dfa99)

**Insight**: The parquet footer (request #2) contains the file schema, row group offsets, column chunk locations, and min/max statistics. This metadata rarely changes — certainly not between queries seconds apart. But DuckDB's existing in-memory cache (`ParquetFileMetadataCache` in the `ObjectCache`) doesn't survive process restart.

**Solution**: A new `parquet_cache` extension that persists parquet metadata in a local DuckDB database.

#### Architecture

```
extension/parquet_cache/
├── parquet_cache_extension.cpp      # Provider, settings, table functions
├── duckdb_metadata_store.cpp        # DuckDB-backed storage
├── metadata_serialization.cpp       # Thrift blob serialize/deserialize
└── include/
    ├── persistent_metadata_store.hpp # Abstract interface (swappable for PostgreSQL)
    ├── duckdb_metadata_store.hpp
    └── metadata_serialization.hpp
```

The cache stores the complete Thrift-serialized `FileMetaData` as a BLOB:

```sql
CREATE TABLE parquet_metadata_cache (
    file_path VARCHAR PRIMARY KEY,
    etag BLOB,              -- binary version tag for validation
    last_modified TIMESTAMP,
    file_size UBIGINT,
    footer_size UBIGINT,
    num_rows BIGINT,
    num_row_groups INTEGER,
    footer_blob BLOB,       -- Thrift-serialized FileMetaData
    schema_version INTEGER,
    cached_at TIMESTAMP DEFAULT current_timestamp
);
```

#### Hook into the Parquet Reader

A new `ParquetMetadataProvider` interface (`extension/parquet/include/parquet_metadata_provider.hpp`) allows any extension to intercept the metadata loading path without compile-time coupling:

```cpp
class ParquetMetadataProvider : public ObjectCacheEntry {
    // Called before metadata is loaded from HTTP
    virtual shared_ptr<ParquetFileMetadataCache> TryGetMetadata(...) = 0;
    // Called after metadata is freshly loaded — opportunity to persist
    virtual void OnMetadataLoaded(...) = 0;
};
```

The provider is registered in the `ObjectCache` and checked in `ParquetReader::ParquetReader()` before the existing `LoadMetadata()` path:

```cpp
// parquet_reader.cpp — metadata loading block
auto provider = ObjectCache::Get<ParquetMetadataProvider>(CACHE_KEY);
if (provider) {
    metadata = provider->TryGetMetadata(context, file, *file_handle);
}
if (!metadata) {
    metadata = LoadMetadata(...);  // falls back to HTTP
    if (provider) {
        provider->OnMetadataLoaded(context, file, *file_handle, metadata);
    }
}
```

#### Settings (following DuckDB conventions)

```sql
SET parquet_persistent_cache_path='/path/to/cache.duckdb';  -- empty = disabled
SET parquet_persistent_cache_ttl=0;    -- 0=validate, -1=always trust
SET parquet_persistent_cache_max_entries=-1;  -- -1=no limit
```

#### Result

| # | Request | Purpose |
|---|---------|---------|
| 1 | HEAD | Open file |
| 2 | ~~GET (262KB)~~ | ~~Footer~~ **eliminated — served from cache** |
| 3 | GET (252KB) | `id` column data |
| 4 | GET (5.1MB) | `payload` column data |
| | **3 requests, 256ms** | **24% faster** |

### Step 2: Eliminate the HEAD Request (commits 0396950 + 06c41c2)

**Insight**: The HEAD request (request #1) exists only to learn the file size before issuing range GETs. But we already store `file_size` in the persistent cache. If we check the cache *before* opening the file, we can inject the file size into httpfs so it skips the HEAD.

**Discovery**: httpfs already supports this. The `HTTPFileHandle` constructor (lines 372-396 of httpfs.cpp) checks `extended_info->options` for `file_size`, `etag`, and `last_modified`. If all three are present, it sets `initialized=true` and skips `LoadFileInfo()` (the HEAD request). DuckLake already uses this mechanism.

**Implementation**: Added `TryGetMetadataBeforeOpen()` to the provider interface — called *before* `fs.OpenFile()`. When TTL=-1 (always trust cache), it:

1. Looks up cached metadata from the persistent store
2. Injects `file_size`, `etag`, and `last_modified` into `extended_info->options`
3. httpfs sees all three fields, sets `initialized=true`, skips HEAD

**Two bugs found and fixed along the way**:

1. **Metadata overwrite**: The `else` branch in the ParquetReader constructor unconditionally executed `metadata = std::move(metadata_p)`, destroying the cached metadata when `metadata_p` was null. Fixed: `else` → `else if (metadata_p)`.

2. **GeoParquet NULL dereference**: The cached `ParquetFileMetadataCache` had `geo_metadata=nullptr` because the persistent store can't reconstruct it (requires `ClientContext`). But `InitializeSchema` → `ParseSchemaRecursive` accesses `geo_metadata` safely via `&&`. The actual crash was caused by bug #1 — `metadata` itself was null. After fixing #1, added defensive geo_metadata reconstruction: `GeoParquetFileMetadata::TryRead(*metadata->metadata, context)` after cache hit.

#### Result

| # | Request | Purpose |
|---|---------|---------|
| 1 | ~~HEAD~~ | ~~Open file~~ **eliminated — file_size from cache** |
| 2 | GET (252KB) | `id` column data |
| 3 | GET (5.1MB) | `payload` column data |
| | **2 requests, 181ms** | **46% faster** |

### Step 3: Merge Column Prefetches into One GET (commit 6046a77)

**Insight**: The remaining 2 GETs are for the `id` column (252KB) and `payload` column (5.1MB) of the same row group. They're contiguous in the file (gap = 0 bytes). DuckDB's `ThriftFileTransport` merges adjacent ranges within 16KB. So why aren't they merged?

**Root cause**: The parquet reader's prefetch logic (line 1487 of `parquet_reader.cpp`) has two paths:

```cpp
if (!filters && scan_percentage > 0.95) {
    // Whole row group prefetch — one GET for everything
    trans.Prefetch(GetGroupOffset(state), total_row_group_span);
} else {
    // Per-column lazy fetch — separate GET per column
    for (column : columns) {
        column.RegisterPrefetch(trans, !has_filter);  // lazy if no filter
    }
}
```

The `!filters` gate means **any query with a WHERE clause** takes the lazy path — even when scanning 100% of the row group data. The lazy path reads filter columns first (to potentially skip non-filter columns), causing sequential GETs instead of one merged GET.

**The tradeoff**: Lazy fetching saves bandwidth when the filter eliminates the row group (skip the 5.1MB payload read). But for point lookups where the caller knows the row exists, the extra round trip (75ms) costs far more than the saved bandwidth.

**Fix**: Remove the `!filters` gate. The `scan_percentage > 0.95` threshold already ensures we only whole-group prefetch when nearly all data will be read. For a 2-column `SELECT *`, scan_percentage = 100%, so both columns merge into one GET.

```cpp
// Before:
if (!filters && scan_percentage > 0.95) {
// After:
if (scan_percentage > 0.95) {
```

One line changed.

#### Result

| # | Request | Purpose |
|---|---------|---------|
| 1 | GET (5.36MB) | **Entire row group — both columns, one request** |
| | **1 request, 98ms** | **71% faster** |

---

## Summary

| Step | Change | Requests | Time | vs Baseline |
|------|--------|:---:|---:|---:|
| Baseline (cold) | — | 1 HEAD + 3 GET = 4 | 337ms | — |
| Footer cached | Persistent metadata cache | 1 HEAD + 2 GET = 3 | 256ms | -24% |
| HEAD eliminated | Pre-open cache check + httpfs extended_info | 2 GET = 2 | 181ms | -46% |
| **Merged prefetch** | **Remove `!filters` gate on whole-group prefetch** | **1 GET = 1** | **98ms** | **-71%** |

At 75ms simulated latency per HTTP request on a 1GB parquet file (10M rows, 392 row groups, 113KB footer), sorted by a 12-byte ID column with a 100-byte payload column.

---

## Files Changed (17 files, +1754 lines)

### Parquet Extension (core DuckDB)

| File | Lines | Purpose |
|------|------:|---------|
| `extension/parquet/include/parquet_metadata_provider.hpp` | +59 | Provider interface for external metadata caches |
| `extension/parquet/include/parquet_file_metadata_cache.hpp` | +4 | Persistent-reconstruction constructor |
| `extension/parquet/parquet_file_metadata_cache.cpp` | +9 | Constructor implementation |
| `extension/parquet/parquet_reader.cpp` | +69/-10 | Pre-open cache check, footer_blob support, geo_metadata fixup, prefetch merge |

### parquet_cache Extension (new)

| File | Lines | Purpose |
|------|------:|---------|
| `extension/parquet_cache/CMakeLists.txt` | +36 | Build config |
| `extension/parquet_cache/parquet_cache_extension.cpp` | +343 | Entry point, provider, settings, table functions |
| `extension/parquet_cache/duckdb_metadata_store.cpp` | +218 | DuckDB-backed persistent store |
| `extension/parquet_cache/metadata_serialization.cpp` | +33 | Thrift blob round-trip |
| `extension/parquet_cache/include/*.hpp` | +149 | Headers for store, serialization, extension |
| `extension/parquet_cache/DESIGN.md` | +389 | Full design document |
| `extension/parquet_cache/IMPLEMENTATION_LEARNINGS.md` | +77 | 9 implementation learnings |

### Tests & Tooling

| File | Lines | Purpose |
|------|------:|---------|
| `test/sql/copy/parquet/parquet_persistent_cache.test` | +117 | 38 assertions: populate, invalidate, clear, TTL |
| `extension/parquet_cache/test/slow_http_server.py` | +102 | HTTP server with Range support + configurable latency |
| `extension/parquet_cache/test/perf_test.sh` | +159 | Automated perf comparison script |

---

## Key Technical Discoveries

### 1. Version Tags Are Binary

`LocalFileSystem::GetVersionTag()` packs `device_id + file_id + size + mtime` as raw `uint64_t` values — not UTF-8. Constructing `Value(etag)` throws "Invalid unicode". The cache stores etag as `BLOB`, not `VARCHAR`.

### 2. crypto_metadata Must Be Non-Null

`ParquetReader::PrepareRowGroupBuffer()` unconditionally dereferences `metadata->crypto_metadata` to check encryption flags. Even for unencrypted files, `LoadMetadata()` creates an empty `FileCryptoMetaData`. The persistent cache must do the same — passing `nullptr` crashes at scan time.

### 3. httpfs Already Supports HEAD Elimination

The `HTTPFileHandle` constructor (httpfs.cpp:372-396) checks `extended_info->options` for `file_size`, `etag`, and `last_modified`. If all three are present, it sets `initialized=true` and skips `LoadFileInfo()` (the HEAD). DuckLake already uses this. No httpfs patch needed — just inject the right extended_info options.

### 4. Lazy Column Fetch Costs a Round Trip

The parquet reader's prefetch path is gated on `!filters` — any WHERE clause forces lazy per-column fetching. For point lookups where the row is expected to exist, this is counterproductive: the 75ms round trip to "check if we need the data column" costs more than just fetching it. The `scan_percentage > 0.95` threshold is sufficient to guard against wasteful prefetch.

### 5. Extension Setting Callbacks Fire Before Storage

`ExtensionOption::set_function` callbacks run before the new value is persisted. Reading the setting back via `TryGetCurrentSetting` returns the old value. The provider reads settings lazily at query time to avoid this.

---

## Future Work

### DuckLake Integration (Phase 3)

The parquet reader already recognizes `footer_blob` in `extended_info->options` (implemented in commit c7dfa99). To complete the DuckLake path:

1. Add `footer_blob BYTEA` column to `ducklake_data_file` table
2. Serialize footer at write time in `DuckLakeInsert`
3. Pass blob through `extended_info->options` in `ducklake_multi_file_list.cpp`
4. Result: zero HTTP metadata requests for DuckLake-managed files

### PostgreSQL Backend

The `PersistentMetadataStore` interface is abstract. Implement `PostgreSQLMetadataStore` storing `footer_blob` as `BYTEA` for shared cache across processes.

### Selective Column Prefetch

The current fix removes `!filters` globally. A more surgical approach: add a setting like `parquet_prefetch_whole_group_with_filters=true` that controls this behavior per-session, preserving the lazy optimization for analytics queries while enabling merged prefetch for point lookups.

---

## How to Run

```bash
# Build (requires extension_config_local.cmake with parquet_cache and httpfs)
make -j$(nproc)

# Run unit tests
./build/release/test/unittest "test/sql/copy/parquet/parquet_persistent_cache.test"

# Generate 1GB test file
./build/release/duckdb -c "
COPY (
    SELECT lpad(i::VARCHAR, 12, '0') AS id,
           (md5(i::VARCHAR) || md5((i*7+13)::VARCHAR) || md5((i*13+7)::VARCHAR) || md5((i*31)::VARCHAR))[:100] AS payload
    FROM range(10000000) t(i) ORDER BY id
) TO 'data/parquet-testing/cache/perf/lookup_10m.parquet'
  (FORMAT 'parquet', ROW_GROUP_SIZE 50000, COMPRESSION 'snappy');"

# Run perf test (starts HTTP server with 75ms latency)
./extension/parquet_cache/test/perf_test.sh 75
```

## How to Use

```sql
-- Load extensions
LOAD parquet_cache;
LOAD httpfs;

-- Enable persistent cache (path = enabled, empty = disabled)
SET parquet_persistent_cache_path='/path/to/cache.duckdb';

-- Trust cache without revalidation (skip HEAD)
SET parquet_persistent_cache_ttl=-1;

-- First query: populates cache (full HTTP cost)
SELECT * FROM read_parquet('s3://bucket/data.parquet') WHERE id = '000005000000';

-- Subsequent queries: 1 GET only
SELECT * FROM read_parquet('s3://bucket/data.parquet') WHERE id = '000005000000';

-- Inspect cache
SELECT * FROM parquet_cache_info();

-- Clear cache
SELECT * FROM parquet_cache_clear();
```
