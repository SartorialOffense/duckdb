# Persistent Parquet Metadata Cache — Design Document

## Problem Statement

When doing single-point lookups on remote parquet files via S3/httpfs, the current flow requires **2-3 HTTP round trips just for metadata** (footer detection + footer read) before any data bytes can be fetched. For latency-sensitive lookups this is the dominant cost.

DuckDB already has two in-memory caching layers:

1. **`ParquetFileMetadataCache`** (ObjectCache-based) — caches deserialized `FileMetaData` keyed by file path, controlled by `parquet_metadata_cache` setting. Validates via ETags/timestamps. Lives in `extension/parquet/include/parquet_file_metadata_cache.hpp`.

2. **`ExternalFileCache`** (buffer-manager-backed block cache) — caches raw file blocks (2MB for remote files). Lives in `src/include/duckdb/storage/external_file_cache/`.

Neither survives process restart. DuckLake already stores `footer_size` and column-level statistics in its metadata tables and passes them through `OpenFileInfo.extended_info->options` — but it does **not** store the full footer blob (byte offsets needed to construct targeted range requests).

## Goals

1. **POC**: Store parquet metadata in a local DuckDB database file so that subsequent queries against the same remote parquet file skip the metadata HTTP requests entirely.
2. **Generic mechanism**: Implementable as a standalone extension that hooks into the parquet metadata path without modifying core DuckDB.
3. **Eventual PostgreSQL support**: Architecture allows swapping the local DuckDB store for PostgreSQL (matching DuckLake's metadata manager pattern).
4. **Minimal invasiveness**: Hooks into existing extension points rather than deep surgery on core code.
5. **Cache invalidation**: Handles file changes via ETags, timestamps, and configurable TTL.

---

## Architecture Overview

```
Query time — ParquetReader constructor (parquet_reader.cpp:891)
  │
  ├─ 0. Check for footer_blob in extended_info->options (DuckLake path)
  │     → deserialize directly, zero HTTP requests
  │
  ├─ 1. Check for ParquetMetadataProvider in ObjectCache
  │     → persistent cache lookup (1 HEAD request for validation, or 0 with TTL)
  │
  ├─ 2. Check in-memory ObjectCache (existing parquet_metadata_cache)
  │
  └─ 3. Fall back to HTTP range requests (existing, 2-3 round trips)
        → on success, notify ParquetMetadataProvider to persist
```

## Round Trip Analysis

| Scenario | Metadata Round Trips | Data Round Trips |
|----------|---------------------|-----------------|
| No caching (current default) | 2-3 (footer detect + footer read) | 1+ per row group |
| In-memory ObjectCache (existing) | 0 (until process restart) | 1+ per row group |
| **Persistent cache, validated** | **1 (HEAD only)** | **1 (targeted range)** |
| **Persistent cache, TTL mode** | **0** | **1 (targeted range)** |
| **DuckLake with footer_blob** | **0** | **1 (targeted range)** |

For a point lookup: total latency drops from 3-4 round trips to 1 (or 0 with TTL/DuckLake).

---

## Current Caching Architecture (Pre-Change)

### Parquet Metadata Reading Flow for Remote Files

1. **Footer Detection** (8 bytes from end of file):
   - HTTP GET with `Range: bytes=-8` — read magic bytes "PAR1"/"PARE" + footer length
   - Optimization: prefetches 1/1000th of file (min 16KB, max 256KB) to potentially avoid second round trip

2. **Full Footer Read**:
   - HTTP GET with `Range: bytes=[file_size - (footer_len + 8), file_size)`
   - Deserialized via Thrift's `TCompactProtocol` → `FileMetaData`

3. **FileMetaData Contains**:
   - Schema (column names, types, nesting)
   - Row groups (offsets, row counts, column chunks)
   - Per-column metadata (data_page_offset, total_compressed_size, statistics, bloom_filter_offset)
   - Key-value metadata (GeoParquet, etc.)

4. **Data Fetch**:
   - Column readers use offsets from metadata to construct targeted HTTP range requests
   - Prefetch system merges adjacent ranges within 16KB gap tolerance

### Existing ObjectCache Metadata Cache

- **Class**: `ParquetFileMetadataCache` extends `ObjectCacheEntry`
- **Location**: `extension/parquet/include/parquet_file_metadata_cache.hpp`
- **Cache Key**: file path
- **What's Cached**: `FileMetaData`, `GeoParquetFileMetadata`, `FileCryptoMetaData`, footer_size
- **Validation**: ETags and/or `last_modified` timestamps
- **Lifetime**: Per-`DatabaseInstance`, in-memory only, LRU-evicted under memory pressure
- **Setting**: `SET parquet_metadata_cache=true` (default: false)

### DuckLake's Existing Metadata Pass-Through

DuckLake passes file metadata through `OpenFileInfo.extended_info->options` (`ducklake_multi_file_list.cpp:176-204`):

```cpp
extended_info->options["file_size"] = Value::UBIGINT(file.file_size_bytes);
extended_info->options["footer_size"] = Value::UBIGINT(file.footer_size.GetIndex());
extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
extended_info->options["etag"] = Value("");
extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
```

The parquet reader already reads these in its constructor (`parquet_reader.cpp:872-885`). DuckLake-managed files use `validate_external_file_cache=false` and dummy ETags because they are never modified in place.

---

## Implementation: Phase 1 — Parquet Extension Hook Point

**Goal**: Add a minimal, generic extension point so external extensions can provide cached metadata.

### ParquetMetadataProvider Interface

New file: `extension/parquet/include/parquet_metadata_provider.hpp`

```cpp
class ParquetMetadataProvider : public ObjectCacheEntry {
public:
    static constexpr const char *CACHE_KEY = "__parquet_metadata_provider";
    static string ObjectType() { return "parquet_metadata_provider"; }

    // Return cached metadata, or nullptr if not found/invalid
    virtual shared_ptr<ParquetFileMetadataCache> TryGetMetadata(
        ClientContext &context, const OpenFileInfo &file, CachingFileHandle &handle) = 0;

    // Called when metadata is freshly loaded — opportunity to persist
    virtual void OnMetadataLoaded(
        ClientContext &context, const OpenFileInfo &file, CachingFileHandle &handle,
        shared_ptr<ParquetFileMetadataCache> metadata) = 0;
};
```

Registered as a non-evictable entry in the `ObjectCache` under a well-known key. Any extension can register a provider without compile-time coupling to the parquet extension.

### ParquetReader Constructor Modification

`extension/parquet/parquet_reader.cpp` (lines 891-915):

```cpp
if (!metadata_p) {
    // 1. Try external persistent metadata provider
    auto metadata_provider = ObjectCache::GetObjectCache(context_p)
        .Get<ParquetMetadataProvider>(ParquetMetadataProvider::CACHE_KEY);
    if (metadata_provider) {
        metadata = metadata_provider->TryGetMetadata(context_p, file, *file_handle);
    }

    // 2. Fall back to existing in-memory cache or HTTP
    if (!metadata) {
        // ... existing MetadataCacheEnabled / LoadMetadata logic ...

        // 3. Notify provider of freshly loaded metadata
        if (metadata_provider) {
            metadata_provider->OnMetadataLoaded(context_p, file, *file_handle, metadata);
        }
    }
}
```

### Persistent-Reconstruction Constructor

`extension/parquet/include/parquet_file_metadata_cache.hpp`:

```cpp
// Constructor for persistent cache reconstruction (without a live CachingFileHandle)
ParquetFileMetadataCache(unique_ptr<duckdb_parquet::FileMetaData> file_metadata,
                         const string &version_tag, timestamp_t last_modified,
                         unique_ptr<GeoParquetFileMetadata> geo_metadata,
                         unique_ptr<FileCryptoMetaData> crypto_metadata, idx_t footer_size);
```

Allows the persistent cache to create a valid cache entry without needing the original file handle.

---

## Implementation: Phase 2 — `parquet_cache` Extension

**Goal**: Standalone extension that persists parquet metadata in a local DuckDB database.

### Extension Structure

```
extension/parquet_cache/
├── CMakeLists.txt
├── parquet_cache_extension.cpp          # Entry point, provider, settings, table functions
├── duckdb_metadata_store.cpp            # DuckDB-backed storage implementation
├── metadata_serialization.cpp           # Thrift blob serialize/deserialize
├── include/
│   ├── parquet_cache_extension.hpp
│   ├── persistent_metadata_store.hpp    # Abstract storage interface
│   ├── duckdb_metadata_store.hpp
│   └── metadata_serialization.hpp
└── DESIGN.md                            # This file
```

### Metadata Serialization

Uses Thrift's existing `FileMetaData::write()`/`read()` methods with `TMemoryBuffer` + `TCompactProtocol` for lossless round-tripping to/from a binary blob:

```cpp
string MetadataSerialization::Serialize(const duckdb_parquet::FileMetaData &metadata) {
    auto mem_buffer = make_shared<TMemoryBuffer>();
    TCompactProtocolT<TMemoryBuffer> protocol(mem_buffer);
    metadata.write(&protocol);
    // return raw bytes
}

unique_ptr<duckdb_parquet::FileMetaData> MetadataSerialization::Deserialize(const string &blob) {
    auto mem_buffer = make_shared<TMemoryBuffer>(blob_data, blob_size, TMemoryBuffer::OBSERVE);
    TCompactProtocolT<TMemoryBuffer> protocol(mem_buffer);
    auto metadata = make_uniq<duckdb_parquet::FileMetaData>();
    metadata->read(&protocol);
    return metadata;
}
```

### Cache Database Schema

```sql
CREATE TABLE parquet_metadata_cache (
    file_path VARCHAR PRIMARY KEY,
    etag BLOB,                   -- binary version tag (not UTF-8 safe!)
    last_modified TIMESTAMP,
    file_size UBIGINT,
    footer_size UBIGINT,
    num_rows BIGINT,
    num_row_groups INTEGER,
    footer_blob BLOB,            -- Thrift-serialized FileMetaData
    schema_version INTEGER,      -- for forward-compat
    cached_at TIMESTAMP DEFAULT current_timestamp
);
```

Key design decisions:
- `etag` is `BLOB` not `VARCHAR` because local file version tags contain raw binary data
- `footer_blob` contains the complete serialized `FileMetaData` — all row group offsets, column chunk offsets, statistics, bloom filter locations, page indexes
- Scalar columns alongside the blob enable efficient cache management queries
- `schema_version` tracks Thrift schema version to handle DuckDB upgrades

### Abstract Storage Interface

```cpp
class PersistentMetadataStore {
public:
    virtual shared_ptr<ParquetFileMetadataCache> Get(
        const string &file_path, const string &current_etag,
        timestamp_t current_last_modified) = 0;
    virtual void Put(const string &file_path, const string &etag,
        timestamp_t last_modified, idx_t file_size,
        shared_ptr<ParquetFileMetadataCache> metadata) = 0;
    virtual void Remove(const string &file_path) = 0;
    virtual void Clear() = 0;
    virtual idx_t EntryCount() = 0;
    virtual vector<CacheEntryInfo> GetAllEntries() = 0;
};
```

This interface is what gets swapped for PostgreSQL later.

### Cache Validation Modes

1. **Validated mode** (TTL=0, default): The file handle is already open at this point (line 865 of `parquet_reader.cpp`), which triggers a HEAD request. Compare returned ETag/timestamp against stored values. **1 round trip instead of 2-3** (HEAD only, no footer reads).

2. **TTL mode** (TTL=-1): Trust cached entries without any HTTP request. **0 round trips**. Best for data that rarely changes (e.g., immutable S3 objects).

3. **No-validation mode**: For DuckLake-managed files (which are never modified in place). DuckLake already sets `validate_external_file_cache = false` and dummy ETags.

### Configuration

Follows DuckDB conventions: sentinel values instead of extra boolean toggles.

| Setting | Type | Default | Sentinel Values | Description |
|---------|------|---------|-----------------|-------------|
| `parquet_persistent_cache_path` | VARCHAR | `''` | `''` = disabled, any path = enabled | Path to cache database |
| `parquet_persistent_cache_ttl` | BIGINT | `0` | `0` = always validate, `-1` = always trust | Seconds to trust without revalidation |
| `parquet_persistent_cache_max_entries` | BIGINT | `-1` | `-1` = no limit | Maximum cached entries |

Convention references:
- `DConstants::INVALID_INDEX` (-1) for "unlimited" (`src/include/duckdb/common/constants.hpp:66`)
- `validate_external_file_cache` enum (`src/common/settings.json:1093`) for validation modes
- Extension options via `config.AddExtensionOption()` (`extension/parquet/parquet_extension.cpp:922-937`)

### Provider Registration

The `PersistentCacheMetadataProvider` is registered in the `ObjectCache` via the `parquet_persistent_cache_path` setting callback (`OnCachePathSet`). It reads settings lazily at query time:

- `TryGetMetadata`: reads path/TTL settings, gets-or-creates the store, looks up cached metadata
- `OnMetadataLoaded`: reads path settings, persists freshly loaded metadata to the store

### Table Functions

- `parquet_cache_info()` — returns all cached entries (file_path, etag, last_modified, file_size, footer_size, num_rows, num_row_groups, cached_at)
- `parquet_cache_clear()` — clears all entries
- `parquet_cache_clear(path)` — removes a specific entry

---

## Implementation: Phase 3 — DuckLake Integration Path

**Goal**: Enable DuckLake to store and serve the full parquet footer blob, eliminating all metadata HTTP requests for DuckLake-managed files.

### footer_blob in extended_info->options

The parquet reader constructor now recognizes a `footer_blob` key in `extended_info->options`. When present, it deserializes the blob directly using `TMemoryBuffer`/`TCompactProtocol`, constructs a `ParquetFileMetadataCache`, and skips all HTTP metadata fetching.

```cpp
auto footer_blob_entry = open_options.find("footer_blob");
if (footer_blob_entry != open_options.end()) {
    auto blob_str = StringValue::Get(footer_blob_entry->second);
    // Thrift deserialization → FileMetaData
    // Construct ParquetFileMetadataCache directly
    // Zero HTTP requests for metadata
}
```

### Remaining DuckLake Work (Not Yet Implemented)

To complete the DuckLake integration:

1. **Add `footer_blob BYTEA` column** to `ducklake_data_file` and `ducklake_delete_file` tables (schema migration)
2. **Capture serialized footer at write time**: Extend `DuckLakeInsert` to serialize `FileMetaData` after writing a parquet file
3. **Pass through `extended_info->options`**: In `ducklake_multi_file_list.cpp`, add:
   ```cpp
   if (!file.footer_blob.empty()) {
       extended_info->options["footer_blob"] = Value::BLOB_RAW(file.footer_blob);
   }
   ```
4. **Add `footer_blob` to `DuckLakeFileInfo` struct** in `ducklake_metadata_info.hpp`
5. **Extend `GetFileSelectList` and `ReadDataFile`** in `ducklake_metadata_manager.cpp`

### PostgreSQL Backend

Implement `PostgreSQLMetadataStore` conforming to the `PersistentMetadataStore` interface, storing `footer_blob` as `BYTEA`. Options:
- Live in the `parquet_cache` extension with an optional postgres dependency
- Live in DuckLake itself, extending the existing `PostgresMetadataManager`

---

## Key Files Reference

| File | Role |
|------|------|
| `extension/parquet/parquet_reader.cpp:860-925` | Constructor — primary integration point (provider check, footer_blob handling) |
| `extension/parquet/include/parquet_metadata_provider.hpp` | Provider interface for external extensions |
| `extension/parquet/include/parquet_file_metadata_cache.hpp` | Cache entry class with persistent-reconstruction constructor |
| `extension/parquet_cache/parquet_cache_extension.cpp` | Extension entry point, provider, settings, table functions |
| `extension/parquet_cache/duckdb_metadata_store.cpp` | DuckDB-backed persistent storage |
| `extension/parquet_cache/metadata_serialization.cpp` | Thrift serialize/deserialize helpers |
| `extension/parquet_cache/include/persistent_metadata_store.hpp` | Abstract storage interface (swappable backend) |
| `src/include/duckdb/storage/object_cache.hpp` | ObjectCache — where provider is registered |
| `third_party/parquet/parquet_types.h:3228+` | `FileMetaData` with Thrift `read()`/`write()` |
| `ducklake/src/storage/ducklake_multi_file_list.cpp:176-204` | DuckLake's `extended_info->options` pass-through |
| `ducklake/src/storage/ducklake_metadata_manager.cpp:141` | DuckLake's `ducklake_data_file` table schema |

---

## Testing Strategy

### How HTTP Requests Are Quantified

DuckDB has built-in structured HTTP logging (`src/logging/log_types.cpp:56-117`):

```sql
CALL enable_logging('HTTP');
-- ... do work ...
SELECT request.type, COUNT(*) FROM duckdb_logs_parsed('HTTP') GROUP BY request.type;
```

For C++ level testing, `TrackingFileSystem` (`test/common/test_caching_file_system_wrapper.cpp:79-98`) records every `Read()` call with path, offset, and size.

### Test Conventions

Both DuckDB and DuckLake use SQLLogicTest `.test` files as the primary format. C++ Catch2 tests for infrastructure-level testing only. DuckLake uses MinIO for S3 tests via `test/configs/minio.json`.

### Implemented Tests

`test/sql/copy/parquet/parquet_persistent_cache.test` — 38 assertions covering:

1. **Cache population** — read file, verify entry in `parquet_cache_info()`
2. **Multiple files** — read two files, verify both cached
3. **Read from cache** — second read returns correct data from cached metadata
4. **Cache info correctness** — `num_rows`, `num_row_groups` match actual file
5. **Selective clear** — `parquet_cache_clear(path)` removes one entry
6. **Full clear** — `parquet_cache_clear()` removes all entries
7. **Cache invalidation** — overwrite file, verify new data returned (version tag changed)
8. **Disable via empty path** — `SET parquet_persistent_cache_path=''` disables caching

### Future Tests (Require httpfs/MinIO)

- **HTTP request counting** — verify metadata round trips reduced to 1 (HEAD) or 0 (TTL)
- **TTL mode** — verify zero HTTP requests when `parquet_persistent_cache_ttl=-1`
- **S3 integration** — same tests against MinIO using `require-env S3_TEST_SERVER_AVAILABLE 1`
- **Corruption fallback** — corrupt cache DB, verify graceful fallback to normal loading
