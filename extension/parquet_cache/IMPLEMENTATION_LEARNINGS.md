# Implementation Learnings: Persistent Parquet Metadata Cache

Things discovered during implementation that were not anticipated in the planning documents.

## 1. Version Tags Are Raw Binary, Not UTF-8

`LocalFileSystem::GetVersionTag()` packs `device_id`, `file_id`, `file_size`, and `last_modification_time` as 4 raw `uint64_t` values into a `std::string` (32 bytes of binary data). This is explicitly **not valid UTF-8**.

DuckDB itself stores these as `Value::BLOB_RAW()` when passing through `extended_info->options` (see `local_file_system.cpp:1562`), but the API returns a bare `string` from `CachingFileHandle::GetVersionTag()`.

**Impact:** The plan assumed etag/version_tag could be stored as `VARCHAR`. Any `Value(string)` construction on a version tag triggers DuckDB's UTF-8 validation (`Value::StringIsValid`) and throws "Invalid unicode (byte sequence mismatch) detected in value construction". The cache schema had to use `BLOB` for the etag column, and all Value construction for etags uses `Value::BLOB(ptr, size)`.

**Lesson:** Always check what `GetVersionTag()` actually returns for each filesystem implementation before assuming it's a human-readable string. HTTP ETags are strings; local file version tags are binary hashes.

## 2. `crypto_metadata` Must Be Non-Null

`ParquetReader::PrepareRowGroupBuffer()` dereferences `metadata->crypto_metadata` unconditionally (line ~1208):

```cpp
if (metadata->crypto_metadata->encryption_algorithm.__isset.AES_GCM_CTR_V1) {
```

In the normal code path, `LoadMetadata()` always creates a `FileCryptoMetaData` object (even for unencrypted files — line 152 of `parquet_reader.cpp`). The plan's persistent cache constructor passed `nullptr` for `crypto_metadata`, causing a NULL dereference crash on the second read.

**Impact:** The persistent cache must construct an empty `FileCryptoMetaData` when reconstructing a `ParquetFileMetadataCache`. Passing `nullptr` crashes at scan time, not at construction time, making this hard to catch without an end-to-end test.

**Lesson:** When reconstructing cached objects, match all invariants of the original construction path — even for seemingly optional fields. Check unconditional dereferences in downstream consumers, not just the constructor.

## 3. `Connection::Query()` Has Two Return Types

DuckDB's `Connection` class has two `Query` overloads with different return types:

- `Query(const string &query)` returns `unique_ptr<MaterializedQueryResult>` (has `GetValue`, `RowCount`)
- `Query(const string &query, ARGS... args)` (variadic template) returns `unique_ptr<QueryResult>` (needs `Cast<MaterializedQueryResult>()` for value access)

**Impact:** Initial implementation used `result->GetValue()` and `result->RowCount()` uniformly, which compiled for the no-args overload but failed for the parameterized overload. Required splitting access patterns or using `Cast<MaterializedQueryResult>()` for parameterized queries.

**Lesson:** When using DuckDB's internal C++ API for queries, check the return type of the specific overload you're calling. The variadic template version returns the base class.

## 4. Extension Setting Callbacks Fire Before Value Is Stored

`ExtensionOption::set_function` callbacks are invoked **before** the new value is persisted to settings storage (see `physical_set.cpp:23-30`). Calling `context.TryGetCurrentSetting("same_setting")` inside the callback returns the **old** value, not the new one.

**Impact:** The initial design used callbacks to register the metadata provider, reading back all settings via `TryGetCurrentSetting`. The path setting read its own old value (empty string), so the provider was never registered. Had to restructure to either use the `parameter` argument directly or adopt a lazy-initialization pattern where the provider reads settings at query time instead of at set time.

**Solution chosen:** Register a singleton `PersistentCacheMetadataProvider` at `SET` time via the callback (using the `parameter` value directly), but have it read all settings lazily from `ClientContext` during `TryGetMetadata`/`OnMetadataLoaded` calls.

## 5. `ObjectCache::GetObjectCache` Only Takes `ClientContext`

There is no `ObjectCache::GetObjectCache(DatabaseInstance &)` overload. The only way to access the object cache is through a `ClientContext` reference. This means:

- You cannot register an `ObjectCacheEntry` at extension load time (which only provides `ExtensionLoader` / `DatabaseInstance`)
- Registration must happen either in a setting callback (which provides `ClientContext`) or lazily at first use

**Impact:** The plan assumed the provider could be registered during `LoadInternal()`. Had to defer registration to the `OnCachePathSet` callback which has access to `ClientContext`.

## 6. Loadable Extension Linking Requires Thrift Sources

The `parquet_cache` extension links against thrift types (`TMemoryBuffer`, `TCompactProtocol`, `FileMetaData`) which are compiled into the parquet extension's static library. For the **static** build, these symbols resolve at final link time. For the **loadable** extension build (`.duckdb_extension` shared library), all symbols must be resolved at link time.

**Impact:** Had to include the thrift/parquet source files directly in the `parquet_cache` CMakeLists.txt (same pattern as the parquet extension itself). Also needed `parquet_file_metadata_cache.cpp` and `parquet_geometry.cpp` for the `ParquetFileMetadataCache` constructor.

**Lesson:** DuckDB's extension-to-extension dependency model is source-level, not library-level. If extension B uses types from extension A, B must compile A's sources (or the relevant subset). There's no `target_link_libraries(B_loadable A_extension)` pattern.

## 7. GeoParquet Metadata Can Be Left Null (With Caveats)

Unlike `crypto_metadata`, `geo_metadata` can safely be `nullptr` in the cached `ParquetFileMetadataCache`. The GeoParquet metadata is reconstructed by `ParquetReader::InitializeSchema` from the `FileMetaData`'s `key_value_metadata` (specifically the `"geo"` key). Since the full `FileMetaData` is serialized in the cache blob, the geo metadata is implicitly preserved and re-derived.

However, for the `footer_blob` path (Phase 3), where we construct metadata directly in `parquet_reader.cpp`, we explicitly call `GeoParquetFileMetadata::TryRead()` to reconstruct it, since `InitializeSchema` may rely on it being already set.

## 8. `INSERT OR REPLACE` Works in DuckDB

DuckDB supports `INSERT OR REPLACE INTO` syntax for upserts against tables with PRIMARY KEY constraints. This works with the parameterized `Prepare`/`Execute` path. No need for separate `DELETE` + `INSERT` or `INSERT ... ON CONFLICT` syntax.

## 9. Prepared Statements Are Necessary for BLOB Parameters

The variadic `Connection::Query(string, ARGS...)` converts each argument via `Value::CreateValue<T>()`. While `Value` types pass through correctly, building a `vector<Value>` explicitly and using `PreparedStatement::Execute(vector<Value>)` provides more control and avoids potential type inference issues with mixed argument types (VARCHAR, BLOB, UBIGINT, INTEGER, TIMESTAMP in a single call).
