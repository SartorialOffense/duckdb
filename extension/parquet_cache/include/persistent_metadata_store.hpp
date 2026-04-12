//===----------------------------------------------------------------------===//
//                         DuckDB
//
// persistent_metadata_store.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb.hpp"
#include "parquet_file_metadata_cache.hpp"

namespace duckdb {
struct CachingFileHandle;

//! Abstract interface for persistent parquet metadata storage.
//! Implementations can back onto DuckDB, PostgreSQL, etc.
class PersistentMetadataStore {
public:
	virtual ~PersistentMetadataStore() = default;

	//! Look up cached metadata for a file path.
	//! current_etag and current_last_modified are from the file handle for validation.
	//! Returns nullptr if not found or if the cached entry is stale.
	virtual shared_ptr<ParquetFileMetadataCache> Get(const string &file_path, const string &current_etag,
	                                                 timestamp_t current_last_modified) = 0;

	//! Store metadata for a file path.
	virtual void Put(const string &file_path, const string &etag, timestamp_t last_modified, idx_t file_size,
	                 shared_ptr<ParquetFileMetadataCache> metadata) = 0;

	//! Remove a specific cache entry.
	virtual void Remove(const string &file_path) = 0;

	//! Clear all cache entries.
	virtual void Clear() = 0;

	//! Get the number of cached entries.
	virtual idx_t EntryCount() = 0;

	//! Get information about all cached entries (for parquet_cache_info table function).
	struct CacheEntryInfo {
		string file_path;
		string etag;
		timestamp_t last_modified;
		idx_t file_size;
		idx_t footer_size;
		int64_t num_rows;
		int32_t num_row_groups;
		timestamp_t cached_at;
	};
	virtual vector<CacheEntryInfo> GetAllEntries() = 0;
};

} // namespace duckdb
