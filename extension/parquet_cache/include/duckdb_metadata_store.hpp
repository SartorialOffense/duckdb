//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_metadata_store.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "persistent_metadata_store.hpp"
#include "duckdb.hpp"

namespace duckdb {

//! DuckDB-backed persistent metadata store.
//! Stores parquet metadata as Thrift-serialized blobs in a local DuckDB database file.
class DuckDBMetadataStore : public PersistentMetadataStore {
public:
	explicit DuckDBMetadataStore(const string &cache_db_path, idx_t max_entries);
	~DuckDBMetadataStore() override;

	shared_ptr<ParquetFileMetadataCache> Get(const string &file_path, const string &current_etag,
	                                         timestamp_t current_last_modified) override;

	void Put(const string &file_path, const string &etag, timestamp_t last_modified, idx_t file_size,
	         shared_ptr<ParquetFileMetadataCache> metadata) override;

	void Remove(const string &file_path) override;
	void Clear() override;
	idx_t GetFileSize(const string &file_path) override;
	idx_t EntryCount() override;
	vector<CacheEntryInfo> GetAllEntries() override;

private:
	void InitializeSchema();
	void EvictIfNeeded();

	unique_ptr<DuckDB> cache_db;
	unique_ptr<Connection> connection;
	idx_t max_entries;
};

} // namespace duckdb
