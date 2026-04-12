//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_metadata_provider.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "parquet_file_metadata_cache.hpp"

namespace duckdb {
struct CachingFileHandle;

//! ParquetMetadataProvider is an interface that external extensions can implement
//! to provide cached parquet metadata (e.g., from a persistent store).
//! Register an instance in the ObjectCache under CACHE_KEY to intercept metadata loading.
class ParquetMetadataProvider : public ObjectCacheEntry {
public:
	static constexpr const char *CACHE_KEY = "__parquet_metadata_provider";

	static string ObjectType() {
		return "parquet_metadata_provider";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	//! Non-evictable: this is a singleton provider, not cached data
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx();
	}

	~ParquetMetadataProvider() override = default;

	//! Try to retrieve cached metadata for the given file.
	//! Returns nullptr if not found or invalid.
	virtual shared_ptr<ParquetFileMetadataCache> TryGetMetadata(ClientContext &context, const OpenFileInfo &file,
	                                                            CachingFileHandle &handle) = 0;

	//! Called when metadata is freshly loaded from the file (e.g., via HTTP).
	//! This gives the provider an opportunity to persist the metadata for future use.
	virtual void OnMetadataLoaded(ClientContext &context, const OpenFileInfo &file, CachingFileHandle &handle,
	                              shared_ptr<ParquetFileMetadataCache> metadata) = 0;
};

} // namespace duckdb
