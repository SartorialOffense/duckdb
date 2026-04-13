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

	//! Try to retrieve cached metadata BEFORE the file is opened.
	//! Called before fs.OpenFile() — no file handle available, no validation possible.
	//! Only returns metadata when TTL-based trust is enabled (e.g., TTL=-1).
	//! If this returns non-null, the parquet reader injects file_size and
	//! validate_external_file_cache=false into extended_info to skip the HEAD request.
	virtual shared_ptr<ParquetFileMetadataCache> TryGetMetadataBeforeOpen(ClientContext &context,
	                                                                      const OpenFileInfo &file) {
		return nullptr;
	}

	//! Try to retrieve cached metadata for the given file.
	//! Called after the file is opened — can use the file handle for ETag/timestamp validation.
	//! Returns nullptr if not found or invalid.
	virtual shared_ptr<ParquetFileMetadataCache> TryGetMetadata(ClientContext &context, const OpenFileInfo &file,
	                                                            CachingFileHandle &handle) = 0;

	//! Called when metadata is freshly loaded from the file (e.g., via HTTP).
	//! This gives the provider an opportunity to persist the metadata for future use.
	virtual void OnMetadataLoaded(ClientContext &context, const OpenFileInfo &file, CachingFileHandle &handle,
	                              shared_ptr<ParquetFileMetadataCache> metadata) = 0;
};

} // namespace duckdb
