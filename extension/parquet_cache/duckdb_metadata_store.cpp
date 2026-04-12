#include "duckdb_metadata_store.hpp"
#include "metadata_serialization.hpp"
#include "parquet_file_metadata_cache.hpp"
#include "parquet_geometry.hpp"
#include "duckdb/main/materialized_query_result.hpp"

namespace duckdb {

DuckDBMetadataStore::DuckDBMetadataStore(const string &cache_db_path, idx_t max_entries)
    : max_entries(max_entries) {
	DBConfig config;
	config.options.access_mode = AccessMode::AUTOMATIC;
	cache_db = make_uniq<DuckDB>(cache_db_path, &config);
	connection = make_uniq<Connection>(*cache_db);
	InitializeSchema();
}

DuckDBMetadataStore::~DuckDBMetadataStore() {
	connection.reset();
	cache_db.reset();
}

void DuckDBMetadataStore::InitializeSchema() {
	connection->Query(R"(
		CREATE TABLE IF NOT EXISTS parquet_metadata_cache (
			file_path VARCHAR PRIMARY KEY,
			etag BLOB,
			last_modified TIMESTAMP,
			file_size UBIGINT,
			footer_size UBIGINT,
			num_rows BIGINT,
			num_row_groups INTEGER,
			footer_blob BLOB,
			schema_version INTEGER,
			cached_at TIMESTAMP DEFAULT current_timestamp
		)
	)");
}

void DuckDBMetadataStore::EvictIfNeeded() {
	if (max_entries == DConstants::INVALID_INDEX) {
		return; // no limit
	}
	auto result = connection->Query("SELECT count(*) FROM parquet_metadata_cache");
	if (result->HasError()) {
		return;
	}
	auto count = result->GetValue(0, 0).GetValue<idx_t>();
	if (count <= max_entries) {
		return;
	}
	auto to_evict = count - max_entries;
	connection->Query("DELETE FROM parquet_metadata_cache WHERE file_path IN ("
	                  "SELECT file_path FROM parquet_metadata_cache ORDER BY cached_at ASC LIMIT " +
	                  to_string(to_evict) + ")");
}

shared_ptr<ParquetFileMetadataCache> DuckDBMetadataStore::Get(const string &file_path, const string &current_etag,
                                                              timestamp_t current_last_modified) {
	// Parameterized query returns unique_ptr<QueryResult>, needs cast
	auto result = connection->Query(
	    "SELECT etag, last_modified, footer_size, footer_blob, schema_version "
	    "FROM parquet_metadata_cache WHERE file_path = $1",
	    file_path);

	if (result->HasError()) {
		return nullptr;
	}
	auto &materialized = result->Cast<MaterializedQueryResult>();
	if (materialized.RowCount() == 0) {
		return nullptr;
	}

	// Check schema version compatibility
	auto schema_version = materialized.GetValue(4, 0).GetValue<int32_t>();
	if (schema_version != MetadataSerialization::SCHEMA_VERSION) {
		Remove(file_path);
		return nullptr;
	}

	// Validate against current file state
	// etag is stored as BLOB (binary version tag), compare raw bytes
	auto cached_etag_val = materialized.GetValue(0, 0);
	auto cached_etag = cached_etag_val.IsNull() ? "" : StringValue::Get(cached_etag_val);
	auto cached_last_modified = materialized.GetValue(1, 0).GetValue<timestamp_t>();

	if (!current_etag.empty() && !cached_etag.empty()) {
		if (current_etag != cached_etag) {
			Remove(file_path);
			return nullptr;
		}
	} else {
		if (current_last_modified != cached_last_modified) {
			Remove(file_path);
			return nullptr;
		}
	}

	// Deserialize the footer blob
	auto footer_size = materialized.GetValue(2, 0).GetValue<idx_t>();
	auto blob_value = materialized.GetValue(3, 0);
	auto blob_str = StringValue::Get(blob_value);

	unique_ptr<duckdb_parquet::FileMetaData> file_metadata;
	try {
		file_metadata = MetadataSerialization::Deserialize(blob_str);
	} catch (...) {
		Remove(file_path);
		return nullptr;
	}

	// crypto_metadata must be non-null (ParquetReader dereferences it unconditionally)
	auto crypto_metadata = make_uniq<duckdb_parquet::FileCryptoMetaData>();
	return make_shared_ptr<ParquetFileMetadataCache>(std::move(file_metadata), cached_etag, cached_last_modified,
	                                                 nullptr, std::move(crypto_metadata), footer_size);
}

void DuckDBMetadataStore::Put(const string &file_path, const string &etag, timestamp_t last_modified, idx_t file_size,
                              shared_ptr<ParquetFileMetadataCache> metadata) {
	if (!metadata || !metadata->metadata) {
		return;
	}

	string blob;
	try {
		blob = MetadataSerialization::Serialize(*metadata->metadata);
	} catch (...) {
		return;
	}

	auto num_rows = metadata->metadata->num_rows;
	auto num_row_groups = static_cast<int32_t>(metadata->metadata->row_groups.size());
	auto footer_size = metadata->footer_size;

	// Build parameter values
	vector<Value> params;
	params.push_back(Value(file_path));
	// version_tag is raw binary (device_id + file_id + size + mtime), store as BLOB
	params.push_back(Value::BLOB(const_data_ptr_cast(etag.c_str()), etag.size()));
	params.push_back(Value::TIMESTAMP(last_modified));
	params.push_back(Value::UBIGINT(file_size));
	params.push_back(Value::UBIGINT(footer_size));
	params.push_back(Value::BIGINT(num_rows));
	params.push_back(Value::INTEGER(num_row_groups));
	params.push_back(Value::BLOB(const_data_ptr_cast(blob.c_str()), blob.size()));
	params.push_back(Value::INTEGER(MetadataSerialization::SCHEMA_VERSION));

	auto prepared = connection->Prepare(
	    "INSERT OR REPLACE INTO parquet_metadata_cache "
	    "(file_path, etag, last_modified, file_size, footer_size, num_rows, num_row_groups, "
	    "footer_blob, schema_version, cached_at) "
	    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, current_timestamp)");
	if (prepared->HasError()) {
		return;
	}
	auto insert_result = prepared->Execute(params);
	if (insert_result->HasError()) {
		return;
	}
	EvictIfNeeded();
}

void DuckDBMetadataStore::Remove(const string &file_path) {
	connection->Query("DELETE FROM parquet_metadata_cache WHERE file_path = $1", file_path);
}

void DuckDBMetadataStore::Clear() {
	connection->Query("DELETE FROM parquet_metadata_cache");
}

idx_t DuckDBMetadataStore::EntryCount() {
	auto result = connection->Query("SELECT count(*) FROM parquet_metadata_cache");
	if (result->HasError() || result->RowCount() == 0) {
		return 0;
	}
	return result->GetValue(0, 0).GetValue<idx_t>();
}

vector<PersistentMetadataStore::CacheEntryInfo> DuckDBMetadataStore::GetAllEntries() {
	auto result = connection->Query(
	    "SELECT file_path, etag, last_modified, file_size, footer_size, "
	    "num_rows, num_row_groups, cached_at FROM parquet_metadata_cache ORDER BY cached_at DESC");

	vector<CacheEntryInfo> entries;
	if (result->HasError()) {
		return entries;
	}

	for (idx_t i = 0; i < result->RowCount(); i++) {
		CacheEntryInfo info;
		info.file_path = result->GetValue(0, i).ToString();
		auto etag_val = result->GetValue(1, i);
		info.etag = etag_val.IsNull() ? "" : StringValue::Get(etag_val);
		info.last_modified = result->GetValue(2, i).GetValue<timestamp_t>();
		info.file_size = result->GetValue(3, i).GetValue<idx_t>();
		info.footer_size = result->GetValue(4, i).GetValue<idx_t>();
		info.num_rows = result->GetValue(5, i).GetValue<int64_t>();
		info.num_row_groups = result->GetValue(6, i).GetValue<int32_t>();
		info.cached_at = result->GetValue(7, i).GetValue<timestamp_t>();
		entries.push_back(std::move(info));
	}
	return entries;
}

} // namespace duckdb
