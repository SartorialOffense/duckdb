#include "parquet_cache_extension.hpp"

#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/external_file_cache/caching_file_system.hpp"

#include "parquet_metadata_provider.hpp"
#include "duckdb_metadata_store.hpp"
#include "persistent_metadata_store.hpp"

namespace duckdb {

//! Global state: the active metadata store, shared across connections.
struct ParquetCacheState {
	mutex lock;
	string current_path;
	shared_ptr<PersistentMetadataStore> store;
};

static ParquetCacheState &GetCacheState() {
	static ParquetCacheState state;
	return state;
}

static shared_ptr<PersistentMetadataStore> GetOrCreateStore(const string &path, idx_t max_entries) {
	auto &state = GetCacheState();
	lock_guard<mutex> guard(state.lock);
	if (state.store && state.current_path == path) {
		return state.store;
	}
	if (path.empty()) {
		state.store = nullptr;
		state.current_path = "";
		return nullptr;
	}
	try {
		state.store = make_shared_ptr<DuckDBMetadataStore>(path, max_entries);
		state.current_path = path;
	} catch (...) {
		state.store = nullptr;
		state.current_path = "";
		return nullptr;
	}
	return state.store;
}

static string GetCachePath(ClientContext &context) {
	Value val;
	if (context.TryGetCurrentSetting("parquet_persistent_cache_path", val)) {
		return val.ToString();
	}
	return "";
}

static int64_t GetCacheTTL(ClientContext &context) {
	Value val;
	if (context.TryGetCurrentSetting("parquet_persistent_cache_ttl", val)) {
		return val.GetValue<int64_t>();
	}
	return 0;
}

static idx_t GetCacheMaxEntries(ClientContext &context) {
	Value val;
	if (context.TryGetCurrentSetting("parquet_persistent_cache_max_entries", val)) {
		auto v = val.GetValue<int64_t>();
		return (v < 0) ? DConstants::INVALID_INDEX : static_cast<idx_t>(v);
	}
	return DConstants::INVALID_INDEX;
}

//! PersistentCacheMetadataProvider reads settings lazily at query time.
class PersistentCacheMetadataProvider : public ParquetMetadataProvider {
public:
	PersistentCacheMetadataProvider() = default;

	shared_ptr<ParquetFileMetadataCache> TryGetMetadata(ClientContext &context, const OpenFileInfo &file,
	                                                    CachingFileHandle &handle) override {
		auto cache_path = GetCachePath(context);
		if (cache_path.empty()) {
			return nullptr;
		}

		auto store = GetOrCreateStore(cache_path, GetCacheMaxEntries(context));
		if (!store) {
			return nullptr;
		}

		auto ttl = GetCacheTTL(context);
		string current_etag = handle.GetVersionTag();
		timestamp_t current_last_modified = handle.GetLastModifiedTime();

		if (ttl == -1) {
			// TTL=-1: always trust cache, skip validation
			current_etag = "";
			current_last_modified = timestamp_t(0);
		}

		try {
			return store->Get(file.path, current_etag, current_last_modified);
		} catch (...) {
			return nullptr;
		}
	}

	void OnMetadataLoaded(ClientContext &context, const OpenFileInfo &file, CachingFileHandle &handle,
	                      shared_ptr<ParquetFileMetadataCache> metadata) override {
		auto cache_path = GetCachePath(context);
		if (cache_path.empty() || !metadata) {
			return;
		}

		auto store = GetOrCreateStore(cache_path, GetCacheMaxEntries(context));
		if (!store) {
			return;
		}

		try {
			store->Put(file.path, handle.GetVersionTag(), handle.GetLastModifiedTime(), handle.GetFileSize(),
			           metadata);
		} catch (...) {
			// Caching is best-effort
		}
	}
};

//! Callback for parquet_persistent_cache_path setting.
//! Registers the provider in the ObjectCache when the path is set.
static void OnCachePathSet(ClientContext &context, SetScope scope, Value &parameter) {
	auto &object_cache = ObjectCache::GetObjectCache(context);
	auto cache_path = parameter.ToString();
	if (cache_path.empty()) {
		object_cache.Delete(ParquetMetadataProvider::CACHE_KEY);
	} else {
		// Ensure the provider is registered
		auto existing = object_cache.Get<ParquetMetadataProvider>(ParquetMetadataProvider::CACHE_KEY);
		if (!existing) {
			auto provider = make_shared_ptr<PersistentCacheMetadataProvider>();
			object_cache.Put(ParquetMetadataProvider::CACHE_KEY, std::move(provider));
		}
	}
}

//! parquet_cache_info() table function
struct ParquetCacheInfoData : public TableFunctionData {
	vector<PersistentMetadataStore::CacheEntryInfo> entries;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> ParquetCacheInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	names = {"file_path", "etag", "last_modified", "file_size", "footer_size",
	         "num_rows",  "num_row_groups", "cached_at"};
	return_types = {LogicalType::VARCHAR,   LogicalType::BLOB,     LogicalType::TIMESTAMP,
	                LogicalType::UBIGINT,   LogicalType::UBIGINT,  LogicalType::BIGINT,
	                LogicalType::INTEGER,   LogicalType::TIMESTAMP};

	auto result = make_uniq<ParquetCacheInfoData>();
	auto cache_path = GetCachePath(context);
	if (!cache_path.empty()) {
		auto store = GetOrCreateStore(cache_path, DConstants::INVALID_INDEX);
		if (store) {
			try {
				result->entries = store->GetAllEntries();
			} catch (...) {
			}
		}
	}
	return std::move(result);
}

static void ParquetCacheInfoExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ParquetCacheInfoData>();
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset];
		output.SetValue(0, count, Value(entry.file_path));
		output.SetValue(1, count, Value::BLOB(const_data_ptr_cast(entry.etag.c_str()), entry.etag.size()));
		output.SetValue(2, count, Value::TIMESTAMP(entry.last_modified));
		output.SetValue(3, count, Value::UBIGINT(entry.file_size));
		output.SetValue(4, count, Value::UBIGINT(entry.footer_size));
		output.SetValue(5, count, Value::BIGINT(entry.num_rows));
		output.SetValue(6, count, Value::INTEGER(entry.num_row_groups));
		output.SetValue(7, count, Value::TIMESTAMP(entry.cached_at));
		count++;
		data.offset++;
	}
	output.SetCardinality(count);
}

//! parquet_cache_clear() table function
struct ParquetCacheClearData : public TableFunctionData {
	idx_t cleared = 0;
	bool done = false;
};

static unique_ptr<FunctionData> ParquetCacheClearBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names = {"cleared"};
	return_types = {LogicalType::BIGINT};

	auto result = make_uniq<ParquetCacheClearData>();
	auto cache_path = GetCachePath(context);
	if (!cache_path.empty()) {
		auto store = GetOrCreateStore(cache_path, DConstants::INVALID_INDEX);
		if (store) {
			auto before = store->EntryCount();
			if (!input.inputs.empty()) {
				store->Remove(input.inputs[0].ToString());
			} else {
				store->Clear();
			}
			auto after = store->EntryCount();
			result->cleared = before - after;
		}
	}
	return std::move(result);
}

static void ParquetCacheClearExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->CastNoConst<ParquetCacheClearData>();
	if (data.done) {
		return;
	}
	output.SetValue(0, 0, Value::BIGINT(static_cast<int64_t>(data.cleared)));
	output.SetCardinality(1);
	data.done = true;
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &db_instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db_instance);

	// Register settings
	config.AddExtensionOption("parquet_persistent_cache_path",
	                          "Path to the persistent parquet metadata cache database. "
	                          "Empty string (default) disables persistent caching.",
	                          LogicalType::VARCHAR, Value(""), OnCachePathSet);

	config.AddExtensionOption("parquet_persistent_cache_ttl",
	                          "Seconds to trust cached metadata without revalidation. "
	                          "0 = always validate via HEAD request, -1 = never validate (always trust cache).",
	                          LogicalType::BIGINT, Value::BIGINT(0));

	config.AddExtensionOption("parquet_persistent_cache_max_entries",
	                          "Maximum number of cached metadata entries. -1 = no limit.",
	                          LogicalType::BIGINT, Value::BIGINT(-1));

	// Register table functions
	TableFunction cache_info("parquet_cache_info", {}, ParquetCacheInfoExecute, ParquetCacheInfoBind);
	loader.RegisterFunction(cache_info);

	TableFunctionSet cache_clear_set("parquet_cache_clear");
	TableFunction cache_clear_all({}, ParquetCacheClearExecute, ParquetCacheClearBind);
	cache_clear_set.AddFunction(cache_clear_all);
	TableFunction cache_clear_one({LogicalType::VARCHAR}, ParquetCacheClearExecute, ParquetCacheClearBind);
	cache_clear_set.AddFunction(cache_clear_one);
	loader.RegisterFunction(cache_clear_set);
}

void ParquetCacheExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string ParquetCacheExtension::Name() {
	return "parquet_cache";
}

std::string ParquetCacheExtension::Version() const {
#ifdef EXT_VERSION_PARQUET_CACHE
	return EXT_VERSION_PARQUET_CACHE;
#else
	return "";
#endif
}

} // namespace duckdb

#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(parquet_cache, loader) { // NOLINT
	duckdb::LoadInternal(loader);
}
}
#endif
