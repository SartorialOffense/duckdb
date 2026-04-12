//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_serialization.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb.hpp"
#include "parquet_types.h"

namespace duckdb {

class MetadataSerialization {
public:
	//! Serialize a FileMetaData to a binary blob using Thrift TCompactProtocol
	static string Serialize(const duckdb_parquet::FileMetaData &metadata);

	//! Deserialize a FileMetaData from a binary blob
	static unique_ptr<duckdb_parquet::FileMetaData> Deserialize(const string &blob);

	//! Current schema version for cache compatibility checking
	static constexpr int32_t SCHEMA_VERSION = 1;
};

} // namespace duckdb
