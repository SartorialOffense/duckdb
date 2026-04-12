cmake_minimum_required(VERSION 3.5...3.29)

project(ParquetCacheExtension)

include_directories(
  include
  ../parquet/include
  ../../third_party/parquet
  ../../third_party/thrift)

set(PARQUET_CACHE_EXTENSION_FILES
    parquet_cache_extension.cpp
    duckdb_metadata_store.cpp
    metadata_serialization.cpp)

if(NOT CLANG_TIDY)
  # Thrift and parquet type sources needed for metadata serialization
  set(PARQUET_CACHE_EXTENSION_FILES
      ${PARQUET_CACHE_EXTENSION_FILES}
      ../../third_party/parquet/parquet_types.cpp
      ../../third_party/thrift/thrift/protocol/TProtocol.cpp
      ../../third_party/thrift/thrift/transport/TTransportException.cpp
      ../../third_party/thrift/thrift/transport/TBufferTransports.cpp
      ../parquet/parquet_file_metadata_cache.cpp
      ../parquet/parquet_geometry.cpp)
endif()

build_static_extension(parquet_cache ${PARQUET_CACHE_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(parquet_cache ${PARAMETERS} ${PARQUET_CACHE_EXTENSION_FILES})

install(
  TARGETS parquet_cache_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
