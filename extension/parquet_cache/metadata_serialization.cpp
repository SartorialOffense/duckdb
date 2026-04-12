#include "metadata_serialization.hpp"

#include "thrift/protocol/TCompactProtocol.h"
#include "thrift/transport/TBufferTransports.h"

namespace duckdb {

using duckdb_apache::thrift::protocol::TCompactProtocolT;
using duckdb_apache::thrift::transport::TMemoryBuffer;

string MetadataSerialization::Serialize(const duckdb_parquet::FileMetaData &metadata) {
	auto mem_buffer = duckdb_base_std::make_shared<TMemoryBuffer>();
	TCompactProtocolT<TMemoryBuffer> protocol(mem_buffer);
	metadata.write(&protocol);

	uint8_t *buf;
	uint32_t size;
	mem_buffer->getBuffer(&buf, &size);
	return string(reinterpret_cast<const char *>(buf), size);
}

unique_ptr<duckdb_parquet::FileMetaData> MetadataSerialization::Deserialize(const string &blob) {
	auto mem_buffer = duckdb_base_std::make_shared<TMemoryBuffer>(
	    reinterpret_cast<uint8_t *>(const_cast<char *>(blob.data())), static_cast<uint32_t>(blob.size()),
	    TMemoryBuffer::OBSERVE);
	TCompactProtocolT<TMemoryBuffer> protocol(mem_buffer);

	auto metadata = make_uniq<duckdb_parquet::FileMetaData>();
	metadata->read(&protocol);
	return metadata;
}

} // namespace duckdb
