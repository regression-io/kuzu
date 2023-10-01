#pragma once

#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class StructColumnChunk : public ColumnChunk {
public:
    StructColumnChunk(common::LogicalType dataType,
        std::unique_ptr<common::CSVReaderConfig> csvReaderConfig, bool enableCompression);

protected:
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;
    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;

private:
    // TODO(Guodong): These methods are duplicated from `InMemStructColumnChunk`, which will be
    // merged later.
    void setStructFields(const char* value, uint64_t length, uint64_t pos);
    void setValueToStructField(common::offset_t pos, const std::string& structFieldValue,
        common::struct_field_idx_t structFiledIdx);
    void write(const common::Value& val, uint64_t posToWrite) final;
};

} // namespace storage
} // namespace kuzu
