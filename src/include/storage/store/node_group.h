#pragma once

#include "catalog/catalog.h"
#include "processor/result/result_set.h"
#include "storage/store/column_chunk.h"
#include "transaction/transaction.h"

namespace kuzu {
namespace storage {

class TableData;

class NodeGroup {
public:
    NodeGroup(const std::vector<std::unique_ptr<common::LogicalType>>& columnTypes,
        bool enableCompression);
    explicit NodeGroup(TableData* table);

    inline void setNodeGroupIdx(uint64_t nodeGroupIdx_) { this->nodeGroupIdx = nodeGroupIdx_; }
    inline uint64_t getNodeGroupIdx() const { return nodeGroupIdx; }
    inline common::offset_t getNumNodes() const { return numNodes; }
    inline ColumnChunk* getColumnChunk(common::column_id_t columnID) {
        assert(columnID < chunks.size());
        return chunks[columnID].get();
    }
    inline bool isFull() const { return numNodes == common::StorageConstants::NODE_GROUP_SIZE; }

    void resetToEmpty();

    uint64_t append(const std::vector<common::ValueVector*>& columnVectors,
        common::DataChunkState* columnState, uint64_t numValuesToAppend);

    common::offset_t append(NodeGroup* other, common::offset_t offsetInOtherNodeGroup);

private:
    uint64_t nodeGroupIdx;
    common::offset_t numNodes;
    std::vector<std::unique_ptr<ColumnChunk>> chunks;
};

} // namespace storage
} // namespace kuzu
