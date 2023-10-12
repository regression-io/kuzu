#include "storage/store/string_column.h"

#include "storage/store/string_column_chunk.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StringColumn::StringColumn(LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, RWPropertyStats stats, bool enableCompression)
    : Column{std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
          transaction, stats, enableCompression, true /* requireNullColumn */} {
    dataColumn = std::make_unique<AuxiliaryColumn>(LogicalType(LogicalTypeID::UINT8),
        *metaDAHeaderInfo.childrenInfos[0], dataFH, metadataFH, bufferManager, wal, transaction,
        stats, false, false /*requireNullColumn*/);
    offsetColumn = std::make_unique<AuxiliaryColumn>(LogicalType(LogicalTypeID::UINT64),
        *metaDAHeaderInfo.childrenInfos[1], dataFH, metadataFH, bufferManager, wal, transaction,
        stats, enableCompression, false /*requireNullColumn*/);
}

void StringColumn::scanOffsets(Transaction* transaction, const ReadState& state,
    string_offset_t* offsets, uint64_t index, uint64_t dataSize) {
    // We either need to read the next value, or store the maximum string offset at the end.
    // Otherwise we won't know what the length of the last string is.
    if (index < state.metadata.numValues - 1) {
        offsetColumn->scan(transaction, state, index, index + 2, (uint8_t*)offsets);
    } else {
        offsetColumn->scan(transaction, state, index, index + 1, (uint8_t*)offsets);
        offsets[1] = dataSize;
    }
}

void StringColumn::scanValueToVector(Transaction* transaction, const ReadState& dataState,
    string_offset_t startOffset, string_offset_t endOffset, ValueVector* resultVector,
    uint64_t offsetInVector) {
    // TODO: don't scan the same string multiple times when values are duplicated
    KU_ASSERT(endOffset >= startOffset);
    // Add string to vector first and read directly into the vector
    auto& kuString =
        StringVector::reserveString(resultVector, offsetInVector, endOffset - startOffset);
    dataColumn->scan(transaction, dataState, startOffset, endOffset, (uint8_t*)kuString.getData());
    // Update prefix to match the scanned string data
    if (!ku_string_t::isShortString(kuString.len)) {
        memcpy(kuString.prefix, kuString.getData(), ku_string_t::PREFIX_LENGTH);
    }
}

void StringColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    scanUnfiltered(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
}

void StringColumn::scan(node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) {
    Column::scan(nodeGroupIdx, columnChunk);
    auto stringColumnChunk = reinterpret_cast<StringColumnChunk*>(columnChunk);
    dataColumn->scan(nodeGroupIdx, stringColumnChunk->getDataChunk());
    offsetColumn->scan(nodeGroupIdx, stringColumnChunk->getOffsetChunk());
}

void StringColumn::append(ColumnChunk* columnChunk, node_group_idx_t nodeGroupIdx) {
    BaseColumn::append(columnChunk, nodeGroupIdx);
    auto stringColumnChunk = reinterpret_cast<StringColumnChunk*>(columnChunk);
    dataColumn->append(stringColumnChunk->getDataChunk(), nodeGroupIdx);
    offsetColumn->append(stringColumnChunk->getOffsetChunk(), nodeGroupIdx);
}

void StringColumn::writeValue(const ColumnChunkMetadata& chunkMeta, offset_t nodeOffset,
    ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto& kuStr = vectorToWriteFrom->getValue<ku_string_t>(posInVectorToWriteFrom);
    // Write string data to end of dataColumn
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto startOffset =
        dataColumn->appendValues(nodeGroupIdx, (const uint8_t*)kuStr.getData(), kuStr.len);

    // Write offset
    string_index_t index =
        offsetColumn->appendValues(nodeGroupIdx, (const uint8_t*)&startOffset, 1);

    // Write index to main column
    Column::writeValue(chunkMeta, nodeOffset, (uint8_t*)&index);
}

void StringColumn::checkpointInMemory() {
    BaseColumn::checkpointInMemory();
    dataColumn->checkpointInMemory();
    offsetColumn->checkpointInMemory();
}

void StringColumn::rollbackInMemory() {
    BaseColumn::rollbackInMemory();
    dataColumn->rollbackInMemory();
    offsetColumn->rollbackInMemory();
}

void StringColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    KU_ASSERT(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);
    auto startOffsetInGroup =
        startNodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, nodeGroupIdx, startOffsetInGroup,
            startOffsetInGroup + nodeIDVector->state->selVector->selectedSize, resultVector);
    } else {
        scanFiltered(transaction, nodeGroupIdx, startOffsetInGroup, nodeIDVector, resultVector);
    }
}

void StringColumn::scanUnfiltered(transaction::Transaction* transaction,
    node_group_idx_t nodeGroupIdx, offset_t startOffsetInGroup, offset_t endOffsetInGroup,
    common::ValueVector* resultVector, uint64_t startPosInVector) {
    auto numValuesToRead = endOffsetInGroup - startOffsetInGroup;
    auto indices = std::make_unique<string_index_t[]>(numValuesToRead);
    auto indexState = getReadState(transaction->getType(), nodeGroupIdx);
    auto offsetState = offsetColumn->getReadState(transaction->getType(), nodeGroupIdx);
    auto dataState = dataColumn->getReadState(transaction->getType(), nodeGroupIdx);
    BaseColumn::scan(
        transaction, indexState, startOffsetInGroup, endOffsetInGroup, (uint8_t*)indices.get());
    for (auto i = 0u; i < numValuesToRead; i++) {
        if (resultVector->isNull(startPosInVector + i)) {
            continue;
        }
        string_offset_t offsets[2];
        scanOffsets(transaction, offsetState, offsets, indices[i], dataState.metadata.numValues);
        scanValueToVector(
            transaction, dataState, offsets[0], offsets[1], resultVector, startPosInVector + i);
    }
}

void StringColumn::scanFiltered(transaction::Transaction* transaction,
    node_group_idx_t nodeGroupIdx, common::offset_t startOffsetInGroup,
    common::ValueVector* nodeIDVector, common::ValueVector* resultVector) {

    auto indexState = getReadState(transaction->getType(), nodeGroupIdx);
    auto offsetState = offsetColumn->getReadState(transaction->getType(), nodeGroupIdx);
    auto dataState = dataColumn->getReadState(transaction->getType(), nodeGroupIdx);
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (resultVector->isNull(pos)) {
            // Ignore positions which were scanned as null
            continue;
        }
        auto offsetInGroup = startOffsetInGroup + pos;
        string_index_t index;
        BaseColumn::scan(
            transaction, indexState, offsetInGroup, offsetInGroup + 1, (uint8_t*)&index);
        string_offset_t offsets[2];
        scanOffsets(transaction, offsetState, offsets, index, dataState.metadata.numValues);
        scanValueToVector(transaction, dataState, offsets[0], offsets[1], resultVector, pos);
    }
}

void StringColumn::lookupInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);

    auto indexState = getReadState(transaction->getType(), nodeGroupIdx);
    auto offsetState = offsetColumn->getReadState(transaction->getType(), nodeGroupIdx);
    auto dataState = dataColumn->getReadState(transaction->getType(), nodeGroupIdx);
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        if (resultVector->isNull(pos)) {
            // Ignore positions which were scanned as null
            continue;
        }
        string_offset_t offsets[2];
        auto offsetInGroup =
            startNodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx) + pos;
        string_index_t index;
        BaseColumn::scan(
            transaction, indexState, offsetInGroup, offsetInGroup + 1, (uint8_t*)&index);
        scanOffsets(transaction, offsetState, offsets, index, dataState.metadata.numValues);
        scanValueToVector(transaction, dataState, offsets[0], offsets[1], resultVector, pos);
    }
}

} // namespace storage
} // namespace kuzu
