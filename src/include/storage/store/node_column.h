#pragma once

#include "catalog/catalog.h"
#include "storage/stats/property_statistics.h"
#include "storage/stats/table_statistics.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/storage_structure.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

struct CompressionMetadata;

using read_values_to_vector_func_t = std::function<void(uint8_t* frame,
    PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
    uint32_t numValuesToRead, const CompressionMetadata& metadata)>;
using write_values_from_vector_func_t = std::function<void(uint8_t* frame, uint16_t posInFrame,
    common::ValueVector* vector, uint32_t posInVector, const CompressionMetadata& metadata)>;

using read_values_to_page_func_t =
    std::function<void(uint8_t* frame, PageElementCursor& pageCursor, uint8_t* result,
        uint32_t posInResult, uint64_t numValues, const CompressionMetadata& metadata)>;
// This is a special usage for the `batchLookup` interface.
using batch_lookup_func_t = read_values_to_page_func_t;

class NullNodeColumn;
class StructNodeColumn;
// TODO(Guodong): This is intentionally duplicated with `Column`, as for now, we don't change rel
// tables. `Column` is used for rel tables only. Eventually, we should remove `Column`.
class NodeColumn {
    friend class LocalColumn;
    friend class StringLocalColumn;
    friend class VarListLocalColumn;
    friend class StructNodeColumn;

public:
    NodeColumn(common::LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats PropertyStatistics, bool compress,
        bool requireNullColumn = true);
    virtual ~NodeColumn() = default;

    // Expose for feature store
    virtual void batchLookup(transaction::Transaction* transaction,
        const common::offset_t* nodeOffsets, size_t size, uint8_t* result);

    virtual void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);
    virtual void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0);
    virtual void scan(common::node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk);
    virtual void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    virtual void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx);

    virtual void setNull(common::offset_t nodeOffset);

    inline common::LogicalType getDataType() const { return dataType; }
    inline uint32_t getNumBytesPerValue() const { return numBytesPerFixedSizedValue; }
    inline uint64_t getNumNodeGroups(transaction::Transaction* transaction) const {
        return metadataDA->getNumElements(transaction->getType());
    }
    inline NodeColumn* getChildColumn(common::vector_idx_t childIdx) {
        assert(childIdx < childrenColumns.size());
        return childrenColumns[childIdx].get();
    }

    virtual void checkpointInMemory();
    virtual void rollbackInMemory();

    void populateWithDefaultVal(const catalog::Property& property, NodeColumn* nodeColumn,
        common::ValueVector* defaultValueVector, uint64_t numNodeGroups);

    inline CompressionMetadata getCompressionMetadata(
        common::node_group_idx_t nodeGroupIdx, transaction::TransactionType transaction) const {
        return metadataDA->get(nodeGroupIdx, transaction).compMeta;
    }

protected:
    virtual void scanInternal(transaction::Transaction* transaction,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    void scanUnfiltered(transaction::Transaction* transaction, PageElementCursor& pageCursor,
        uint64_t numValuesToScan, common::ValueVector* resultVector,
        const CompressionMetadata& compMeta, uint64_t startPosInVector = 0);
    void scanFiltered(transaction::Transaction* transaction, PageElementCursor& pageCursor,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector,
        const CompressionMetadata& compMeta);
    virtual void lookupInternal(transaction::Transaction* transaction,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    virtual void lookupValue(transaction::Transaction* transaction, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t posInVector);

    void readFromPage(transaction::Transaction* transaction, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& func);

    void write(common::ValueVector* nodeIDVector, common::ValueVector* vectorToWriteFrom);
    inline void write(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) {
        writeInternal(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    }
    virtual void writeInternal(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom);
    virtual void writeValue(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom);

    PageElementCursor getPageCursorForOffset(
        transaction::TransactionType transactionType, common::offset_t nodeOffset);
    // TODO(Guodong): This is mostly duplicated with
    // StorageStructure::createWALVersionOfPageIfNecessaryForElement(). Should be cleared later.
    WALPageIdxPosInPageAndFrame createWALVersionOfPageForValue(common::offset_t nodeOffset);

protected:
    StorageStructureID storageStructureID;
    common::LogicalType dataType;
    // TODO(bmwinger): Remove. Only used by var_list_column_chunk for something which should be
    // rewritten
    uint32_t numBytesPerFixedSizedValue;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    BufferManager* bufferManager;
    WAL* wal;
    std::unique_ptr<InMemDiskArray<ColumnChunkMetadata>> metadataDA;
    std::unique_ptr<NodeColumn> nullColumn;
    std::vector<std::unique_ptr<NodeColumn>> childrenColumns;
    read_values_to_vector_func_t readToVectorFunc;
    write_values_from_vector_func_t writeFromVectorFunc;
    read_values_to_page_func_t readToPageFunc;
    batch_lookup_func_t batchLookupFunc;
    RWPropertyStats propertyStatistics;
    bool compress;
};

class BoolNodeColumn : public NodeColumn {
public:
    BoolNodeColumn(const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats propertyStatistics, bool compress,
        bool requireNullColumn = true);
};

class NullNodeColumn : public NodeColumn {
    friend StructNodeColumn;

public:
    NullNodeColumn(common::page_idx_t metaDAHPageIdx, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats propertyStatistics, bool compress);

    void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) final;
    void scan(common::node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) final;

    void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) final;
    void setNull(common::offset_t nodeOffset) final;

protected:
    void writeInternal(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) final;
};

class SerialNodeColumn : public NodeColumn {
public:
    SerialNodeColumn(const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction);

    void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) final;
};

struct NodeColumnFactory {
    static std::unique_ptr<NodeColumn> createNodeColumn(const common::LogicalType& dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats propertyStatistics, bool compress);
};

} // namespace storage
} // namespace kuzu
