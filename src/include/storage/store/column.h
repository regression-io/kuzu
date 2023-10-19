#pragma once

#include "catalog/catalog.h"
#include "storage/stats/metadata_dah_info.h"
#include "storage/stats/property_statistics.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

struct CompressionMetadata;

using read_values_to_vector_func_t = std::function<void(uint8_t* frame,
    PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
    uint32_t numValuesToRead, const CompressionMetadata& metadata)>;
using write_values_from_vector_func_t = std::function<void(uint8_t* frame, uint16_t posInFrame,
    common::ValueVector* vector, uint32_t posInVector, const CompressionMetadata& metadata)>;
using write_values_func_t = std::function<void(uint8_t* frame, uint16_t posInFrame,
    const uint8_t* data, common::offset_t dataOffset, common::offset_t numValues,
    const CompressionMetadata& metadata)>;

using read_values_to_page_func_t =
    std::function<void(uint8_t* frame, PageElementCursor& pageCursor, uint8_t* result,
        uint32_t posInResult, uint64_t numValues, const CompressionMetadata& metadata)>;
// This is a special usage for the `batchLookup` interface.
using batch_lookup_func_t = read_values_to_page_func_t;

struct ReadState {
    ColumnChunkMetadata metadata;
    uint64_t numValuesPerPage;
};

class NullColumn;
class StructColumn;
class BaseColumn {
    friend class LocalColumn;
    friend class StringLocalColumn;
    friend class StringColumn;
    friend class VarListLocalColumn;
    friend class StructColumn;

public:
    BaseColumn(common::LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats propertyStatistics,
        bool enableCompression, bool requireNullColumn = true);
    virtual ~BaseColumn() = default;

    virtual void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx);

    virtual void setNull(common::offset_t nodeOffset);

    inline const common::LogicalType& getDataType() const { return dataType; }
    inline uint32_t getNumBytesPerValue() const { return numBytesPerFixedSizedValue; }
    inline uint64_t getNumNodeGroups(transaction::Transaction* transaction) const {
        return metadataDA->getNumElements(transaction->getType());
    }

    virtual void checkpointInMemory();
    virtual void rollbackInMemory();

    void populateWithDefaultVal(const catalog::Property& property, Column* column,
        InMemDiskArray<ColumnChunkMetadata>* metadataDA, common::ValueVector* defaultValueVector,
        uint64_t numNodeGroups) const;

    inline ColumnChunkMetadata getMetadata(
        common::node_group_idx_t nodeGroupIdx, transaction::TransactionType transaction) const {
        return metadataDA->get(nodeGroupIdx, transaction);
    }
    inline InMemDiskArray<ColumnChunkMetadata>* getMetadataDA() const { return metadataDA.get(); }

    virtual void scan(common::node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk);
    virtual void scan(transaction::Transaction* transaction, const ReadState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup, uint8_t* result);

protected:
    void scanUnfiltered(transaction::Transaction* transaction, PageElementCursor& pageCursor,
        uint64_t numValuesToScan, common::ValueVector* resultVector,
        const ColumnChunkMetadata& chunkMeta, uint64_t startPosInVector = 0);
    void scanFiltered(transaction::Transaction* transaction, PageElementCursor& pageCursor,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector,
        const ColumnChunkMetadata& chunkMeta);

    void readFromPage(transaction::Transaction* transaction, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& func);

    // Produces a page cursor for the offset relative to the given node group
    PageElementCursor getPageCursorForOffsetInGroup(
        common::offset_t nodeOffset, const ReadState& state);

    ReadState getReadState(
        transaction::TransactionType transactionType, common::node_group_idx_t nodeGroupIdx) const;

private:
    // check if val is in range [start, end)
    static inline bool isInRange(uint64_t val, uint64_t start, uint64_t end) {
        return val >= start && val < end;
    }

protected:
    DBFileID dbFileID;
    common::LogicalType dataType;
    // TODO(bmwinger): Remove. Only used by var_list_column_chunk for something which should be
    // rewritten
    uint32_t numBytesPerFixedSizedValue;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    BufferManager* bufferManager;
    WAL* wal;
    std::unique_ptr<InMemDiskArray<ColumnChunkMetadata>> metadataDA;
    std::unique_ptr<Column> nullColumn;
    read_values_to_vector_func_t readToVectorFunc;
    write_values_from_vector_func_t writeFromVectorFunc;
    write_values_func_t writeFunc;
    read_values_to_page_func_t readToPageFunc;
    batch_lookup_func_t batchLookupFunc;
    RWPropertyStats propertyStatistics;
    bool enableCompression;
};

// Column where we assume it the underlying storage always stores NodeGroupSize values
// Data is indexed using a global offset (which is internally used to find the node group via the
// node group size)
class Column : public BaseColumn {
public:
    Column(common::LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats propertyStatistics,
        bool enableCompression, bool requireNullColumn = true)
        : BaseColumn{std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
              transaction, propertyStatistics, enableCompression, requireNullColumn} {}

    // Expose for feature store
    virtual void batchLookup(transaction::Transaction* transaction,
        const common::offset_t* nodeOffsets, size_t size, uint8_t* result);

    virtual void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);
    virtual void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector);
    inline void scan(common::node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) override {
        BaseColumn::scan(nodeGroupIdx, columnChunk);
    }
    virtual void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    virtual void write(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom);

protected:
    virtual void writeValue(const ColumnChunkMetadata& chunkMeta, common::offset_t nodeOffset,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom);
    virtual void writeValue(
        const ColumnChunkMetadata& chunkMeta, common::offset_t nodeOffset, const uint8_t* data);

    virtual void scanInternal(transaction::Transaction* transaction,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    virtual void lookupInternal(transaction::Transaction* transaction,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    virtual void lookupValue(transaction::Transaction* transaction, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t posInVector);

    WALPageIdxPosInPageAndFrame createWALVersionOfPageForValue(common::offset_t nodeOffset);

    // Produces a page cursor for the absolute node offset
    PageElementCursor getPageCursorForOffset(
        transaction::TransactionType transactionType, common::offset_t nodeOffset);
};

class InternalIDColumn : public Column {
public:
    InternalIDColumn(const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats stats);

    inline void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) {
        Column::scan(transaction, nodeIDVector, resultVector);
        populateCommonTableID(resultVector);
    }

    inline void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) {
        Column::scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
            offsetInVector);
        populateCommonTableID(resultVector);
    }

    inline void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) {
        Column::lookup(transaction, nodeIDVector, resultVector);
        populateCommonTableID(resultVector);
    }

    // TODO(Guodong): Should figure out a better way to set tableID, and remove this function.
    inline void setCommonTableID(common::table_id_t tableID) { commonTableID = tableID; }

private:
    void populateCommonTableID(common::ValueVector* resultVector) const;

private:
    common::table_id_t commonTableID;
};

// Column for data adjacent to a NodeGroup
// Data is indexed using the node group identifier and the offset within the node group
class AuxiliaryColumn : public BaseColumn {
public:
    AuxiliaryColumn(common::LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats propertyStatistics,
        bool enableCompression, bool requireNullColumn = true)
        : BaseColumn{std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
              transaction, propertyStatistics, enableCompression, requireNullColumn} {}

    // Append values to the end of the node group, resizing it if necessary
    common::offset_t appendValues(
        common::node_group_idx_t nodeGroupIdx, const uint8_t* data, common::offset_t numValues);

protected:
};

struct ColumnFactory {
    static std::unique_ptr<Column> createColumn(const common::LogicalType& dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression);
};

} // namespace storage
} // namespace kuzu
