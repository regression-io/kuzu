#pragma once

#include "catalog/rel_table_schema.h"
#include "common/cast.h"
#include "storage/store/table_data.h"

namespace kuzu {
namespace storage {

struct RelDataReadState : public TableReadState {
    common::RelDataDirection direction;
    common::ColumnDataFormat dataFormat;
    common::offset_t startNodeOffsetInState;
    common::offset_t numNodesInState;
    common::offset_t currentCSRNodeOffset;
    common::offset_t posInCurrentCSR;
    std::vector<common::list_entry_t> csrListEntries;
    // Temp auxiliary data structure to scan the offset of each CSR node in the offset column chunk.
    std::unique_ptr<ColumnChunk> csrOffsetChunk;

    RelDataReadState(common::ColumnDataFormat dataFormat);
    inline bool isOutOfRange(common::offset_t nodeOffset) {
        return nodeOffset < startNodeOffsetInState ||
               nodeOffset >= (startNodeOffsetInState + numNodesInState);
    }
    inline bool hasMoreToRead() {
        return dataFormat == common::ColumnDataFormat::CSR &&
               posInCurrentCSR <
                   csrListEntries[(currentCSRNodeOffset - startNodeOffsetInState)].size;
    }
    void populateCSRListEntries();
    std::pair<common::offset_t, common::offset_t> getStartAndEndOffset();
};

class RelsStoreStats;
class RelTableData final : public TableData {
public:
    static constexpr common::column_id_t REL_ID_COLUMN_ID = 0;

    RelTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager,
        WAL* wal, catalog::RelTableSchema* tableSchema, RelsStoreStats* relsStoreStats,
        common::RelDataDirection direction, bool enableCompression);

    void initializeReadState(transaction::Transaction* transaction,
        std::vector<common::column_id_t> columnIDs, common::ValueVector* inNodeIDVector,
        TableReadState* readState);
    inline void scan(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors) {
        auto& relReadState = common::ku_dynamic_cast<TableReadState&, RelDataReadState&>(readState);
        dataFormat == common::ColumnDataFormat::REGULAR ?
            scanRegularColumns(transaction, relReadState, inNodeIDVector, outputVectors) :
            scanCSRColumns(transaction, relReadState, inNodeIDVector, outputVectors);
    }

    void insert(transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector,
        common::ValueVector* dstNodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::ValueVector* srcNodeIDVector, common::ValueVector* dstNodeIDVector,
        common::ValueVector* relIDVector, common::ValueVector* propertyVector);
    void delete_(transaction::Transaction* transaction, common::ValueVector* srcNodeIDVector,
        common::ValueVector* dstNodeIDVector, common::ValueVector* relIDVector);

    void lookup(transaction::Transaction* transaction, TableReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);

    void append(NodeGroup* nodeGroup);

    inline Column* getAdjColumn() const { return adjColumn.get(); }
    inline common::ColumnDataFormat getDataFormat() const { return dataFormat; }

    void prepareCommit(LocalTable* localTable);
    void checkpointInMemory();
    void rollbackInMemory();

private:
    void scanRegularColumns(transaction::Transaction* transaction, RelDataReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);
    void scanCSRColumns(transaction::Transaction* transaction, RelDataReadState& readState,
        common::ValueVector* inNodeIDVector,
        const std::vector<common::ValueVector*>& outputVectors);

    void prepareCommitRegularColumns(LocalRelTableData* localTableData);
    void prepareCommitCSRColumns(LocalRelTableData* localTableData);

    static inline common::ColumnDataFormat getDataFormatFromSchema(
        catalog::RelTableSchema* tableSchema, common::RelDataDirection direction) {
        return tableSchema->isSingleMultiplicityInDirection(direction) ?
                   common::ColumnDataFormat::REGULAR :
                   common::ColumnDataFormat::CSR;
    }

private:
    common::RelDataDirection direction;
    std::unique_ptr<Column> adjColumn;
    std::unique_ptr<Column> csrOffsetColumn;
};

} // namespace storage
} // namespace kuzu
