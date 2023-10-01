#pragma once

#include "storage/store/node_column.h"
#include "storage/store/node_group.h"

namespace kuzu {
namespace storage {

class NodesStoreStatsAndDeletedIDs;

class TableData {
public:
    TableData(BMFileHandle* dataFH, BMFileHandle* metadataFH, common::table_id_t tableID,
        BufferManager* bufferManager, WAL* wal, const std::vector<catalog::Property*>& properties,
        TablesStatistics* tablesStatistics, bool compress);

    void read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void insert(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::ValueVector*>& propertyVectors);
    void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::ValueVector* nodeIDVector, common::ValueVector* propertyVector);
    void update(transaction::Transaction* transaction, common::column_id_t columnID,
        common::offset_t nodeOffset, common::ValueVector* propertyVector,
        common::sel_t posInPropertyVector) const;
    void append(NodeGroup* nodeGroup);

    inline void dropColumn(common::column_id_t columnID) {
        columns.erase(columns.begin() + columnID);
    }
    void addColumn(transaction::Transaction* transaction, const catalog::Property& property,
        common::ValueVector* defaultValueVector, TablesStatistics* tableStats);

    inline common::vector_idx_t getNumColumns() const { return columns.size(); }
    inline NodeColumn* getColumn(common::column_id_t columnID) {
        assert(columnID < columns.size());
        return columns[columnID].get();
    }
    inline common::node_group_idx_t getNumNodeGroups(transaction::Transaction* transaction) const {
        assert(!columns.empty());
        return columns[0]->getNumNodeGroups(transaction);
    }
    inline BMFileHandle* getDataFH() const { return dataFH; }

    void checkpointInMemory();
    void rollbackInMemory();

    inline bool compressionEnabled() const { return compress; }

private:
    void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);
    void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors);

private:
    std::vector<std::unique_ptr<NodeColumn>> columns;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    common::table_id_t tableID;
    BufferManager* bufferManager;
    WAL* wal;
    bool compress;
};

} // namespace storage
} // namespace kuzu
