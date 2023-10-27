#include "storage/local_storage.h"

#include <memory>
#include <vector>

#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/local_table.h"
#include "storage/storage_manager.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

LocalStorage::LocalStorage(StorageManager* storageManager, MemoryManager* mm)
    : nodesStore{&storageManager->getNodesStore()}, mm{mm},
      enableCompression{storageManager->compressionEnabled()} {}

void LocalStorage::scan(table_id_t tableID, ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    if (!tables.contains(tableID)) {
        return;
    }
    tables.at(tableID)->scan(nodeIDVector, columnIDs, outputVectors);
}

void LocalStorage::lookup(table_id_t tableID, ValueVector* nodeIDVector,
    const std::vector<column_id_t>& columnIDs, const std::vector<ValueVector*>& outputVectors) {
    if (!tables.contains(tableID)) {
        return;
    }
    tables.at(tableID)->lookup(nodeIDVector, columnIDs, outputVectors);
}

void LocalStorage::update(table_id_t tableID, column_id_t columnID, ValueVector* nodeIDVector,
    ValueVector* propertyVector) {
    if (!tables.contains(tableID)) {
        tables.emplace(tableID,
            std::make_unique<LocalTable>(nodesStore->getNodeTable(tableID), enableCompression));
    }
    tables.at(tableID)->update(columnID, nodeIDVector, propertyVector, mm);
}

void LocalStorage::update(table_id_t tableID, column_id_t columnID, offset_t nodeOffset,
    ValueVector* propertyVector, sel_t posInPropertyVector) {
    if (!tables.contains(tableID)) {
        tables.emplace(tableID,
            std::make_unique<LocalTable>(nodesStore->getNodeTable(tableID), enableCompression));
    }
    tables.at(tableID)->update(columnID, nodeOffset, propertyVector, posInPropertyVector, mm);
}

void LocalStorage::prepareCommit() {
    for (auto& [_, table] : tables) {
        table->prepareCommit();
    }
    tables.clear();
}

void LocalStorage::prepareRollback() {
    tables.clear();
}

} // namespace storage
} // namespace kuzu
