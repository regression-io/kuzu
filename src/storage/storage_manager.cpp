#include "storage/storage_manager.h"

#include <memory>

#include "catalog/catalog.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/file_handle.h"
#include "storage/storage_utils.h"
#include "storage/store/nodes_store.h"
#include "storage/store/rels_store.h"
#include "storage/wal/wal.h"
#include "storage/wal_replayer.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

StorageManager::StorageManager(
    Catalog& catalog, MemoryManager& memoryManager, WAL* wal, bool enableCompression)
    : catalog{catalog}, memoryManager{memoryManager}, wal{wal}, enableCompression{
                                                                    enableCompression} {
    dataFH = memoryManager.getBufferManager()->getBMFileHandle(
        StorageUtils::getDataFName(wal->getDirectory()),
        FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE);
    metadataFH = memoryManager.getBufferManager()->getBMFileHandle(
        StorageUtils::getMetadataFName(wal->getDirectory()),
        FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS,
        BMFileHandle::FileVersionedType::VERSIONED_FILE);
    nodesStore = std::make_unique<NodesStore>(dataFH.get(), metadataFH.get(), catalog,
        *memoryManager.getBufferManager(), wal, enableCompression);
    relsStore = std::make_unique<RelsStore>(metadataFH.get(), catalog, memoryManager, wal);
    nodesStore->getNodesStatisticsAndDeletedIDs()->setAdjListsAndColumns(relsStore.get());
}

} // namespace storage
} // namespace kuzu
