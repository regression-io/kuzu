#include "storage/store/nodes_store.h"

#include <memory>

#include "catalog/catalog.h"
#include "catalog/node_table_schema.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/stats/nodes_store_statistics.h"
#include "storage/store/node_table.h"
#include "storage/wal/wal.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

NodesStore::NodesStore(BMFileHandle* dataFH, BMFileHandle* metadataFH, const Catalog& catalog,
    BufferManager& bufferManager, WAL* wal, bool enableCompression)
    : wal{wal}, dataFH{dataFH}, metadataFH{metadataFH}, enableCompression{enableCompression} {
    nodesStatisticsAndDeletedIDs =
        std::make_unique<NodesStoreStatsAndDeletedIDs>(metadataFH, &bufferManager, wal);
    for (auto& schema : catalog.getReadOnlyVersion()->getNodeTableSchemas()) {
        auto nodeTableSchema = reinterpret_cast<NodeTableSchema*>(schema);
        nodeTables[schema->tableID] =
            std::make_unique<NodeTable>(dataFH, metadataFH, nodesStatisticsAndDeletedIDs.get(),
                bufferManager, wal, nodeTableSchema, enableCompression);
    }
}

} // namespace storage
} // namespace kuzu
