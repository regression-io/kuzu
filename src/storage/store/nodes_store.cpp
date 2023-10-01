#include "storage/store/nodes_store.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace storage {

NodesStore::NodesStore(BMFileHandle* dataFH, BMFileHandle* metadataFH, const Catalog& catalog,
    BufferManager& bufferManager, WAL* wal, bool compress)
    : wal{wal}, dataFH{dataFH}, metadataFH{metadataFH}, compress{compress} {
    nodesStatisticsAndDeletedIDs =
        std::make_unique<NodesStoreStatsAndDeletedIDs>(metadataFH, &bufferManager, wal);
    for (auto& schema : catalog.getReadOnlyVersion()->getNodeTableSchemas()) {
        auto nodeTableSchema = reinterpret_cast<NodeTableSchema*>(schema);
        nodeTables[schema->tableID] = std::make_unique<NodeTable>(dataFH, metadataFH,
            nodesStatisticsAndDeletedIDs.get(), bufferManager, wal, nodeTableSchema, compress);
    }
}

} // namespace storage
} // namespace kuzu
