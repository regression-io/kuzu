#include "processor/operator/ddl/create_node_table.h"

#include "common/string_utils.h"
#include "storage/storage_manager.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void CreateNodeTable::executeDDLInternal() {
    for (auto& property : info->properties) {
        property->setMetadataDAHInfo(
            storageManager->createMetadataDAHInfo(*property->getDataType()));
    }
    auto newTableID = catalog->addNodeTableSchema(*info);
    auto newNodeTableSchema =
        (catalog::NodeTableSchema*)catalog->getWriteVersion()->getNodeTableSchema(newTableID);
    nodesStatistics->addNodeStatisticsAndDeletedIDs(newNodeTableSchema);
}

std::string CreateNodeTable::getOutputMsg() {
    return StringUtils::string_format("NodeTable: {} has been created.", info->tableName);
}

} // namespace processor
} // namespace kuzu
