#pragma once

#include <map>
#include <string>

#include "catalog/catalog.h"
#include "storage/store/column.h"

namespace kuzu {
namespace catalog {
class NodeTableSchema;
class RelTableSchema;
} // namespace catalog

namespace storage {

class WALReplayerUtils {
public:
    static inline void removeHashIndexFile(
        catalog::NodeTableSchema* tableSchema, const std::string& directory) {
        fileOperationOnNodeFiles(tableSchema, directory, removeColumnFilesIfExists);
    }

    // Create empty hash index file for the new node table.
    static void createEmptyHashIndexFiles(
        catalog::NodeTableSchema* nodeTableSchema, const std::string& directory);

private:
    static void removeColumnFilesIfExists(const std::string& fileName);

    static void fileOperationOnNodeFiles(catalog::NodeTableSchema* nodeTableSchema,
        const std::string& directory,
        std::function<void(std::string fileName)> columnFileOperation);
};

} // namespace storage
} // namespace kuzu
