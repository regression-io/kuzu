#include "processor/operator/ddl/drop_table.h"

#include <string>

#include "common/string_format.h"
#include "common/table_type.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

void DropTable::executeDDLInternal() {
    catalog->dropTableSchema(tableID);
}

std::string DropTable::getOutputMsg() {
    auto tableSchema = catalog->getReadOnlyVersion()->getTableSchema(tableID);
    return stringFormat("{} table: {} has been dropped.",
        tableSchema->tableType == TableType::NODE ? "Node" : "Rel", tableSchema->tableName);
}

} // namespace processor
} // namespace kuzu
