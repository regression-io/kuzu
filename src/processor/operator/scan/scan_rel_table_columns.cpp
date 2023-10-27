#include "processor/operator/scan/scan_rel_table_columns.h"

#include <memory>

#include "processor/execution_context.h"
#include "processor/operator/scan/scan_rel_table.h"
#include "processor/result/result_set.h"
#include "storage/store/rel_table.h"

namespace kuzu {
namespace processor {

void ScanRelTableColumns::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ScanRelTable::initLocalStateInternal(resultSet, context);
    scanState = std::make_unique<storage::RelTableScanState>(
        scanInfo->relStats, scanInfo->propertyIds, storage::RelTableDataType::COLUMNS);
}

bool ScanRelTableColumns::getNextTuplesInternal(ExecutionContext* context) {
    do {
        restoreSelVector(inNodeVector->state->selVector);
        if (!children[0]->getNextTuple(context)) {
            return false;
        }
        saveSelVector(inNodeVector->state->selVector);
        scanInfo->tableData->scan(transaction, *scanState, inNodeVector, outVectors);
    } while (inNodeVector->state->selVector->selectedSize == 0);
    metrics->numOutputTuple.increase(inNodeVector->state->selVector->selectedSize);
    return true;
}

} // namespace processor
} // namespace kuzu
