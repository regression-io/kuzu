#include <memory>
#include <utility>
#include <vector>

#include "planner/operator/logical_operator.h"
#include "planner/operator/scan/logical_scan_file.h"
#include "processor/data_pos.h"
#include "processor/operator/persistent/reader.h"
#include "processor/operator/persistent/reader_state.h"
#include "processor/operator/physical_operator.h"
#include "processor/plan_mapper.h"

using namespace kuzu::storage;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanFile(LogicalOperator* logicalOperator) {
    auto outSchema = logicalOperator->getSchema();
    auto scanFile = reinterpret_cast<LogicalScanFile*>(logicalOperator);
    auto info = scanFile->getInfo();
    auto readerSharedState = std::make_shared<ReaderSharedState>(info->readerConfig->copy());
    std::vector<DataPos> dataColumnsPos;
    dataColumnsPos.reserve(info->columns.size());
    for (auto& expression : info->columns) {
        dataColumnsPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    auto internalIDPos = DataPos{};
    if (info->internalID != nullptr) {
        internalIDPos = DataPos(outSchema->getExpressionPos(*info->internalID));
    }
    auto readInfo = std::make_unique<ReaderInfo>(internalIDPos, dataColumnsPos, info->tableType);
    return std::make_unique<Reader>(std::move(readInfo), readerSharedState, getOperatorID(),
        logicalOperator->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
