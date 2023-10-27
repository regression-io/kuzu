#include "processor/operator/scan/scan_columns.h"

#include "processor/execution_context.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

void ScanColumns::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* /*context*/) {
    inputNodeIDVector = resultSet->getValueVector(inputNodeIDVectorPos).get();
    for (auto& dataPos : outPropertyVectorsPos) {
        auto vector = resultSet->getValueVector(dataPos);
        outPropertyVectors.push_back(vector.get());
    }
}

} // namespace processor
} // namespace kuzu
