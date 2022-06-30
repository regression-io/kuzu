#include "src/processor/include/physical_plan/operator/update/create.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> CreateNode::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    return resultSet;
}

bool CreateNode::getNextTuples() {
    metrics->executionTime.start();
    if (!children[0]->getNextTuples()) {
        metrics->executionTime.stop();
        return false;
    }
    for (auto& nodeTable : nodeTables) {
        nodeTable->addNode();
    }
    metrics->executionTime.stop();
    return true;
}

} // namespace processor
} // namespace graphflow