#include "processor/operator/index_builder.h"

#include "storage/index/hash_index_builder.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void IndexBuilderSharedState::init(const std::string& path, const LogicalType& logicalType) {
    assert(logicalType.getLogicalTypeID() != LogicalTypeID::SERIAL);
    index = std::make_unique<PrimaryKeyIndexBuilder>(path, logicalType);
    index->bulkReserve(numRows);
}

void IndexBuilder::initGlobalStateInternal(ExecutionContext* context) {
    assert(sharedState->index);
    sharedState->index->bulkReserve(sharedState->numRows);
}

bool IndexBuilder::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    return true;
}

}
}
