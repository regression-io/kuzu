#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace storage {
class PrimaryKeyIndexBuilder;
}
namespace processor {

struct IndexBuilderSharedState {
    std::unique_ptr<storage::PrimaryKeyIndexBuilder> index;
    uint64_t& numRows;
    common::column_id_t indexColumnID = common::INVALID_COLUMN_ID;

    void init(const std::string& path, const common::LogicalType& logicalType);
};

class IndexBuilder : public PhysicalOperator {
public:
    IndexBuilder(std::shared_ptr<IndexBuilderSharedState> sharedState, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::INDEX_BUILDER, std::move(child), id, paramsString},
          sharedState{std::move(sharedState)} {}

    void initGlobalStateInternal(ExecutionContext* context) final;
    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<IndexBuilder>(sharedState, children[0]->clone(), getOperatorID(),
            paramsString);
    }

private:
    std::shared_ptr<IndexBuilderSharedState> sharedState;
};

}
}
