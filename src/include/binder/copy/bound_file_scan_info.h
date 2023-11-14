#pragma once

#include "binder/expression/expression.h"
#include "common/enums/table_type.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"

namespace kuzu {
namespace binder {

struct BoundFileScanInfo {
    function::TableFunction* copyFunc;
    std::unique_ptr<function::TableFuncBindData> bindData;
    binder::expression_vector columns;
    // TODO: rename
    std::shared_ptr<Expression> internalID;

    BoundFileScanInfo(function::TableFunction* copyFunc,
        std::unique_ptr<function::TableFuncBindData> bindData, binder::expression_vector columns,
        std::shared_ptr<Expression> internalID)
        : copyFunc{copyFunc}, bindData{std::move(bindData)}, columns{std::move(columns)},
          internalID{std::move(internalID)} {}
    BoundFileScanInfo(const BoundFileScanInfo& other)
        : copyFunc{other.copyFunc}, bindData{other.bindData->copy()}, columns{other.columns},
          internalID{other.internalID} {}

    inline std::unique_ptr<BoundFileScanInfo> copy() const {
        return std::make_unique<BoundFileScanInfo>(*this);
    }
};

} // namespace binder
} // namespace kuzu
