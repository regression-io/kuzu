#include "planner/logical_plan/logical_operator/base_logical_operator.h"

#include "common/exception.h"

namespace kuzu {
namespace planner {

std::string LogicalOperatorUtils::logicalOperatorTypeToString(LogicalOperatorType type) {
    switch (type) {
    case LogicalOperatorType::ACCUMULATE: {
        return "ACCUMULATE";
    }
    case LogicalOperatorType::AGGREGATE: {
        return "AGGREGATE";
    }
    case LogicalOperatorType::COPY_CSV: {
        return "COPY_CSV";
    }
    case LogicalOperatorType::CREATE_NODE: {
        return "CREATE_NODE";
    }
    case LogicalOperatorType::CREATE_REL: {
        return "CREATE_REL";
    }
    case LogicalOperatorType::CREATE_NODE_TABLE: {
        return "CREATE_NODE_TABLE";
    }
    case LogicalOperatorType::CREATE_REL_TABLE: {
        return "CREATE_REL_TABLE";
    }
    case LogicalOperatorType::CROSS_PRODUCT: {
        return "CROSS_PRODUCT";
    }
    case LogicalOperatorType::DELETE_NODE: {
        return "DELETE_NODE";
    }
    case LogicalOperatorType::DELETE_REL: {
        return "DELETE_REL";
    }
    case LogicalOperatorType::DISTINCT: {
        return "DISTINCT";
    }
    case LogicalOperatorType::DROP_TABLE: {
        return "DROP_TABLE";
    }
    case LogicalOperatorType::EXPRESSIONS_SCAN: {
        return "EXPRESSIONS_SCAN";
    }
    case LogicalOperatorType::EXTEND: {
        return "EXTEND";
    }
    case LogicalOperatorType::FILTER: {
        return "FILTER";
    }
    case LogicalOperatorType::FLATTEN: {
        return "FLATTEN";
    }
    case LogicalOperatorType::FTABLE_SCAN: {
        return "FTABLE_SCAN";
    }
    case LogicalOperatorType::HASH_JOIN: {
        return "HASH_JOIN";
    }
    case LogicalOperatorType::INTERSECT: {
        return "INTERSECT";
    }
    case LogicalOperatorType::LIMIT: {
        return "LIMIT";
    }
    case LogicalOperatorType::MULTIPLICITY_REDUCER: {
        return "MULTIPLICITY_REDUCER";
    }
    case LogicalOperatorType::ORDER_BY: {
        return "ORDER_BY";
    }
    case LogicalOperatorType::PROJECTION: {
        return "PROJECTION";
    }
    case LogicalOperatorType::SCAN_NODE: {
        return "SCAN_NODE";
    }
    case LogicalOperatorType::INDEX_SCAN_NODE: {
        return "INDEX_SCAN_NODE";
    }
    case LogicalOperatorType::SCAN_NODE_PROPERTY: {
        return "SCAN_NODE_PROPERTY";
    }
    case LogicalOperatorType::SEMI_MASKER: {
        return "SEMI_MASKER";
    }
    case LogicalOperatorType::SET_NODE_PROPERTY: {
        return "SET_NODE_PROPERTY";
    }
    case LogicalOperatorType::SKIP: {
        return "SKIP";
    }
    case LogicalOperatorType::UNION_ALL: {
        return "UNION_ALL";
    }
    case LogicalOperatorType::UNWIND: {
        return "UNWIND";
    }
    default:
        throw NotImplementedException("LogicalOperatorTypeToString()");
    }
}

LogicalOperator::LogicalOperator(
    LogicalOperatorType operatorType, shared_ptr<LogicalOperator> child)
    : operatorType{operatorType} {
    children.push_back(std::move(child));
}

LogicalOperator::LogicalOperator(LogicalOperatorType operatorType, shared_ptr<LogicalOperator> left,
    shared_ptr<LogicalOperator> right)
    : operatorType{operatorType} {
    children.push_back(std::move(left));
    children.push_back(std::move(right));
}

LogicalOperator::LogicalOperator(
    LogicalOperatorType operatorType, vector<shared_ptr<LogicalOperator>> children)
    : operatorType{operatorType} {
    for (auto& child : children) {
        this->children.push_back(child);
    }
}

void LogicalOperator::computeSchemaRecursive() {
    for (auto& child : children) {
        child->computeSchemaRecursive();
    }
    computeSchema();
}

bool LogicalOperator::descendantsContainType(
    const unordered_set<LogicalOperatorType>& types) const {
    if (types.contains(operatorType)) {
        return true;
    }
    for (auto& child : children) {
        if (child->descendantsContainType(types)) {
            return true;
        }
    }
    return false;
}

string LogicalOperator::toString(uint64_t depth) const {
    auto padding = string(depth * 4, ' ');
    string result = padding;
    result += LogicalOperatorUtils::logicalOperatorTypeToString(operatorType) + "[" +
              getExpressionsForPrinting() + "]";
    if (children.size() == 1) {
        result += "\n" + children[0]->toString(depth);
    } else {
        for (auto& child : children) {
            result += "\n" + padding + "CHILD:\n" + child->toString(depth + 1);
        }
    }
    return result;
}

} // namespace planner
} // namespace kuzu