#include "binder/binder.h"

using namespace kuzu::parser;
using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::shared_ptr<NodeExpression> Binder::bindQueryNode(
    const NodePattern& nodePattern, QueryGraph& queryGraph, PropertyKeyValCollection& collection) {
    auto parsedName = nodePattern.getVariableName();
    std::shared_ptr<NodeExpression> queryNode;
    if (scope->contains(parsedName)) { // bind to node in scope
        auto prevVariable = scope->getExpression(parsedName);
        ExpressionBinder::validateExpectedDataType(*prevVariable, LogicalTypeID::NODE);
        queryNode = static_pointer_cast<NodeExpression>(prevVariable);
        // E.g. MATCH (a:person) MATCH (a:organisation)
        // We bind to a single node with both labels
        if (!nodePattern.getTableNames().empty()) {
            auto otherTableIDs = bindTableIDs(nodePattern.getTableNames(), true);
            auto otherNodeTableIDs = getNodeTableIDs(otherTableIDs);
            queryNode->addTableIDs(otherNodeTableIDs);
        }
    } else {
        queryNode = createQueryNode(nodePattern);
        if (!parsedName.empty()) {
            scope->addExpression(parsedName, queryNode);
        }
    }
    for (auto& [propertyName, rhs] : nodePattern.getPropertyKeyVals()) {
        auto boundLhs = expressionBinder.bindNodeOrRelPropertyExpression(*queryNode, propertyName);
        auto boundRhs = expressionBinder.bindExpression(*rhs);
        boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
        collection.addKeyVal(queryNode, propertyName, std::make_pair(boundLhs, boundRhs));
    }
    queryGraph.addQueryNode(queryNode);
    return queryNode;
}

std::shared_ptr<NodeExpression> Binder::createQueryNode(const NodePattern& nodePattern) {
    auto parsedName = nodePattern.getVariableName();
    return createQueryNode(parsedName, bindTableIDs(nodePattern.getTableNames(), true));
}

std::shared_ptr<NodeExpression> Binder::createQueryNode(
    const std::string& parsedName, const std::vector<table_id_t>& tableIDs) {
    auto nodeTableIDs = getNodeTableIDs(tableIDs);
    auto queryNode = make_shared<NodeExpression>(LogicalType(LogicalTypeID::NODE),
        getUniqueExpressionName(parsedName), parsedName, nodeTableIDs);
    queryNode->setAlias(parsedName);
    std::vector<std::string> fieldNames;
    std::vector<std::unique_ptr<LogicalType>> fieldTypes;
    // Bind internal expressions
    queryNode->setInternalID(expressionBinder.createInternalNodeIDExpression(*queryNode));
    queryNode->setLabelExpression(expressionBinder.bindLabelFunction(*queryNode));
    fieldNames.emplace_back(InternalKeyword::ID);
    fieldNames.emplace_back(InternalKeyword::LABEL);
    fieldTypes.push_back(queryNode->getInternalID()->getDataType().copy());
    fieldTypes.push_back(queryNode->getLabelExpression()->getDataType().copy());
    // Bind properties.
    bindQueryNodeProperties(*queryNode);
    for (auto& expression : queryNode->getPropertyExpressions()) {
        auto property = reinterpret_cast<PropertyExpression*>(expression.get());
        fieldNames.emplace_back(property->getPropertyName());
        fieldTypes.emplace_back(property->dataType.copy());
    }
    auto extraInfo = std::make_unique<StructTypeInfo>(fieldNames, fieldTypes);
    NodeType::setExtraTypeInfo(queryNode->getDataTypeReference(), std::move(extraInfo));
    return queryNode;
}

}
}
