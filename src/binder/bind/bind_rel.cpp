#include "binder/binder.h"
#include "common/string_format.h"
#include "function/cast/cast_utils.h"
#include "catalog/rel_table_schema.h"
#include "common/exception/binder.h"
#include "main/client_context.h"
#include "catalog/rdf_graph_schema.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

//static std::vector<table_id_t> pruneRelTableIDs(const Catalog& catalog_,
//    const std::vector<table_id_t>& relTableIDs, const NodeExpression& srcNode,
//    const NodeExpression& dstNode) {
//    auto srcNodeTableIDs = srcNode.getTableIDsSet();
//    auto dstNodeTableIDs = dstNode.getTableIDsSet();
//    std::vector<table_id_t> result;
//    for (auto& relTableID : relTableIDs) {
//        auto relTableSchema = reinterpret_cast<RelTableSchema*>(
//            catalog_.getReadOnlyVersion()->getTableSchema(relTableID));
//        if (!srcNodeTableIDs.contains(relTableSchema->getSrcTableID()) ||
//            !dstNodeTableIDs.contains(relTableSchema->getDstTableID())) {
//            continue;
//        }
//        result.push_back(relTableID);
//    }
//    return result;
//}

std::shared_ptr<RelExpression> Binder::bindQueryRel(const RelPattern& relPattern,
    const std::shared_ptr<NodeExpression>& leftNode,
    const std::shared_ptr<NodeExpression>& rightNode, QueryGraph& queryGraph,
    PropertyKeyValCollection& collection) {
    auto parsedName = relPattern.getVariableName();
    if (scope->contains(parsedName)) {
        auto prevVariable = scope->getExpression(parsedName);
        auto expectedDataType = QueryRelTypeUtils::isRecursive(relPattern.getRelType()) ?
                                    LogicalTypeID::RECURSIVE_REL :
                                    LogicalTypeID::REL;
        ExpressionBinder::validateExpectedDataType(*prevVariable, expectedDataType);
        throw BinderException("Bind relationship " + parsedName +
                              " to relationship with same name is not supported.");
    }
    auto tableIDs = bindTableIDs(relPattern.getTableNames(), false);
    // bind src & dst node
    RelDirectionType directionType;
    std::shared_ptr<NodeExpression> srcNode;
    std::shared_ptr<NodeExpression> dstNode;
    switch (relPattern.getDirection()) {
    case ArrowDirection::LEFT: {
        srcNode = rightNode;
        dstNode = leftNode;
        directionType = RelDirectionType::SINGLE;
    } break;
    case ArrowDirection::RIGHT: {
        srcNode = leftNode;
        dstNode = rightNode;
        directionType = RelDirectionType::SINGLE;
    } break;
    case ArrowDirection::BOTH: {
        // For both direction, left and right will be written with the same label set. So either one
        // being src will be correct.
        srcNode = leftNode;
        dstNode = rightNode;
        directionType = RelDirectionType::BOTH;
    } break;
    default:
        throw NotImplementedException("Binder::bindQueryRel");
    }
    // bind variable length
    std::shared_ptr<RelExpression> queryRel;
    if (QueryRelTypeUtils::isRecursive(relPattern.getRelType())) {
        queryRel = createRecursiveQueryRel(relPattern, tableIDs, srcNode, dstNode, directionType);
    } else {
        queryRel = createNonRecursiveQueryRel(
            relPattern.getVariableName(), tableIDs, srcNode, dstNode, directionType);
        for (auto& [propertyName, rhs] : relPattern.getPropertyKeyVals()) {
            auto boundLhs =
                expressionBinder.bindNodeOrRelPropertyExpression(*queryRel, propertyName);
            auto boundRhs = expressionBinder.bindExpression(*rhs);
            boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
            collection.addKeyVal(queryRel, propertyName, std::make_pair(boundLhs, boundRhs));
        }
    }
    queryRel->setAlias(parsedName);
    if (!parsedName.empty()) {
        scope->addExpression(parsedName, queryRel);
    }
    queryGraph.addQueryRel(queryRel);
    return queryRel;
}

std::shared_ptr<RelExpression> Binder::createNonRecursiveQueryRel(const std::string& parsedName,
    const std::vector<table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
    std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType) {
    // tableIDs can be relTableIDs or rdfGraphTableIDs.
    auto relTableIDs = getRelTableIDs(tableIDs);
    if (directionType == RelDirectionType::SINGLE && srcNode && dstNode) {
        // We perform table ID pruning as an optimization. BOTH direction type requires a more
        // advanced pruning logic because it does not have notion of src & dst by nature.
        relTableIDs = pruneRelTableIDs(catalog, relTableIDs, *srcNode, *dstNode);
    }
    if (relTableIDs.empty()) {
        throw BinderException("Nodes " + srcNode->toString() + " and " + dstNode->toString() +
                              " are not connected through rel " + parsedName + ".");
    }
    auto queryRel = make_shared<RelExpression>(LogicalType(LogicalTypeID::REL),
        getUniqueExpressionName(parsedName), parsedName, relTableIDs, std::move(srcNode),
        std::move(dstNode), directionType, QueryRelType::NON_RECURSIVE);
    queryRel->setAlias(parsedName);
    bindQueryRelProperties(*queryRel);
    std::vector<std::string> fieldNames;
    std::vector<std::unique_ptr<LogicalType>> fieldTypes;
    fieldNames.emplace_back(InternalKeyword::SRC);
    fieldNames.emplace_back(InternalKeyword::DST);
    fieldTypes.push_back(std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID));
    fieldTypes.push_back(std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID));
    // Bind internal expressions.
    queryRel->setLabelExpression(expressionBinder.bindLabelFunction(*queryRel));
    fieldNames.emplace_back(InternalKeyword::LABEL);
    fieldTypes.push_back(queryRel->getLabelExpression()->getDataType().copy());
    // Bind properties.
    for (auto& expression : queryRel->getPropertyExpressions()) {
        auto property = reinterpret_cast<PropertyExpression*>(expression.get());
        fieldNames.push_back(property->getPropertyName());
        fieldTypes.push_back(property->getDataType().copy());
    }
    auto extraInfo = std::make_unique<StructTypeInfo>(fieldNames, fieldTypes);
    RelType::setExtraTypeInfo(queryRel->getDataTypeReference(), std::move(extraInfo));
    auto readVersion = catalog.getReadOnlyVersion();
    if (readVersion->getTableSchema(tableIDs[0])->getTableType() == TableType::RDF) {
        auto predicateID =
            expressionBinder.bindNodeOrRelPropertyExpression(*queryRel, RDFKeyword::PREDICT_ID);
        auto rdfGraphSchema = reinterpret_cast<RdfGraphSchema*>(readVersion->getTableSchema(tableIDs[0]));
        std::vector<common::table_id_t> resourceTableIDs;
        resourceTableIDs.push_back(rdfGraphSchema->getResourceTableID());
        auto resourceTableSchemas = readVersion->getTableSchemas(resourceTableIDs);
        auto predicateIRI = createProperty(common::RDFKeyword::IRI,
            queryRel->getUniqueName(), queryRel->getVariableName(), resourceTableSchemas);
        auto rdfInfo =
            std::make_unique<RdfPredicateInfo>(std::move(resourceTableIDs), std::move(predicateID));
        queryRel->setRdfPredicateInfo(std::move(rdfInfo));
        queryRel->addPropertyExpression(common::RDFKeyword::IRI, std::move(predicateIRI));
    }
    return queryRel;
}

static void bindRecursiveRelProjectionList(const expression_vector& projectionList,
    std::vector<std::string>& fieldNames, std::vector<std::unique_ptr<LogicalType>>& fieldTypes) {
    for (auto& expression : projectionList) {
        if (expression->expressionType != common::PROPERTY) {
            throw BinderException(stringFormat(
                "Unsupported projection item {} on recursive rel.", expression->toString()));
        }
        auto property = reinterpret_cast<PropertyExpression*>(expression.get());
        fieldNames.push_back(property->getPropertyName());
        fieldTypes.push_back(property->getDataType().copy());
    }
}

std::shared_ptr<RelExpression> Binder::createRecursiveQueryRel(const parser::RelPattern& relPattern,
    const std::vector<table_id_t>& tableIDs, std::shared_ptr<NodeExpression> srcNode,
    std::shared_ptr<NodeExpression> dstNode, RelDirectionType directionType) {
    auto relTableIDs = getRelTableIDs(tableIDs);
    std::unordered_set<table_id_t> nodeTableIDs;
    for (auto relTableID : relTableIDs) {
        auto relTableSchema = reinterpret_cast<RelTableSchema*>(
            catalog.getReadOnlyVersion()->getTableSchema(relTableID));
        nodeTableIDs.insert(relTableSchema->getSrcTableID());
        nodeTableIDs.insert(relTableSchema->getDstTableID());
    }
    auto recursiveRelPatternInfo = relPattern.getRecursiveInfo();
    auto prevScope = saveScope();
    scope->clear();
    // Bind intermediate node.
    auto node = createQueryNode(recursiveRelPatternInfo->nodeName,
        std::vector<table_id_t>{nodeTableIDs.begin(), nodeTableIDs.end()});
    scope->addExpression(node->toString(), node);
    std::vector<std::string> nodeFieldNames;
    std::vector<std::unique_ptr<LogicalType>> nodeFieldTypes;
    nodeFieldNames.emplace_back(InternalKeyword::ID);
    nodeFieldNames.emplace_back(InternalKeyword::LABEL);
    nodeFieldTypes.push_back(node->getInternalID()->getDataType().copy());
    nodeFieldTypes.push_back(node->getLabelExpression()->getDataType().copy());
    expression_vector nodeProjectionList;
    if (!recursiveRelPatternInfo->hasProjection) {
        for (auto& expression : node->getPropertyExpressions()) {
            nodeProjectionList.push_back(expression->copy());
        }
    } else {
        for (auto& expression : recursiveRelPatternInfo->nodeProjectionList) {
            nodeProjectionList.push_back(expressionBinder.bindExpression(*expression));
        }
    }
    bindRecursiveRelProjectionList(nodeProjectionList, nodeFieldNames, nodeFieldTypes);
    auto nodeExtraInfo = std::make_unique<StructTypeInfo>(nodeFieldNames, nodeFieldTypes);
    node->getDataTypeReference().setExtraTypeInfo(std::move(nodeExtraInfo));
    auto nodeCopy = createQueryNode(recursiveRelPatternInfo->nodeName,
        std::vector<table_id_t>{nodeTableIDs.begin(), nodeTableIDs.end()});
    // Bind intermediate rel
    auto rel = createNonRecursiveQueryRel(
        recursiveRelPatternInfo->relName, tableIDs, nullptr, nullptr, directionType);
    scope->addExpression(rel->toString(), rel);
    expression_vector relProjectionList;
    if (!recursiveRelPatternInfo->hasProjection) {
        for (auto& expression : rel->getPropertyExpressions()) {
            if (((PropertyExpression*)expression.get())->isInternalID()) {
                continue;
            }
            relProjectionList.push_back(expression->copy());
        }
    } else {
        for (auto& expression : recursiveRelPatternInfo->relProjectionList) {
            relProjectionList.push_back(expressionBinder.bindExpression(*expression));
        }
    }
    std::vector<std::string> relFieldNames;
    std::vector<std::unique_ptr<LogicalType>> relFieldTypes;
    relFieldNames.emplace_back(InternalKeyword::SRC);
    relFieldNames.emplace_back(InternalKeyword::DST);
    relFieldNames.emplace_back(InternalKeyword::LABEL);
    relFieldNames.emplace_back(InternalKeyword::ID);
    relFieldTypes.push_back(std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID));
    relFieldTypes.push_back(std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID));
    relFieldTypes.push_back(rel->getLabelExpression()->getDataType().copy());
    relFieldTypes.push_back(std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID));
    bindRecursiveRelProjectionList(relProjectionList, relFieldNames, relFieldTypes);
    auto relExtraInfo = std::make_unique<StructTypeInfo>(relFieldNames, relFieldTypes);
    rel->getDataTypeReference().setExtraTypeInfo(std::move(relExtraInfo));
    // Bind predicates in recursive pattern
    scope->removeExpression(node->toString()); // We don't support running predicate on node.
    expression_vector predicates;
    for (auto& [propertyName, rhs] : relPattern.getPropertyKeyVals()) {
        auto boundLhs = expressionBinder.bindNodeOrRelPropertyExpression(*rel, propertyName);
        auto boundRhs = expressionBinder.bindExpression(*rhs);
        boundRhs = ExpressionBinder::implicitCastIfNecessary(boundRhs, boundLhs->dataType);
        predicates.push_back(
            expressionBinder.createEqualityComparisonExpression(boundLhs, boundRhs));
    }
    if (recursiveRelPatternInfo->whereExpression != nullptr) {
        predicates.push_back(
            expressionBinder.bindExpression(*recursiveRelPatternInfo->whereExpression));
    }
    // Bind rel
    restoreScope(std::move(prevScope));
    auto parsedName = relPattern.getVariableName();
    auto queryRel = make_shared<RelExpression>(
        *getRecursiveRelType(node->getDataType(), rel->getDataType()),
        getUniqueExpressionName(parsedName), parsedName, relTableIDs, std::move(srcNode),
        std::move(dstNode), directionType, relPattern.getRelType());
    auto lengthExpression = expressionBinder.createInternalLengthExpression(*queryRel);
    auto [lowerBound, upperBound] = bindVariableLengthRelBound(relPattern);
    auto recursiveInfo = std::make_unique<RecursiveInfo>(lowerBound, upperBound, std::move(node),
        std::move(nodeCopy), std::move(rel), std::move(lengthExpression), std::move(predicates),
        std::move(nodeProjectionList), std::move(relProjectionList));
    queryRel->setRecursiveInfo(std::move(recursiveInfo));
    return queryRel;
}

std::pair<uint64_t, uint64_t> Binder::bindVariableLengthRelBound(
    const kuzu::parser::RelPattern& relPattern) {
    auto recursiveInfo = relPattern.getRecursiveInfo();
    uint32_t lowerBound;
    function::simpleIntegerCast(
        recursiveInfo->lowerBound.c_str(), recursiveInfo->lowerBound.length(), lowerBound);
    auto upperBound = clientContext->varLengthExtendMaxDepth;
    if (!recursiveInfo->upperBound.empty()) {
        function::simpleIntegerCast(
            recursiveInfo->upperBound.c_str(), recursiveInfo->upperBound.length(), upperBound);
    }
    if (lowerBound > upperBound) {
        throw BinderException(
            "Lower bound of rel " + relPattern.getVariableName() + " is greater than upperBound.");
    }
    if (upperBound > clientContext->varLengthExtendMaxDepth) {
        throw BinderException(
            "Upper bound of rel " + relPattern.getVariableName() +
            " exceeds maximum: " + std::to_string(clientContext->varLengthExtendMaxDepth) + ".");
    }
    if ((relPattern.getRelType() == QueryRelType::ALL_SHORTEST ||
            relPattern.getRelType() == QueryRelType::SHORTEST) &&
        lowerBound != 1) {
        throw BinderException("Lower bound of shortest/all_shortest path must be 1.");
    }
    return std::make_pair(lowerBound, upperBound);
}

}
}
