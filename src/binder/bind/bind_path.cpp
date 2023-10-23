#include "binder/binder.h"
#include "binder/expression/path_expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::unique_ptr<LogicalType> Binder::getRecursiveRelType(
    const LogicalType& nodeType, const LogicalType& relType) {
    auto nodesType = std::make_unique<LogicalType>(
        LogicalTypeID::VAR_LIST, std::make_unique<VarListTypeInfo>(nodeType.copy()));
    auto relsType = std::make_unique<LogicalType>(
        LogicalTypeID::VAR_LIST, std::make_unique<VarListTypeInfo>(relType.copy()));
    std::vector<std::unique_ptr<StructField>> recursiveRelFields;
    recursiveRelFields.push_back(
        std::make_unique<StructField>(InternalKeyword::NODES, std::move(nodesType)));
    recursiveRelFields.push_back(
        std::make_unique<StructField>(InternalKeyword::RELS, std::move(relsType)));
    return std::make_unique<LogicalType>(LogicalTypeID::RECURSIVE_REL,
        std::make_unique<StructTypeInfo>(std::move(recursiveRelFields)));
}

static void extraFieldFromStructType(const LogicalType& structType,
    std::unordered_set<std::string>& nameSet, std::vector<std::string>& names,
    std::vector<std::unique_ptr<LogicalType>>& types) {
    for (auto& field : StructType::getFields(&structType)) {
        if (!nameSet.contains(field->getName())) {
            nameSet.insert(field->getName());
            names.push_back(field->getName());
            types.push_back(field->getType()->copy());
        }
    }
}

std::shared_ptr<Expression> Binder::createPath(
    const std::string& pathName, const expression_vector& children) {
    std::unordered_set<std::string> nodeFieldNameSet;
    std::vector<std::string> nodeFieldNames;
    std::vector<std::unique_ptr<LogicalType>> nodeFieldTypes;
    std::unordered_set<std::string> relFieldNameSet;
    std::vector<std::string> relFieldNames;
    std::vector<std::unique_ptr<LogicalType>> relFieldTypes;
    for (auto& child : children) {
        switch (child->getDataType().getLogicalTypeID()) {
        case LogicalTypeID::NODE: {
            auto node = reinterpret_cast<NodeExpression*>(child.get());
            extraFieldFromStructType(
                node->getDataType(), nodeFieldNameSet, nodeFieldNames, nodeFieldTypes);
        } break;
        case LogicalTypeID::REL: {
            auto rel = reinterpret_cast<RelExpression*>(child.get());
            extraFieldFromStructType(
                rel->getDataType(), relFieldNameSet, relFieldNames, relFieldTypes);
        } break;
        case LogicalTypeID::RECURSIVE_REL: {
            auto recursiveRel = reinterpret_cast<RelExpression*>(child.get());
            auto recursiveInfo = recursiveRel->getRecursiveInfo();
            extraFieldFromStructType(recursiveInfo->node->getDataType(), nodeFieldNameSet,
                nodeFieldNames, nodeFieldTypes);
            extraFieldFromStructType(
                recursiveInfo->rel->getDataType(), relFieldNameSet, relFieldNames, relFieldTypes);
        } break;
        default:
            throw NotImplementedException("Binder::createPath");
        }
    }
    auto nodeExtraInfo = std::make_unique<StructTypeInfo>(nodeFieldNames, nodeFieldTypes);
    auto nodeType = std::make_unique<LogicalType>(LogicalTypeID::NODE, std::move(nodeExtraInfo));
    auto relExtraInfo = std::make_unique<StructTypeInfo>(relFieldNames, relFieldTypes);
    auto relType = std::make_unique<LogicalType>(LogicalTypeID::REL, std::move(relExtraInfo));
    auto uniqueName = getUniqueExpressionName(pathName);
    return std::make_shared<PathExpression>(*getRecursiveRelType(*nodeType, *relType),
        uniqueName, pathName, std::move(nodeType), std::move(relType), children);
}

}
}
