#include "binder/binder.h"
#include "common/string_format.h"
#include "catalog/node_table_schema.h"
#include "common/exception/binder.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

static std::vector<std::string> getPropertyNames(const std::vector<TableSchema*>& tableSchemas) {
    std::vector<std::string> result;
    std::unordered_set<std::string> propertyNamesSet;
    for (auto& tableSchema : tableSchemas) {
        for (auto& property : tableSchema->properties) {
            if (propertyNamesSet.contains(property->getName())) {
                continue;
            }
            propertyNamesSet.insert(property->getName());
            result.push_back(property->getName());
        }
    }
    return result;
}

void Binder::bindQueryNodeProperties(NodeExpression& node) {
    auto tableSchemas = catalog.getReadOnlyVersion()->getTableSchemas(node.getTableIDs());
    auto propertyNames = getPropertyNames(tableSchemas);
    for (auto& propertyName : propertyNames) {
        auto property = createProperty(
            propertyName, node.getUniqueName(), node.getVariableName(), tableSchemas);
        node.addPropertyExpression(propertyName, std::move(property));
    }
}

void Binder::bindQueryRelProperties(RelExpression& rel) {
    auto tableSchemas = catalog.getReadOnlyVersion()->getTableSchemas(rel.getTableIDs());
    auto propertyNames = getPropertyNames(tableSchemas);
    for (auto& propertyName : propertyNames) {
        rel.addPropertyExpression(
            propertyName, createProperty(propertyName, rel.getUniqueName(),
                              rel.getVariableName(), tableSchemas));
    }
}

std::unique_ptr<Expression> Binder::createProperty(const std::string& propertyName, const std::string& uniqueVariableName, const std::string& rawVariableName, const std::vector<catalog::TableSchema*>& tableSchemas) {
    bool isPrimaryKey = false;
    if (tableSchemas.size() == 1 && tableSchemas[0]->tableType == TableType::NODE) {
        auto nodeTableSchema = reinterpret_cast<NodeTableSchema*>(tableSchemas[0]);
        isPrimaryKey = nodeTableSchema->getPrimaryKeyPropertyID() ==
                       nodeTableSchema->getPropertyID(propertyName);
    }
    std::unordered_map<common::table_id_t, common::property_id_t> tableIDToPropertyID;
    std::vector<LogicalType*> propertyDataTypes;
    for (auto& tableSchema : tableSchemas) {
        if (!tableSchema->containProperty(propertyName)) {
            continue;
        }
        auto propertyID = tableSchema->getPropertyID(propertyName);
        propertyDataTypes.push_back(tableSchema->getProperty(propertyID)->getDataType());
        tableIDToPropertyID.insert({tableSchema->getTableID(), propertyID});
    }
    for (auto type : propertyDataTypes) {
        if (*propertyDataTypes[0] != *type) {
            throw BinderException(
                stringFormat("Expected the same data type for property {} but found {} and {}.",
                    propertyName, LogicalTypeUtils::dataTypeToString(*type),
                    LogicalTypeUtils::dataTypeToString(*propertyDataTypes[0])));
        }
    }
    return make_unique<PropertyExpression>(*propertyDataTypes[0], propertyName, uniqueVariableName,
        rawVariableName, tableIDToPropertyID, isPrimaryKey);
}

}
}
