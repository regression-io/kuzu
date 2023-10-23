#include "binder/binder.h"
#include "common/exception/binder.h"
#include "catalog/rdf_graph_schema.h"
#include "catalog/rel_table_group_schema.h"

using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

std::vector<table_id_t> Binder::bindTableIDs(
    const std::vector<std::string>& tableNames, bool nodePattern) {
    auto catalogContent = catalog.getReadOnlyVersion();
    std::unordered_set<common::table_id_t> tableIDSet;
    if (tableNames.empty()) { // Rewrite empty table names as all tables.
        if (catalogContent->containsRdfGraph()) {
            // If catalog contains rdf graph then it should NOT have any property graph table.
            for (auto tableID : catalogContent->getRdfGraphIDs()) {
                tableIDSet.insert(tableID);
            }
        } else if (nodePattern) {
            if (!catalogContent->containsNodeTable()) {
                throw BinderException("No node table exists in database.");
            }
            for (auto tableID : catalogContent->getNodeTableIDs()) {
                tableIDSet.insert(tableID);
            }
        } else { // rel
            if (!catalogContent->containsRelTable()) {
                throw BinderException("No rel table exists in database.");
            }
            for (auto tableID : catalogContent->getRelTableIDs()) {
                tableIDSet.insert(tableID);
            }
        }
    } else {
        for (auto& tableName : tableNames) {
            tableIDSet.insert(bindTableID(tableName));
        }
    }
    auto result = std::vector<table_id_t>{tableIDSet.begin(), tableIDSet.end()};
    std::sort(result.begin(), result.end());
    return result;
}

std::vector<table_id_t> Binder::getNodeTableIDs(const std::vector<table_id_t>& tableIDs) {
    auto readVersion = catalog.getReadOnlyVersion();
    std::vector<table_id_t > result;
    for (auto tableID : tableIDs) {
        auto schema = readVersion->getTableSchema(tableID);
        switch (schema->tableType) {
        case TableType::RDF: { // extract node table ID from rdf graph schema.
            auto rdfGraphSchema =
                reinterpret_cast<RdfGraphSchema*>(readVersion->getTableSchema(tableID));
            result.push_back(rdfGraphSchema->getResourceTableID());
            result.push_back(rdfGraphSchema->getLiteralTableID());
        }
        case TableType::NODE: {
            result.push_back(tableID);
        }
        default:
            throw NotImplementedException("Binder::getNodeTableIDs");
        }
    }
    return result;
}

std::vector<table_id_t> Binder::getRelTableIDs(const std::vector<table_id_t>& tableIDs) {
    auto readVersion = catalog.getReadOnlyVersion();
    std::vector<table_id_t > result;
    for (auto tableID : tableIDs) {
        auto schema = readVersion->getTableSchema(tableID);
        switch (schema->tableType) {
        case TableType::RDF: { // extract rel table ID from rdf graph schema.
            auto rdfGraphSchema =
                reinterpret_cast<RdfGraphSchema*>(readVersion->getTableSchema(tableID));
            result.push_back(rdfGraphSchema->getResourceTripleTableID());
            result.push_back(rdfGraphSchema->getLiteralTripleTableID());
        }
        case TableType::REL_GROUP: { // extract rel table ID from rel group schema.
            auto relGroupSchema =
                reinterpret_cast<RelTableGroupSchema*>(readVersion->getTableSchema(tableID));
            for (auto& relTableID : relGroupSchema->getRelTableIDs()) {
                result.push_back(relTableID);
            }
        }
        case TableType::REL: {
            result.push_back(tableID);
        }
        default:
            throw NotImplementedException("Binder::getRelTableIDs");
        }
    }
    return result;
}

}
}
