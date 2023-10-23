#include "binder/query/query_graph_label_analyzer.h"

#include "catalog/rel_table_schema.h"

using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

void QueryGraphLabelAnalyzer::prune(const QueryGraph& graph) {
    for (auto i = 0u; i < graph.getNumQueryNodes(); ++i) {
        pruneNode(graph, *graph.getQueryNode(i));
    }
}

static void prune(NodeOrRelExpression& node, const common::table_id_set& tableIDSet) {
    for (auto tableID : node.getTableIDs()) {
        if (!tableIDSet.contains())
    }
}

void QueryGraphLabelAnalyzer::pruneNode(const QueryGraph& graph, const NodeExpression& node) {
    for (auto i = 0u; i < graph.getNumQueryRels(); ++i) {
        auto queryRel = graph.getQueryRel(i);
        if (queryRel->isRecursive()) {
            continue;
        }
        if (queryRel->getDirectionType() == RelDirectionType::BOTH) {
            continue;
        }
        if (*queryRel->getSrcNode() == node) {
            std::unordered_set<common::table_id_t> srcTableIDs;
            for (auto relTableID : queryRel->getTableIDs()) {
                auto relTableSchema = reinterpret_cast<RelTableSchema*>(catalog.getReadOnlyVersion()->getTableSchema(relTableID));
                srcTableIDs.insert(relTableSchema->getSrcTableID());
            }

        } else if (*queryRel->getDstNode() == node) {
            std::unordered_set<common::table_id_t> dstTableIDs;
            for (auto relTableID : queryRel->getTableIDs()) {
                auto relTableSchema = reinterpret_cast<RelTableSchema*>(catalog.getReadOnlyVersion()->getTableSchema(relTableID));
                srcTableIDs.insert(relTableSchema->getSrcTableID());
            }
        }
    }
}

}
}