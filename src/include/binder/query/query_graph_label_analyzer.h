#include "query_graph.h"

#include "catalog/catalog.h"

namespace kuzu {
namespace binder {

class QueryGraphLabelAnalyzer {
public:
    QueryGraphLabelAnalyzer(const catalog::Catalog& catalog) : catalog{catalog} {}

    void prune(const QueryGraph& graph);

private:
    void pruneNode(const QueryGraph& graph, const NodeExpression& node);
    void pruneRels();

private:
    const catalog::Catalog& catalog;
};

}
}
