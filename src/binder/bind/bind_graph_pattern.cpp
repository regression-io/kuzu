#include "binder/binder.h"
#include "common/exception/binder.h"
#include "main/client_context.h"

using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

// A graph pattern contains node/rel and a set of key-value pairs associated with the variable. We
// bind node/rel as query graph and key-value pairs as a separate collection. This collection is
// interpreted in two different ways.
//    - In MATCH clause, these are additional predicates to WHERE clause
//    - In UPDATE clause, there are properties to set.
// We do not store key-value pairs in query graph primarily because we will merge key-value
// std::pairs with other predicates specified in WHERE clause.
std::pair<std::unique_ptr<QueryGraphCollection>, std::unique_ptr<PropertyKeyValCollection>>
Binder::bindGraphPattern(const std::vector<std::unique_ptr<PatternElement>>& graphPattern) {
    auto propertyCollection = std::make_unique<PropertyKeyValCollection>();
    auto queryGraphCollection = std::make_unique<QueryGraphCollection>();
    for (auto& patternElement : graphPattern) {
        queryGraphCollection->addAndMergeQueryGraphIfConnected(
            bindPatternElement(*patternElement, *propertyCollection));
    }
    return std::make_pair(std::move(queryGraphCollection), std::move(propertyCollection));
}

// Grammar ensures pattern element is always connected and thus can be bound as a query graph.
std::unique_ptr<QueryGraph> Binder::bindPatternElement(
    const PatternElement& patternElement, PropertyKeyValCollection& collection) {
    auto queryGraph = std::make_unique<QueryGraph>();
    expression_vector nodeAndRels;
    auto leftNode = bindQueryNode(*patternElement.getFirstNodePattern(), *queryGraph, collection);
    nodeAndRels.push_back(leftNode);
    for (auto i = 0u; i < patternElement.getNumPatternElementChains(); ++i) {
        auto patternElementChain = patternElement.getPatternElementChain(i);
        auto rightNode =
            bindQueryNode(*patternElementChain->getNodePattern(), *queryGraph, collection);
        auto rel = bindQueryRel(
            *patternElementChain->getRelPattern(), leftNode, rightNode, *queryGraph, collection);
        nodeAndRels.push_back(rel);
        nodeAndRels.push_back(rightNode);
        leftNode = rightNode;
    }
    if (patternElement.hasPathName()) {
        auto pathName = patternElement.getPathName();
        auto pathExpression = createPath(pathName, nodeAndRels);
        scope->addExpression(pathName, pathExpression);
    }
    return queryGraph;
}



} // namespace binder
} // namespace kuzu
