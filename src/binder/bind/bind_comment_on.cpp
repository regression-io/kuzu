#include "binder/binder.h"
#include "binder/bound_comment_on.h"
#include "main/client_context.h"
#include "parser/comment_on.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCommentOn(const parser::Statement& statement) {
    auto& commentOnStatement =
        common::ku_dynamic_cast<const parser::Statement&, const parser::CommentOn&>(statement);
    auto tableName = commentOnStatement.getTable();
    auto comment = commentOnStatement.getComment();
    validateTableExist(tableName);
    auto tableID = catalog.getTableID(clientContext->getTx(), tableName);
    return std::make_unique<BoundCommentOn>(tableID, tableName, comment);
}

} // namespace binder
} // namespace kuzu
