#pragma once

#include "parser/expression/parsed_expression.h"
#include "parser/query/regular_query.h"
#include "parser/statement.h"
#include <vector>

namespace kuzu {
namespace parser {

class CopyFrom : public Statement {
public:
    explicit CopyFrom(std::vector<std::string> filePaths, std::string tableName)
        : Statement{common::StatementType::COPY_FROM}, byColumn_{false}, filePaths{std::move(filePaths)},
          tableName{std::move(tableName)}  {}

    inline void setByColumn() { byColumn_ = true; }
    inline bool byColumn() const { return byColumn_; }

    inline std::vector<std::string> getFilePaths() const { return filePaths; }
    inline std::string getTableName() const { return tableName; }

    inline void setColumnNames(std::vector<std::string> names) {columnNames = std::move(names);}
    inline std::vector<std::string> getColumnNames() const { return columnNames; }

    inline void setParsingOption(parsing_option_t options) { parsingOptions = std::move(options); }
    inline const parsing_option_t& getParsingOptionsRef() const { return parsingOptions; }

private:
    bool byColumn_;
    std::vector<std::string> filePaths;
    std::string tableName;
    std::vector<std::string> columnNames;
    parsing_option_t parsingOptions;
};

class CopyTo : public Statement {
public:
    explicit CopyTo(std::string filePath, std::unique_ptr<RegularQuery> regularQuery)
        : Statement{common::StatementType::COPY_TO},
          regularQuery{std::move(regularQuery)}, filePath{std::move(filePath)} {}

    inline std::string getFilePath() const { return filePath; }
    inline std::unique_ptr<RegularQuery> getRegularQuery() { return std::move(regularQuery); }

private:
    std::string filePath;
    std::unique_ptr<RegularQuery> regularQuery;
};

} // namespace parser
} // namespace kuzu
