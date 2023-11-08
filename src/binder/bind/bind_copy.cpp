#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "catalog/node_table_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/enums/table_type.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "parser/copy.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

static constexpr uint64_t NUM_COLUMNS_TO_SKIP_IN_REL_FILE = 2;

std::unique_ptr<BoundStatement> Binder::bindCopyToClause(const Statement& statement) {
    auto& copyToStatement = (CopyTo&)statement;
    auto boundFilePath = copyToStatement.getFilePath();
    auto fileType = bindFileType(boundFilePath);
    std::vector<std::string> columnNames;
    std::vector<std::unique_ptr<LogicalType>> columnTypes;
    auto query = bindQuery(*copyToStatement.getRegularQuery());
    auto columns = query->getStatementResult()->getColumns();
    for (auto& column : columns) {
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        columnNames.push_back(columnName);
        columnTypes.push_back(column->getDataType().copy());
    }
    if (fileType != FileType::CSV && fileType != FileType::PARQUET) {
        throw BinderException(ExceptionMessage::validateCopyToCSVParquetExtensionsException());
    }
    auto readerConfig = std::make_unique<ReaderConfig>(
        fileType, std::vector<std::string>{boundFilePath}, columnNames, std::move(columnTypes));
    return std::make_unique<BoundCopyTo>(std::move(readerConfig), std::move(query));
}

// As a temporary constraint, we require npy files loaded with COPY FROM BY COLUMN keyword.
// And csv and parquet files loaded with COPY FROM keyword.
static void validateByColumnKeyword(FileType fileType, bool byColumn) {
    if (fileType == FileType::NPY && !byColumn) {
        throw BinderException(ExceptionMessage::validateCopyNPYByColumnException());
    }
    if (fileType != FileType::NPY && byColumn) {
        throw BinderException(ExceptionMessage::validateCopyCSVParquetByColumnException());
    }
}

static void validateCopyNpyNotForRelTables(TableSchema* schema) {
    if (schema->tableType == TableType::REL) {
        throw BinderException(
            ExceptionMessage::validateCopyNpyNotForRelTablesException(schema->tableName));
    }
}

static bool bindContainsSerial(TableSchema* tableSchema) {
    bool containsSerial = false;
    for (auto& property : tableSchema->properties) {
        if (property->getDataType()->getLogicalTypeID() == LogicalTypeID::SERIAL) {
            containsSerial = true;
            break;
        }
    }
    return containsSerial;
}

std::unique_ptr<BoundStatement> Binder::bindCopyFromClause(const Statement& statement) {
    auto& copyStatement = (CopyFrom&)statement;
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableName = copyStatement.getTableName();
    validateTableExist(tableName);
    // Bind to table schema.
    auto tableID = catalogContent->getTableID(tableName);
    auto tableSchema = catalogContent->getTableSchema(tableID);
    switch (tableSchema->tableType) {
    case TableType::REL_GROUP:
    case TableType::RDF: {
        throw BinderException(stringFormat("Cannot copy into {} table with type {}.", tableName,
            TableTypeUtils::toString(tableSchema->tableType)));
    }
    default:
        break;
    }
    auto csvReaderConfig = bindParsingOptions(copyStatement.getParsingOptionsRef());
    auto filePaths = bindFilePaths(copyStatement.getFilePaths());
    auto fileType = bindFileType(filePaths);
    auto readerConfig =
        std::make_unique<ReaderConfig>(fileType, std::move(filePaths), std::move(csvReaderConfig));
    validateByColumnKeyword(readerConfig->fileType, copyStatement.byColumn());
    if (readerConfig->fileType == FileType::NPY) {
        validateCopyNpyNotForRelTables(tableSchema);
    }
    switch (tableSchema->tableType) {
    case TableType::NODE:
        if (readerConfig->fileType == FileType::TURTLE) {
            return bindCopyRdfNodeFrom(std::move(readerConfig), tableSchema);
        } else {
            return bindCopyNodeFrom(std::move(readerConfig), tableSchema);
        }
    case TableType::REL: {
        if (readerConfig->fileType == FileType::TURTLE) {
            return bindCopyRdfRelFrom(std::move(readerConfig), tableSchema);
        } else {
            return bindCopyRelFrom(std::move(readerConfig), tableSchema);
        }
    }
        // LCOV_EXCL_START
    default: {
        KU_UNREACHABLE;
    }
        // LCOV_EXCL_STOP
    }
}

std::unique_ptr<BoundStatement> Binder::bindCopyNodeFrom(
    std::unique_ptr<ReaderConfig> readerConfig, TableSchema* tableSchema) {
    // For table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = bindContainsSerial(tableSchema);
    auto columnsToRead = bindColumnsToReadForNode(tableSchema, *readerConfig);
    auto nodeID = createVariable(std::string(Property::INTERNAL_ID_NAME), LogicalTypeID::INT64);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), columnsToRead, std::move(nodeID), TableType::NODE);
    // TODO(Guodong): We might need to separate columns to read and columns to copy.
    // SERIAL columns should not be read from Reader, but should still be fed to CopyNode.
    auto boundCopyFromInfo =
        std::make_unique<BoundCopyFromInfo>(tableSchema, std::move(boundFileScanInfo),
            containsSerial, std::move(columnsToRead), nullptr /* extraInfo */);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRelFrom(
    std::unique_ptr<ReaderConfig> readerConfig, TableSchema* tableSchema) {
    // For table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = bindContainsSerial(tableSchema);
    KU_ASSERT(containsSerial == false);
    auto columnsToRead = bindColumnsToReadForRel(tableSchema, *readerConfig);
    auto relID = createVariable(std::string(Property::INTERNAL_ID_NAME), LogicalTypeID::INT64);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), columnsToRead, relID->copy(), TableType::REL);
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    auto srcTableSchema =
        catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getSrcTableID());
    auto dstTableSchema =
        catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getDstTableID());
    auto srcKey = columnsToRead[0];
    auto dstKey = columnsToRead[1];
    auto srcNodeID =
        createVariable(std::string(Property::REL_BOUND_OFFSET_NAME), LogicalTypeID::INT64);
    auto dstNodeID =
        createVariable(std::string(Property::REL_NBR_OFFSET_NAME), LogicalTypeID::INT64);
    auto extraCopyRelInfo = std::make_unique<ExtraBoundCopyRelInfo>(
        srcTableSchema, dstTableSchema, srcNodeID, dstNodeID, srcKey, dstKey);
    // Skip the first two columns.
    // TODO(Guodong): columnsToCopy should be populated from tableSchema instead of columnsToRead.
    expression_vector columnsToCopy{std::move(srcNodeID), std::move(dstNodeID), std::move(relID)};
    for (auto i = NUM_COLUMNS_TO_SKIP_IN_REL_FILE; i < columnsToRead.size(); i++) {
        columnsToCopy.push_back(std::move(columnsToRead[i]));
    }
    auto boundCopyFromInfo =
        std::make_unique<BoundCopyFromInfo>(tableSchema, std::move(boundFileScanInfo),
            containsSerial, std::move(columnsToCopy), std::move(extraCopyRelInfo));
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

static bool skipPropertyInFile(const Property& property) {
    return property.getDataType()->getLogicalTypeID() == LogicalTypeID::SERIAL ||
           TableSchema::isReservedPropertyName(property.getName());
}

expression_vector Binder::bindColumnsToReadForNode(
    catalog::TableSchema* tableSchema, common::ReaderConfig& readerConfig) {
    // Resolve expected columns.
    KU_ASSERT(readerConfig.fileType == FileType::CSV ||
              readerConfig.fileType == FileType::PARQUET || readerConfig.fileType == FileType::NPY);
    std::vector<std::string> expectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> expectedColumnTypes;
    for (auto& property : tableSchema->properties) {
        if (skipPropertyInFile(*property)) {
            continue;
        }
        expectedColumnNames.push_back(property->getName());
        expectedColumnTypes.push_back(property->getDataType()->copy());
    }
    // Detect columns from file.
    std::vector<std::string> detectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> detectedColumnTypes;
    sniffFiles(readerConfig, detectedColumnNames, detectedColumnTypes);
    // Validate.
    validateNumColumns(expectedColumnTypes.size(), detectedColumnTypes.size());
    if (readerConfig.fileType == common::FileType::PARQUET) {
        // HACK(Ziyi): We should allow casting in Parquet reader.
        validateColumnTypes(expectedColumnNames, expectedColumnTypes, detectedColumnTypes);
    }
    return createColumnExpressions(readerConfig, expectedColumnNames, expectedColumnTypes);
}

expression_vector Binder::bindColumnsToReadForRel(
    catalog::TableSchema* tableSchema, common::ReaderConfig& readerConfig) {
    KU_ASSERT(readerConfig.fileType == FileType::CSV ||
              readerConfig.fileType == FileType::PARQUET || readerConfig.fileType == FileType::NPY);
    auto relTableSchema = ku_dynamic_cast<TableSchema*, RelTableSchema*>(tableSchema);
    auto srcColumnName = std::string(Property::REL_FROM_PROPERTY_NAME);
    auto dstColumnName = std::string(Property::REL_TO_PROPERTY_NAME);
    std::vector<std::string> expectedColumnNames;
    std::vector<std::unique_ptr<LogicalType>> expectedColumnTypes;
    expectedColumnNames.push_back(srcColumnName);
    expectedColumnNames.push_back(dstColumnName);
    auto srcTable = ku_dynamic_cast<TableSchema*, NodeTableSchema*>(
        catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getSrcTableID()));
    auto dstTable = ku_dynamic_cast<TableSchema*, NodeTableSchema*>(
        catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getDstTableID()));
    KU_ASSERT(srcTable->tableType == TableType::NODE && dstTable->tableType == TableType::NODE);
    auto srcPKColumnType = srcTable->getPrimaryKey()->getDataType()->copy();
    auto dstPKColumnType = dstTable->getPrimaryKey()->getDataType()->copy();
    expectedColumnTypes.push_back(std::move(srcPKColumnType));
    expectedColumnTypes.push_back(std::move(dstPKColumnType));
    for (auto& property : tableSchema->properties) {
        if (skipPropertyInFile(*property)) {
            continue;
        }
        expectedColumnNames.push_back(property->getName());
        expectedColumnTypes.push_back(property->getDataType()->copy());
    }
    // Detect columns from file.
    std::vector<std::string> detectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> detectedColumnTypes;
    sniffFiles(readerConfig, detectedColumnNames, detectedColumnTypes);
    // Validate number of columns.
    validateNumColumns(readerConfig.getNumColumns(), detectedColumnTypes.size());
    if (readerConfig.fileType == common::FileType::PARQUET) {
        validateColumnTypes(
            readerConfig.columnNames, readerConfig.columnTypes, detectedColumnTypes);
    }
    return createColumnExpressions(readerConfig, expectedColumnNames, expectedColumnTypes);
}

} // namespace binder
} // namespace kuzu
