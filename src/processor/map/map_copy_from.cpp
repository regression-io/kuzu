#include "binder/copy/bound_copy_from.h"
#include "catalog/node_table_schema.h"
#include "planner/operator/persistent/logical_copy_from.h"
#include "processor/operator/call/in_query_call.h"
#include "processor/operator/partitioner.h"
#include "processor/operator/persistent/copy_node.h"
#include "processor/operator/persistent/copy_rdf_resource.h"
#include "processor/operator/persistent/copy_rel.h"
#include "processor/plan_mapper.h"
#include "binder/expression/variable_expression.h"
#include "planner/operator/logical_partitioner.h"
#include "processor/operator/partitioner.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyFrom(LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    switch (copyFrom->getInfo()->tableSchema->getTableType()) {
    case TableType::NODE:
        return mapCopyNodeFrom(logicalOperator);
    case TableType::REL:
        return mapCopyRelFrom(logicalOperator);
        // LCOV_EXCL_START
    default:
        KU_UNREACHABLE;
    }
    // LCOV_EXCL_STOP
}

static std::shared_ptr<Expression> matchColumnExpression(const expression_vector& columnExpressions, const std::string& columnName) {
    for (auto& expression : columnExpressions) {
        KU_ASSERT(expression->expressionType == ExpressionType::VARIABLE);
        auto var = reinterpret_cast<binder::VariableExpression*>(expression.get());
        if (columnName == var->getVariableName()) {
            return expression;
        }
    }
    return nullptr;
}

static std::vector<DataPos> getColumnDataPositions(TableSchema* tableSchema, const expression_vector& inputExpressions) {

}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyNodeFrom(LogicalOperator* logicalOperator) {
    auto copyFrom = reinterpret_cast<LogicalCopyFrom*>(logicalOperator);
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto tableSchema = (catalog::NodeTableSchema*)copyFromInfo->tableSchema;
    // Map reader.
    auto prevOperator = mapOperator(copyFrom->getChild(0).get());
    auto inQueryCall = reinterpret_cast<InQueryCall*>(prevOperator.get());
    // Map copy node.
    auto nodeTable = storageManager.getNodeTable(tableSchema->tableID);
    auto sharedState = std::make_shared<CopyNodeSharedState>(inQueryCall->getSharedState());
    sharedState->wal = storageManager.getWAL();
    sharedState->table = nodeTable;
    for (auto& property : tableSchema->getProperties()) {
        sharedState->columnTypes.push_back(property->getDataType()->copy());
    }
    auto properties = tableSchema->getProperties();
    auto pk = tableSchema->getPrimaryKey();
    for (auto i = 0u; i < properties.size(); ++i) {
        if (properties[i]->getPropertyID() == pk->getPropertyID()) {
            sharedState->pkColumnIdx = i;
        }
    }

    sharedState->pkType = pk->getDataType()->copy();
    sharedState->fTable = getSingleStringColumnFTable();
    std::vector<DataPos> columnPositions;
    for (auto& property : properties) {
        auto expr = matchColumnExpression(copyFromInfo->columns, property->getName());
        if (expr != nullptr) {
            columnPositions.emplace_back(outFSchema->getExpressionPos(*expr));
        } else {
            columnPositions.push_back(DataPos());
        }
    }
    auto info = std::make_unique<CopyNodeInfo>(std::move(columnPositions), nodeTable,
        tableSchema->tableName, copyFromInfo->containsSerial, storageManager.compressionEnabled());
    std::unique_ptr<PhysicalOperator> copyNode;
    auto readerConfig = reinterpret_cast<function::ScanBindData*>(
        copyFromInfo->fileScanInfo->bindData.get())->config;
    if (readerConfig.fileType == FileType::TURTLE &&
        readerConfig.rdfReaderConfig->mode == RdfReaderMode::RESOURCE) {
        copyNode = std::make_unique<CopyRdfResource>(sharedState, std::move(info),
            std::make_unique<ResultSetDescriptor>(copyFrom->getSchema()), std::move(prevOperator),
            getOperatorID(), copyFrom->getExpressionsForPrinting());
    } else {
        copyNode = std::make_unique<CopyNode>(sharedState, std::move(info),
            std::make_unique<ResultSetDescriptor>(copyFrom->getSchema()), std::move(prevOperator),
            getOperatorID(), copyFrom->getExpressionsForPrinting());
    }
    auto outputExpressions = binder::expression_vector{copyFrom->getOutputExpression()->copy()};
    return createFactorizedTableScanAligned(outputExpressions, copyFrom->getSchema(),
        sharedState->fTable, DEFAULT_VECTOR_CAPACITY /* maxMorselSize */, std::move(copyNode));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapPartitioner(LogicalOperator* logicalOperator) {
    auto logicalPartitioner = reinterpret_cast<LogicalPartitioner*>(logicalOperator);
    auto prevOperator = mapOperator(logicalPartitioner->getChild(0).get());
    auto outFSchema = logicalPartitioner->getSchema();
    std::vector<std::unique_ptr<PartitioningInfo>> infos;
    infos.reserve(logicalPartitioner->getNumInfos());
    for (auto i = 0u; i < logicalPartitioner->getNumInfos(); i++) {
        auto info = logicalPartitioner->getInfo(i);
        auto keyPos = getDataPos(*info->key, *outFSchema);
        std::vector<DataPos> columnPositions;
        for (auto& property : info->tableSchema->getProperties()) {
            auto expr = matchColumnExpression(info->payloads, property->getName());
            if (expr != nullptr) {
                columnPositions.emplace_back(outFSchema->getExpressionPos(*expr));
            } else {
                columnPositions.push_back(DataPos());
            }
        }
            infos.push_back(std::make_unique<PartitioningInfo>(
                DataPos{outFSchema->getExpressionPos(*logicalInfo->key)},
                getExpressionsDataPos(logicalInfo->payloads, *outFSchema),
                PartitionerFunctions::partitionRelData));
    }
    auto sharedState = std::make_shared<PartitionerSharedState>();
    return std::make_unique<Partitioner>(std::make_unique<ResultSetDescriptor>(outFSchema),
        std::move(infos), std::move(sharedState), std::move(prevOperator), getOperatorID(),
        logicalPartitioner->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::createCopyRel(
    std::shared_ptr<PartitionerSharedState> partitionerSharedState,
    std::shared_ptr<CopyRelSharedState> sharedState, LogicalCopyFrom* copyFrom,
    RelDataDirection direction) {
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto tableSchema = dynamic_cast<RelTableSchema*>(copyFromInfo->tableSchema);
    auto partitioningIdx = direction == RelDataDirection::FWD ? 0 : 1;
    auto maxBoundNodeOffset = storageManager.getNodesStatisticsAndDeletedIDs()->getMaxNodeOffset(
        transaction::Transaction::getDummyReadOnlyTrx().get(),
        tableSchema->getBoundTableID(direction));
    // TODO(Guodong/Xiyang): Consider moving this to Partitioner::initGlobalStateInternal.
    auto numPartitions = (maxBoundNodeOffset + StorageConstants::NODE_GROUP_SIZE) /
                         StorageConstants::NODE_GROUP_SIZE;
    partitionerSharedState->numPartitions[partitioningIdx] = numPartitions;
    auto relIDDataPos =
        DataPos{outFSchema->getExpressionPos(*copyFromInfo->fileScanInfo->internalID)};
    DataPos srcOffsetPos, dstOffsetPos;
    auto readerConfig = reinterpret_cast<function::ScanBindData*>(
        copyFromInfo->fileScanInfo->copyFuncBindData.get())
                            ->config;
    if (readerConfig.fileType == FileType::TURTLE) {
        auto extraInfo = reinterpret_cast<ExtraBoundCopyRdfRelInfo*>(copyFromInfo->extraInfo.get());
        srcOffsetPos = DataPos{outFSchema->getExpressionPos(*extraInfo->subjectOffset)};
        dstOffsetPos = DataPos{outFSchema->getExpressionPos(*extraInfo->objectOffset)};
    } else {
        auto extraInfo = reinterpret_cast<ExtraBoundCopyRelInfo*>(copyFromInfo->extraInfo.get());
        srcOffsetPos = DataPos{outFSchema->getExpressionPos(*extraInfo->srcOffset)};
        dstOffsetPos = DataPos{outFSchema->getExpressionPos(*extraInfo->dstOffset)};
    }
    auto dataColumnPositions = getExpressionsDataPos(copyFromInfo->columns, *outFSchema);
    auto dataFormat = tableSchema->isSingleMultiplicityInDirection(direction) ?
                          ColumnDataFormat::REGULAR :
                          ColumnDataFormat::CSR;
    auto copyRelInfo =
        std::make_unique<CopyRelInfo>(tableSchema, partitioningIdx, direction, dataFormat,
            dataColumnPositions, direction == RelDataDirection::FWD ? srcOffsetPos : dstOffsetPos,
            relIDDataPos, storageManager.getWAL(), storageManager.compressionEnabled());
    return std::make_unique<CopyRel>(std::move(copyRelInfo), std::move(partitionerSharedState),
        std::move(sharedState), std::make_unique<ResultSetDescriptor>(outFSchema), getOperatorID(),
        copyFrom->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyRelFrom(
    planner::LogicalOperator* logicalOperator) {
    auto copyFrom = (LogicalCopyFrom*)logicalOperator;
    auto copyFromInfo = copyFrom->getInfo();
    auto outFSchema = copyFrom->getSchema();
    auto tableSchema = reinterpret_cast<RelTableSchema*>(copyFromInfo->tableSchema);
    auto prevOperator = mapOperator(copyFrom->getChild(0).get());
    KU_ASSERT(prevOperator->getOperatorType() == PhysicalOperatorType::PARTITIONER);
    auto partitionerSharedState = dynamic_cast<Partitioner*>(prevOperator.get())->getSharedState();
    partitionerSharedState->numPartitions.resize(2);
    std::vector<std::unique_ptr<LogicalType>> columnTypes;
    // TODO(Xiyang): Move binding of column types to binder.
    columnTypes.push_back(std::make_unique<LogicalType>(LogicalTypeID::INTERNAL_ID)); // ADJ COLUMN.
    for (auto& property : tableSchema->properties) {
        columnTypes.push_back(property->getDataType()->copy());
    }
    auto copyRelSharedState = std::make_shared<CopyRelSharedState>(tableSchema->tableID,
        storageManager.getRelTable(tableSchema->tableID), std::move(columnTypes),
        storageManager.getRelsStatistics(), memoryManager);
    auto copyRelFWD =
        createCopyRel(partitionerSharedState, copyRelSharedState, copyFrom, RelDataDirection::FWD);
    auto copyRelBWD =
        createCopyRel(partitionerSharedState, copyRelSharedState, copyFrom, RelDataDirection::BWD);
    auto outputExpressions = expression_vector{copyFrom->getOutputExpression()->copy()};
    auto fTableScan = createFactorizedTableScanAligned(outputExpressions, outFSchema,
        copyRelSharedState->getFTable(), DEFAULT_VECTOR_CAPACITY /* maxMorselSize */,
        std::move(copyRelBWD));
    // Pipelines are scheduled as the order: partitioner -> copyRelFWD -> copyRelBWD.
    fTableScan->addChild(std::move(copyRelFWD));
    fTableScan->addChild(std::move(prevOperator));
    return fTableScan;
}

} // namespace processor
} // namespace kuzu
