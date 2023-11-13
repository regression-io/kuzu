#include "storage/store/rel_table_data.h"

#include "common/assert.h"
#include "common/exception/not_implemented.h"
#include "storage/stats/rels_store_statistics.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

RelDataReadState::RelDataReadState(ColumnDataFormat dataFormat)
    : dataFormat{dataFormat}, startNodeOffsetInState{0}, numNodesInState{0},
      currentCSRNodeOffset{0}, posInCurrentCSR{0} {
    csrListEntries.resize(StorageConstants::NODE_GROUP_SIZE, {0, 0});
    csrOffsetChunk = ColumnChunkFactory::createColumnChunk(
        LogicalType{LogicalTypeID::INT64}, false /* enableCompression */);
}

void RelDataReadState::populateCSRListEntries() {
    auto csrOffsets = (offset_t*)csrOffsetChunk->getData();
    csrListEntries[0].offset = 0;
    csrListEntries[0].size = csrOffsets[0];
    for (auto i = 1; i < numNodesInState; i++) {
        csrListEntries[i].offset = csrOffsets[i - 1];
        csrListEntries[i].size = csrOffsets[i] - csrOffsets[i - 1];
    }
}

std::pair<offset_t, offset_t> RelDataReadState::getStartAndEndOffset() {
    auto currCSRListEntry = csrListEntries[currentCSRNodeOffset - startNodeOffsetInState];
    auto currCSRSize = currCSRListEntry.size;
    auto startOffset = currCSRListEntry.offset + posInCurrentCSR;
    auto numRowsToRead = std::min(currCSRSize - posInCurrentCSR, DEFAULT_VECTOR_CAPACITY);
    posInCurrentCSR += numRowsToRead;
    return {startOffset, startOffset + numRowsToRead};
}

RelTableData::RelTableData(BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, RelTableSchema* tableSchema,
    RelsStoreStats* relsStoreStats, RelDataDirection direction, bool enableCompression)
    : TableData{dataFH, metadataFH, tableSchema->tableID, bufferManager, wal, enableCompression,
          getDataFormatFromSchema(tableSchema, direction)},
      direction{direction}, csrOffsetColumn{nullptr} {
    if (dataFormat == ColumnDataFormat::CSR) {
        auto csrOffsetMetadataDAHInfo = relsStoreStats->getCSROffsetMetadataDAHInfo(
            Transaction::getDummyWriteTrx().get(), tableID, direction);
        // No NULL values is allowed for the csr offset column.
        csrOffsetColumn =
            std::make_unique<Column>(LogicalType{LogicalTypeID::INT64}, *csrOffsetMetadataDAHInfo,
                dataFH, metadataFH, bufferManager, wal, Transaction::getDummyReadOnlyTrx().get(),
                RWPropertyStats(relsStoreStats, tableID, INVALID_PROPERTY_ID), enableCompression,
                false /* requireNUllColumn */);
    }
    auto adjMetadataDAHInfo = relsStoreStats->getAdjMetadataDAHInfo(
        Transaction::getDummyWriteTrx().get(), tableID, direction);
    adjColumn =
        ColumnFactory::createColumn(LogicalType{LogicalTypeID::INTERNAL_ID}, *adjMetadataDAHInfo,
            dataFH, metadataFH, bufferManager, wal, Transaction::getDummyReadOnlyTrx().get(),
            RWPropertyStats(relsStoreStats, tableID, INVALID_PROPERTY_ID), enableCompression);
    auto properties = tableSchema->getProperties();
    columns.reserve(properties.size());
    for (auto i = 0u; i < properties.size(); i++) {
        auto property = tableSchema->getProperties()[i];
        auto metadataDAHInfo = relsStoreStats->getPropertyMetadataDAHInfo(
            Transaction::getDummyWriteTrx().get(), tableID, i, direction);
        columns.push_back(
            ColumnFactory::createColumn(*properties[i]->getDataType(), *metadataDAHInfo, dataFH,
                metadataFH, bufferManager, wal, Transaction::getDummyReadOnlyTrx().get(),
                RWPropertyStats(relsStoreStats, tableID, property->getPropertyID()),
                enableCompression));
    }
    // Set common tableID for adjColumn and relIDColumn.
    dynamic_cast<InternalIDColumn*>(adjColumn.get())
        ->setCommonTableID(tableSchema->getNbrTableID(direction));
    dynamic_cast<InternalIDColumn*>(columns[REL_ID_COLUMN_ID].get())->setCommonTableID(tableID);
}

void RelTableData::initializeReadState(Transaction* /*transaction*/,
    std::vector<column_id_t> columnIDs, ValueVector* inNodeIDVector, TableReadState* readState) {
    auto relReadState = ku_dynamic_cast<TableReadState*, RelDataReadState*>(readState);
    relReadState->direction = direction;
    relReadState->columnIDs = std::move(columnIDs);
    if (dataFormat == ColumnDataFormat::REGULAR) {
        return;
    }
    auto nodeOffset =
        inNodeIDVector->readNodeOffset(inNodeIDVector->state->selVector->selectedPositions[0]);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto startNodeOffset = StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    relReadState->posInCurrentCSR = 0;
    if (relReadState->isOutOfRange(nodeOffset)) {
        // Scan csr offsets and populate csr list entries for the new node group.
        relReadState->startNodeOffsetInState = startNodeOffset;
        csrOffsetColumn->scan(nodeGroupIdx, relReadState->csrOffsetChunk.get());
        relReadState->numNodesInState = relReadState->csrOffsetChunk->getNumValues();
        relReadState->populateCSRListEntries();
    }
    if (nodeOffset != relReadState->currentCSRNodeOffset) {
        relReadState->currentCSRNodeOffset = nodeOffset;
    }
}

void RelTableData::scanRegularColumns(Transaction* transaction, RelDataReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    // TODO: If write transaction, apply local changes.
    adjColumn->scan(transaction, inNodeIDVector, outputVectors[0]);
    if (!ValueVector::discardNull(*outputVectors[0])) {
        return;
    }
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        auto columnID = readState.columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        columns[readState.columnIDs[i]]->scan(
            transaction, inNodeIDVector, outputVectors[outputVectorId]);
    }
}

void RelTableData::scanCSRColumns(Transaction* transaction, RelDataReadState& readState,
    ValueVector* /*inNodeIDVector*/, const std::vector<ValueVector*>& outputVectors) {
    // TODO: If write transaction, apply local changes.
    KU_ASSERT(dataFormat == ColumnDataFormat::CSR);
    auto [startOffset, endOffset] = readState.getStartAndEndOffset();
    auto numRowsToRead = endOffset - startOffset;
    outputVectors[0]->state->selVector->resetSelectorToUnselectedWithSize(numRowsToRead);
    outputVectors[0]->state->setOriginalSize(numRowsToRead);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(readState.currentCSRNodeOffset);
    adjColumn->scan(transaction, nodeGroupIdx, startOffset, endOffset, outputVectors[0],
        0 /* offsetInVector */);
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        auto columnID = readState.columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        columns[readState.columnIDs[i]]->scan(transaction, nodeGroupIdx, startOffset, endOffset,
            outputVectors[outputVectorId], 0 /* offsetInVector */);
    }
}

void RelTableData::lookup(Transaction* transaction, TableReadState& readState,
    ValueVector* inNodeIDVector, const std::vector<ValueVector*>& outputVectors) {
    // TODO: If write transaction, apply local changes.
    // Note: The scan operator should guarantee that the first property in the output is adj column.
    adjColumn->lookup(transaction, inNodeIDVector, outputVectors[0]);
    if (!ValueVector::discardNull(*outputVectors[0])) {
        return;
    }
    for (auto i = 0u; i < readState.columnIDs.size(); i++) {
        auto columnID = readState.columnIDs[i];
        auto outputVectorId = i + 1; // Skip output from adj column.
        if (columnID == INVALID_COLUMN_ID) {
            outputVectors[outputVectorId]->setAllNull();
            continue;
        }
        columns[readState.columnIDs[i]]->lookup(
            transaction, inNodeIDVector, outputVectors[outputVectorId]);
    }
}

void RelTableData::insert(transaction::Transaction* transaction, ValueVector* srcNodeIDVector,
    ValueVector* dstNodeIDVector, const std::vector<ValueVector*>& propertyVectors) {
    auto localTableData = transaction->getLocalStorage()->getOrCreateLocalRelTableData(
        tableID, direction, dataFormat, columns);

    localTableData->insert(direction == RelDataDirection::FWD ? srcNodeIDVector : dstNodeIDVector,
        direction == RelDataDirection::FWD ? dstNodeIDVector : srcNodeIDVector, propertyVectors);
}

void RelTableData::update(transaction::Transaction* transaction, column_id_t columnID,
    ValueVector* srcNodeIDVector, ValueVector* dstNodeIDVector, ValueVector* relIDVector,
    ValueVector* propertyVector) {
    auto localTableData = transaction->getLocalStorage()->getOrCreateLocalRelTableData(
        tableID, direction, dataFormat, columns);
    localTableData->update(direction == RelDataDirection::FWD ? srcNodeIDVector : dstNodeIDVector,
        direction == RelDataDirection::FWD ? dstNodeIDVector : srcNodeIDVector, relIDVector,
        columnID, propertyVector);
}

void RelTableData::delete_(transaction::Transaction* transaction, ValueVector* srcNodeIDVector,
    ValueVector* dstNodeIDVector, ValueVector* relIDVector) {
    auto localTableData = transaction->getLocalStorage()->getOrCreateLocalRelTableData(
        tableID, direction, dataFormat, columns);
    localTableData->delete_(direction == RelDataDirection::FWD ? srcNodeIDVector : dstNodeIDVector,
        direction == RelDataDirection::FWD ? dstNodeIDVector : srcNodeIDVector, relIDVector);
}

void RelTableData::append(NodeGroup* nodeGroup) {
    if (dataFormat == ColumnDataFormat::CSR) {
        auto csrNodeGroup = static_cast<CSRNodeGroup*>(nodeGroup);
        csrOffsetColumn->append(csrNodeGroup->getCSROffsetChunk(), nodeGroup->getNodeGroupIdx());
    }
    adjColumn->append(nodeGroup->getColumnChunk(0), nodeGroup->getNodeGroupIdx());
    for (auto columnID = 0; columnID < columns.size(); columnID++) {
        columns[columnID]->append(
            nodeGroup->getColumnChunk(columnID + 1), nodeGroup->getNodeGroupIdx());
    }
}

void RelTableData::prepareCommit(LocalTable* localTable) {
    auto localRelTable = ku_dynamic_cast<LocalTable*, LocalRelTable*>(localTable);
    KU_ASSERT(localRelTable);
    auto localRelTableData = localRelTable->getRelTableData(direction);
    if (dataFormat == ColumnDataFormat::REGULAR) {
        prepareCommitRegularColumns(localRelTableData);
    } else {
        prepareCommitCSRColumns(localRelTableData);
    }
}

void RelTableData::prepareCommitRegularColumns(LocalRelTableData* localTableData) {
    for (auto& [nodeGroupIdx, nodeGroup] : localTableData->nodeGroups) {
        auto relNodeGroupInfo =
            ku_dynamic_cast<RelNGInfo*, RegularRelNGInfo*>(nodeGroup->getRelNodeGroupInfo());
        // TODO(Guodong): Should apply constratin check here. Cannot create a rel if already exists.
        adjColumn->prepareCommitForChunk(nodeGroupIdx, nodeGroup->getAdjColumn(),
            relNodeGroupInfo->adjInsertInfo, {} /* updateInfo */, relNodeGroupInfo->deleteInfo);
        for (auto columnID = 0; columnID < columns.size(); columnID++) {
            auto column = columns[columnID].get();
            auto columnChunk = nodeGroup->getLocalColumnChunk(columnID);
            columns[columnID]->prepareCommitForChunk(nodeGroupIdx, columnChunk,
                relNodeGroupInfo->insertInfoPerColumn[columnID],
                relNodeGroupInfo->updateInfoPerColumn[columnID], relNodeGroupInfo->deleteInfo);
        }
    }
}

void RelTableData::prepareCommitCSRColumns(LocalRelTableData* localTableData) {
    KU_UNREACHABLE;
    for (auto& [nodeGroupIdx, nodeGroup] : localTableData->nodeGroups) {
        auto relNodeGroupInfo =
            ku_dynamic_cast<RelNGInfo*, CSRRelNGInfo*>(nodeGroup->getRelNodeGroupInfo());
        if (relNodeGroupInfo->deleteInfo.empty() && relNodeGroupInfo->adjInsertInfo.empty()) {
            // We don't need to update the csr offset column if there is no deletion or insertion.
            // Thus, we can fall back to directly update the adj column and property columns based
            // on csr offsets.
            // 1): we need to first scan csr and relID chunks out.
            // 2): then we can figure out the actual csr offset of each value to be updated based on
            // csr and relID chunks.
        }
    }
}

void RelTableData::checkpointInMemory() {
    if (csrOffsetColumn) {
        csrOffsetColumn->checkpointInMemory();
    }
    adjColumn->checkpointInMemory();
    TableData::checkpointInMemory();
}

void RelTableData::rollbackInMemory() {
    if (csrOffsetColumn) {
        csrOffsetColumn->rollbackInMemory();
    }
    adjColumn->rollbackInMemory();
    TableData::rollbackInMemory();
}

} // namespace storage
} // namespace kuzu
