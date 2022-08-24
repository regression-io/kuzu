#include "src/storage/wal/include/wal_record.h"

namespace graphflow {
namespace storage {

StorageStructureID StorageStructureID::newStructuredNodePropertyColumnID(
    table_id_t tableID, uint32_t propertyID, bool isOverflow) {
    StorageStructureID retVal;
    retVal.storageStructureType = STRUCTURED_NODE_PROPERTY_COLUMN;
    retVal.isOverflow = isOverflow;
    retVal.structuredNodePropertyColumnID = StructuredNodePropertyColumnID(tableID, propertyID);
    return retVal;
}

StorageStructureID StorageStructureID::newNodeIndexID(table_id_t tableID) {
    StorageStructureID retVal;
    retVal.storageStructureType = NODE_INDEX;
    retVal.nodeIndexID = NodeIndexID(tableID);
    return retVal;
}

StorageStructureID StorageStructureID::newUnstructuredNodePropertyListsID(
    table_id_t tableID, ListFileType listFileType) {
    StorageStructureID retVal;
    retVal.isOverflow = false;
    retVal.storageStructureType = LISTS;
    retVal.listFileID = ListFileID(listFileType, UnstructuredNodePropertyListsID(tableID));
    return retVal;
}

StorageStructureID StorageStructureID::newAdjListsID(
    table_id_t tableID, table_id_t srcNodeTableID, RelDirection dir, ListFileType listFileType) {
    StorageStructureID retVal;
    retVal.isOverflow = false;
    retVal.storageStructureType = LISTS;
    retVal.listFileID =
        ListFileID(listFileType, AdjListsID(RelNodeTableAndDir(tableID, srcNodeTableID, dir)));
    return retVal;
}

StorageStructureID StorageStructureID::newRelPropertyListsID(table_id_t nodeTableID,
    table_id_t relTableID, RelDirection dir, uint32_t propertyID, ListFileType listFileType) {
    StorageStructureID retVal;
    retVal.isOverflow = false;
    retVal.storageStructureType = LISTS;
    retVal.listFileID = ListFileID(listFileType,
        RelPropertyListID(RelNodeTableAndDir(nodeTableID, relTableID, dir), propertyID));
    return retVal;
}

WALRecord WALRecord::newPageInsertOrUpdateRecord(StorageStructureID storageStructureID_,
    uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL, bool isInsert) {
    WALRecord retVal;
    retVal.recordType = PAGE_UPDATE_OR_INSERT_RECORD;
    retVal.pageInsertOrUpdateRecord = PageUpdateOrInsertRecord(
        storageStructureID_, pageIdxInOriginalFile, pageIdxInWAL, isInsert);
    return retVal;
}

WALRecord WALRecord::newPageUpdateRecord(
    StorageStructureID storageStructureID_, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL) {
    return WALRecord::newPageInsertOrUpdateRecord(
        storageStructureID_, pageIdxInOriginalFile, pageIdxInWAL, false /* is update */);
}

WALRecord WALRecord::newPageInsertRecord(
    StorageStructureID storageStructureID_, uint64_t pageIdxInOriginalFile, uint64_t pageIdxInWAL) {
    return WALRecord::newPageInsertOrUpdateRecord(
        storageStructureID_, pageIdxInOriginalFile, pageIdxInWAL, true /* is insert */);
}

WALRecord WALRecord::newCommitRecord(uint64_t transactionID) {
    WALRecord retVal;
    retVal.recordType = COMMIT_RECORD;
    retVal.commitRecord = CommitRecord(transactionID);
    return retVal;
}

WALRecord WALRecord::newNodeMetadataRecord() {
    WALRecord retVal;
    retVal.recordType = NODES_METADATA_RECORD;
    return retVal;
}

WALRecord WALRecord::newCatalogRecord() {
    WALRecord retVal;
    retVal.recordType = CATALOG_RECORD;
    return retVal;
}

WALRecord WALRecord::newNodeTableRecord(table_id_t tableID) {
    WALRecord retVal;
    retVal.recordType = NODE_TABLE_RECORD;
    retVal.nodeTableRecord = NodeTableRecord(tableID);
    return retVal;
}

WALRecord WALRecord::newRelTableRecord(table_id_t tableID) {
    WALRecord retVal;
    retVal.recordType = REL_TABLE_RECORD;
    retVal.relTableRecord = RelTableRecord(tableID);
    return retVal;
}

WALRecord WALRecord::newOverflowFileNextBytePosRecord(
    StorageStructureID storageStructureID_, uint64_t prevNextByteToWriteTo_) {
    WALRecord retVal;
    retVal.recordType = OVERFLOW_FILE_NEXT_BYTE_POS_RECORD;
    retVal.overflowFileNextBytePosRecord =
        OverflowFileNextBytePosRecord(storageStructureID_, prevNextByteToWriteTo_);
    return retVal;
}

WALRecord WALRecord::newCopyNodeCSVRecord(table_id_t tableID) {
    WALRecord retVal;
    retVal.recordType = COPY_NODE_CSV_RECORD;
    retVal.copyNodeCsvRecord = CopyNodeCSVRecord(tableID);
    return retVal;
}

WALRecord WALRecord::newCopyRelCSVRecord(table_id_t tableID) {
    WALRecord retVal;
    retVal.recordType = COPY_REL_CSV_RECORD;
    retVal.copyRelCsvRecord = CopyRelCSVRecord(tableID);
    return retVal;
}

void WALRecord::constructWALRecordFromBytes(WALRecord& retVal, uint8_t* bytes, uint64_t& offset) {
    ((WALRecord*)&retVal)[0] = ((WALRecord*)(bytes + offset))[0];
    offset += sizeof(WALRecord);
}

void WALRecord::writeWALRecordToBytes(uint8_t* bytes, uint64_t& offset) {
    ((WALRecord*)(bytes + offset))[0] = *this;
    offset += sizeof(WALRecord);
}

} // namespace storage
} // namespace graphflow
