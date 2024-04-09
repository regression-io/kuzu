#pragma once

#include <functional>

#include "common/types/types.h"

namespace kuzu {
namespace transaction {
enum class TransactionType : uint8_t;
} // namespace transaction

namespace storage {

struct DBFileID;
class BMFileHandle;
class BufferManager;
class WAL;
class DBFileUtils {
public:
    constexpr static common::page_idx_t NULL_PAGE_IDX = common::INVALID_PAGE_IDX;

public:
    static std::pair<BMFileHandle*, common::page_idx_t> getFileHandleAndPhysicalPageIdxToPin(
        BMFileHandle& fileHandle, common::page_idx_t physicalPageIdx, WAL& wal,
        transaction::TransactionType trxType);

    static void readWALVersionOfPage(BMFileHandle& fileHandle, common::page_idx_t originalPageIdx,
        BufferManager& bufferManager, WAL& wal, const std::function<void(uint8_t*)>& readOp);

    static common::page_idx_t insertNewPage(
        BMFileHandle& fileHandle, DBFileID dbFileID, BufferManager& bufferManager, WAL& wal,
        const std::function<void(uint8_t*)>& insertOp = [](uint8_t*) -> void {
            // DO NOTHING.
        });

    // Note: This function updates a page "transactionally", i.e., creates the WAL version of the
    // page if it doesn't exist. For the original page to be updated, the current WRITE trx needs to
    // commit and checkpoint.
    static void updatePage(BMFileHandle& fileHandle, DBFileID dbFileID,
        common::page_idx_t originalPageIdx, bool isInsertingNewPage, BufferManager& bufferManager,
        WAL& wal, const std::function<void(uint8_t*)>& updateOp);
};
} // namespace storage
} // namespace kuzu
