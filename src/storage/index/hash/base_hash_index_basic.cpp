#include "storage/index/hash/base_hash_index.h"
#include "storage/index/hash/int_hash_key.h"

namespace kuzu {
namespace storage {

template class BaseHashIndex<IntHashKey>;

template<typename T>
BaseHashIndex<T>::BaseHashIndex(uint64_t segments, uint64_t overflow_buckets)
    : segment_locks(segments), overflow_bucket_locks(overflow_buckets) {}

static bool checkVersions(std::vector<std::pair<BucketLock*, uint64_t>> initialVersions) {
    for (auto [lock, version] : initialVersions) {
        if (!lock->unlockForRead(version)) {
            return false;
        }
    }
    return true;
}

template<typename T>
bool BaseHashIndex<T>::lookup(const KeyType& key, common::offset_t& result) {
    TryLookupResult lookupResult;
    std::vector<std::pair<BucketLock*, uint64_t>> initialVersions;
    uint8_t slotIndex;
    Bucket* bucket;
    do {
        initialVersions.clear();
        lookupResult = tryLookup(key, slotIndex, bucket, [&initialVersions](BucketLock& lock) {
            initialVersions.emplace_back(&lock, lock.lockForRead());
        });
    } while (!checkVersions(initialVersions));

    switch (lookupResult) {
    case TryLookupResult::FOUND: {
        result = bucket->items[slotIndex].value;
        return true;
    }
    case TryLookupResult::NOT_FOUND:
        return false;
    case TryLookupResult::INVALID:
        [[fallthrough]];
    default:
        KU_UNREACHABLE;
    }
}

template<typename T>
bool BaseHashIndex<T>::insert(KeyType key, common::offset_t value) {
    std::vector<BucketLock*> locks;
    if (tryInsert(std::move(key), value, locks)) {
        for (BucketLock* lock : locks) {
            lock->unlockBumpVersion();
        }
    } else {
        for (BucketLock* lock : locks) {
            lock->unlockSameVersion();
        }
    }
}

template<typename T>
bool BaseHashIndex<T>::remove(const KeyType& key) {
    std::vector<BucketLock*> locks;
    if (tryRemove(key, locks)) {
        for (BucketLock* lock : locks) {
            lock->unlockBumpVersion();
        }
    } else {
        for (BucketLock* lock : locks) {
            lock->unlockSameVersion();
        }
    }
}

} // namespace storage
} // namespace kuzu
