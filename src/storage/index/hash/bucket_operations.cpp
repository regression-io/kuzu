#include "storage/index/hash/bucket_operations.h"

#include "storage/index/hash/bucket.h"
#include "storage/index/hash/int_hash_key.h" // IWYU pragma: keep (used for explicit instantiation)

namespace kuzu {
namespace storage {

template<typename T>
BucketLookupResult searchBucketNoLock(
    const Bucket& bucket, const HashKey<T>& key, uint8_t& slotIndex) {
    for (int i = 0; i < Bucket::SLOTS_PER_BUCKET; ++i) {
        if (bucket.allocatedBitmap & (1 << i) && bucket.fingerprints[i] == key.getFingerprint() &&
            key == bucket.items[i].key) {
            slotIndex = i;
            return BucketLookupResult::Found;
        }
    }
    if (bucket.overflowCount > 0) {
        return BucketLookupResult::CheckOverflow;
    }
    return BucketLookupResult::NotFound;
}

template BucketLookupResult searchBucketNoLock<IntHashKey>(
    const Bucket&, const HashKey<IntHashKey>&, uint8_t&);

template<typename HashKeyImpl>
bool insertIntoBucket(Bucket& bucket, const HashKey<HashKeyImpl>& key, common::offset_t value) {
    constexpr static uint16_t FULL_BITMAP = (1 << Bucket::SLOTS_PER_BUCKET) - 1;
    if (bucket.allocatedBitmap == FULL_BITMAP) {
        return false;
    }
}

template bool insertIntoBucket<IntHashKey>(
    Bucket&, const HashKey<IntHashKey>& key, common::offset_t value);

} // namespace storage
} // namespace kuzu
