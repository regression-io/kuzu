#pragma once

#include "storage/index/hash/hash_key.h"

namespace kuzu {
namespace storage {
struct Bucket;

enum BucketLookupResult {
    Found,
    CheckOverflow,
    NotFound,
};

template<typename HashKeyImpl>
BucketLookupResult searchBucketNoLock(
    const Bucket& bucket, const HashKey<HashKeyImpl>& key, uint8_t& slotIndex);

template<typename HashKeyImpl>
bool insertIntoBucket(Bucket& bucket, const HashKey<HashKeyImpl>& key, common::offset_t value);

} // namespace storage
} // namespace kuzu
