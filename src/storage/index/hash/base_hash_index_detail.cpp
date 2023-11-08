#include "common/types/types.h"
#include "storage/index/hash/base_hash_index.h"
#include "storage/index/hash/bucket_operations.h"

namespace kuzu {
namespace storage {

static uint64_t getSegmentIdx(common::hash_t hash, uint64_t segment_count) {
    KU_ASSERT(segment_count != 0); // __builtin_clzll is undefined on 0.
    uint64_t log2 = 63 - __builtin_clzll(segment_count);
    // Get power of two just below and just above the number of segments.
    uint64_t lowPower = 1 << log2;
    uint64_t highPower = lowPower << 1;

    // This algorithm is faster than using mod, and ensures that when the segment count increases,
    // at most one segment has to split.
    uint64_t index = hash & (highPower - 1);
    if (index >= segment_count) {
        index &= (lowPower - 1);
    }
    KU_ASSERT(0 <= index && index < segment_count);
    return index;
}

// Ref: https://arxiv.org/abs/1902.01961 and
// https://lemire.me/blog/2019/02/08/faster-remainders-when-the-divisor-is-a-constant-beating-compilers-and-libdivide
static uint8_t fastBucketMod(uint8_t x) {
    constexpr static uint8_t d = Segment::BUCKETS_PER_SEGMENT;
    constexpr static uint16_t c = 65535 / d + 1;

    uint16_t lowbits = c * x;
    uint8_t result = (uint32_t(lowbits) * d) >> 16;
    KU_ASSERT(0 <= result && result < Segment::BUCKETS_PER_SEGMENT);
    return result;
}

static uint8_t getBucketIdx(common::hash_t hash) {
    // Second highest byte.
    constexpr static common::hash_t BUCKET_IDX_MASK = 0x00FF'0000'0000'0000;

    uint8_t bucket_byte = (hash & BUCKET_IDX_MASK) >> 48;
    return fastBucketMod(bucket_byte);
}

template<typename T>
static bool doneAfterSearchingBucket(
    const Bucket& bucket, const HashKey<T>& key, uint8_t& slotIndex, bool& found) {
    switch (searchBucketNoLock(bucket, key, slotIndex)) {
    case BaseHashIndex<T>::BucketLookupResult::Found: {
        found = true;
        return true;
    }
    case BaseHashIndex<T>::BucketLookupResult::NotFound: {
        found = false;
        return true;
    }
    case BaseHashIndex<T>::BucketLookupResult::CheckOverflow: {
        return false;
    }
    default:
        KU_UNREACHABLE;
    }
}

template<typename T>
template<typename F, typename G>
BaseHashIndex<T>::TryLookupResult BaseHashIndex<T>::tryLookup(
    const KeyType& key, uint8_t& slotIndex, F enterBucket, G addLock) {

    common::hash_t hash = key.getHash();
    uint64_t segmentIdx = getSegmentIdx(hash, segment_locks.size());
    Segment& segment = getSegment(segmentIdx);
    uint8_t bucketIdx = getBucketIdx(hash);

    bool found = false;
    addLock(segment_locks[segmentIdx].bucket_locks[bucketIdx]);
    const Bucket* bucket = &segment.buckets[bucketIdx];
    enterBucket(*bucket);
    if (doneAfterSearchingBucket(*bucket, key, slotIndex, found)) {
        return found;
    }

    for (int i = 0; i < Segment::OVERFLOW_BUCKETS_PER_SEGMENT; ++i) {
        addLock(segment_locks[segmentIdx].overflow_bucket_locks[i]);
        bucket = &segment.overflow_buckets[bucketIdx];
        enterBucket(*bucket);
        if (doneAfterSearchingBucket(*bucket, key, slotIndex, found)) {
            return found;
        }
    }

    uint64_t overflowBucketIdx =
        segment.overflow_buckets[Segment::OVERFLOW_BUCKETS_PER_SEGMENT - 1].overflowBucketOffset;
    while (true) {
        if (overflowBucketIdx >= overflow_bucket_locks.size()) {
            return TryLookupResult::INVALID;
        }
        addLock(overflow_bucket_locks[overflowBucketIdx]);
        bucket = &getOverflowBucket(overflowBucketIdx);
        enterBucket(*bucket);
        if (doneAfterSearchingBucket(*bucket, key, slotIndex, found)) {
            return found ? TryLookupResult::FOUND : TryLookupResult::NOT_FOUND;
        }
    }
}

template<typename T>
bool BaseHashIndex<T>::tryInsert(
    KeyType key, common::offset_t value, std::vector<BucketLock*>& locks) {

    uint8_t slotIndex;
    switch (tryLookup(
        key, slotIndex, [](Bucket& _) {},
        [&locks](BucketLock& lock) {
            lock.lockForWrite();
            locks.push_back(&lock);
        })) {
    case TryLookupResult::FOUND:
        return false;
    case TryLookupResult::NOT_FOUND:
        break;
    case TryLookupResult::INVALID:
        [[fallthrough]];
    default:
        KU_UNREACHABLE;
    }

    common::hash_t hash = key.getHash();
    uint64_t segmentIdx = getSegmentIdx(hash, segment_locks.size());
    Segment& segment = getSegment(segmentIdx);
    uint8_t bucketIdx = getBucketIdx(hash);

    KU_ASSERT(segment_locks[segmentIdx].bucket_locks[bucketIdx].isLocked());
    if (insertIntoBucket(segment.buckets[bucketIdx], key, value)) {
        return true;
    }

    for (int i = 0; i < Segment::OVERFLOW_BUCKETS_PER_SEGMENT; ++i) {
        KU_ASSERT(segment_locks[segmentIdx].overflow_bucket_locks[i].isLocked());
        if (insertIntoBucket(segment.overflow_buckets[bucketIdx], key, value)) {
            return true;
        }
    }

    Bucket* curBucket = &segment.overflow_buckets[Segment::OVERFLOW_BUCKETS_PER_SEGMENT - 1];
    while (curBucket->overflowBucketOffset != -1) {
        KU_ASSERT(curBucket->overflowBucketOffset < overflow_bucket_locks.size());
        curBucket = &getOverflowBucket(curBucket->overflowBucketOffset);
        if (insertIntoBucket(curBucket, key, value)) {
            return true;
        }
    }
}

template<typename T>
bool BaseHashIndex<T>::tryRemove(const KeyType& key, std::vector<BucketLock*>& locks) {
    std::vector<Bucket*> buckets;
    uint8_t slotIndex;
    switch (tryLookup(
        key, slotIndex, [&buckets](Bucket& bucket) { buckets.push_back(&bucket); },
        [&locks](BucketLock& lock) {
            lock.lockForWrite();
            locks.push_back(&lock);
        })) {
    case TryLookupResult::FOUND: {
        for (auto i = 0U; i < buckets.size() - 1; ++i) {
            buckets[i]->overflowCount--;
        }
        KU_ASSERT(buckets.size() > 0);
        Bucket& bucket = *buckets.back();
        handleRemoval(key, bucket.items[slotIndex].value);
        bucket.allocatedBitmap &= ~(1 << slotIndex);
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

} // namespace storage
} // namespace kuzu
