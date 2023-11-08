#pragma once

#include "storage/index/hash/bucket_lock.h"
#include "storage/index/hash/hash_key.h"
#include "storage/index/hash/segment_lock.h"

namespace kuzu {
namespace storage {

struct Bucket;
struct Segment;

//! Base class that abstracts away the actual storage of Segments and Buckets.
// Though we could make this also use the CRTP here, it would probably provide little to no
// benefit, and would complicate the code.
template<typename HashKeyImpl>
class BaseHashIndex {
    enum TryLookupResult {
        FOUND,
        NOT_FOUND,
        INVALID,
    };

public:
    using KeyType = HashKey<HashKeyImpl>;

    BaseHashIndex(uint64_t segments, uint64_t overflow_buckets);
    virtual ~BaseHashIndex() {}

    bool lookup(const KeyType& key, common::offset_t& result);
    //! Returns whether insert succeeded. If false, key exists.
    bool insert(KeyType key, common::offset_t value);
    //! Returns whether remove succeeded. If false, key did not exist.
    bool remove(const KeyType& key);

protected:
    virtual Segment& getSegment(uint64_t segment) = 0;
    virtual Bucket& getOverflowBucket(uint64_t index) = 0;

    // Must be thread-safe.
    virtual void addOverflowBucket() = 0;
    // Called when an object is actually deleted. Must be thread safe.
    virtual void handleRemoval(const KeyType& key, common::offset_t value) = 0;

private:
    template<typename F, typename G>
    TryLookupResult tryLookup(const KeyType& key, uint8_t& slotIndex, F enterBucket, G addLock);

    bool tryInsert(KeyType key, common::offset_t value, std::vector<BucketLock*>& locks);
    bool tryRemove(const KeyType& key, std::vector<BucketLock*>& locks);

    std::vector<SegmentLock> segment_locks;
    std::vector<BucketLock> overflow_bucket_locks;
};

} // namespace storage
} // namespace kuzu
