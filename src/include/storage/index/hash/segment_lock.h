#pragma once

#include <array>

#include "storage/index/hash/bucket_lock.h"
#include "storage/index/hash/segment.h"

namespace kuzu {
namespace storage {

struct SegmentLock {
    std::array<BucketLock, Segment::BUCKETS_PER_SEGMENT> bucket_locks;
    std::array<BucketLock, Segment::OVERFLOW_BUCKETS_PER_SEGMENT> overflow_bucket_locks;
};

} // namespace storage
} // namespace kuzu
