#pragma once

#include <array>

#include "bucket.h"

namespace kuzu {
namespace storage {

struct Segment {
    static constexpr int BUCKETS_PER_SEGMENT = 14;
    static constexpr int OVERFLOW_BUCKETS_PER_SEGMENT = 2;

    std::array<Bucket, BUCKETS_PER_SEGMENT> buckets;
    std::array<Bucket, OVERFLOW_BUCKETS_PER_SEGMENT> overflow_buckets;
};

static_assert(sizeof(Segment) <= 4096, "Segment should be no greater than 4096 bytes in size");
static_assert(
    std::is_trivially_destructible<Segment>::value, "Segment must be trivially destructible");

} // namespace storage
} // namespace kuzu
