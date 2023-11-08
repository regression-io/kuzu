#pragma once

#include <array>
#include <cstdint>

#include "common/types/internal_id_t.h"

namespace kuzu {
namespace storage {

struct Item {
    uint64_t key;
    common::offset_t value;
};

struct Bucket {
    static constexpr int SLOTS_PER_BUCKET = 14;

    std::array<Item, SLOTS_PER_BUCKET> items;
    std::array<uint8_t, SLOTS_PER_BUCKET> fingerprints;

    // This would need to change if SLOTS_PER_BUCKET changes.
    uint16_t allocatedBitmap;

    uint64_t overflowCount;

    // Used only for the last overflow bucket in a segment or in
    // the overflow linked list, where it represents the next index
    // in the disk array to check.
    uint64_t overflowBucketOffset;
};

static_assert(sizeof(Bucket) <= 256, "Bucket should be no greater than 256 bytes in size");
static_assert(
    std::is_trivially_destructible<Bucket>::value, "Bucket must be trivially destructible");

} // namespace storage
} // namespace kuzu
