#pragma once

#include "function/hash/hash_functions.h"
#include "storage/index/hash/hash_key.h"

namespace kuzu {
namespace storage {

class IntHashKey : public HashKey<IntHashKey> {
private:
    IntHashKey(uint64_t key, uint64_t hash) : HashKey(key, hash) {}

public:
    static IntHashKey fromKey(uint64_t key) {
        common::hash_t result;
        function::Hash::operation(key, result);
        return IntHashKey(key, result);
    }

    bool operator==(uint64_t other_key) const { return getKey() == other_key; }
}; // namespace storage

} // namespace storage
} // namespace kuzu
