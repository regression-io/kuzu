#pragma once

#include <cstdint>

#include "common/types/types.h"

namespace kuzu {
namespace storage {

// We make use of the CRTP[1] to achieve compile-time polymorphism.
// This makes the code more complex, but means that we don't pay the price of an indirect branch at
// runtime.
//   [1]: https://en.cppreference.com/w/cpp/language/crtp
template<class Derived>
class HashKey {
protected:
    HashKey(uint64_t key, common::hash_t hash)
        : key(key), hash(hash), fingerprint(fingerprintFromHash(hash)) {}

public:
    inline uint64_t getKey() const { return key; }
    inline common::hash_t getHash() const { return hash; }
    inline uint8_t getFingerprint() const { return fingerprint; }

    bool operator==(uint64_t other_key) const {
        return *static_cast<const Derived*>(this) == other_key;
    }

private:
    uint64_t key;
    common::hash_t hash;
    uint8_t fingerprint;

    static constexpr common::hash_t FINGERPRINT_MASK = 0xFF00'0000'0000'0000;
    static uint8_t fingerprintFromHash(common::hash_t hash) {
        // Uppermost byte of hash.
        return (hash & FINGERPRINT_MASK) >> 56;
    }
};

} // namespace storage
} // namespace kuzu
