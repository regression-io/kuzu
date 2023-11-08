#pragma once

#include <atomic>

#include "common/assert.h"

namespace kuzu {
namespace storage {

class BucketLock {
public:
    BucketLock() = default;
    inline void lockForWrite() {
        while (true) {
            uint64_t state = lockAndVersion.load(std::memory_order::relaxed);
            if (!isStateLocked(state) &&
                lockAndVersion.compare_exchange_weak(
                    state, lock(state), std::memory_order::acquire, std::memory_order::relaxed)) {
                return;
            }
        }
    }

    inline uint64_t lockForRead() {
        uint64_t state;
        do {
            // Ordering: must acquire any changes to memory synchronized by this atomic.
            state = lockAndVersion.load(std::memory_order::acquire);
        } while (isStateLocked(state));
        return state & VERSION_MASK;
    }

    inline bool unlockForRead(uint64_t version) {
        uint64_t state = lockAndVersion.load(std::memory_order::acquire);
        return !isStateLocked(state) && (state & VERSION_MASK) == version;
    }

    inline void unlockBumpVersion() {
        uint64_t state = lockAndVersion.load(std::memory_order::relaxed);
        KU_ASSERT(isStateLocked(state));
        lockAndVersion.store(unlock(incrementVersion(state)), std::memory_order::release);
    }

    inline void unlockSameVersion() {
        uint64_t state = lockAndVersion.load(std::memory_order::relaxed);
        KU_ASSERT(isStateLocked(state));
        // If the version is the same we guarantee we made NO CHANGES synchronized by this
        // atomic.
        lockAndVersion.store(unlock(state), std::memory_order::relaxed);
    }

    inline bool isLocked() {
        return isStateLocked(lockAndVersion.load(std::memory_order::relaxed));
    }

private:
    std::atomic<uint64_t> lockAndVersion;

    // Static helpers.

    // Lock bit is the last bit.
    constexpr static uint64_t VERSION_MASK = 0xFFFF'FFFF'FFFF'FFFE;

    inline static bool isStateLocked(uint64_t state) { return state & 1; }

    inline static uint64_t lock(uint64_t state) { return state | 1; }

    inline static uint64_t unlock(uint64_t state) { return state & ~1; }

    inline static uint64_t incrementVersion(uint64_t state) { return state + 2; }
};

} // namespace storage
} // namespace kuzu
