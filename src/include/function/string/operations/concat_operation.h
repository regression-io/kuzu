#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"
#include "common/vector/value_vector.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct Concat {
    template<class A, class B, class R>
    static inline void operation(A& left, B& right, R& result, ValueVector& valueVector) {
        assert(false);
    }

    static void concat(const char* left, uint32_t leftLen, const char* right, uint32_t rightLen,
        ku_string_t& result, ValueVector& resultValueVector) {
        auto len = leftLen + rightLen;
        if (len <= ku_string_t::SHORT_STR_LENGTH /* concat's result is short */) {
            memcpy(result.prefix, left, leftLen);
            memcpy(result.prefix + leftLen, right, rightLen);
        } else {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(len));
            auto buffer = reinterpret_cast<char*>(result.overflowPtr);
            memcpy(buffer, left, leftLen);
            memcpy(buffer + leftLen, right, rightLen);
            memcpy(result.prefix, buffer, ku_string_t::PREFIX_LENGTH);
        }
        result.len = len;
    }
};

template<>
inline void Concat::operation(
    ku_string_t& left, ku_string_t& right, ku_string_t& result, ValueVector& resultValueVector) {
    concat((const char*)left.getData(), left.len, (const char*)right.getData(), right.len, result,
        resultValueVector);
}

template<>
inline void Concat::operation(
    string& left, string& right, ku_string_t& result, ValueVector& resultValueVector) {
    concat(left.c_str(), left.length(), right.c_str(), right.length(), result, resultValueVector);
}

} // namespace operation
} // namespace function
} // namespace kuzu