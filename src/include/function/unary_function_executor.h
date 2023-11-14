#pragma once

#include "common/vector/value_vector.h"
#include "function/function.h"

namespace kuzu {
namespace function {

/**
 * Unary operator assumes operation with null returns null. This does NOT applies to IS_NULL and
 * IS_NOT_NULL operation.
 */

struct UnaryFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos));
    }
};

struct UnaryStringFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), resultVector_);
    }
};

struct UnaryCastStringFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* dataPtr) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto resultVector_ = (common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_->getValue<RESULT_TYPE>(resultPos), resultVector_, inputPos,
            &reinterpret_cast<StringCastFunctionBindData*>(dataPtr)->csvConfig);
    }
};

struct UnaryListFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), inputVector_, resultVector_);
    }
};

struct UnaryCastFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), inputVector_, resultVector_);
    }
};

struct UnaryTryCastFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* /*dataPtr*/) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::template operation<RESULT_TYPE>(inputVector_, inputPos,
            resultVector_.getValue<RESULT_TYPE>(resultPos), resultVector_, resultPos);
    }
};

struct UnaryUDFFunctionWrapper {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static inline void operation(void* inputVector, uint64_t inputPos, void* resultVector,
        uint64_t resultPos, void* dataPtr) {
        auto& inputVector_ = *(common::ValueVector*)inputVector;
        auto& resultVector_ = *(common::ValueVector*)resultVector;
        FUNC::operation(inputVector_.getValue<OPERAND_TYPE>(inputPos),
            resultVector_.getValue<RESULT_TYPE>(resultPos), dataPtr);
    }
};

struct CastChildFunctionExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeSwitch(
        common::ValueVector& operand, common::ValueVector& result, void* dataPtr) {
        // this vector is of var list type and the child vector is of non-nested types then cast
        KU_ASSERT(operand.dataType.getLogicalTypeID() == common::LogicalTypeID::VAR_LIST &&
                  result.dataType.getLogicalTypeID() == common::LogicalTypeID::VAR_LIST);
        auto childNum = common::ListVector::getDataVectorSize(&operand);
        auto inputChildVector = common::ListVector::getDataVector(&operand);
        auto resultChildVector = (common::ListVector::getDataVector(&result));
        for (auto i = 0u; i < childNum; i++) {
            resultChildVector->setNull(i, inputChildVector->isNull(i));
            if (!resultChildVector->isNull(i)) {
                // cast position i in child data vector
                OP_WRAPPER::template operation<OPERAND_TYPE, RESULT_TYPE, FUNC>(
                    (void*)(inputChildVector), i, (void*)(resultChildVector), i, dataPtr);
            }
        }
    }
};

struct UnaryFunctionExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeOnValue(common::ValueVector& inputVector, uint64_t inputPos,
        common::ValueVector& resultVector, uint64_t resultPos, void* dataPtr) {
        OP_WRAPPER::template operation<OPERAND_TYPE, RESULT_TYPE, FUNC>(
            (void*)&inputVector, inputPos, (void*)&resultVector, resultPos, dataPtr);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC, typename OP_WRAPPER>
    static void executeSwitch(
        common::ValueVector& operand, common::ValueVector& result, void* dataPtr) {
        result.resetAuxiliaryBuffer();
        if (operand.state->isFlat()) {
            auto inputPos = operand.state->selVector->selectedPositions[0];
            auto resultPos = result.state->selVector->selectedPositions[0];
            result.setNull(resultPos, operand.isNull(inputPos));
            if (!result.isNull(inputPos)) {
                executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                    operand, inputPos, result, resultPos, dataPtr);
            }
        } else {
            if (operand.hasNoNullsGuarantee()) {
                if (operand.state->selVector->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            operand, i, result, i, dataPtr);
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        auto pos = operand.state->selVector->selectedPositions[i];
                        executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                            operand, pos, result, pos, dataPtr);
                    }
                }
            } else {
                if (operand.state->selVector->isUnfiltered()) {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        result.setNull(i, operand.isNull(i));
                        if (!result.isNull(i)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                                operand, i, result, i, dataPtr);
                        }
                    }
                } else {
                    for (auto i = 0u; i < operand.state->selVector->selectedSize; i++) {
                        auto pos = operand.state->selVector->selectedPositions[i];
                        result.setNull(pos, operand.isNull(pos));
                        if (!result.isNull(pos)) {
                            executeOnValue<OPERAND_TYPE, RESULT_TYPE, FUNC, OP_WRAPPER>(
                                operand, pos, result, pos, dataPtr);
                        }
                    }
                }
            }
        }
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(common::ValueVector& operand, common::ValueVector& result) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryFunctionWrapper>(
            operand, result, nullptr /* dataPtr */);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void executeUDF(
        common::ValueVector& operand, common::ValueVector& result, void* dataPtr) {
        executeSwitch<OPERAND_TYPE, RESULT_TYPE, FUNC, UnaryUDFFunctionWrapper>(
            operand, result, dataPtr);
    }
};

} // namespace function
} // namespace kuzu
