#pragma once

#include <vector>

#include "src/common/include/gf_string.h"
#include "src/common/include/memory_manager.h"

namespace graphflow {
namespace common {

struct BufferBlock {
public:
    explicit BufferBlock(unique_ptr<MemoryBlock> block)
        : size{block->size}, currentOffset{0}, data{block->data}, block{move(block)} {}

public:
    uint64_t size;
    uint64_t currentOffset;
    uint8_t* data;

private:
    unique_ptr<MemoryBlock> block;
};

class StringBuffer {
    static constexpr uint64_t MIN_BUFFER_BLOCK_SIZE = 4096;

public:
    explicit StringBuffer(MemoryManager& memoryManager)
        : memoryManager{memoryManager}, currentBlock{nullptr} {};

public:
    void allocateLargeString(gf_string_t& result, uint64_t len);

public:
    vector<unique_ptr<BufferBlock>> blocks;

private:
    MemoryManager& memoryManager;
    BufferBlock* currentBlock;
};
} // namespace common
} // namespace graphflow
