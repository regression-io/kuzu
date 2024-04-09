#pragma once

#include <cstdint>
#include <string>

#include "common/api.h"

namespace kuzu {
namespace common {

class FileSystem;

struct KUZU_API FileInfo {
    FileInfo(std::string path, FileSystem* fileSystem)
        : path{std::move(path)}, fileSystem{fileSystem} {}

    virtual ~FileInfo() = default;

    // TODO: This function should be marked as const.
    uint64_t getFileSize();

    void readFromFile(void* buffer, uint64_t numBytes, uint64_t position);

    int64_t readFile(void* buf, size_t nbyte);

    void writeFile(const uint8_t* buffer, uint64_t numBytes, uint64_t offset);

    int64_t seek(uint64_t offset, int whence);

    void truncate(uint64_t size);

    const std::string path;

    FileSystem* fileSystem;
};

} // namespace common
} // namespace kuzu
