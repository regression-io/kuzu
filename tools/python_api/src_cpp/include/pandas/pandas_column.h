#pragma once

#include <cstdint>

namespace kuzu {

// We currently only support NUMPY as backend.
enum class PandasColumnBackend : uint8_t { NUMPY = 0 };

class PandasColumn {
public:
    PandasColumn(PandasColumnBackend backend) : backend(backend) {}
    virtual ~PandasColumn() = default;

public:
    PandasColumnBackend getBackEnd() const { return backend; }

    virtual std::unique_ptr<PandasColumn> copy() const = 0;

protected:
    PandasColumnBackend backend;
};

class PandasNumpyColumn : public PandasColumn {
public:
    PandasNumpyColumn(py::array array)
        : PandasColumn{PandasColumnBackend::NUMPY}, array{std::move(array)} {}

    std::unique_ptr<PandasColumn> copy() const override {
        return std::make_unique<PandasNumpyColumn>(array);
    }

public:
    py::array array;
};

} // namespace kuzu
