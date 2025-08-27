#pragma once

#include <cassert>
#include <map>
#include <set>
#include <stdexcept>
#include <vector>

namespace omnisketch {

class MinHashSketch {
public:
    class SketchIterator {
    public:
        virtual ~SketchIterator() = default;
        virtual uint64_t Current() = 0;
        virtual uint64_t CurrentValueOrDefault(uint64_t default_val) = 0;
        virtual size_t CurrentIdx() = 0;
        virtual void Next() = 0;
        virtual bool IsAtEnd() = 0;
    };

    class SketchFactory {
    public:
        virtual ~SketchFactory() = default;
        virtual std::shared_ptr<MinHashSketch> Create(size_t max_sample_count) = 0;
    };

public:
    virtual ~MinHashSketch() = default;
    virtual void AddRecord(uint64_t hash) = 0;
    virtual void EraseRecord(uint64_t hash) = 0;
    virtual size_t Size() const = 0;
    virtual size_t MaxCount() const = 0;
    virtual std::shared_ptr<MinHashSketch> Resize(size_t size) const = 0;
    virtual std::shared_ptr<MinHashSketch> Flatten() const = 0;
    virtual std::shared_ptr<MinHashSketch> Intersect(const std::vector<std::shared_ptr<MinHashSketch>>& sketches,
                                                     size_t max_sample_count = 0) = 0;
    virtual void Combine(const MinHashSketch& other) = 0;
    virtual std::shared_ptr<MinHashSketch> Combine(const std::vector<std::shared_ptr<MinHashSketch>>& others) const = 0;
    virtual std::shared_ptr<MinHashSketch> Copy() const = 0;
    virtual size_t EstimateByteSize() const = 0;
    virtual std::unique_ptr<SketchIterator> Iterator() const = 0;
    virtual std::unique_ptr<SketchIterator> Iterator(size_t max_sample_count) const = 0;
};

}  // namespace omnisketch
