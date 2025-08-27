#pragma once

#include "min_hash_sketch.hpp"

namespace omnisketch {

class ValidityMask {
public:
    explicit ValidityMask(size_t size) : mask_((size + 7) / 8, 0xFF) {
    }

    ValidityMask(const ValidityMask& other) {
        mask_ = other.mask_;
        invalid_counter = other.invalid_counter;
    }

    void SetValid(size_t index) {
        size_t byte_idx = index >> 3;
        size_t bit_idx = index & 7;
        mask_[byte_idx] |= (1 << bit_idx);
    }

    void SetInvalid(size_t index) {
        size_t byte_idx = index >> 3;
        size_t bit_idx = index & 7;
        mask_[byte_idx] &= ~(1 << bit_idx);
        // TODO: implement a more robust method to track invalids. Repeated SetInvalid calls would be tracked twice
        invalid_counter++;
    }

    bool IsValid(size_t index) const {
        size_t byte_idx = index >> 3;
        size_t bit_idx = index & 7;
        return (mask_[byte_idx] >> bit_idx) & 1;
    }

    size_t InvalidCount() const {
        return invalid_counter;
    }

private:
    std::vector<std::uint8_t> mask_;
    size_t invalid_counter = 0;
};

class MinHashSketchVector : public MinHashSketch {
public:
    class SketchIterator : public MinHashSketch::SketchIterator {
    public:
        SketchIterator(std::vector<uint64_t>::const_iterator it_p, const ValidityMask* validity_p, size_t value_count_p)
            : it(it_p), validity(validity_p), offset(0), value_count(value_count_p) {
        }
        uint64_t Current() override {
            return *it;
        }
        size_t CurrentIdx() override {
            return offset;
        }
        void Next() override {
            ++offset;
            ++it;
            while (validity && !validity->IsValid(offset) && offset < value_count) {
                ++offset;
                ++it;
            }
        }
        uint64_t CurrentValueOrDefault(uint64_t default_val) override {
            return default_val;
        }
        bool IsAtEnd() override {
            return offset == value_count;
        }

    private:
        std::vector<uint64_t>::const_iterator it;
        const ValidityMask* validity;
        size_t offset;
        const size_t value_count;
    };

    class SketchFactory : public MinHashSketch::SketchFactory {
    public:
        virtual std::shared_ptr<MinHashSketch> Create(size_t max_sample_count) {
            return std::make_shared<MinHashSketchVector>(max_sample_count);
        }
    };

public:
    MinHashSketchVector(std::vector<uint64_t> data_p, std::unique_ptr<ValidityMask> validity_p)
        : data(std::move(data_p)), validity(std::move(validity_p)), max_count(data.size()) {
    }
    MinHashSketchVector(size_t max_count_p, std::unique_ptr<ValidityMask> validity_p)
        : validity(std::move(validity_p)), max_count(max_count_p) {
        data.reserve(max_count);
    }
    explicit MinHashSketchVector(std::vector<uint64_t> data_p) : data(std::move(data_p)), max_count(data.size()) {
    }
    explicit MinHashSketchVector(size_t max_count_p) : max_count(max_count_p) {
        data.reserve(max_count);
    }
    MinHashSketchVector(std::vector<uint64_t> data_p, size_t max_count_p)
        : data(std::move(data_p)), max_count(max_count_p) {
    }

    void AddRecord(uint64_t hash) override;
    void EraseRecord(uint64_t hash) override;
    size_t Size() const override;
    size_t MaxCount() const override;
    std::shared_ptr<MinHashSketch> Resize(size_t size) const override;
    std::shared_ptr<MinHashSketch> Flatten() const override;
    std::shared_ptr<MinHashSketch> Intersect(const std::vector<std::shared_ptr<MinHashSketch>>& sketches,
                                             size_t max_sample_count = 0) override;
    void Combine(const MinHashSketch& other) override;
    std::shared_ptr<MinHashSketch> Combine(const std::vector<std::shared_ptr<MinHashSketch>>& others) const override;
    std::shared_ptr<MinHashSketch> Copy() const override;
    size_t EstimateByteSize() const override;
    std::unique_ptr<MinHashSketch::SketchIterator> Iterator() const override;
    std::unique_ptr<MinHashSketch::SketchIterator> Iterator(size_t max_sample_count) const override;
    std::vector<uint64_t>& Data();
    const std::vector<uint64_t>& Data() const;

    static std::shared_ptr<MinHashSketch> ComputeIntersection(
        const std::vector<std::shared_ptr<MinHashSketch>>& sketches, ValidityMask* mask = nullptr,
        size_t max_sample_size = 0);

private:
    void ShrinkToFit();

    std::vector<uint64_t> data;
    std::unique_ptr<ValidityMask> validity;
    size_t max_count;
    static constexpr double SHRINK_TO_FIT_THRESHOLD = 1.0 / 8.0;
};

}  // namespace omnisketch
