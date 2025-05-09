#pragma once

#include "min_hash_sketch.hpp"

namespace omnisketch {

class MinHashSketchSet : public MinHashSketch {
public:
	class SketchIterator : public MinHashSketch::SketchIterator {
	public:
		SketchIterator(std::set<uint64_t>::const_iterator it_p, size_t value_count_p)
		    : it(it_p), offset(0), value_count(value_count_p) {
		}
		uint64_t Current() override {
			return *it;
		}
		size_t CurrentIdx() override {
			return offset;
		};
		void Next() override {
			++offset;
			++it;
		}
		bool IsAtEnd() override {
			return offset == value_count;
		}

	private:
		std::set<uint64_t>::const_iterator it;
		size_t offset;
		size_t value_count;
	};

	class SketchFactory : public MinHashSketch::SketchFactory {
	public:
		virtual std::shared_ptr<MinHashSketch> Create(size_t max_sample_count) {
			return std::make_shared<MinHashSketchSet>(max_sample_count);
		}
	};

public:
	explicit MinHashSketchSet(size_t max_count_p) : max_count(max_count_p) {
	}

	void AddRecord(uint64_t hash) override;
	void EraseRecord(uint64_t hash) override;
	size_t Size() const override;
	size_t MaxCount() const override;
	std::shared_ptr<MinHashSketch> Resize(size_t size) const override;
	std::shared_ptr<MinHashSketch> Flatten() const override;
	std::shared_ptr<MinHashSketch> Intersect(const std::vector<std::shared_ptr<MinHashSketch>> &sketches,
	                                         size_t max_sample_count = 0) override;
	void Combine(const MinHashSketch &other) override;
	std::shared_ptr<MinHashSketch> Combine(const std::vector<std::shared_ptr<MinHashSketch>> &others) const override;
	std::shared_ptr<MinHashSketch> Copy() const override;
	size_t EstimateByteSize() const override;
	std::unique_ptr<MinHashSketch::SketchIterator> Iterator() const override;
	std::unique_ptr<MinHashSketch::SketchIterator> Iterator(size_t max_sample_count) const override;
	std::set<uint64_t> &Data();
	const std::set<uint64_t> &Data() const;

private:
	std::set<uint64_t> data;
	size_t max_count;
};

} // namespace omnisketch
