#pragma once

#include "min_hash_sketch.hpp"

namespace omnisketch {

class MinHashSketchMap : public MinHashSketch {
public:
	class SketchIterator : public MinHashSketch::SketchIterator {
	public:
		SketchIterator(std::map<uint64_t, uint64_t>::const_iterator map_it_p, size_t value_count_p)
		    : map_it(map_it_p), offset(0), value_count(value_count_p) {
		}
		void Next() override {
			++offset;
			++map_it;
		}
		uint64_t Current() override {
			return map_it->first;
		}
		uint64_t CurrentValueOrDefault(uint64_t) override {
			return map_it->second;
		}
		size_t CurrentIdx() override {
			return offset;
		}
		std::pair<uint64_t, uint64_t> CurrentKeyVal() {
			return {map_it->first, map_it->second};
		}
		bool IsAtEnd() override {
			return offset == value_count;
		}

	private:
		std::map<uint64_t, uint64_t>::const_iterator map_it;
		size_t offset;
		size_t value_count;
	};

	class SketchFactory : public MinHashSketch::SketchFactory {
	public:
		virtual std::shared_ptr<MinHashSketch> Create(size_t max_sample_count) {
			return std::make_shared<MinHashSketchMap>(max_sample_count);
		}
	};

public:
	explicit MinHashSketchMap(size_t max_count_p) : max_count(max_count_p) {
	}

	void AddRecord(uint64_t hash, uint64_t value);
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
	std::map<uint64_t, uint64_t> &Data();
	const std::map<uint64_t, uint64_t> &Data() const;
	void ShrinkToSize() {
		max_count = data.size();
	}

	static std::shared_ptr<MinHashSketchMap> IntersectMap(std::vector<std::shared_ptr<MinHashSketch>> &sketches,
	                                                      size_t n_max, size_t max_sample_size = 0);

private:
	std::map<uint64_t, uint64_t> data;
	size_t max_count;
};

} // namespace omnisketch
