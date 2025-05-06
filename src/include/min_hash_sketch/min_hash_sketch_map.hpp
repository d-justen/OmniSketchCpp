#pragma once

#include "min_hash_sketch.hpp"

namespace omnisketch {

class MinHashSketchMap : public MinHashSketch {
public:
	using HashIter = std::set<uint64_t>::iterator;
	class SketchIterator : public MinHashSketch::SketchIterator {
	public:
		SketchIterator(std::map<uint64_t, HashIter>::const_iterator map_it_p,
		               std::set<uint64_t>::const_iterator set_it_p, size_t value_count_p)
		    : map_it(map_it_p), set_it(set_it_p), values_left(value_count_p) {
		}
		void Next() override {
			values_left--;
			set_it++;
		}
		uint64_t Current() override {
			return *set_it;
		}
		std::pair<uint64_t, uint64_t> CurrentKeyVal() {
			return {map_it->first, *map_it->second};
		}
		void NextKeyVal() {
			values_left--;
			map_it++;
		}
		bool IsAtEnd() override {
			return values_left == 0;
		}

	private:
		std::map<uint64_t, HashIter>::const_iterator map_it;
		std::set<uint64_t>::const_iterator set_it;
		size_t values_left;
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

	void AddRecord(uint64_t order_key, uint64_t hash);
	void AddRecord(uint64_t hash) override;
	void EraseRecord(uint64_t hash) override;
	size_t Size() const override;
	size_t MaxCount() const override;
	std::shared_ptr<MinHashSketch> Resize(size_t size) const override;
	std::shared_ptr<MinHashSketch> Flatten() const override;
	std::shared_ptr<MinHashSketch>
	Intersect(const std::vector<std::shared_ptr<MinHashSketch>> &sketches) const override;
	void Combine(const MinHashSketch &other) override;
	std::shared_ptr<MinHashSketch> Combine(const std::vector<std::shared_ptr<MinHashSketch>> &others) const override;
	std::shared_ptr<MinHashSketch> Copy() const override;
	size_t EstimateByteSize() const override;
	std::unique_ptr<MinHashSketch::SketchIterator> Iterator() const override;
	std::unique_ptr<MinHashSketch::SketchIterator> Iterator(size_t max_sample_count) const override;
	std::set<uint64_t> &Data();
	const std::set<uint64_t> &Data() const;

private:
	std::map<uint64_t, HashIter> hash_index;
	std::set<uint64_t> data;
	size_t max_count;
};

} // namespace omnisketch
