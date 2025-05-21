#pragma once

#include "min_hash_sketch.hpp"
#include "validity_mask.hpp"

namespace omnisketch {

class MinHashSketchPair : public MinHashSketch {
public:
	class SketchIterator : public MinHashSketch::SketchIterator {
	public:
		SketchIterator(std::map<uint64_t, uint64_t>::const_iterator map_it_p, const ValidityMask *validity_p,
		               size_t value_count_p)
		    : map_it(map_it_p), validity(validity_p), offset(0), value_count(value_count_p) {
		}
		void Next() override {
			++offset;
			++map_it;
			while (validity && !validity->IsValid(offset) && offset < value_count) {
				++offset;
				++map_it;
			}
		}
		uint64_t Current() override {
			return map_it->first;
		}
		size_t CurrentIdx() override {
			return offset;
		}
		std::pair<uint64_t, uint64_t> CurrentKeyVal() {
			return {map_it->first, map_it->second};
		}
		uint64_t CurrentVal() {
			return map_it->second;
		}
		bool IsAtEnd() override {
			return offset == value_count;
		}

	private:
		std::map<uint64_t, uint64_t>::const_iterator map_it;
		const ValidityMask* validity;
		size_t offset;
		size_t value_count;
	};

	class SketchFactory : public MinHashSketch::SketchFactory {
	public:
		virtual std::shared_ptr<MinHashSketch> Create(size_t max_sample_count) {
			return std::make_shared<MinHashSketchPair>(max_sample_count, nullptr);
		}
	};

public:
	explicit MinHashSketchPair(size_t max_count_p, std::unique_ptr<ValidityMask> validity_p) : max_count(max_count_p), validity(std::move(validity_p)) {
	}

	void AddRecord(uint64_t primary_rid, uint64_t secondary_rid);
	void AddRecord(uint64_t hash) override;
	void EraseRecord(uint64_t hash) override;
	size_t Size() const override;
	size_t MaxCount() const override;
	std::shared_ptr<MinHashSketch> Resize(size_t size) const override;
	std::shared_ptr<MinHashSketch> Flatten() const override;
	std::shared_ptr<MinHashSketch> Intersect(const std::vector<std::shared_ptr<MinHashSketch>> &sketches,
	                                         size_t max_sample_count = 0) override;
	void Combine(const MinHashSketch &other) override;
	void CombineWithSecondaryHash(const MinHashSketch &other, uint64_t secondary_hash);
	std::shared_ptr<MinHashSketch> Combine(const std::vector<std::shared_ptr<MinHashSketch>> &others) const override;
	std::shared_ptr<MinHashSketch> Copy() const override;
	size_t EstimateByteSize() const override;
	std::unique_ptr<MinHashSketch::SketchIterator> Iterator() const override;
	std::unique_ptr<MinHashSketch::SketchIterator> Iterator(size_t max_sample_count) const override;
	std::map<uint64_t, uint64_t> &Data();
	const std::map<uint64_t, uint64_t> &Data() const;
	std::set<uint64_t> PrimaryRids() const;
	std::set<uint64_t> SecondaryRids() const;

private:
	std::map<uint64_t, uint64_t> data;
	size_t max_count;
	std::unique_ptr<ValidityMask> validity;
};

} // namespace omnisketch
