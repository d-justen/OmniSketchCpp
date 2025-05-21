#pragma once

#include "min_hash_sketch/min_hash_sketch_pair.hpp"
#include "omni_sketch.hpp"

namespace omnisketch {

template <typename T>
class PreJoinedOmniSketch : public PointOmniSketch {
public:
	PreJoinedOmniSketch(std::shared_ptr<OmniSketch> sketch, size_t width_p, size_t depth_p, size_t max_sample_count_p,
	                    std::shared_ptr<HashFunction<T>> hash_function_p,
	                    std::shared_ptr<SetMembershipAlgorithm> set_membership_algo_p,
	                    std::shared_ptr<CellIdxMapper> hash_processor_p)
	    : PointOmniSketch(width_p, depth_p, max_sample_count_p, std::move(set_membership_algo_p),
	                      std::move(hash_processor_p), std::make_shared<MinHashSketchPair::SketchFactory>()),
	      referenced_sketch(std::move(sketch)), hf(std::move(hash_function_p)) {
		probe_buffer.resize(referenced_sketch->Depth());
	}

	PreJoinedOmniSketch(std::shared_ptr<OmniSketch> sketch, size_t width, size_t depth, size_t max_sample_count_p)
	    : PreJoinedOmniSketch(std::move(sketch), width, depth, max_sample_count_p,
	                          std::make_shared<MurmurHashFunction<T>>(), std::make_shared<ProbeAllSum>(),
	                          std::make_shared<BarrettModSplitHashMapper>(width)) {
	}

	void AddValueRecord(const Value &value, uint64_t record_id) override {
		AddRecordHashed(value.GetHash(), hf->HashRid(record_id));
	}

	void AddRecordHashed(uint64_t value_hash, uint64_t record_id_hash) override {
		auto probe_result = referenced_sketch->ProbeHash(record_id_hash, probe_buffer);
		hash_processor->SetHash(value_hash);
		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			const size_t col_idx = hash_processor->ComputeCellIdx(row_idx);
			auto &cell = cells[row_idx][col_idx];
			auto *min_hash_sketch = dynamic_cast<MinHashSketchPair *>(cell->GetMinHashSketch().get());
			min_hash_sketch->CombineWithSecondaryHash(*probe_result->GetMinHashSketch(), record_id_hash);
			cell->SetRecordCount(cell->RecordCount() + probe_result->RecordCount());
		}
		record_count += probe_result->RecordCount();
	}

	void AddRecord(const T &value, uint64_t record_id) {
		min = std::min(min, value);
		max = std::max(max, value);
		AddRecordHashed(hf->Hash(value), hf->HashRid(record_id));
	}

	std::shared_ptr<OmniSketchCell> Probe(const T &value) const {
		std::vector<std::shared_ptr<OmniSketchCell>> matches(depth);
		return PointOmniSketch::ProbeHash(hf->Hash(value), matches);
	}

	std::shared_ptr<OmniSketchCell> ProbeSet(const T *values, size_t count) const {
		std::vector<uint64_t> hashes;
		hashes.reserve(count);

		for (size_t value_idx = 0; value_idx < count; value_idx++) {
			hashes.push_back(hf->Hash(values[value_idx]));
		}

		return PointOmniSketch::ProbeHashedSet(std::make_shared<MinHashSketchVector>(hashes));
	}

	std::shared_ptr<OmniSketchCell> ProbeRange(const T &lower_bound, const T &upper_bound) const {
		std::vector<uint64_t> hashes;
		hashes.reserve((upper_bound - lower_bound) + 1);
		for (T value = lower_bound; value <= upper_bound; value++) {
			hashes.push_back(hf->Hash(value));
		}

		return PointOmniSketch::ProbeHashedSet(std::make_shared<MinHashSketchVector>(hashes));
	}

	T GetMin() const {
		return min;
	}

	T GetMax() const {
		return max;
	}

	OmniSketchType Type() const override {
		return OmniSketchType::PRE_JOINED;
	}

protected:
	std::shared_ptr<OmniSketch> referenced_sketch;
	std::vector<std::shared_ptr<OmniSketchCell>> probe_buffer;
	std::shared_ptr<HashFunction<T>> hf;
	T min = std::numeric_limits<T>::max();
	T max = std::numeric_limits<T>::min();
};

} // namespace omnisketch
