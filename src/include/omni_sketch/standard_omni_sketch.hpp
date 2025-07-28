#pragma once

#include "min_hash_sketch/min_hash_sketch_vector.hpp"
#include "omni_sketch.hpp"

namespace omnisketch {

template <typename T>
class TypedPointOmniSketch : public PointOmniSketch {
public:
	TypedPointOmniSketch(size_t width_p, size_t depth_p, size_t max_sample_count_p,
	                     std::shared_ptr<HashFunction<T>> hash_function_p,
	                     std::shared_ptr<SetMembershipAlgorithm> set_membership_algo_p,
	                     std::shared_ptr<CellIdxMapper> hash_processor_p)
	    : PointOmniSketch(width_p, depth_p, max_sample_count_p, std::move(set_membership_algo_p),
	                      std::move(hash_processor_p)),
	      hf(std::move(hash_function_p)) {
	}

	TypedPointOmniSketch(size_t width, size_t depth, size_t max_sample_count_p)
	    : TypedPointOmniSketch(width, depth, max_sample_count_p, std::make_shared<MurmurHashFunction<T>>(),
	                           std::make_shared<ProbeAllSum>(), std::make_shared<BarrettModSplitHashMapper>(width)) {
	}

	void AddRecord(const T &value, uint64_t record_id) {
		min = std::min(min, value);
		max = std::max(max, value);
		PointOmniSketch::AddRecordHashed(hf->Hash(value), hf->HashRid(record_id));
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

	double EstimateAverageMatchesPerProbe() const override {
		size_t filled_cells_front_row = 0;
		for (auto &cell : cells.front()) {
			if (cell->RecordCount() > 0) {
				filled_cells_front_row++;
			}
		}
		return (double)record_count / (double)filled_cells_front_row;
	}

	T GetMin() const {
		return min;
	}

	T GetMax() const {
		return max;
	}

	void SetMin(T &min_p) {
		min = min_p;
	}

	void SetMax(T &max_p) {
		max = max_p;
	}

	OmniSketchType Type() const override {
		return OmniSketchType::STANDARD;
	}

protected:
	std::shared_ptr<HashFunction<T>> hf;
	T min = std::numeric_limits<T>::max();
	T max = std::numeric_limits<T>::min();
};

template <>
inline double TypedPointOmniSketch<size_t>::EstimateAverageMatchesPerProbe() const {
	size_t domain = max - min;
	return (double)record_count / (double)domain;
}

} // namespace omnisketch
