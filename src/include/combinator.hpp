#pragma once

#include "min_hash_sketch/min_hash_sketch_map.hpp"
#include "omni_sketch/omni_sketch.hpp"
#include "util/value.hpp"

namespace omnisketch {

class PredicateConverter {
public:
	template <typename T>
	static std::shared_ptr<OmniSketchCell> ConvertPoint(const T &value) {
		auto result = std::make_shared<OmniSketchCell>(1);
		result->AddRecord(Value::From(value).GetHash());
		return result;
	}

	template <typename T>
	static std::shared_ptr<OmniSketchCell> ConvertSet(const std::vector<T> &values) {
		auto result = std::make_shared<OmniSketchCell>(values.size());
		for (auto &value : values) {
			result->AddRecord(Value::From(value).GetHash());
		}
		return result;
	}

	template <typename T>
	static std::shared_ptr<OmniSketchCell> ConvertRange(const T &lower_bound, const T &upper_bound) {
		std::vector<T> values;
		values.reserve((upper_bound - lower_bound) + 1);
		for (T value = lower_bound; value <= upper_bound; value++) {
			values.push_back(value);
		}

		return ConvertSet(values);
	}
};

struct PredicateResult {
	double GetSel() const {
		return selectivity == 0 ? fallback_selectivity : selectivity;
	}

	std::shared_ptr<MinHashSketch> sketch;
	double selectivity;
	double fallback_selectivity;
	double sampling_probability;
	bool is_set_membership;
	size_t n_max;
};

class CombinedPredicateEstimator {
public:
	explicit CombinedPredicateEstimator(size_t max_sample_count_p) : max_sample_count(max_sample_count_p) {
	}
	void AddPredicate(const std::shared_ptr<OmniSketch> &omni_sketch,
	                  const std::shared_ptr<OmniSketchCell> &probe_sample);
	void AddUnfilteredRids(const std::shared_ptr<OmniSketch> &omni_sketch);
	void AddUnfilteredRids(const std::shared_ptr<OmniSketchCell> &probe_sample, size_t base_card_p);
	bool HasPredicates() const;
	std::shared_ptr<OmniSketchCell> ComputeResult(size_t max_output_size) const;
	std::shared_ptr<OmniSketchCell> FilterProbeSet(const std::shared_ptr<OmniSketch> &omni_sketch,
	                                               const std::shared_ptr<OmniSketchCell> &probe_sample) const;
	void Finalize();

protected:
	std::vector<PredicateResult> intermediate_results;
	size_t max_sample_count;
	size_t base_card = 0;
};

} // namespace omnisketch
