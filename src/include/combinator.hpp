#pragma once

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

class OmniSketchCombinator {
public:
	virtual ~OmniSketchCombinator() = default;
	virtual void AddPredicate(std::shared_ptr<OmniSketch> omni_sketch,
	                          std::shared_ptr<OmniSketchCell> probe_sample) = 0;
	virtual void AddUnfilteredRids(std::shared_ptr<OmniSketch> omni_sketch) = 0;
	virtual void AddUnfilteredRids(std::shared_ptr<OmniSketchCell> rid_sample) = 0;
	virtual void AddFKMultiplier(double multiplier_p) = 0;
	virtual bool HasPredicates() const = 0;
	virtual std::shared_ptr<OmniSketchCell> ComputeResult(size_t max_output_size) const = 0;
	virtual std::shared_ptr<OmniSketchCell> FilterProbeSet(std::shared_ptr<OmniSketch> omni_sketch,
	                                                       std::shared_ptr<OmniSketchCell> probe_sample) const = 0;

	void SetBaseCard(size_t card) {
		base_card = card;
	}
protected:
	size_t base_card;
};

struct ExhaustiveCombinatorItem {
	ExhaustiveCombinatorItem(std::shared_ptr<OmniSketchCell> cell_p, size_t n_max_p)
	    : cell(std::move(cell_p)), n_max(n_max_p) {
	}

	std::shared_ptr<OmniSketchCell> cell;
	size_t n_max;

	static size_t GetNMax(const std::vector<std::shared_ptr<OmniSketchCell>> &cells) {
		size_t n_max = 0;
		for (const auto &cell : cells) {
			n_max = std::max(n_max, cell->RecordCount());
		}
		return n_max;
	}
};

class ExhaustiveCombinator : public OmniSketchCombinator {
public:
	void AddPredicate(std::shared_ptr<OmniSketch> omni_sketch, std::shared_ptr<OmniSketchCell> probe_sample) override;
	void AddUnfilteredRids(std::shared_ptr<OmniSketch> omni_sketch) override;
	void AddUnfilteredRids(std::shared_ptr<OmniSketchCell> rid_sample) override;
	void AddFKMultiplier(double multiplier_p) override;
	bool HasPredicates() const override;
	std::shared_ptr<OmniSketchCell> ComputeResult(size_t max_output_size) const override;
	std::shared_ptr<OmniSketchCell> FilterProbeSet(std::shared_ptr<OmniSketch> omni_sketch,
	                                               std::shared_ptr<OmniSketchCell> probe_sample) const override;

protected:
	std::vector<std::vector<ExhaustiveCombinatorItem>> join_key_matches;
	std::vector<double> sampling_probabilities;
	std::vector<double> join_sels;
	size_t max_sample_count;
	std::vector<std::shared_ptr<OmniSketch>> sketch_filters;
	double multiplier = 1;

protected:
	void FindMatchesInNextJoin(const std::shared_ptr<MinHashSketch> &current, size_t join_idx, size_t current_n_max,
	                           std::vector<double> &match_counts, std::shared_ptr<OmniSketchCell> &result) const;
};

class UncorrelatedCombinator : public OmniSketchCombinator {
public:
	void AddPredicate(std::shared_ptr<OmniSketch> omni_sketch, std::shared_ptr<OmniSketchCell> probe_sample) override;
	void AddUnfilteredRids(std::shared_ptr<OmniSketch> omni_sketch) override;
	void AddUnfilteredRids(std::shared_ptr<OmniSketchCell> rid_sample) override;
	void AddFKMultiplier(double /*multiplier_p*/) override {}
	bool HasPredicates() const override;
	std::shared_ptr<OmniSketchCell> ComputeResult(size_t max_output_size) const override;
	std::shared_ptr<OmniSketchCell> FilterProbeSet(std::shared_ptr<OmniSketch> omni_sketch,
	                                               std::shared_ptr<OmniSketchCell> probe_sample) const override;

protected:
	std::vector<std::pair<std::shared_ptr<OmniSketch>, std::shared_ptr<OmniSketchCell>>> joins;
	std::vector<std::shared_ptr<MinHashSketch>> join_results;
	double query_selectivity = 1;
};

} // namespace omnisketch
