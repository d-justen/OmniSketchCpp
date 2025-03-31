#pragma once

#include "omni_sketch.hpp"

namespace omnisketch {

class PredicateConverter {
public:
	template <typename T>
	static std::shared_ptr<OmniSketchCell> ConvertPoint(const T &value) {
		auto result = std::make_shared<OmniSketchCell>(1);
		result->AddRecord(Hash(value));
		return result;
	}

	template <typename T>
	static std::shared_ptr<OmniSketchCell> ConvertSet(const std::vector<T> &values) {
		auto result = std::make_shared<OmniSketchCell>(values.size());
		for (auto &value : values) {
			result->AddRecord(Hash(value));
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
	virtual void AddPredicate(std::shared_ptr<OmniSketchBase> omni_sketch,
	                          std::shared_ptr<OmniSketchCell> probe_sample) = 0;
	virtual std::shared_ptr<OmniSketchCell> Execute(size_t max_output_size) const = 0;
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
	void AddPredicate(std::shared_ptr<OmniSketchBase> omni_sketch,
	                  std::shared_ptr<OmniSketchCell> probe_sample) override;
	std::shared_ptr<OmniSketchCell> Execute(size_t max_output_size) const override;

protected:
	std::vector<std::pair<std::shared_ptr<OmniSketchBase>, std::shared_ptr<OmniSketchCell>>> joins;
	size_t max_sample_count;
	double sampling_probability = 1;

protected:
	void FindMatchesInNextJoin(const std::vector<std::vector<ExhaustiveCombinatorItem>> &join_key_matches,
	                           const std::shared_ptr<OmniSketchCell> &current, size_t join_idx, size_t current_n_max,
	                           std::shared_ptr<OmniSketchCell> &result) const;
};

class UncorrelatedCombinator : public OmniSketchCombinator {
public:
	void AddPredicate(std::shared_ptr<OmniSketchBase> omni_sketch,
	                  std::shared_ptr<OmniSketchCell> probe_sample) override;
	std::shared_ptr<OmniSketchCell> Execute(size_t max_output_size) const override;

protected:
	std::vector<std::pair<std::shared_ptr<OmniSketchBase>, std::shared_ptr<OmniSketchCell>>> joins;
	size_t base_card;
};

} // namespace omnisketch
