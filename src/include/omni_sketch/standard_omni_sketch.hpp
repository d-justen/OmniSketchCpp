#pragma once

#include "min_hash_sketch/min_hash_sketch_vector.hpp"
#include "min_hash_sketch/min_hash_value_sample.hpp"
#include "omni_sketch.hpp"

#include <functional>
#include <regex>

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
	      hf(std::move(hash_function_p)), sample(max_sample_count_p) {
	}

	TypedPointOmniSketch(size_t width, size_t depth, size_t max_sample_count_p)
	    : TypedPointOmniSketch(width, depth, max_sample_count_p, std::make_shared<MurmurHashFunction<T>>(),
	                           std::make_shared<ProbeAllSum>(), std::make_shared<BarrettModSplitHashMapper>(width)) {
	}

	void AddRecord(const T &value, uint64_t record_id) {
		min = std::min(min, value);
		max = std::max(max, value);
		auto rid_hash = hf->HashRid(record_id);
		sample.AddRecord(rid_hash, value);
		PointOmniSketch::AddRecordHashed(hf->Hash(value), rid_hash);
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

	std::shared_ptr<OmniSketchCell> S_Between(const T &lower_bound, const T &upper_bound) const {
		return EvalExpressionInternal(
		    [&lower_bound, &upper_bound](const T &value) { return value >= lower_bound && value <= upper_bound; });
	}

	std::shared_ptr<OmniSketchCell> S_LowerThan(const T &upper_bound) const {
		return EvalExpressionInternal([&upper_bound](const T &value) { return value < upper_bound; });
	}

	std::shared_ptr<OmniSketchCell> S_LowerThanEq(const T &upper_bound) const {
		return EvalExpressionInternal([&upper_bound](const T &value) { return value <= upper_bound; });
	}

	std::shared_ptr<OmniSketchCell> S_GreaterThan(const T &lower_bound) const {
		return EvalExpressionInternal([&lower_bound](const T &value) { return value > lower_bound; });
	}

	std::shared_ptr<OmniSketchCell> S_GreaterThanEq(const T &lower_bound) const {
		return EvalExpressionInternal([&lower_bound](const T &value) { return value <= lower_bound; });
	}

	std::shared_ptr<OmniSketchCell> S_NotEq(const T &constant) const {
		return EvalExpressionInternal([&constant](const T &value) { return value != constant; });
	}

	std::shared_ptr<OmniSketchCell> S_NotNull() const {
		return EvalExpressionInternal([](const T &) { return true; });
	}

	std::shared_ptr<OmniSketchCell> S_Like(const std::string &) const {
		throw std::runtime_error("Like predicate not allowed for this type.");
	}

	T GetMin() const {
		return min;
	}

	T GetMax() const {
		return max;
	}

	OmniSketchType Type() const override {
		return OmniSketchType::STANDARD;
	}

	std::shared_ptr<OmniSketch> MultiplyRecordCounts(const std::shared_ptr<OmniSketch> &other,
	                                                 double multiplier) const override {
		auto result = std::make_shared<TypedPointOmniSketch<T>>(width, depth, max_sample_count, hf, set_membership_algo,
		                                                        hash_processor);
		result->record_count = UINT64_MAX;
		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			size_t current_record_count = 0;
			for (size_t col_idx = 0; col_idx < width; col_idx++) {
				auto &this_cell = PointOmniSketch::GetCell(row_idx, col_idx);
				auto &other_cell = other->GetCell(row_idx, col_idx);
				if (this_cell.RecordCount() > 0 && other_cell.RecordCount() > 0) {
					auto &result_cell = result->cells[row_idx][col_idx];
					result_cell->GetMinHashSketch() = this_cell.GetMinHashSketch();
					result_cell->SetRecordCount(
					    std::max((double)this_cell.RecordCount(),
					             this_cell.RecordCount() * other_cell.RecordCount() * multiplier));
					current_record_count += result_cell->RecordCount();
				}
			}
			result->record_count = std::min(result->record_count, current_record_count);
		}
		return result;
	}

protected:
	template <typename Compare>
	std::shared_ptr<OmniSketchCell> EvalExpressionInternal(Compare cmp) const {
		auto &sample_data = sample.Data();
		size_t matches = 0;
		auto probe_set = std::make_shared<MinHashSketchSet>(sample.MaxCount());
		for (const auto &value_pair : sample_data) {
			const auto &value = value_pair.second;
			if (cmp(value)) {
				matches++;
				probe_set->AddRecord(hf->Hash(value));
			}
		}
		return ProbeSampleInternal(probe_set, matches);
	}

	std::shared_ptr<OmniSketchCell> ProbeSampleInternal(const std::shared_ptr<MinHashSketch> &probe_set,
	                                                    size_t matches) const {
		auto probe_result = PointOmniSketch::ProbeHashedSet(probe_set);
		const double card_est = (double)PointOmniSketch::ValueCount() * ((double)matches / (double)sample.MaxCount());
		probe_result->SetRecordCount((size_t)card_est);
		return probe_result;
	}

	std::shared_ptr<HashFunction<T>> hf;
	MinHashValueSample<T> sample;
	T min = std::numeric_limits<T>::max();
	T max = std::numeric_limits<T>::min();
};

template <>
inline std::shared_ptr<OmniSketchCell> TypedPointOmniSketch<std::string>::S_Like(const std::string &predicate) const {
	static const std::string regex_special_chars = ".^$|()[]{}*+?\\";
	std::string regex_pattern = "^";

	for (char ch : predicate) {
		if (ch == '%')
			regex_pattern += ".*";
		else if (ch == '_')
			regex_pattern += ".";
		else {
			if (regex_special_chars.find(ch) != std::string::npos) {
				regex_pattern += std::string("\\") + ch;
			} else {
				regex_pattern += ch;
			}
		}
	}

	regex_pattern += "$";
	std::regex pattern(regex_pattern);
	return EvalExpressionInternal([&pattern](const std::string &value) { return std::regex_match(value, pattern); });
}

} // namespace omnisketch
