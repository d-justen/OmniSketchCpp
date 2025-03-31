#include "combinator.hpp"

namespace omnisketch {

void ExhaustiveCombinator::AddPredicate(std::shared_ptr<OmniSketchBase> omni_sketch,
                                        std::shared_ptr<OmniSketchCell> probe_sample) {
	if (joins.empty()) {
		max_sample_count = omni_sketch->MinHashSketchSize();
	}
	assert(max_sample_count == omni_sketch->MinHashSketchSize());
	sampling_probability *= (double)probe_sample->SampleCount() / (double)probe_sample->RecordCount();
	joins.emplace_back(std::move(omni_sketch), std::move(probe_sample));
}

void ExhaustiveCombinator::FindMatchesInNextJoin(
    const std::vector<std::vector<ExhaustiveCombinatorItem>> &join_key_matches,
    const std::shared_ptr<OmniSketchCell> &current, size_t join_idx, size_t current_n_max,
    std::shared_ptr<OmniSketchCell> &result) const {
	for (auto &item : join_key_matches[join_idx]) {
		auto intersection = OmniSketchCell::Intersect({current, item.cell});
		if (intersection->SampleCount() == 0) {
			continue;
		}
		current_n_max = std::max(current_n_max, item.n_max);

		if (join_idx < join_key_matches.size() - 1) {
			FindMatchesInNextJoin(join_key_matches, intersection, join_idx + 1, current_n_max, result);
		} else {
			double card_est = ((double)current_n_max / (double)max_sample_count) * intersection->SampleCount();
			card_est /= sampling_probability;
			intersection->SetRecordCount((size_t)std::round(card_est));
			result->Combine(*intersection);
		}
	}
}

std::shared_ptr<OmniSketchCell> ExhaustiveCombinator::Execute(size_t max_output_size) const {
	assert(!joins.empty());
	std::vector<std::vector<ExhaustiveCombinatorItem>> join_key_matches;
	join_key_matches.reserve(joins.size());

	for (auto &join : joins) {
		const auto &omni_sketch = join.first;
		const auto &probe_set = join.second;

		std::vector<ExhaustiveCombinatorItem> join_results;
		join_results.reserve(probe_set->SampleCount());

		std::vector<std::shared_ptr<OmniSketchCell>> matches(omni_sketch->Depth());
		auto join_key_it = probe_set->GetMinHashSketch()->Iterator();
		for (size_t join_key_idx = 0; join_key_idx < probe_set->SampleCount(); join_key_idx++) {
			auto probe_result = omni_sketch->ProbeHash(join_key_it->Next(), matches);
			join_results.emplace_back(probe_result, ExhaustiveCombinatorItem::GetNMax(matches));
		}

		join_key_matches.push_back(join_results);
	}

	auto result = std::make_shared<OmniSketchCell>(max_output_size);

	if (join_key_matches.size() == 1) {
		for (auto &match : join_key_matches.front()) {
			result->Combine(*match.cell);
		}
		result->SetRecordCount((size_t)std::round((double)result->RecordCount() / sampling_probability));
		return result;
	}

	for (auto &join_key_match : join_key_matches.front()) {
		FindMatchesInNextJoin(join_key_matches, join_key_match.cell, 1, join_key_match.n_max, result);
	}
	return result;
}

void UncorrelatedCombinator::AddPredicate(std::shared_ptr<OmniSketchBase> omni_sketch,
                                          std::shared_ptr<OmniSketchCell> probe_sample) {
	if (joins.empty()) {
		base_card = omni_sketch->RecordCount();
	}
	assert(base_card == omni_sketch->RecordCount());
	joins.emplace_back(std::move(omni_sketch), std::move(probe_sample));
}

std::shared_ptr<OmniSketchCell> UncorrelatedCombinator::Execute(size_t max_output_size) const {
	assert(!joins.empty());

	auto query_result = std::make_shared<OmniSketchCell>(max_output_size);
	double query_selectivity = 1;

	std::vector<std::shared_ptr<MinHashSketch>> join_results;
	join_results.reserve(joins.size());

	for (auto &join : joins) {
		const auto &omni_sketch = join.first;
		const auto &probe_set = join.second;

		const auto join_result = omni_sketch->ProbeSet(probe_set->GetMinHashSketch());
		const double join_key_sampling_probability =
		    (double)probe_set->SampleCount() / (double)probe_set->RecordCount();
		const double join_selectivity =
		    std::min(1.0, ((double)join_result->RecordCount() / join_key_sampling_probability) / (double)base_card);
		query_selectivity *= join_selectivity;

		join_results.push_back(join_result->GetMinHashSketch());
	}

	const double card_est = std::round((double)base_card * query_selectivity);
	query_result->SetRecordCount((size_t)card_est);
	query_result->SetMinHashSketch(join_results.front()->Intersect(join_results));

	return query_result;
}

} // namespace omnisketch
