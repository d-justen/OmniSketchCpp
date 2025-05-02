#include "combinator.hpp"

namespace omnisketch {

void ExhaustiveCombinator::AddPredicate(std::shared_ptr<OmniSketch> omni_sketch,
                                        std::shared_ptr<OmniSketchCell> probe_sample) {
	if (join_key_matches.empty()) {
		max_sample_count = omni_sketch->MinHashSketchSize();
	}
	if (omni_sketch->MinHashSketchSize() < max_sample_count) {
		for (auto &join_key_match : join_key_matches) {
			for (auto &item : join_key_match) {
				item.cell->GetMinHashSketch()->Resize(omni_sketch->MinHashSketchSize());
			}
		}
	}

	max_sample_count = std::min(max_sample_count, omni_sketch->MinHashSketchSize());
	base_card = std::max(base_card, omni_sketch->RecordCount());
	if (omni_sketch->Type() == OmniSketchType::FOREIGN_SORTED) {
		sampling_probabilities.push_back(1);
	} else {
		sampling_probabilities.push_back(probe_sample->SamplingProbability());
	}

	std::vector<ExhaustiveCombinatorItem> join_results;
	join_results.reserve(probe_sample->SampleCount());

	std::vector<std::shared_ptr<OmniSketchCell>> matches(omni_sketch->Depth());
	auto join_key_it = probe_sample->GetMinHashSketch()->Iterator();
	size_t card_sum = 0;
	for (; !join_key_it->IsAtEnd(); join_key_it->Next()) {
		auto probe_result = omni_sketch->ProbeHash(join_key_it->Current(), matches);
		if (probe_result->SampleCount() > 0) {
			if (probe_result->MaxSampleCount() > max_sample_count) {
				// TODO(perf): Allow probing with reduced sample count to avoid copying hashes
				probe_result->SetMinHashSketch(probe_result->GetMinHashSketch()->Resize(max_sample_count));
			}
			join_results.emplace_back(probe_result, ExhaustiveCombinatorItem::GetNMax(matches));
			card_sum += probe_result->RecordCount();
		}
	}
	join_sels.push_back((double)card_sum / (double)base_card / probe_sample->SamplingProbability());
	join_key_matches.push_back(join_results);
}

void ExhaustiveCombinator::FindMatchesInNextJoin(const std::shared_ptr<MinHashSketch> &current, size_t join_idx,
                                                 size_t current_n_max, std::vector<double> &match_counts,
                                                 std::shared_ptr<OmniSketchCell> &result) const {
	for (auto &item : join_key_matches[join_idx]) {
		auto intersection = current->Intersect({current, item.cell->GetMinHashSketch()});
		if (intersection->Size() == 0) {
			continue;
		}

		current_n_max = std::max(current_n_max, item.n_max);
		double card_est = ((double)current_n_max / (double)max_sample_count) * (double)intersection->Size();
		card_est = std::max(card_est, (double)intersection->Size());
		match_counts[join_idx] += card_est;

		if (join_idx < join_key_matches.size() - 1) {
			FindMatchesInNextJoin(intersection, join_idx + 1, current_n_max, match_counts, result);
		} else {
			OmniSketchCell intersection_cell(std::move(intersection), (size_t)std::round(card_est));
			result->Combine(intersection_cell);
		}
	}
}

std::shared_ptr<OmniSketchCell> ExhaustiveCombinator::ComputeResult(size_t max_output_size) const {
	auto result = std::make_shared<OmniSketchCell>(max_output_size);

	if (join_key_matches.size() == 1) {
		for (auto &match : join_key_matches.front()) {
			result->Combine(*match.cell);
		}
		result->SetRecordCount((size_t)std::round((double)result->RecordCount() / sampling_probabilities[0]));
		return result;
	}

	std::vector<double> match_counts(join_key_matches.size());
	for (auto &join_key_match : join_key_matches.front()) {
		match_counts[0] += (double)join_key_match.cell->RecordCount();
		FindMatchesInNextJoin(join_key_match.cell->GetMinHashSketch(), 1, join_key_match.n_max, match_counts, result);
	}

	auto result_card = (double)base_card;
	for (size_t predicate_idx = 0; predicate_idx < match_counts.size(); predicate_idx++) {
		double last_card_unscaled = predicate_idx == 0 ? (double)base_card : match_counts[predicate_idx - 1];
		double next_card_scaled =
		    std::min(last_card_unscaled, match_counts[predicate_idx] / sampling_probabilities[predicate_idx]);
		double sel = next_card_scaled / last_card_unscaled;
		result_card *= sel;
	}

	result->SetRecordCount((size_t)result_card);
	return result;
}

std::shared_ptr<OmniSketchCell>
ExhaustiveCombinator::FilterProbeSet(std::shared_ptr<OmniSketch> omni_sketch,
                                     std::shared_ptr<OmniSketchCell> probe_sample) const {
	auto filtered_probe_set = std::make_shared<OmniSketchCell>(probe_sample->MaxSampleCount());
	std::vector<double> match_counts(join_key_matches.size());

	std::vector<std::shared_ptr<OmniSketchCell>> matches(omni_sketch->Depth());
	auto probe_sample_it = probe_sample->GetMinHashSketch()->Iterator();
	for (; !probe_sample_it->IsAtEnd(); probe_sample_it->Next()) {
		const uint64_t hash = probe_sample_it->Current();
		const auto probe_result = omni_sketch->ProbeHash(hash, matches);
		size_t probe_card = probe_result->RecordCount();

		if (!join_key_matches.empty() && probe_card > 0) {
			// Intersect with other predicates
			auto combinator_result = std::make_shared<OmniSketchCell>(probe_result->MaxSampleCount());
			const size_t n_max = ExhaustiveCombinatorItem::GetNMax(matches);
			FindMatchesInNextJoin(probe_result->GetMinHashSketch(), 0, n_max, match_counts, combinator_result);
			probe_card = combinator_result->RecordCount();
		}

		if (probe_card > 0) {
			filtered_probe_set->AddRecord(hash);
			filtered_probe_set->SetRecordCount(filtered_probe_set->RecordCount() + probe_card);
		}
	}

	if (!match_counts.empty()) {
		std::vector<double> relative_selectivities;
		relative_selectivities.reserve(match_counts.size());

		auto result_card = (double)base_card;
		for (size_t predicate_idx = 0; predicate_idx < match_counts.size(); predicate_idx++) {
			double last_card_unscaled = predicate_idx == 0 ? (double)base_card : match_counts[predicate_idx - 1];
			double next_card_scaled =
			    std::min(last_card_unscaled, match_counts[predicate_idx] / sampling_probabilities[predicate_idx]);
			double sel = next_card_scaled / last_card_unscaled;
			result_card *= sel;
		}

		filtered_probe_set->SetRecordCount((size_t)std::round(result_card));
	}

	const double card_est = std::min((double)omni_sketch->RecordCount(),
	                                 (double)filtered_probe_set->RecordCount() / probe_sample->SamplingProbability());
	filtered_probe_set->SetRecordCount((size_t)std::round(card_est));

	return filtered_probe_set;
}

void ExhaustiveCombinator::AddUnfilteredRids(std::shared_ptr<OmniSketch> omni_sketch) {
	auto all_rids = omni_sketch->GetRids();
	AddUnfilteredRids(all_rids);
}

bool ExhaustiveCombinator::HasPredicates() const {
	return !join_key_matches.empty();
}

void ExhaustiveCombinator::AddUnfilteredRids(std::shared_ptr<OmniSketchCell> rid_sample) {
	base_card = rid_sample->RecordCount();
	max_sample_count = rid_sample->MaxSampleCount();
	sampling_probabilities.push_back(1);

	join_key_matches.emplace_back();
	join_key_matches.back().emplace_back(rid_sample, rid_sample->RecordCount());
}

void UncorrelatedCombinator::AddPredicate(std::shared_ptr<OmniSketch> omni_sketch,
                                          std::shared_ptr<OmniSketchCell> probe_sample) {
	base_card = std::max(base_card, omni_sketch->RecordCount());
	const auto join_result = omni_sketch->ProbeHashedSet(probe_sample->GetMinHashSketch());
	const double join_key_sampling_probability = probe_sample->SamplingProbability();
	const double join_selectivity =
	    std::min(1.0, ((double)join_result->RecordCount() / join_key_sampling_probability) / (double)base_card);
	query_selectivity *= join_selectivity;

	join_results.push_back(join_result->GetMinHashSketch());
}

std::shared_ptr<OmniSketchCell> UncorrelatedCombinator::ComputeResult(size_t max_output_size) const {
	auto query_result = std::make_shared<OmniSketchCell>(max_output_size);
	if (!join_results.empty()) {
		const double card_est = std::round((double)base_card * query_selectivity);
		query_result->SetRecordCount((size_t)card_est);
		query_result->SetMinHashSketch(join_results.front()->Intersect(join_results));
	}

	return query_result;
}

std::shared_ptr<OmniSketchCell>
UncorrelatedCombinator::FilterProbeSet(std::shared_ptr<OmniSketch> omni_sketch,
                                       std::shared_ptr<OmniSketchCell> probe_sample) const {
	auto filtered_probe_set = std::make_shared<OmniSketchCell>(probe_sample->MaxSampleCount());

	const double card_est_per_match = ((double)base_card * query_selectivity) / (double)probe_sample->SampleCount();

	std::vector<std::shared_ptr<MinHashSketch>> sketches_to_intersect;
	sketches_to_intersect.reserve(join_results.size() + 1);
	sketches_to_intersect.insert(sketches_to_intersect.end(), join_results.begin(), join_results.end());
	sketches_to_intersect.push_back(nullptr);

	double card_est_sum = 0;
	std::vector<std::shared_ptr<OmniSketchCell>> matches(omni_sketch->Depth());
	auto probe_sample_it = probe_sample->GetMinHashSketch()->Iterator();
	for (; !probe_sample_it->IsAtEnd(); probe_sample_it->Next()) {
		const uint64_t hash = probe_sample_it->Current();
		const auto probe_result = omni_sketch->ProbeHash(hash, matches);
		sketches_to_intersect.back() = probe_result->GetMinHashSketch();
		bool has_result = sketches_to_intersect.back()->Size() > 0;

		if (!join_results.empty() && has_result) {
			// Intersect with other predicates
			auto combinator_result = sketches_to_intersect.front()->Intersect(sketches_to_intersect);
			has_result = combinator_result->Size() > 0;
		}

		if (has_result) {
			filtered_probe_set->AddRecord(hash);
			card_est_sum += card_est_per_match;
		}
	}

	card_est_sum /= probe_sample->SamplingProbability();

	filtered_probe_set->SetRecordCount((size_t)card_est_sum);
	return filtered_probe_set;
}

void UncorrelatedCombinator::AddUnfilteredRids(std::shared_ptr<OmniSketch> omni_sketch) {
	AddUnfilteredRids(omni_sketch->GetRids());
}

bool UncorrelatedCombinator::HasPredicates() const {
	return !join_results.empty();
}

void UncorrelatedCombinator::AddUnfilteredRids(std::shared_ptr<OmniSketchCell> rid_sample) {
	base_card = rid_sample->RecordCount();
	join_results.push_back(rid_sample->GetMinHashSketch());
}

} // namespace omnisketch
