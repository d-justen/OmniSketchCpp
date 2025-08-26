#include "execution/plan_node.hpp"

#include "combinator.hpp"
#include "min_hash_sketch/min_hash_sketch_map.hpp"
#include "registry.hpp"

namespace omnisketch {

PlanNode::PlanNode(std::string table_name_p, const size_t base_card_p, const size_t max_sample_count_p)
    : table_name(std::move(table_name_p)), base_card(base_card_p), max_sample_count(max_sample_count_p) {
}

void PlanNode::AddFilter(const std::string &column_name, std::shared_ptr<OmniSketchCell> probe_values) {
	filters.push_back(Filter {column_name, std::move(probe_values)});
}

void PlanNode::AddPKJoinExpansion(const std::string &join_column_name, std::shared_ptr<PlanNode> fk_side) {
	pk_join_expansions.push_back(PKJoinExpansion {std::move(fk_side), join_column_name});
}

void PlanNode::AddFKFKJoinExpansion(const std::string &this_column_name, std::shared_ptr<PlanNode> other_side,
                                    const std::string &other_column_name) {
	fk_fk_join_expansions.push_back(FKFKJoinExpansion {this_column_name, std::move(other_side), other_column_name});
}

void PlanNode::FindMatchesInNextJoin(std::vector<OmniSketchProbeResultSet> &filter_results,
                                     const std::shared_ptr<MinHashSketch> &current, size_t join_idx,
                                     size_t current_n_max, std::vector<double> &match_counts,
                                     OmniSketchCell &result) const {
	for (auto &item : filter_results[join_idx].results) {
		auto intersection = current->Intersect({current, item.rids->GetMinHashSketch()}, result.MaxSampleCount());
		if (intersection->Size() == 0) {
			continue;
		}

		current_n_max = std::max(current_n_max, item.n_max);
		double card_est = ((double)current_n_max / (double)result.MaxSampleCount()) * (double)intersection->Size();
		card_est = std::max(card_est, (double)intersection->Size());
		match_counts[join_idx] += card_est;

		if (join_idx < filter_results.size() - 1) {
			FindMatchesInNextJoin(filter_results, intersection, join_idx + 1, current_n_max, match_counts, result);
		} else {
			OmniSketchCell intersection_cell(std::move(intersection), (size_t)std::round(card_est));
			result.Combine(intersection_cell);
		}

		if (current->Size() == 0) {
			// All hashes processed
			return;
		}
	}
}

std::shared_ptr<OmniSketchCell> PlanNode::Estimate() const {
	std::vector<OmniSketchProbeResultSet> filter_results;
	filter_results.reserve(filters.size() + secondary_filters.size());

	auto &registry = Registry::Get();

	size_t min_max_sample_count = UINT64_MAX;
	std::vector<std::pair<std::shared_ptr<OmniSketch>, std::shared_ptr<OmniSketchCell>>> resolved_filters;
	resolved_filters.reserve(filters.size() + secondary_filters.size());

	for (auto &filter : filters) {
		auto omni_sketch = registry.GetOmniSketch(table_name, filter.column_name);
		min_max_sample_count = std::min(min_max_sample_count, omni_sketch->MinHashSketchSize());
		resolved_filters.emplace_back(omni_sketch, filter.probe_values);
	}

	for (auto &filter : secondary_filters) {
		auto omni_sketch = registry.FindReferencingOmniSketch(filter.table_name, filter.column_name, table_name);
		min_max_sample_count = std::min(min_max_sample_count, omni_sketch->MinHashSketchSize());
		resolved_filters.emplace_back(omni_sketch, filter.probe_values);
	}

	CombinedPredicateEstimator estimator(min_max_sample_count);
	estimator.SetBaseCard(base_card);
	for (auto &filter : resolved_filters) {
		estimator.AddPredicate(filter.first, filter.second);
	}
	if (!estimator.HasPredicates()) {
		estimator.AddUnfilteredRids(registry.GetRidSample(table_name), base_card);
	}
	estimator.Finalize();

	auto result = estimator.ComputeResult(UINT64_MAX);

	// PK-FK Join Expansion
	//double fallback_sel = 1;
	for (auto &pk_join : pk_join_expansions) {
		/*size_t primary_key_count = std::min(result->RecordCount(), MAX_JOIN_PROBE_COUNT);*/
		auto tmp_result = pk_join.foreign_key_node->ExpandPrimaryKeys(pk_join.foreign_key_column, *result);
		if (tmp_result->SampleCount() == 0) {
				// double filter_sel = /*1.0 / ((double)primary_key_count * 2)*/1;
			    // double multiplicity = std::max(1.0, (double)tmp_result->RecordCount() / (double )result->RecordCount());
			    // result->SetRecordCount((size_t)((double)result->RecordCount() * filter_sel * multiplicity));
			    result->SetRecordCount(tmp_result->RecordCount());
		} else {
			result = tmp_result;
		}
	}
	//if (fallback_sel < 1) {
	//	result->SetRecordCount(static_cast<size_t>((double)result->RecordCount() * fallback_sel));
	//}

	// FK/FK Join Multiple
	double fk_fk_multiple = CalculateFKFKMultiple();
	result->SetRecordCount((size_t)std::round((double)result->RecordCount() * fk_fk_multiple));

	return result;
}

std::shared_ptr<OmniSketchCell> PlanNode::ExpandPrimaryKeys(const std::string &column_name,
                                                            const OmniSketchCell &primary_keys) const {
	auto &registry = Registry::Get();
	auto omni_sketch = registry.GetOmniSketch(table_name, column_name);
	auto typed_omni_sketch = std::dynamic_pointer_cast<TypedPointOmniSketch<size_t>>(omni_sketch);

	std::shared_ptr<OmniSketchCell> filtered_rids;
	double filter_selectivity = 1.0;
	bool has_predicates = !filters.empty() || !secondary_filters.empty();
	if (has_predicates) {
		filtered_rids = Estimate();
		filter_selectivity = (double)filtered_rids->RecordCount() / (double)omni_sketch->RecordCount();
	}

	/*double multiplicity = 1.0;
	if (typed_omni_sketch) {
		size_t domain =
		    std::min(typed_omni_sketch->GetMax() - typed_omni_sketch->GetMin(), typed_omni_sketch->RecordCount());
		multiplicity = domain >= typed_omni_sketch->RecordCount()
		                          ? 1
		                          : (double)typed_omni_sketch->RecordCount() / (double)domain;

	}*/
	auto remaining_primary_keys = std::make_shared<OmniSketchCell>(primary_keys.MaxSampleCount());
	double result_card = 0;

	std::vector<std::shared_ptr<OmniSketchCell>> matches(omni_sketch->Depth());
	size_t probe_count = 0;
	for (auto pk_it = primary_keys.GetMinHashSketch()->Iterator();
	     !pk_it->IsAtEnd() && probe_count++ < MAX_JOIN_PROBE_COUNT; pk_it->Next()) {
		auto probe_result = omni_sketch->ProbeHash(pk_it->Current(), matches);
		size_t n_max = 0;
		for (auto &match : matches) {
			n_max = std::max(n_max, match->RecordCount());
		}
		if (probe_result->RecordCount() > 0) {
			if (has_predicates) {
				auto filtered_probe_result = OmniSketchCell::Intersect({probe_result, filtered_rids});
				if (filtered_probe_result->RecordCount() > 0) {
					remaining_primary_keys->GetMinHashSketch()->AddRecord(pk_it->Current());
					result_card += (double)filtered_probe_result->RecordCount();
				} else {
					double match_granularity = (double)n_max / (double)omni_sketch->MinHashSketchSize();
					result_card += match_granularity * filter_selectivity;
					// double filtered_rids_sel = filtered_rids->SampleCount() == 0 ? 1.0 : (double)filtered_rids->RecordCount() / (double)omni_sketch->RecordCount();
					// result_card += static_cast<size_t>(((double)primary_keys.RecordCount() / (double)primary_keys.SampleCount()) * multiplicity * filtered_rids_sel);
				}
			}
		} else {
			double match_granularity = (double)n_max / (double)omni_sketch->MinHashSketchSize();
			result_card += match_granularity * filter_selectivity;
			// double filtered_rids_sel = filtered_rids->SampleCount() == 0 ? 1.0 : (double)filtered_rids->RecordCount() / (double)omni_sketch->RecordCount();
			// result_card += static_cast<size_t>(((double)primary_keys.RecordCount() / (double)primary_keys.SampleCount()) * multiplicity * filtered_rids_sel);
		}
	}

	double p_sample = (double)std::min(primary_keys.SampleCount(), MAX_JOIN_PROBE_COUNT) / (double )primary_keys.RecordCount();
	remaining_primary_keys->SetRecordCount((size_t)(result_card / p_sample));
	return remaining_primary_keys;
}

std::string PlanNode::TableName() const {
	return table_name;
}

size_t PlanNode::BaseCard() const {
	return base_card;
}

double PlanNode::CalculateFKFKMultiple() const {
	if (fk_fk_join_expansions.empty()) {
		return 1.0;
	}

	auto &registry = Registry::Get();

	double multiple = 1;

	for (const auto &join : fk_fk_join_expansions) {
		const auto other_side_card_est = join.other_node->Estimate();
		const double other_side_multiple =
		    (double)other_side_card_est->RecordCount() / (double)join.other_node->BaseCard();
		// TODO: This multiplication is a uniformity assumption, remove if it hurts more than it helps
		multiple *= other_side_multiple;

		const auto this_omni_sketch = registry.GetOmniSketch(table_name, join.this_foreign_key_column);
		const auto other_omni_sketch =
		    registry.GetOmniSketch(join.other_node->TableName(), join.other_foreign_key_column);
		// TODO: Implement a tighter bound way to estimate FK-FK join
		const size_t combined_card = this_omni_sketch->RecordCount() * other_omni_sketch->RecordCount();
		// const size_t combined_card = this_omni_sketch->MultiplyRecordCounts(other_omni_sketch)->RecordCount();
		const double combined_multiple = (double)combined_card / (double)base_card;
		multiple *= combined_multiple;
	}

	return multiple;
}

PlanNode::OmniSketchProbeResultSet PlanNode::EstimatePredicate(const std::shared_ptr<PointOmniSketch> &omni_sketch,
                                                               const OmniSketchCell &probe_values) {
	if (probe_values.SampleCount() == 0) {
		// We only filter out the null values
		OmniSketchProbeResultSet result_set;
		result_set.p_sample = 1;
		result_set.results.push_back(OmniSketchProbeResult {omni_sketch->RecordCount(), omni_sketch->GetRids()});
		return result_set;
	}

	OmniSketchProbeResultSet result_set;
	result_set.p_sample = probe_values.SamplingProbability();

	std::vector<std::shared_ptr<OmniSketchCell>> matches(omni_sketch->Depth());

	if (probe_values.SampleCount() == 1) {
		auto probe_result = omni_sketch->ProbeHash(probe_values.GetMinHashSketch()->Iterator()->Current(), matches);
		if (probe_result->RecordCount() > 0) {
			size_t n_max = 0;
			for (auto &match : matches) {
				n_max = std::max(n_max, match->RecordCount());
			}
			result_set.results.push_back(OmniSketchProbeResult {n_max, probe_result});
		} else {
			result_set.results.push_back(OmniSketchProbeResult {0, std::make_shared<OmniSketchCell>()});
		}
		return result_set;
	}

	auto probe_result_map = std::make_shared<MinHashSketchMap>(UINT64_MAX);

	size_t card_sum = 0;
	size_t n_max_sum = 0;
	for (auto join_key_it = probe_values.GetMinHashSketch()->Iterator(); !join_key_it->IsAtEnd(); join_key_it->Next()) {
		auto hash = join_key_it->Current();
		auto probe_result = omni_sketch->ProbeHash(hash, matches);
		size_t n_max = 0;
		for (auto &match : matches) {
			n_max = std::max(n_max, match->RecordCount());
		}
		n_max_sum += n_max;

		for (auto it = probe_result->GetMinHashSketch()->Iterator(); !it->IsAtEnd(); it->Next()) {
			probe_result_map->AddRecord(it->Current(), n_max);
		}
		card_sum += probe_result->RecordCount();
	}
	probe_result_map->ShrinkToSize();

	if (omni_sketch->MinHashSketchSize() == 2) {
		result_set.results.push_back(
		    OmniSketchProbeResult {card_sum, std::make_shared<OmniSketchCell>(probe_result_map->Flatten(), card_sum)});
	} else {
		result_set.results.push_back(
		    OmniSketchProbeResult {static_cast<size_t>((double)n_max_sum / (double)probe_result_map->Size()),
		                           std::make_shared<OmniSketchCell>(probe_result_map, card_sum)});
	}
	return result_set;
}

void PlanNode::AddSecondaryFilter(const std::string &other_table_name, const std::string &other_column_name,
                                  const std::shared_ptr<OmniSketchCell> &probe_values) {
	secondary_filters.push_back(SecondaryFilter {other_table_name, other_column_name, probe_values});
}

} // namespace omnisketch
