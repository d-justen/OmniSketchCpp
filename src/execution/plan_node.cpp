#include "execution/plan_node.hpp"

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

	// Primary Filters
	for (const auto &filter : filters) {
		min_max_sample_count =
		    std::min(min_max_sample_count, registry.GetOmniSketch(table_name, filter.column_name)->MinHashSketchSize());
		filter_results.push_back(
		    EstimatePredicate(registry.GetOmniSketch(table_name, filter.column_name), *filter.probe_values));
	}

	// Secondary Filters
	for (const auto &filter : secondary_filters) {
		min_max_sample_count = std::min(
		    min_max_sample_count,
		    registry.FindReferencingOmniSketch(filter.table_name, filter.column_name, table_name)->MinHashSketchSize());
		filter_results.push_back(
		    EstimatePredicate(registry.FindReferencingOmniSketch(filter.table_name, filter.column_name, table_name),
		                      *filter.probe_values));
	}

	if (min_max_sample_count == UINT64_MAX) {
		min_max_sample_count = registry.GetNextBestSampleCount(table_name);
	}

	// Process Filters
	auto result = std::make_shared<OmniSketchCell>(min_max_sample_count);

	if (filter_results.size() == 1) {
		for (auto &match : filter_results.front().results) {
			result->Combine(*match.rids);
		}
		result->SetRecordCount((size_t)std::round((double)result->RecordCount() / filter_results.front().p_sample));
	} else if (!filter_results.empty()) {
		std::vector<double> match_counts(filter_results.size());
		for (auto &probe_result : filter_results.front().results) {
			match_counts[0] += (double)probe_result.rids->RecordCount();
			FindMatchesInNextJoin(filter_results, probe_result.rids->GetMinHashSketch(), 1, probe_result.n_max,
			                      match_counts, *result);
		}

		auto result_card = (double)base_card;
		for (size_t predicate_idx = 0; predicate_idx < match_counts.size(); predicate_idx++) {
			double last_card_unscaled = predicate_idx == 0 ? (double)base_card : match_counts[predicate_idx - 1];
			double next_card_scaled = match_counts[predicate_idx] / filter_results[predicate_idx].p_sample;
			double sel = next_card_scaled / last_card_unscaled;
			result_card *= sel;
		}

		result->SetRecordCount((size_t)(std::round(result_card)));
	}

	// PK-FK Join Expansion
	if (filter_results.empty()) {
		result = registry.ProduceRidSample(table_name);
	}

	for (auto &pk_join : pk_join_expansions) {
		result = pk_join.foreign_key_node->ExpandPrimaryKeys(pk_join.foreign_key_column, *result);
	}

	// FK/FK Join Multiple
	double fk_fk_multiple = CalculateFKFKMultiple();
	result->SetRecordCount((size_t)std::round((double)result->RecordCount() * fk_fk_multiple));

	return result;
}

std::shared_ptr<OmniSketchCell> PlanNode::ExpandPrimaryKeys(const std::string &column_name,
                                                            const OmniSketchCell &primary_keys) const {
	auto &registry = Registry::Get();
	auto omni_sketch = registry.GetOmniSketch(table_name, column_name);

	auto filtered_rids = Estimate();
	auto remaining_primary_keys = std::make_shared<OmniSketchCell>(primary_keys.MaxSampleCount());
	size_t result_card = 0;

	std::vector<std::shared_ptr<OmniSketchCell>> matches(omni_sketch->Depth());
	for (auto pk_it = primary_keys.GetMinHashSketch()->Iterator(); !pk_it->IsAtEnd(); pk_it->Next()) {
		auto probe_result = omni_sketch->ProbeHash(pk_it->Current(), matches);
		size_t n_max = 0;
		for (auto &match : matches) {
			n_max = std::max(n_max, match->RecordCount());
		}
		auto filtered_probe_result = OmniSketchCell::Intersect({probe_result, filtered_rids});
		if (filtered_probe_result->RecordCount() > 0) {
			remaining_primary_keys->GetMinHashSketch()->AddRecord(pk_it->Current());
			result_card += filtered_probe_result->RecordCount();
		}
	}

	filtered_rids->SetRecordCount(result_card);
	return filtered_rids;
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
		const size_t combined_card = this_omni_sketch->MultiplyRecordCounts(other_omni_sketch)->RecordCount();
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

	const auto &probe_hash_it = probe_values.GetMinHashSketch()->Iterator();

	OmniSketchProbeResultSet result_set;
	result_set.p_sample = probe_values.SamplingProbability();
	result_set.results.reserve(probe_values.SampleCount());

	std::vector<std::shared_ptr<OmniSketchCell>> matches(omni_sketch->Depth());
	for (; !probe_hash_it->IsAtEnd(); probe_hash_it->Next()) {
		auto probe_result = omni_sketch->ProbeHash(probe_hash_it->Current(), matches);
		if (probe_result->RecordCount() > 0) {
			size_t n_max = 0;
			for (auto &match : matches) {
				n_max = std::max(n_max, match->RecordCount());
			}
			result_set.results.push_back(OmniSketchProbeResult {n_max, probe_result});
		}
	}

	if (result_set.results.empty()) {
		result_set.results.push_back(OmniSketchProbeResult {0, std::make_shared<OmniSketchCell>()});
	}

	return result_set;
}

void PlanNode::AddSecondaryFilter(const std::string &other_table_name, const std::string &other_column_name,
                                  const std::shared_ptr<OmniSketchCell> &probe_values) {
	secondary_filters.push_back(SecondaryFilter {other_table_name, other_column_name, probe_values});
}

} // namespace omnisketch
