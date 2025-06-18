#pragma once

#include "omni_sketch/omni_sketch.hpp"
#include "omni_sketch/omni_sketch_cell.hpp"

namespace omnisketch {

class PlanNode {
public:
	PlanNode(std::string table_name, size_t base_card, size_t max_sample_count);

	// Node manipulation
	void AddFilter(const std::string &column_name, std::shared_ptr<OmniSketchCell> probe_values);
	void AddSecondaryFilter(const std::string &other_table_name, const std::string &other_column_name, const std::shared_ptr<OmniSketchCell> &probe_values);
	// TODO: AddTwoSidedPredicate (e.g., a.col1 = a.col2)
	void AddPKJoinExpansion(const std::string &join_column_name, std::shared_ptr<PlanNode> fk_side);
	void AddFKFKJoinExpansion(const std::string &this_column_name, std::shared_ptr<PlanNode> other_side, const std::string &other_column_name);

	// Execution
	std::shared_ptr<OmniSketchCell> Estimate() const;
	std::shared_ptr<OmniSketchCell> ExpandPrimaryKeys(const std::string &column_name, const OmniSketchCell &primary_keys) const;

	// Info
	std::string TableName() const;
	size_t BaseCard() const;

protected:
	double CalculateFKFKMultiple() const;

	struct OmniSketchProbeResult {
		size_t n_max;
		std::shared_ptr<OmniSketchCell> rids;
	};

	struct OmniSketchProbeResultSet {
		double p_sample;
		std::vector<OmniSketchProbeResult> results;
	};

	static OmniSketchProbeResultSet EstimatePredicate(const std::shared_ptr<PointOmniSketch> &omni_sketch, const OmniSketchCell &probe_values) ;
	void FindMatchesInNextJoin(std::vector<OmniSketchProbeResultSet> &filter_results, const std::shared_ptr<MinHashSketch> &current, size_t join_idx,
	                  size_t current_n_max, std::vector<double> &match_counts,
	                  OmniSketchCell &result) const;
	protected:
	const std::string table_name;
	const size_t base_card;
	const size_t max_sample_count;

	struct Filter {
		std::string column_name;
		std::shared_ptr<OmniSketchCell> probe_values;
	};

	struct SecondaryFilter {
		std::string table_name;
		std::string column_name;
		std::shared_ptr<OmniSketchCell> probe_values;
	};

	struct PKJoinExpansion {
		std::shared_ptr<PlanNode> foreign_key_node;
		std::string foreign_key_column;
	};

	struct FKFKJoinExpansion {
		std::string this_foreign_key_column;
		std::shared_ptr<PlanNode> other_node;
		std::string other_foreign_key_column;
	};

	std::vector<Filter> filters;
	std::vector<SecondaryFilter> secondary_filters;
	std::vector<PKJoinExpansion> pk_join_expansions;
	std::vector<FKFKJoinExpansion> fk_fk_join_expansions;
};

} // namespace omnisketch
