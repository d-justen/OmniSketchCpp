#pragma once

#include "combinator.hpp"
#include "registry.hpp"

#include <unordered_set>

namespace omnisketch {

struct PlanItem {
	std::unordered_map<std::string, std::shared_ptr<OmniSketchCell>> predicates;
	std::unordered_map<std::string, std::shared_ptr<PointOmniSketch>> probes_into;
	std::unordered_set<std::string> probed_from;
	std::vector<std::pair<std::shared_ptr<PointOmniSketch>, std::pair<std::string, std::shared_ptr<PointOmniSketch>>>> fk_fk_connections;
};

struct PlanExecItem {
	std::shared_ptr<OmniSketchCombinator> combinator;
	std::unordered_map<std::string, std::shared_ptr<PointOmniSketch>> probes_into;
	std::unordered_set<std::string> probed_from;
	std::vector<std::pair<std::shared_ptr<PointOmniSketch>, std::pair<std::shared_ptr<PlanExecItem>, std::shared_ptr<PointOmniSketch>>>> fk_fk_connections;
	bool done = false;
	std::vector<std::string> included_relations;
};

class PlanGenerator {
public:
	explicit PlanGenerator(
	    const std::shared_ptr<OmniSketchCombinator> &combinator = std::make_shared<ExhaustiveCombinator>(),
	    bool use_referencing_sketches_p = false)
	    : use_referencing_sketches(use_referencing_sketches_p) {
		if (std::dynamic_pointer_cast<ExhaustiveCombinator>(combinator)) {
			combinator_creator = []() {
				return std::make_shared<ExhaustiveCombinator>();
			};
		} else {
			assert(std::dynamic_pointer_cast<UncorrelatedCombinator>(combinator));
			combinator_creator = []() {
				return std::make_shared<UncorrelatedCombinator>();
			};
		}
	}
	void AddPredicate(const std::string &table_name, const std::string &column_name,
	                  const std::shared_ptr<OmniSketchCell> &probe_set);
	void AddJoin(const std::string &fk_table_name, const std::string &fk_column_name, const std::string &pk_table_name);
	void AddFKFKJoin(const std::string &table_name_1, const std::string &column_name_1, const std::string &table_name_2, const std::string &column_name_2);
	double EstimateCardinality() const;

protected:
	std::shared_ptr<PlanItem> GetOrCreatePlanItem(const std::string &table_name);
	bool CheckIfResolvable(const PlanExecItem &item) const;
	std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> CreateInitialExecutionPlan() const;
	std::unordered_map<std::string, std::shared_ptr<PlanExecItem>>
	RemoveUnselectiveJoins(const std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> &before) const;
	std::unordered_map<std::string, std::shared_ptr<PlanExecItem>>
	AddPredicatesToPlan(const std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> &before) const;
	bool RelationIsEar(const PlanExecItem &item) const;
	bool MergeEar(PlanExecItem& item, const std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> &all_items) const;

	std::unordered_map<std::string, std::shared_ptr<PlanItem>> plan_items;
	std::function<std::shared_ptr<OmniSketchCombinator>()> combinator_creator;
	bool use_referencing_sketches = false;
};

} // namespace omnisketch
