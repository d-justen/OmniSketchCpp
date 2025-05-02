#pragma once

#include "combinator.hpp"
#include "registry.hpp"

#include <unordered_set>

namespace omnisketch {

struct PlanItem {
	std::unordered_map<std::string, std::shared_ptr<OmniSketchCell>> predicates;
	std::unordered_map<std::string, std::shared_ptr<PointOmniSketch>> probes_into;
	std::unordered_set<std::string> probed_from;
};

struct PlanExecItem {
	std::shared_ptr<OmniSketchCombinator> combinator;
	std::unordered_map<std::string, std::shared_ptr<PointOmniSketch>> probes_into;
	std::unordered_set<std::string> probed_from;
	bool done = false;
};

class PlanGenerator {
public:
	explicit PlanGenerator(
	    const std::shared_ptr<OmniSketchCombinator> &combinator = std::make_shared<ExhaustiveCombinator>()) {
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
	double EstimateCardinality() const;

protected:
	std::shared_ptr<PlanItem> GetOrCreatePlanItem(const std::string &table_name);
	bool CheckIfResolvable(const std::shared_ptr<PlanExecItem> &item) const;
	std::unordered_map<std::string, std::shared_ptr<PlanItem>> plan_items;
	std::function<std::shared_ptr<OmniSketchCombinator>()> combinator_creator;
};

} // namespace omnisketch
