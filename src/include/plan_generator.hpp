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
    std::shared_ptr<CombinedPredicateEstimator> estimator;
    std::unordered_map<std::string, std::shared_ptr<PointOmniSketch>> probes_into;
    std::unordered_set<std::string> probed_from;
    bool done = false;
};

class PlanGenerator {
public:
    explicit PlanGenerator(bool use_referencing_sketches_p = false)
        : use_referencing_sketches(use_referencing_sketches_p) {
    }
    void AddPredicate(const std::string& table_name, const std::string& column_name,
                      const std::shared_ptr<OmniSketchCell>& probe_set);
    void AddJoin(const std::string& fk_table_name, const std::string& fk_column_name, const std::string& pk_table_name);
    double EstimateCardinality() const;

protected:
    std::shared_ptr<PlanItem> GetOrCreatePlanItem(const std::string& table_name);
    bool CheckIfResolvable(const std::shared_ptr<PlanExecItem>& item) const;
    std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> CreateInitialExecutionPlan() const;
    std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> RemoveUnselectiveJoins(
        const std::unordered_map<std::string, std::shared_ptr<PlanExecItem>>& before) const;
    std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> AddPredicatesToPlan(
        const std::unordered_map<std::string, std::shared_ptr<PlanExecItem>>& before) const;

    std::unordered_map<std::string, std::shared_ptr<PlanItem>> plan_items;
    bool use_referencing_sketches = false;
};

}  // namespace omnisketch
