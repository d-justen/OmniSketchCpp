#include "plan_generator.hpp"

namespace omnisketch {

void PlanGenerator::AddPredicate(const std::string &table_name, const std::string &column_name,
                                 const std::shared_ptr<OmniSketchCell>& probe_set) {
	auto &registry = Registry::Get();
	auto item = GetOrCreatePlanItem(table_name);
	auto sketch = registry.GetOmniSketch(table_name, column_name);
	item->combinator->AddPredicate(sketch, probe_set);
}

void PlanGenerator::AddJoin(const std::string &fk_table_name, const std::string &fk_column_name,
                            const std::string &pk_table_name) {
	auto &registry = Registry::Get();
	auto fk_column_sketch = registry.GetOmniSketch(fk_table_name, fk_column_name);
	auto fk_side_item = GetOrCreatePlanItem(fk_table_name);
	auto pk_side_item = GetOrCreatePlanItem(pk_table_name);
	pk_side_item->probes_into[fk_side_item] = fk_column_sketch;
	fk_side_item->probed_from.insert(pk_side_item);
}

bool CheckIfResolvable(const std::shared_ptr<PlanItem> &item) {
	if (!item->probed_from.empty()) {
		return false;
	}
	size_t not_resolvable_count = 0;
	for (auto &fk_side_item_pair : item->probes_into) {
		auto &fk_side_item = fk_side_item_pair.first;
		if (!fk_side_item->probes_into.empty() || fk_side_item->probed_from.size() != 1) {
			// FK side has other connections!
			not_resolvable_count++;
		}
	}
	return not_resolvable_count <= 1;
}

double PlanGenerator::EstimateCardinality() {
	auto& registry = Registry::Get();
	std::unordered_set<std::shared_ptr<PlanItem>> left_over;
	for (auto &plan_item_pair : plan_items) {
		auto& combinator = plan_item_pair.second->combinator;
		if (!combinator->HasPredicates() && plan_item_pair.second->probed_from.empty()) {
			// This relation is not filtered by any predicates or previous joins
			combinator->AddUnfilteredRids(registry.ProduceRidSample(plan_item_pair.first));
		}
		left_over.insert(plan_item_pair.second);
	}

	while (left_over.size() > 1) {
		std::unordered_set<std::shared_ptr<PlanItem>> left_over_next;
		for (const auto &plan_item : left_over) {
			if (!plan_item->probed_from.empty()) {
				left_over_next.insert(plan_item);
				continue;
			}

			if (plan_item->probes_into.empty()) {
				// This should be our last relation
				assert(left_over_next.empty() && "Unconnected relation.");
				left_over_next.insert(plan_item);
				continue;
			}

			if (plan_item->probes_into.size() == 1) {
				auto &fk_side_item_pair = *plan_item->probes_into.begin();
				auto &fk_side_item = fk_side_item_pair.first;
				auto &fk_sketch = fk_side_item_pair.second;

				fk_side_item->combinator->AddPredicate(fk_sketch, plan_item->combinator->ComputeResult(UINT64_MAX));
				fk_side_item->probed_from.erase(plan_item);
				continue;
			}

			if (CheckIfResolvable(plan_item)) {
				std::shared_ptr<PlanItem> item_to_probe_into = nullptr;
				std::shared_ptr<PointOmniSketch> sketch_to_probe_into = nullptr;
				auto pk_probe_set = plan_item->combinator->ComputeResult(UINT64_MAX);
				for (auto &fk_side_item_pair : plan_item->probes_into) {
					auto &fk_side_item = fk_side_item_pair.first;
					auto &fk_omni_sketch = fk_side_item_pair.second;
					if (!fk_side_item->probes_into.empty() || fk_side_item->probed_from.size() > 1) {
						assert(!item_to_probe_into);
						assert(!sketch_to_probe_into);
						item_to_probe_into = fk_side_item;
						sketch_to_probe_into = fk_omni_sketch;
					} else {
						pk_probe_set = fk_side_item->combinator->FilterProbeSet(fk_omni_sketch, pk_probe_set);
					}
				}
				item_to_probe_into->combinator->AddPredicate(sketch_to_probe_into, pk_probe_set);
				item_to_probe_into->probed_from.erase(item_to_probe_into);
				continue;
			}

			left_over_next.insert(plan_item);
		}
		left_over = left_over_next;
	}
	auto plan_item = *left_over.begin();
	assert(plan_item->probes_into.empty());
	assert(plan_item->probed_from.empty());
	auto card_est = plan_item->combinator->ComputeResult(UINT64_MAX);
	return card_est->RecordCount();
}

std::shared_ptr<PlanItem> PlanGenerator::GetOrCreatePlanItem(const std::string &table_name) {
	auto item = plan_items[table_name];
	if (!item) {
		item = std::make_shared<PlanItem>();
		item->combinator = combinator_creator();
		plan_items[table_name] = item;
	}
	return item;
}

} // namespace omnisketch
