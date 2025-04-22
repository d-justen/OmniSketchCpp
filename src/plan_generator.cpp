#include "plan_generator.hpp"

namespace omnisketch {

void PlanGenerator::AddPredicate(const std::string &table_name, const std::string &column_name,
                                 const std::shared_ptr<OmniSketchCell> &probe_set) {
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

double PlanGenerator::EstimateCardinality() const {
	auto &registry = Registry::Get();
	std::vector<std::pair<std::shared_ptr<PlanItem>, bool>> item_vec;
	for (auto &plan_item_pair : plan_items) {
		auto &combinator = plan_item_pair.second->combinator;
		if (!combinator->HasPredicates() && plan_item_pair.second->probed_from.empty()) {
			// This relation is not filtered by any predicates or previous joins
			combinator->AddUnfilteredRids(registry.ProduceRidSample(plan_item_pair.first));
		}
		item_vec.emplace_back(plan_item_pair.second, true);
	}

	size_t items_alive = item_vec.size();
	while (items_alive > 1) {
		for (const auto &plan_item_pair : item_vec) {
			if (!plan_item_pair.second) {
				continue;
			}

			const auto &plan_item = plan_item_pair.first;
			if (!plan_item->probed_from.empty()) {
				continue;
			}

			if (plan_item->probes_into.empty()) {
				// This should be our last relation
				if (items_alive != 1) {
					throw std::logic_error("Unconnected relation: " + std::to_string(items_alive) + " of " + std::to_string(item_vec.size()) + " left.");
				}
				continue;
			}

			if (plan_item->probes_into.size() == 1) {
				auto &fk_side_item_pair = *plan_item->probes_into.begin();
				auto &fk_side_item = fk_side_item_pair.first;
				auto &fk_sketch = fk_side_item_pair.second;

				auto pk_probe_set = plan_item->combinator->ComputeResult(UINT64_MAX);
				if (pk_probe_set->RecordCount() == 0) {
					return 0;
				}

				fk_side_item->combinator->AddPredicate(fk_sketch, pk_probe_set);
				fk_side_item->probed_from.erase(plan_item);
				std::find(item_vec.begin(), item_vec.end(), std::make_pair(plan_item, true))->second = false;
				items_alive--;
				continue;
			}

			if (CheckIfResolvable(plan_item)) {
				std::shared_ptr<PlanItem> item_to_probe_into = nullptr;
				std::shared_ptr<PointOmniSketch> sketch_to_probe_into = nullptr;
				auto pk_probe_set = plan_item->combinator->ComputeResult(UINT64_MAX);
				if (pk_probe_set->RecordCount() == 0) {
					return 0;
				}

				size_t current_fk_idx = 0;
				for (auto &fk_side_item_pair : plan_item->probes_into) {
					auto &fk_side_item = fk_side_item_pair.first;
					auto &fk_omni_sketch = fk_side_item_pair.second;
					if (!fk_side_item->probes_into.empty() || fk_side_item->probed_from.size() > 1 ||
					    current_fk_idx == plan_item->probes_into.size() - 1) {
						assert(!item_to_probe_into);
						assert(!sketch_to_probe_into);
						item_to_probe_into = fk_side_item;
						sketch_to_probe_into = fk_omni_sketch;
					} else {
						pk_probe_set = fk_side_item->combinator->FilterProbeSet(fk_omni_sketch, pk_probe_set);
						fk_side_item->probed_from.erase(plan_item);
						std::find(item_vec.begin(), item_vec.end(), std::make_pair(fk_side_item, true))->second = false;
						items_alive--;
						if (pk_probe_set->RecordCount() == 0) {
							return 0.0;
						}
					}
					current_fk_idx++;
				}

				if (item_to_probe_into && sketch_to_probe_into) {
					item_to_probe_into->combinator->AddPredicate(sketch_to_probe_into, pk_probe_set);
					item_to_probe_into->probed_from.erase(plan_item);
					std::find(item_vec.begin(), item_vec.end(), std::make_pair(plan_item, true))->second = false;
					items_alive--;
				}
				plan_item->probes_into.clear();
			}
		}
	}

	std::shared_ptr<PlanItem> plan_item;
	for (auto &item : item_vec) {
		if (item.second) {
			assert(!plan_item);
			plan_item = item.first;
		}
	}

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
