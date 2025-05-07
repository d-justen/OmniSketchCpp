#include "plan_generator.hpp"

namespace omnisketch {

void PlanGenerator::AddPredicate(const std::string &table_name, const std::string &column_name,
                                 const std::shared_ptr<OmniSketchCell> &probe_set) {
	auto item = GetOrCreatePlanItem(table_name);
	assert(item->predicates.find(column_name) == item->predicates.end());
	item->predicates[column_name] = probe_set;
}

void PlanGenerator::AddJoin(const std::string &fk_table_name, const std::string &fk_column_name,
                            const std::string &pk_table_name) {
	auto &registry = Registry::Get();
	auto fk_column_sketch = registry.GetOmniSketch(fk_table_name, fk_column_name);
	auto fk_side_item = GetOrCreatePlanItem(fk_table_name);
	auto pk_side_item = GetOrCreatePlanItem(pk_table_name);
	pk_side_item->probes_into[fk_table_name] = fk_column_sketch;
	fk_side_item->probed_from.insert(pk_table_name);
}

bool PlanGenerator::CheckIfResolvable(const std::shared_ptr<PlanExecItem> &item) const {
	if (!item->probed_from.empty()) {
		return false;
	}
	size_t not_resolvable_count = 0;
	for (auto &fk_side_item_pair : item->probes_into) {
		const auto &fk_side_item = plan_items.find(fk_side_item_pair.first)->second;
		if (!fk_side_item->probes_into.empty() || fk_side_item->probed_from.size() != 1) {
			// FK side has other connections!
			not_resolvable_count++;
		}
	}
	return not_resolvable_count <= 1;
}

double PlanGenerator::EstimateCardinality() const {
	auto &registry = Registry::Get();

	// Build join graph
	std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> exec_items;
	for (auto &plan_item_pair : plan_items) {
		const auto &table_name = plan_item_pair.first;
		auto &plan_item = plan_item_pair.second;

		auto exec_item = std::make_shared<PlanExecItem>();
		exec_item->combinator = combinator_creator();
		exec_item->probes_into = plan_item->probes_into;
		exec_item->probed_from = plan_item->probed_from;
		exec_items[table_name] = exec_item;
	}

	// Add constant predicates
	std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> filtered_exec_items;
	for (auto &exec_item_pair : exec_items) {
		const auto &table_name = exec_item_pair.first;
		const auto &exec_item = exec_item_pair.second;
		const auto &plan_item = plan_items.find(table_name)->second;

		if (plan_item->predicates.empty() && plan_item->probed_from.empty()) {
			assert(exec_items.size() == 1 || !exec_item->probes_into.empty());
			// No predicates! If foreign key column has no nulls, we can ignore this join
			if (exec_item->probes_into.size() == 1) {
				// Directly merge into fact table
				const auto &probes_into_table_name = exec_item->probes_into.begin()->first;
				auto &probes_into_sketch = exec_item->probes_into.begin()->second;
				auto &probes_into_item = exec_items[probes_into_table_name];

				if (probes_into_sketch->CountNulls() > 0) {
					// FK column has nulls -> Join has selectivity < 1 even without predicate on dimension table
					probes_into_item->combinator->AddUnfilteredRids(probes_into_sketch);
				}
				probes_into_item->probed_from.erase(table_name);
			} else {
				// Add to dimension table
				exec_item->combinator->AddUnfilteredRids(registry.ProduceRidSample(table_name));
				filtered_exec_items[table_name] = exec_item;
			}
			continue;
		}

		bool all_predicates_referenced = true;
		for (const auto &predicate : plan_item->predicates) {
			const auto &column_name = predicate.first;
			const auto &probe_values = predicate.second;
			if (plan_item->probed_from.empty() && plan_item->probes_into.size() == 1) {
				const auto &probes_into_table_name = exec_item->probes_into.begin()->first;
				auto probes_into_item = exec_items.find(probes_into_table_name)->second;
				auto referencing_sketch =
				    registry.FindReferencingOmniSketch(table_name, column_name, plan_item->probes_into.begin()->first);
				if (referencing_sketch && use_referencing_sketches) {
					if (referencing_sketch->Type() == OmniSketchType::FOREIGN_SORTED) {
						auto &probes_into_sketch = exec_item->probes_into.begin()->second;
						probes_into_item->combinator->AddPredicate(probes_into_sketch,
						                                           referencing_sketch->ProbeHashedSet(probe_values));
					} else {
						probes_into_item->combinator->AddPredicate(referencing_sketch, probe_values);
					}
					probes_into_item->probed_from.erase(table_name);
					continue;
				}
			}
			exec_item->combinator->AddPredicate(registry.GetOmniSketch(table_name, column_name), probe_values);
			all_predicates_referenced = false;
		}
		if (!all_predicates_referenced || plan_item->predicates.empty()) {
			filtered_exec_items[table_name] = exec_item;
		}
	}

	// Execute!
	size_t items_alive = filtered_exec_items.size();
	while (items_alive > 1) {
		for (const auto &exec_item_pair : filtered_exec_items) {
			const auto &table_name = exec_item_pair.first;
			const auto &exec_item = exec_item_pair.second;

			if (exec_item->done) {
				continue;
			}

			if (!exec_item->probed_from.empty()) {
				continue;
			}

			if (exec_item->probes_into.empty()) {
				// This should be our last relation
				if (items_alive != 1) {
					throw std::logic_error("Unconnected relation: " + std::to_string(items_alive) + " of " +
					                       std::to_string(filtered_exec_items.size()) + " left.");
				}
				continue;
			}

			if (exec_item->probes_into.size() == 1) {
				const auto &fk_table_name = exec_item->probes_into.begin()->first;
				const auto &fk_sketch = exec_item->probes_into.begin()->second;
				auto &fk_side_item = filtered_exec_items[fk_table_name];

				auto pk_probe_set = exec_item->combinator->ComputeResult(UINT64_MAX);
				if (pk_probe_set->RecordCount() == 0) {
					return 0;
				}

				fk_side_item->combinator->AddPredicate(fk_sketch, pk_probe_set);
				fk_side_item->probed_from.erase(table_name);
				exec_item->done = true;
				items_alive--;
				continue;
			}

			if (CheckIfResolvable(exec_item)) {
				std::shared_ptr<PlanExecItem> item_to_probe_into = nullptr;
				std::shared_ptr<PointOmniSketch> sketch_to_probe_into = nullptr;
				auto pk_probe_set = exec_item->combinator->ComputeResult(UINT64_MAX);
				if (pk_probe_set->RecordCount() == 0) {
					return 0;
				}

				size_t current_fk_idx = 0;
				for (auto &fk_side_item_pair : exec_item->probes_into) {
					auto &fk_side_item = filtered_exec_items[fk_side_item_pair.first];
					auto &fk_omni_sketch = fk_side_item_pair.second;
					if (!fk_side_item->probes_into.empty() || fk_side_item->probed_from.size() > 1 ||
					    current_fk_idx == exec_item->probes_into.size() - 1) {
						assert(!item_to_probe_into);
						assert(!sketch_to_probe_into);
						item_to_probe_into = fk_side_item;
						sketch_to_probe_into = fk_omni_sketch;
					} else {
						pk_probe_set = fk_side_item->combinator->FilterProbeSet(fk_omni_sketch, pk_probe_set);
						fk_side_item->probed_from.erase(table_name);
						fk_side_item->done = true;
						items_alive--;
						if (pk_probe_set->RecordCount() == 0) {
							return 0;
						}
					}
					current_fk_idx++;
				}

				if (item_to_probe_into && sketch_to_probe_into) {
					item_to_probe_into->combinator->AddPredicate(sketch_to_probe_into, pk_probe_set);
					item_to_probe_into->probed_from.erase(table_name);
					exec_item->done = true;
					items_alive--;
				}
				exec_item->probes_into.clear();
			}
		}
	}

	std::shared_ptr<PlanExecItem> plan_item;
	for (auto &item : filtered_exec_items) {
		if (!item.second->done) {
			assert(!plan_item);
			plan_item = item.second;
		}
	}

	assert(plan_item->probes_into.empty());
	assert(plan_item->probed_from.empty());
	auto card_est = plan_item->combinator->ComputeResult(UINT64_MAX);
	return (double)card_est->RecordCount();
}

std::shared_ptr<PlanItem> PlanGenerator::GetOrCreatePlanItem(const std::string &table_name) {
	auto item = plan_items[table_name];
	if (!item) {
		item = std::make_shared<PlanItem>();
		plan_items[table_name] = item;
	}
	return item;
}

} // namespace omnisketch
