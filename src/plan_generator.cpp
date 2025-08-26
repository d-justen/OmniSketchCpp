#include "plan_generator.hpp"

namespace omnisketch {

void PlanGenerator::AddPredicate(const std::string& table_name, const std::string& column_name,
                                 const std::shared_ptr<OmniSketchCell>& probe_set) {
	auto item = GetOrCreatePlanItem(table_name);
	if (item->predicates.find(column_name) != item->predicates.end()) {
		throw std::runtime_error("Predicate already exists for column: " + table_name + "." + column_name);
	}
	item->predicates[column_name] = probe_set;
}

void PlanGenerator::AddJoin(const std::string& fk_table_name, const std::string& fk_column_name,
                            const std::string& pk_table_name) {
	auto& registry = Registry::Get();
	auto fk_column_sketch = registry.GetOmniSketch(fk_table_name, fk_column_name);
	auto fk_side_item = GetOrCreatePlanItem(fk_table_name);
	auto pk_side_item = GetOrCreatePlanItem(pk_table_name);
	pk_side_item->probes_into[fk_table_name] = fk_column_sketch;
	fk_side_item->probed_from.insert(pk_table_name);
}

bool PlanGenerator::CheckIfResolvable(const std::shared_ptr<PlanExecItem>& item) const {
	if (!item->probed_from.empty()) {
		return false;
	}
	
	size_t notResolvableCount = 0;
	for (const auto& fkSideItemPair : item->probes_into) {
		const auto fkSideItemIt = plan_items.find(fkSideItemPair.first);
		if (fkSideItemIt == plan_items.end()) {
			continue; // Skip if item not found
		}
		
		const auto& fkSideItem = fkSideItemIt->second;
		if (!fkSideItem->probes_into.empty() || fkSideItem->probed_from.size() != 1) {
			// FK side has other connections!
			++notResolvableCount;
		}
	}
	return notResolvableCount <= 1;
}

size_t GetMaxSampleCount(const std::string& table_name, const PlanItem& item) {
	auto& registry = Registry::Get();
	if (item.predicates.empty()) {
		return registry.GetMinHashSketchSize(table_name);
	}
	
	size_t maxSampleCount = UINT64_MAX;
	for (const auto& predicate : item.predicates) {
		auto sketch = registry.GetOmniSketch(table_name, predicate.first);
		maxSampleCount = std::min(maxSampleCount, sketch->MinHashSketchSize());
	}
	return maxSampleCount;
}

std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> PlanGenerator::CreateInitialExecutionPlan() const {
	std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> execItems;
	execItems.reserve(plan_items.size()); // Reserve space to avoid rehashing
	
	for (const auto& planItemPair : plan_items) {
		const auto& tableName = planItemPair.first;
		const auto& planItem = planItemPair.second;

		auto execItem = std::make_shared<PlanExecItem>();
		execItem->estimator = std::make_shared<CombinedPredicateEstimator>(GetMaxSampleCount(tableName, *planItem));
		execItem->probes_into = planItem->probes_into;
		execItem->probed_from = planItem->probed_from;
		execItems.emplace(tableName, std::move(execItem));
	}
	return execItems;
}

std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> PlanGenerator::RemoveUnselectiveJoins(
    const std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> &before) const {
	auto &registry = Registry::Get();

	std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> after;
	for (const auto &plan_pair : before) {
		const auto &table_name = plan_pair.first;
		const auto &pk_side_exec_item = plan_pair.second;
		const auto &plan_item = plan_items.find(table_name)->second;

		if (plan_item->predicates.empty() && plan_item->probed_from.empty()) {
			// No predicates! If foreign key column has no nulls, we can ignore this join
			if (pk_side_exec_item->probes_into.size() == 1) {
				// For now, let's only look into tables that have exactly one join partner
				const auto &fk_side_plan_item = *pk_side_exec_item->probes_into.begin();
				const auto &fk_side_table_name = fk_side_plan_item.first;
				const auto &fk_side_sketch = fk_side_plan_item.second;
				const auto &fk_side_exec_item = before.at(fk_side_table_name);

				if (fk_side_sketch->CountNulls() > 0) {
					// FK column has nulls -> Join has selectivity < 1 even without predicate on dimension table
					// Create MinHashSketch from unfiltered foreign key OmniSketch
					fk_side_exec_item->estimator->AddUnfilteredRids(fk_side_sketch);
				}

				// Remove edge and do not add primary key side to result map
				fk_side_exec_item->probed_from.erase(table_name);
			} else {
				// Primary key side is not filtered out and gets unfiltered record ids as a predicate
				pk_side_exec_item->estimator->AddUnfilteredRids(registry.ProduceRidSample(table_name),
				                                                registry.GetBaseTableCard(table_name));
				after[table_name] = pk_side_exec_item;
			}
		} else {
			after[table_name] = pk_side_exec_item;
		}
	}

	return after;
}

std::unordered_map<std::string, std::shared_ptr<PlanExecItem>>
PlanGenerator::AddPredicatesToPlan(const std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> &before) const {
	auto &registry = Registry::Get();
	std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> after;

	for (const auto &plan_pair : before) {
		const auto &table_name = plan_pair.first;
		const auto &exec_item = plan_pair.second;
		const auto &plan_item = plan_items.find(table_name)->second;

		bool all_predicates_referenced = true;
		for (const auto &predicate : plan_item->predicates) {
			const auto &column_name = predicate.first;
			const auto &probe_values = predicate.second;

			if (plan_item->probed_from.empty() && plan_item->probes_into.size() == 1) {
				// TODO: What happens if graph ear probes into multiple foreign key sides?
				// This is a graph ear!
				const auto &fk_side_table_name = exec_item->probes_into.begin()->first;
				auto fk_side_exec_item = before.find(fk_side_table_name)->second;
				auto referencing_sketch =
				    registry.FindReferencingOmniSketch(table_name, column_name, plan_item->probes_into.begin()->first);

				if (referencing_sketch && use_referencing_sketches) {
					// Coordinated sketch has rids of its join partner: we can treat it like a predicate on the FK
					// side
					fk_side_exec_item->estimator->AddPredicate(referencing_sketch, probe_values);
					fk_side_exec_item->probed_from.erase(table_name);
					continue;
				}
			}
			exec_item->estimator->AddPredicate(registry.GetOmniSketch(table_name, column_name), probe_values);
			all_predicates_referenced = false;
		}
		if (!all_predicates_referenced || plan_item->predicates.empty()) {
			after[table_name] = exec_item;
		}
	}
	return after;
}

double PlanGenerator::EstimateCardinality() const {
	auto &registry = Registry::Get();

	auto exec_items = CreateInitialExecutionPlan();
	exec_items = RemoveUnselectiveJoins(exec_items);
	exec_items = AddPredicatesToPlan(exec_items);

	if (exec_items.size() == 1 && !exec_items.begin()->second->estimator->HasPredicates()) {
		// Single table without any predicates: return base table cardinality
		return (double)registry.GetBaseTableCard(exec_items.begin()->first);
	}

	// Execute!
	size_t items_alive = exec_items.size();
	while (items_alive > 1) {
		for (const auto &exec_item_pair : exec_items) {
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
					                       std::to_string(exec_items.size()) + " left.");
				}
				continue;
			}

			if (exec_item->probes_into.size() == 1) {
				const auto &fk_table_name = exec_item->probes_into.begin()->first;
				const auto &fk_sketch = exec_item->probes_into.begin()->second;
				auto &fk_side_item = exec_items[fk_table_name];

				exec_item->estimator->Finalize();
				auto pk_probe_set = exec_item->estimator->ComputeResult(UINT64_MAX);
				if (pk_probe_set->RecordCount() == 0) {
					return 0;
				}

				fk_side_item->estimator->AddPredicate(fk_sketch, pk_probe_set);
				fk_side_item->probed_from.erase(table_name);
				exec_item->done = true;
				items_alive--;
				continue;
			}

			if (CheckIfResolvable(exec_item)) {
				std::shared_ptr<PlanExecItem> item_to_probe_into = nullptr;
				std::shared_ptr<PointOmniSketch> sketch_to_probe_into = nullptr;
				exec_item->estimator->Finalize();
				auto pk_probe_set = exec_item->estimator->ComputeResult(UINT64_MAX);
				if (pk_probe_set->RecordCount() == 0) {
					return 0;
				}

				size_t current_fk_idx = 0;
				for (auto &fk_side_item_pair : exec_item->probes_into) {
					auto &fk_side_item = exec_items[fk_side_item_pair.first];
					auto &fk_omni_sketch = fk_side_item_pair.second;
					if (!fk_side_item->probes_into.empty() || fk_side_item->probed_from.size() > 1 ||
					    current_fk_idx == exec_item->probes_into.size() - 1) {
						assert(!item_to_probe_into);
						assert(!sketch_to_probe_into);
						item_to_probe_into = fk_side_item;
						sketch_to_probe_into = fk_omni_sketch;
					} else {
						pk_probe_set = fk_side_item->estimator->FilterProbeSet(fk_omni_sketch, pk_probe_set);
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
					item_to_probe_into->estimator->AddPredicate(sketch_to_probe_into, pk_probe_set);
					item_to_probe_into->probed_from.erase(table_name);
					exec_item->done = true;
					items_alive--;
				}
				exec_item->probes_into.clear();
			}
		}
	}

	std::shared_ptr<PlanExecItem> plan_item;
	for (auto &item : exec_items) {
		if (!item.second->done) {
			assert(!plan_item);
			plan_item = item.second;
		}
	}

	assert(plan_item->probes_into.empty());
	assert(plan_item->probed_from.empty());
	plan_item->estimator->Finalize();
	auto card_est = plan_item->estimator->ComputeResult(UINT64_MAX);
	return (double)card_est->RecordCount();
}

std::shared_ptr<PlanItem> PlanGenerator::GetOrCreatePlanItem(const std::string& table_name) {
	auto it = plan_items.find(table_name);
	if (it != plan_items.end()) {
		return it->second;
	}
	
	auto item = std::make_shared<PlanItem>();
	plan_items.emplace(table_name, item);
	return item;
}

} // namespace omnisketch
