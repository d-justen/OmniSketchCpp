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

void PlanGenerator::AddFKFKJoin(const std::string &table_name_1, const std::string &column_name_1,
                                const std::string &table_name_2, const std::string &column_name_2) {
	auto &registry = Registry::Get();
	auto fk_column_sketch_1 = registry.GetOmniSketch(table_name_1, column_name_1);
	auto fk_column_sketch_2 = registry.GetOmniSketch(table_name_2, column_name_2);
	auto item_1 = GetOrCreatePlanItem(table_name_1);
	auto item_2 = GetOrCreatePlanItem(table_name_2);
	item_1->fk_fk_connections.emplace_back(fk_column_sketch_1, std::make_pair(table_name_2, fk_column_sketch_2));
	item_2->fk_fk_connections.emplace_back(fk_column_sketch_2, std::make_pair(table_name_1, fk_column_sketch_1));
}

bool PlanGenerator::CheckIfResolvable(const PlanExecItem &item) const {
	if (!item.probed_from.empty()) {
		return false;
	}
	size_t not_resolvable_count = 0;
	for (auto &fk_side_item_pair : item.probes_into) {
		const auto &fk_side_item = plan_items.find(fk_side_item_pair.first)->second;
		if (!fk_side_item->probes_into.empty() || fk_side_item->probed_from.size() != 1) {
			// FK side has other connections!
			not_resolvable_count++;
		}
	}
	return not_resolvable_count <= 1;
}

std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> PlanGenerator::CreateInitialExecutionPlan() const {
	std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> exec_items;
	for (auto &plan_item_pair : plan_items) {
		const auto &table_name = plan_item_pair.first;
		auto &plan_item = plan_item_pair.second;

		auto exec_item = std::make_shared<PlanExecItem>();
		exec_item->combinator = combinator_creator();
		exec_item->probes_into = plan_item->probes_into;
		exec_item->probed_from = plan_item->probed_from;
		exec_item->included_relations.reserve(plan_items.size());
		exec_item->included_relations.push_back(table_name);
		exec_items[table_name] = exec_item;
	}

	// Add FK-FK connections
	for (auto &plan_item_pair : plan_items) {
		const auto &table_name = plan_item_pair.first;
		auto &plan_item = plan_item_pair.second;

		for (auto &connection : plan_item->fk_fk_connections) {
			exec_items[table_name]->fk_fk_connections.emplace_back(
			    connection.first, std::make_pair(exec_items[connection.second.first], connection.second.second));
		}
	}
	return exec_items;
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
					fk_side_exec_item->combinator->AddUnfilteredRids(fk_side_sketch);
				}

				// Remove edge and do not add primary key side to result map
				fk_side_exec_item->probed_from.erase(table_name);
			} else {
				// Primary key side is not filtered out and gets unfiltered record ids as a predicate
				pk_side_exec_item->combinator->AddUnfilteredRids(registry.ProduceRidSample(table_name));
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
					if (referencing_sketch->Type() == OmniSketchType::FOREIGN_SORTED) {
						// Coordinated sketch contains own table rids: probe foreign key sketch with results
						auto &foreign_key_sketch = exec_item->probes_into.begin()->second;
						fk_side_exec_item->combinator->AddPredicate(foreign_key_sketch,
						                                            referencing_sketch->ProbeHashedSet(probe_values));
					} else {
						// Coordinated sketch has rids of its join partner: we can treat it like a predicate on the FK
						// side
						fk_side_exec_item->combinator->AddPredicate(referencing_sketch, probe_values);
					}
					fk_side_exec_item->probed_from.erase(table_name);
					continue;
				}
			}
			exec_item->combinator->AddPredicate(registry.GetOmniSketch(table_name, column_name), probe_values);
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

	if (exec_items.size() == 1 && !exec_items.begin()->second->combinator->HasPredicates()) {
		// Single table without any predicates: return base table cardinality
		return (double)registry.GetBaseTableCard(exec_items.begin()->first);
	}

	// Execute!
	size_t items_alive = exec_items.size();
	while (items_alive > 1) {
		for (const auto &exec_item_pair : exec_items) {
			const auto &exec_item = exec_item_pair.second;

			if (exec_item->done) {
				continue;
			}

			if (!RelationIsEar(*exec_item)) {
				continue;
			}

			// If this is the last survivor, exit to compute the result
			if (exec_item->probes_into.empty() && exec_item->fk_fk_connections.empty()) {
				// This should be our last relation
				if (items_alive != 1) {
					throw std::logic_error("Unconnected relation: " + std::to_string(items_alive) + " of " +
					                       std::to_string(exec_items.size()) + " left.");
				}
				continue;
			}

			// We have arrived at an ear, let's merge
			size_t items_to_unalive = exec_item->probes_into.empty() ? 1 : exec_item->probes_into.size();
			const bool merge_success = MergeEar(*exec_item, exec_items);
			if (merge_success) {
				items_alive -= items_to_unalive;
			} else {
				return 0;
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

bool PlanGenerator::RelationIsEar(const PlanExecItem &item) const {
	assert(!item.done);
	return item.probed_from.empty();
}

bool PlanGenerator::MergeEar(PlanExecItem &item,
                             const std::unordered_map<std::string, std::shared_ptr<PlanExecItem>> &all_items) const {
	item.done = true;
	std::shared_ptr<OmniSketchCell> pk_probe_set;

	if (!item.fk_fk_connections.empty()) {
		// Compute OmniSketch cross-products
		size_t max_join_card = 1;
		size_t base_card = 0;
		for (auto &connection : item.fk_fk_connections) {
			const auto &this_omni_sketch = connection.first;
			const auto &other_omni_sketch = connection.second.second;

			base_card = std::max(base_card, this_omni_sketch->RecordCount());
			const auto this_side_cross_product = this_omni_sketch->MultiplyRecordCounts(other_omni_sketch);
			// TODO: Use MaxMultiplicity * MaxMultiplicity * min(MinDomain, MinDomain)
			max_join_card *= this_side_cross_product->RecordCount();
			if (this_omni_sketch->CountNulls() > 0) {
				item.combinator->AddUnfilteredRids(this_omni_sketch->GetRids());
			}
		}

		pk_probe_set = item.combinator->ComputeResult(UINT64_MAX);
		double selectivity = (double)pk_probe_set->RecordCount() / (double)base_card;
		const auto final_card = static_cast<size_t>((double)max_join_card * selectivity);
		pk_probe_set->SetRecordCount(final_card);

		for (auto &connection : item.fk_fk_connections) {
			const auto &other_omni_sketch = connection.second.second;

			double other_combinator_multiplier = (double)final_card / (double)other_omni_sketch->RecordCount();
			auto &other_combinator = connection.second.first->combinator;
			other_combinator->AddFKMultiplier(other_combinator_multiplier);

			// TODO: change data structure, this is too complicated
			for (auto fk_connection_it = connection.second.first->fk_fk_connections.begin();
			     fk_connection_it != connection.second.first->fk_fk_connections.end(); ++fk_connection_it) {
				fk_connection_it->second.first->included_relations.push_back(item.included_relations.front());
				if (fk_connection_it->second.first.get() == &item) {
					connection.second.first->fk_fk_connections.erase(fk_connection_it);
					break;
				}
			}
		}
	} else {
		pk_probe_set = item.combinator->ComputeResult(UINT64_MAX);
	}
	if (pk_probe_set->RecordCount() == 0) {
		return false;
	}

	if (item.probes_into.size() == 1) {
		const auto &fk_table_name = item.probes_into.begin()->first;
		const auto &fk_sketch = item.probes_into.begin()->second;
		auto &fk_side_item = all_items.at(fk_table_name);

		fk_side_item->combinator->AddPredicate(fk_sketch, pk_probe_set);
		fk_side_item->probed_from.erase(item.included_relations.front());
		auto &relation_set = fk_side_item->included_relations;
		relation_set.insert(relation_set.end(), item.included_relations.begin(), item.included_relations.end());
		item.done = true;
		return true;
	}

	if (CheckIfResolvable(item)) {
		std::shared_ptr<PlanExecItem> item_to_probe_into = nullptr;
		std::shared_ptr<PointOmniSketch> sketch_to_probe_into = nullptr;

		// TODO: Add method: MultiPKJoin!

		size_t current_fk_idx = 0;
		for (auto &fk_side_item_pair : item.probes_into) {
			auto &fk_side_item = all_items.at(fk_side_item_pair.first);
			auto &fk_omni_sketch = fk_side_item_pair.second;
			if (!fk_side_item->probes_into.empty() || fk_side_item->probed_from.size() > 1 ||
			    current_fk_idx == item.probes_into.size() - 1) {
				assert(!item_to_probe_into);
				assert(!sketch_to_probe_into);
				item_to_probe_into = fk_side_item;
				sketch_to_probe_into = fk_omni_sketch;
			} else {
				pk_probe_set = fk_side_item->combinator->FilterProbeSet(fk_omni_sketch, pk_probe_set);
				fk_side_item->probed_from.erase(item.included_relations.front());
				fk_side_item->done = true;
				if (pk_probe_set->RecordCount() == 0) {
					return false;
				}
			}
			current_fk_idx++;
		}

		if (item_to_probe_into && sketch_to_probe_into) {
			item_to_probe_into->combinator->AddPredicate(sketch_to_probe_into, pk_probe_set);
			item_to_probe_into->probed_from.erase(item.included_relations.front());
			item.done = true;
		}
		item.probes_into.clear();
		return true;
	}
	return false;
}

} // namespace omnisketch
