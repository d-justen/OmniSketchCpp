#include "execution/query_graph.hpp"

#include "registry.hpp"

#include <iostream>
#include <queue>
#include <sstream>

namespace omnisketch {

double QueryGraph::Estimate() {
    while (graph.size() > 1) {
        bool removed_node = TryMergeSingleConnection();

        if (!removed_node) {
            removed_node = TryMergeSingleFkFkConnection();
        }

        if (!removed_node) {
            removed_node = TryMergeMultiPkConnection();
        }

        if (!removed_node) {
            removed_node = TryExpandPkConnection();
        }

        assert(removed_node && "The query seems to be alpha-cyclic.");
    }

    assert(graph.size() == 1);
    // Estimate
    auto& node = graph.begin()->second;
    auto& registry = Registry::Get();
    size_t base_card = registry.GetBaseTableCard(node.name);
    auto plan = std::make_shared<PlanNode>(node.name, base_card, UINT64_MAX);
    for (auto& filter : node.filters) {
        AddFilterToPlan(*plan, filter);
    }

    auto result = plan->Estimate();
    return (double)result->RecordCount();
}

void QueryGraph::AddConstantPredicate(const std::string& table_name, const std::string& column_name,
                                      const std::shared_ptr<OmniSketchCell>& probe_set) {
    auto& node = GetOrCreateNode(table_name);
    node.filters.push_back(TableFilter{column_name, probe_set});
}

void QueryGraph::AddPkFkJoin(const std::string& fk_table_name, const std::string& fk_column_name,
                             const std::string& pk_table_name) {
    AddEdge(fk_table_name, fk_column_name, pk_table_name, {});
}

void QueryGraph::AddFkFkJoin(const std::string& table_name_1, const std::string& column_name_1,
                             const std::string& table_name_2, const std::string& column_name_2) {
    AddEdge(table_name_1, column_name_1, table_name_2, column_name_2);
}

void QueryGraph::AddEdge(const std::string& table_name_1, const std::string& column_name_1,
                         const std::string& table_name_2, const std::string& column_name_2) {
    bool is_fk_fk_join = !column_name_1.empty() && !column_name_2.empty();
    auto& node_1 = GetOrCreateNode(table_name_1);
    auto& node_2 = GetOrCreateNode(table_name_2);
    node_1.connections.insert({table_name_2, RelationEdge{column_name_1, table_name_2, column_name_2, is_fk_fk_join}});
    node_2.connections.insert({table_name_1, RelationEdge{column_name_2, table_name_1, column_name_1, is_fk_fk_join}});
}

void QueryGraph::RemoveEdgeOneSide(const std::string& table_name_1, const std::string& column_name_1,
                                   const std::string& table_name_2, const std::string& column_name_2) {
    auto& connections = graph[table_name_1].connections;
    for (auto it = connections.find(table_name_2); it != connections.end(); it++) {
        auto& edge = it->second;
        if (edge.this_column_name == column_name_1 && edge.other_column_name == column_name_2) {
            connections.erase(it);
            break;
        }
    }
    if (connections.empty() && graph.size() > 1) {
        graph.erase(table_name_1);
    }
}

void QueryGraph::RemoveEdge(const std::string& table_name_1, const std::string& column_name_1,
                            const std::string& table_name_2, const std::string& column_name_2) {
    auto t1 = table_name_1, c1 = column_name_1, t2 = table_name_2, c2 = column_name_2;

    RemoveEdgeOneSide(t1, c1, t2, c2);
    RemoveEdgeOneSide(t2, c2, t1, c1);
}

RelationNode& QueryGraph::GetOrCreateNode(const std::string& table_name) {
    if (graph.find(table_name) == graph.end()) {
        graph[table_name] = RelationNode{table_name, {}, {}, {}};
    }
    return graph[table_name];
}

bool QueryGraph::TryMergeSingleConnection() {
    for (auto& node_entry : graph) {
        auto& node = node_entry.second;

        assert(!node.connections.empty());
        if (node.connections.size() == 1) {
            auto& connection_entry = *node.connections.begin();
            auto& edge = connection_entry.second;
            if (edge.other_column_name.empty()) {
                // We can only merge into a foreign-key side
                continue;
            }

            if (edge.is_fk_fk_join) {
                continue;
            }

            MergePkSideIntoFkSide(node, edge);
            return true;
        }
    }
    return false;
}

void QueryGraph::AddFilterToPlan(PlanNode& plan, const TableFilter& filter) {
    if (filter.other_side_plan && !filter.column_name.empty()) {
        assert(!filter.fk_column_name.empty());
        plan.AddFKFKJoinExpansion(filter.column_name, filter.other_side_plan, filter.fk_column_name);
    } else if (!filter.original_table_name.empty()) {
        plan.AddSecondaryFilter(filter.original_table_name, filter.column_name, filter.probe_set);
    } else if (filter.probe_set == nullptr) {
        plan.AddPKJoinExpansion(filter.fk_column_name, filter.other_side_plan);
    } else {
        plan.AddFilter(filter.column_name, filter.probe_set);
    }
}

bool QueryGraph::TryMergeSingleFkFkConnection() {
    for (auto& node_entry : graph) {
        auto& this_table_name = node_entry.first;
        auto& node = node_entry.second;

        assert(!node.connections.empty());
        if (node.connections.size() == 1) {
            auto& connection_entry = *node.connections.begin();
            auto& edge = connection_entry.second;
            if (edge.other_column_name.empty()) {
                // We can only merge into a foreign-key side
                continue;
            }

            if (!edge.is_fk_fk_join) {
                continue;
            }

            if (graph[edge.other_table_name].connections.size() == 1 &&
                graph[edge.other_table_name].filters.size() < node.filters.size()) {
                // Other side has fewer filters - merge it later into this node
                continue;
            }

            auto& registry = Registry::Get();
            size_t sample_count = UINT64_MAX;

            auto plan =
                std::make_shared<PlanNode>(this_table_name, registry.GetBaseTableCard(this_table_name), sample_count);
            for (auto& filter : node.filters) {
                AddFilterToPlan(*plan, filter);
            }
            graph[edge.other_table_name].filters.push_back(
                TableFilter{edge.other_column_name, nullptr, {}, edge.this_column_name, plan});

            RemoveEdge(this_table_name, edge.this_column_name, edge.other_table_name, edge.other_column_name);
            return true;
        }
    }
    return false;
}
bool QueryGraph::TryMergeMultiPkConnection() {
    for (auto& node_entry : graph) {
        auto& this_table_name = node_entry.first;
        auto& node = node_entry.second;

        assert(!node.connections.empty());
        bool has_unresolved_fk_joins = false;
        for (const auto& connection : node.connections) {
            if (!connection.second.this_column_name.empty()) {
                has_unresolved_fk_joins = true;
                break;
            }
        }

        if (has_unresolved_fk_joins) {
            // This only works for joins on the primary key and FK-FK joins
            continue;
        }

        auto& registry = Registry::Get();
        size_t sample_count = UINT64_MAX;

        auto plan =
            std::make_shared<PlanNode>(this_table_name, registry.GetBaseTableCard(this_table_name), sample_count);
        for (auto& filter : node.filters) {
            AddFilterToPlan(*plan, filter);
        }

        auto cycles = FindCycles(this_table_name);
        if (cycles.empty()) {
            continue;
        }

        if (cycles.size() == 1) {
            // We can just merge this table into its neighbors
            while (!node.connections.empty()) {
                auto& connection = *node.connections.begin();
                auto& other_node = graph[connection.first];
                if (connection.second.is_fk_fk_join) {
                    other_node.filters.push_back(TableFilter{connection.second.other_column_name, nullptr,
                                                             this_table_name, connection.second.this_column_name,
                                                             plan});
                    RemoveEdge(this_table_name, connection.second.this_column_name, connection.second.other_table_name,
                               connection.second.other_column_name);
                } else {
                    MergePkSideIntoFkSide(node, connection.second);
                }
            }
            return true;
        }
        // We can only remove the edges for n-1 edges in each cycle
        for (auto& cycle : cycles) {
            for (size_t i = 0; i < cycle.size() - 1; i++) {
                for (auto connection_it = node.connections.find(cycle[i]); connection_it != node.connections.end();
                     ++connection_it) {
                    auto& connection = *connection_it;
                    auto& other_node = graph[connection.first];

                    if (connection.second.is_fk_fk_join) {
                        other_node.filters.push_back(TableFilter{connection.second.other_column_name, nullptr,
                                                                 this_table_name, connection.second.this_column_name,
                                                                 plan});
                        RemoveEdge(this_table_name, connection.second.this_column_name,
                                   connection.second.other_table_name, connection.second.other_column_name);
                    } else {
                        MergePkSideIntoFkSide(node, connection.second);
                    }
                }
            }
        }
        return true;
    }

    return false;
}

std::vector<std::vector<std::string>> QueryGraph::FindCycles(const std::string& table_name) {
    // Strategy: follow one edge, see with how many other nodes its other side follows to ignoring any edges back to
    // table_name
    std::set<std::string> connected_relations;

    const auto& relation_node = graph[table_name];
    for (auto& connection : relation_node.connections) {
        connected_relations.insert(connection.first);
    }

    std::vector<std::vector<std::string>> result;
    while (!connected_relations.empty()) {
        std::set<std::string> in_cycle;
        in_cycle.insert(table_name);

        auto& connected_table_name = *connected_relations.begin();
        std::queue<std::string> next_nodes;
        next_nodes.push(connected_table_name);

        while (!next_nodes.empty()) {
            const auto& next_relation_node = graph[next_nodes.front()];
            next_nodes.pop();
            in_cycle.insert(next_relation_node.name);

            for (const auto& connection : next_relation_node.connections) {
                if (in_cycle.find(connection.first) == in_cycle.end()) {
                    // We found a new connected node
                    next_nodes.push(connection.first);
                }
            }
        }

        std::vector<std::string> partial_result;
        for (const auto& found_name : in_cycle) {
            auto it = connected_relations.find(found_name);
            if (it != connected_relations.end()) {
                partial_result.push_back(*it);
                connected_relations.erase(it);
            }
        }
        if (partial_result.size() > 1) {
            result.push_back(partial_result);
        }
    }

    return result;
}

void QueryGraph::MergePkSideIntoFkSide(RelationNode& relation, RelationEdge& edge) {
    auto& this_table_name = relation.name;

    auto& registry = Registry::Get();
    std::vector<TableFilter> remaining_filters;
    remaining_filters.reserve(relation.filters.size());
    size_t sample_count = UINT64_MAX;

    for (auto& filter : relation.filters) {
        if (filter.probe_set == nullptr) {
            // This is a primary key expansion
            remaining_filters.push_back(filter);
            continue;
        }
        auto sketch = registry.FindReferencingOmniSketch(this_table_name, filter.column_name, edge.other_table_name);
        if (sketch) {
            graph[edge.other_table_name].filters.push_back(
                TableFilter{filter.column_name, filter.probe_set, this_table_name});
        } else {
            auto omni_sketch = registry.GetOmniSketch(this_table_name, filter.column_name);
            sample_count = std::min(sample_count, omni_sketch->MinHashSketchSize());
            remaining_filters.push_back(filter);
        }
    }

    if (!remaining_filters.empty()) {
        size_t base_card = registry.GetBaseTableCard(this_table_name);
        PlanNode plan(this_table_name, base_card, sample_count);
        for (auto& filter : remaining_filters) {
            AddFilterToPlan(plan, filter);
        }
        graph[edge.other_table_name].filters.push_back(TableFilter{edge.other_column_name, plan.Estimate()});
    }

    if (relation.filters.empty()) {
        auto other_side_sketch = registry.GetOmniSketch(edge.other_table_name, edge.other_column_name);
        // We only have to do anything if the join filters (i.e., the foreign key column contains nulls)
        if (other_side_sketch->CountNulls() > 0) {
            graph[edge.other_table_name].filters.push_back(
                TableFilter{edge.other_column_name, std::make_shared<OmniSketchCell>()});
        }
    }

    RemoveEdge(this_table_name, edge.this_column_name, edge.other_table_name, edge.other_column_name);
}

bool QueryGraph::TryExpandPkConnection() {
    for (auto& node_entry : graph) {
        auto& node = node_entry.second;

        assert(!node.connections.empty());

        for (auto& connection : node.connections) {
            if (!connection.second.is_fk_fk_join && connection.second.this_column_name.empty()) {
                auto& other_node = graph[connection.first];
                if (other_node.connections.size() == 1) {
                    size_t sample_count = UINT64_MAX;

                    auto& registry = Registry::Get();
                    for (auto& filter : other_node.filters) {
                        if (!filter.column_name.empty()) {
                            auto omni_sketch = registry.GetOmniSketch(connection.first, filter.column_name);
                            sample_count = std::min(sample_count, omni_sketch->MinHashSketchSize());
                        }
                    }

                    if (other_node.filters.empty()) {
                        // TODO: How do we set this?
                        sample_count = 1024;
                    }

                    size_t base_card = registry.GetBaseTableCard(connection.first);
                    auto plan = std::make_shared<PlanNode>(connection.first, base_card, sample_count);
                    for (auto& filter : other_node.filters) {
                        AddFilterToPlan(*plan, filter);
                    }

                    node.filters.push_back(TableFilter{{}, nullptr, {}, connection.second.other_column_name, plan});
                    RemoveEdge(node.name, connection.second.this_column_name, connection.second.other_table_name,
                               connection.second.other_column_name);
                    return true;
                }
            }
        }
    }
    return false;
}

struct JoinNode {
    std::string name;
    std::shared_ptr<JoinNode> left_in;
    std::shared_ptr<JoinNode> right_in;
};

struct QueryPlan {
    std::shared_ptr<JoinNode> plan;
    std::set<std::string> relations;
    double card_est;
    double cost;
};

std::string TreeToString(JoinNode* root) {
    if (!root)
        return "";

    std::string result = root->name;

    // If there's a left child or a right child, show left
    if (root->left_in) {
        assert(root->right_in);
        result += "(" + TreeToString(&*root->left_in) + "," + TreeToString(&*root->right_in) + ")";
    }

    return result;
}

std::vector<DpSizeResult> QueryGraph::RunDpSizeAlgo() {
    auto& registry = Registry::Get();
    std::unordered_map<size_t, std::vector<QueryPlan>> best_plans;
    std::unordered_map<std::string, double> estimates;
    std::vector<DpSizeResult> dp_size_results;
    dp_size_results.reserve(1000);

    for (auto& node : graph) {
        QueryPlan plan;
        plan.plan = std::make_shared<JoinNode>(JoinNode{node.first, nullptr, nullptr});
        plan.relations.insert(node.first);
        if (!graph[node.first].filters.empty()) {
            DpSizeResult dp_size_result;
            std::string relations;
            for (auto& r : plan.relations) {
                dp_size_result.relations.insert(r);
                relations += r + ", ";
            }

            QueryGraph g;
            g.graph[node.first] = graph[node.first];

            auto begin = std::chrono::steady_clock::now();
            plan.card_est = g.Estimate();
            auto end = std::chrono::steady_clock::now();
            dp_size_result.duration_ns = (end - begin).count();
            dp_size_result.card_est = plan.card_est;

            estimates[relations] = plan.card_est;
            dp_size_results.push_back(dp_size_result);
        } else {
            plan.card_est = (double)registry.GetBaseTableCard(node.first);
        }

        plan.cost = 0;
        best_plans[1].push_back(plan);
    }

    for (size_t current_size = 2; current_size <= graph.size(); current_size++) {
        for (size_t left_plan_size = 1; left_plan_size <= current_size / 2; left_plan_size++) {
            const size_t right_plan_size = current_size - left_plan_size;
            // Find all plans of that size in best_plans
            for (size_t left_plan_index = 0; left_plan_index < best_plans[left_plan_size].size(); left_plan_index++) {
                auto& left_plan = best_plans[left_plan_size][left_plan_index];
                size_t right_plan_index = left_plan_size == right_plan_size ? left_plan_index + 1 : 0;

                for (; right_plan_index < best_plans[right_plan_size].size(); right_plan_index++) {
                    auto& right_plan = best_plans[right_plan_size][right_plan_index];
                    // Only continue if intersection is empty
                    std::vector<std::string> intersection;
                    std::set_intersection(left_plan.relations.begin(), left_plan.relations.end(),
                                          right_plan.relations.begin(), right_plan.relations.end(),
                                          std::back_inserter(intersection));
                    if (!intersection.empty()) {
                        continue;
                    }

                    // Only go on if there is a connection between the plans
                    bool plans_are_connected = false;
                    for (auto& relation_name : left_plan.relations) {
                        auto& node = graph[relation_name];
                        for (auto& connection : node.connections) {
                            if (right_plan.relations.find(connection.first) != right_plan.relations.end()) {
                                plans_are_connected = true;
                                break;
                            }
                        }
                        if (plans_are_connected) {
                            break;
                        }
                    }

                    if (!plans_are_connected) {
                        continue;
                    }

                    // Let's create the join tree
                    QueryPlan plan;
                    plan.relations.insert(left_plan.relations.begin(), left_plan.relations.end());
                    plan.relations.insert(right_plan.relations.begin(), right_plan.relations.end());

                    QueryGraph g;
                    for (auto& relation_name : plan.relations) {
                        g.graph[relation_name] = graph[relation_name];
                        // BUT remove all connections to relations that are not in current subplan
                        while (true) {
                            std::string connection_to_remove{};
                            for (auto& conn : g.graph[relation_name].connections) {
                                if (plan.relations.find(conn.first) == plan.relations.end()) {
                                    connection_to_remove = conn.first;
                                    break;
                                }
                            }
                            if (connection_to_remove.empty()) {
                                break;
                            } else {
                                g.graph[relation_name].connections.erase(connection_to_remove);
                            }
                        }
                    }

                    DpSizeResult dp_size_result;
                    std::string relations;
                    for (auto& r : plan.relations) {
                        dp_size_result.relations.insert(r);
                        relations += r + ", ";
                    }

                    auto begin = std::chrono::steady_clock::now();
                    auto card_est_it = estimates.find(relations);
                    if (card_est_it != estimates.end()) {
                        plan.card_est = card_est_it->second;
                    } else {
                        plan.card_est = g.Estimate();
                        estimates[relations] = plan.card_est;
                    }

                    auto end = std::chrono::steady_clock::now();
                    dp_size_result.duration_ns = (end - begin).count();
                    dp_size_result.card_est = plan.card_est;
                    dp_size_results.push_back(dp_size_result);

                    plan.cost = left_plan.cost + right_plan.cost + plan.card_est;
                    auto left_in = left_plan.card_est > right_plan.card_est ? left_plan.plan : right_plan.plan;
                    auto right_in = left_plan.card_est > right_plan.card_est ? right_plan.plan : left_plan.plan;
                    plan.plan = std::make_shared<JoinNode>(JoinNode{{}, left_in, right_in});

                    // Now see whether this is the plan with the lowest cost
                    bool has_plan = false;
                    for (auto& best_plan : best_plans[current_size]) {
                        intersection.clear();
                        std::set_intersection(plan.relations.begin(), plan.relations.end(), best_plan.relations.begin(),
                                              best_plan.relations.end(), std::back_inserter(intersection));
                        if (intersection.size() == current_size) {
                            // Identical relations in plan!
                            has_plan = true;
                            if (best_plan.cost > plan.cost) {
                                best_plan = plan;
                                break;
                            }
                        }
                    }

                    if (!has_plan) {
                        best_plans[current_size].push_back(plan);
                    }
                }
            }
        }
    }

    assert(best_plans[graph.size()].size() == 1);
    std::cout << TreeToString(&*best_plans[graph.size()].front().plan) << "\n";
    return dp_size_results;
}

}  // namespace omnisketch
