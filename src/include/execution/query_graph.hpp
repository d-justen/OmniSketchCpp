#pragma once

#include "plan_node.hpp"

namespace omnisketch {

struct TableFilter {
	std::string column_name;
	std::shared_ptr<OmniSketchCell> probe_set;
	std::string original_table_name = {};
	std::string fk_column_name = {};
	std::shared_ptr<PlanNode> other_side_plan = nullptr;
};

struct RelationEdge {
	std::string this_column_name;
	std::string other_table_name;
	std::string other_column_name;

	bool is_fk_fk_join;
};

struct RelationNode {
	std::string name;
	std::vector<TableFilter> filters;
	std::unordered_multimap<std::string, RelationEdge> connections;

	// debug
	std::vector<std::string> contained_relations;
};

class QueryGraph {
public:
	double Estimate();

	void AddConstantPredicate(const std::string &table_name, const std::string &column_name,
	                          const std::shared_ptr<OmniSketchCell> &probe_set);
	void AddPkFkJoin(const std::string &fk_table_name, const std::string &fk_column_name,
	                 const std::string &pk_table_name);
	void AddFkFkJoin(const std::string &table_name_1, const std::string &column_name_1, const std::string &table_name_2,
	                 const std::string &column_name_2);

public:
	void RunDpSizeAlgo();

protected:
	void AddEdge(const std::string &table_name_1, const std::string &column_name_1, const std::string &table_name_2,
	             const std::string &column_name_2);
	void RemoveEdge(const std::string &table_name_1, const std::string &column_name_1, const std::string &table_name_2,
	                const std::string &column_name_2);
	void RemoveEdgeOneSide(const std::string &table_name_1, const std::string &column_name_1,
	                       const std::string &table_name_2, const std::string &column_name_2);
	RelationNode &GetOrCreateNode(const std::string &table_name);

	// Helper methods
	bool TryMergeSingleConnection();
	bool TryMergeSingleFkFkConnection();
	bool TryMergeMultiPkConnection();
	bool TryExpandPkConnection();

	void MergePkSideIntoFkSide(RelationNode &relation, RelationEdge &edge);
	std::vector<std::vector<std::string>> FindCycles(const std::string &table_name);
	static void AddFilterToPlan(PlanNode &plan, const TableFilter &filter);

	std::unordered_map<std::string, RelationNode> graph;
};

} // namespace omnisketch
