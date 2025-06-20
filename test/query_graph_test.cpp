#include <gtest/gtest.h>

#include "include/execution/query_graph.hpp"
#include "include/omni_sketch/omni_sketch_cell.hpp"
#include "include/registry.hpp"
#include "include/util/value.hpp"

TEST(QueryGraphTest, MinimalPlanWithFKsAndCycle) {
	auto &registry = omnisketch::Registry::Get();
	auto r_id = registry.CreateOmniSketch<size_t>("R", "id");
	auto r_att = registry.CreateOmniSketch<size_t>("R", "att");

	auto s_rid = registry.CreateOmniSketch<size_t>("S", "rid");

	auto t_rid = registry.CreateOmniSketch<size_t>("T", "rid");
	auto t_att = registry.CreateOmniSketch<size_t>("T", "att");

	for (size_t i = 0; i < 1000; i++) {
		r_id->AddRecord(i, i);
		r_att->AddRecord(i % 4, i);
		s_rid->AddRecord(i % 100, i);
		t_rid->AddRecord(i % 10, i);
		t_att->AddRecord(i % 2, i);
	}

	omnisketch::QueryGraph graph;
	auto pred = std::make_shared<omnisketch::OmniSketchCell>(1);
	pred->AddRecord(omnisketch::Value::From(1).GetHash());
	graph.AddConstantPredicate("T", "att", pred);
	graph.AddPkFkJoin("S", "rid", "R");
	graph.AddPkFkJoin("T", "rid", "R");
	graph.AddFkFkJoin("S", "rid", "T", "rid");
	auto result = graph.Estimate();
	EXPECT_GE(result, 5000.0);

	omnisketch::QueryGraph graph_2;
	graph_2.AddConstantPredicate("T", "att", pred);
	graph_2.AddPkFkJoin("S", "rid", "R");
	graph_2.AddPkFkJoin("T", "rid", "R");
	graph_2.AddFkFkJoin("S", "rid", "T", "rid");
	graph_2.AddConstantPredicate("R", "att", pred);
	result = graph_2.Estimate();
	EXPECT_GE(result, 0.0);
	registry.Clear();
}

TEST(QueryGraphTest, MinimalPlanWithFKsAndCycleSecondary) {
	auto &registry = omnisketch::Registry::Get();
	auto r_id = registry.CreateOmniSketch<size_t>("R", "id");
	auto r_att = registry.CreateOmniSketch<size_t>("R", "att");

	auto s_rid = registry.CreateOmniSketch<size_t>("S", "rid");

	auto t_rid = registry.CreateOmniSketch<size_t>("T", "rid");
	auto t_att = registry.CreateOmniSketch<size_t>("T", "att");

	auto r_id_s =
	    registry.CreateExtendingOmniSketch<omnisketch::PreJoinedOmniSketch<size_t>, size_t>("R", "id", "S", "rid");
	auto r_att_s =
	    registry.CreateExtendingOmniSketch<omnisketch::PreJoinedOmniSketch<size_t>, size_t>("R", "att", "S", "rid");
	auto r_id_t =
	    registry.CreateExtendingOmniSketch<omnisketch::PreJoinedOmniSketch<size_t>, size_t>("R", "id", "T", "rid");
	auto r_att_t =
	    registry.CreateExtendingOmniSketch<omnisketch::PreJoinedOmniSketch<size_t>, size_t>("R", "att", "T", "rid");

	for (size_t i = 0; i < 1000; i++) {
		r_id->AddRecord(i, i);
		r_att->AddRecord(i % 4, i);
		s_rid->AddRecord(i % 100, i);
		t_rid->AddRecord(i % 10, i);
		t_att->AddRecord(i % 2, i);
	}

	for (size_t i = 0; i < 1000; i++) {
		r_id_s->AddRecord(i, i);
		r_id_t->AddRecord(i, i);
		r_att_s->AddRecord(i % 4, i);
		r_att_t->AddRecord(i % 4, i);
	}

	omnisketch::QueryGraph graph;
	auto pred = std::make_shared<omnisketch::OmniSketchCell>(1);
	pred->AddRecord(omnisketch::Value::From(1).GetHash());
	graph.AddConstantPredicate("T", "att", pred);
	graph.AddPkFkJoin("S", "rid", "R");
	graph.AddPkFkJoin("T", "rid", "R");
	graph.AddFkFkJoin("S", "rid", "T", "rid");
	auto result = graph.Estimate();
	EXPECT_GE(result, 5000.0);

	omnisketch::QueryGraph graph_2;
	graph_2.AddConstantPredicate("T", "att", pred);
	graph_2.AddPkFkJoin("S", "rid", "R");
	graph_2.AddPkFkJoin("T", "rid", "R");
	graph_2.AddFkFkJoin("S", "rid", "T", "rid");
	graph_2.AddConstantPredicate("R", "att", pred);
	result = graph_2.Estimate();
	EXPECT_GE(result, 0.0);
	registry.Clear();
}