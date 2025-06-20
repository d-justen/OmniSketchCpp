#include <gtest/gtest.h>

#include "include/plan_generator.hpp"

TEST(PlanGeneratorTest, StarShape) {
	const size_t FACT_TABLE_SIZE = 1000;
	const size_t DIM_S_SIZE = 500;
	const size_t DIM_T_SIZE = 250;

	auto &registry = omnisketch::Registry::Get();
	auto fact_fk_s = registry.CreateOmniSketch<size_t>("fact", "fk_s");
	auto fact_fk_t = registry.CreateOmniSketch<size_t>("fact", "fk_t");
	auto dim_s_att = registry.CreateOmniSketch<size_t>("dim_s", "att");
	auto dim_t_att = registry.CreateOmniSketch<size_t>("dim_t", "att");

	for (size_t i = 0; i < FACT_TABLE_SIZE; i++) {
		fact_fk_s->AddRecord(i % DIM_S_SIZE, i);
		fact_fk_t->AddRecord(i % DIM_T_SIZE, i);
	}

	for (size_t i = 0; i < DIM_S_SIZE; i++) {
		dim_s_att->AddRecord(i, i);
	}

	for (size_t i = 0; i < DIM_T_SIZE; i++) {
		dim_t_att->AddRecord(i, i);
	}

	omnisketch::PlanGenerator gen;
	gen.AddPredicate("dim_s", "att", omnisketch::PredicateConverter::ConvertRange(0, 249));
	gen.AddPredicate("dim_t", "att", omnisketch::PredicateConverter::ConvertRange(0, 124));
	gen.AddJoin("fact", "fk_s", "dim_s");
	gen.AddJoin("fact", "fk_t", "dim_t");
	const double result = gen.EstimateCardinality();

	auto combinator_s = std::make_shared<omnisketch::ExhaustiveCombinator>();
	combinator_s->AddPredicate(dim_s_att, omnisketch::PredicateConverter::ConvertRange(0, 249));
	auto combinator_t = std::make_shared<omnisketch::ExhaustiveCombinator>();
	combinator_t->AddPredicate(dim_t_att, omnisketch::PredicateConverter::ConvertRange(0, 124));
	auto combinator_fact = std::make_shared<omnisketch::ExhaustiveCombinator>();
	combinator_fact->AddPredicate(fact_fk_s, combinator_s->ComputeResult(UINT64_MAX));
	combinator_fact->AddPredicate(fact_fk_t, combinator_t->ComputeResult(UINT64_MAX));
	auto result_2 = combinator_fact->ComputeResult(UINT64_MAX);
	EXPECT_EQ(result, result_2->RecordCount());
	registry.Clear();
}

TEST(PlanGeneratorTest, FKFKTwoRelations) {
	const size_t TABLE_SIZE = 1000;
	const size_t FK_SIZE = 50;

	auto &registry = omnisketch::Registry::Get();
	auto r_fk_col = registry.CreateOmniSketch<size_t>("R", "fk_col");
	auto r_att = registry.CreateOmniSketch<size_t>("R", "att");
	auto s_fk_col = registry.CreateOmniSketch<size_t>("S", "fk_col");
	auto s_att = registry.CreateOmniSketch<size_t>("S", "att");

	for (size_t i = 0; i < TABLE_SIZE; i++) {
		r_fk_col->AddRecord(i % FK_SIZE, i);
		r_att->AddRecord(i % 2, i);
		s_fk_col->AddRecord(i % (FK_SIZE / 2), i);
		s_att->AddRecord(i % 5, i);
	}

	omnisketch::PlanGenerator gen;
	gen.AddPredicate("R", "att", omnisketch::PredicateConverter::ConvertPoint(1));
	gen.AddPredicate("S", "att", omnisketch::PredicateConverter::ConvertPoint(2));
	gen.AddFKFKJoin("R", "fk_col", "S", "fk_col");
	const double result = gen.EstimateCardinality();
	EXPECT_GT(result, 0.0);
	registry.Clear();
}

TEST(PlanGeneratorTest, FKFKTriangle) {
	const size_t TABLE_SIZE = 1000;
	const size_t FK_SIZE = 50;

	auto &registry = omnisketch::Registry::Get();
	auto r_fk_col = registry.CreateOmniSketch<size_t>("R", "fk_col");
	auto r_att = registry.CreateOmniSketch<size_t>("R", "att");
	auto s_fk_col = registry.CreateOmniSketch<size_t>("S", "fk_col");
	auto s_att = registry.CreateOmniSketch<size_t>("S", "att");
	auto t_fk_col = registry.CreateOmniSketch<size_t>("T", "fk_col");
	auto t_att = registry.CreateOmniSketch<size_t>("T", "att");

	for (size_t i = 0; i < TABLE_SIZE; i++) {
		r_fk_col->AddRecord(i % FK_SIZE, i);
		r_att->AddRecord(i % 2, i);
		s_fk_col->AddRecord(i % (FK_SIZE / 2), i);
		s_att->AddRecord(i % 5, i);
		t_fk_col->AddRecord(i % FK_SIZE, i);
		t_att->AddRecord(i % 10, i);
	}

	omnisketch::PlanGenerator gen;
	gen.AddPredicate("R", "att", omnisketch::PredicateConverter::ConvertPoint(1));
	gen.AddPredicate("S", "att", omnisketch::PredicateConverter::ConvertPoint(2));
	gen.AddPredicate("T", "att", omnisketch::PredicateConverter::ConvertPoint(7));
	gen.AddFKFKJoin("R", "fk_col", "S", "fk_col");
	gen.AddFKFKJoin("R", "fk_col", "T", "fk_col");
	gen.AddFKFKJoin("S", "fk_col", "T", "fk_col");
	const double result = gen.EstimateCardinality();
	EXPECT_GT(result, 0.0);
	registry.Clear();
}
