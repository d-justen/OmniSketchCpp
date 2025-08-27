#include <gtest/gtest.h>

#include "include/plan_generator.hpp"

TEST(PlanGeneratorTest, StarShape) {
    const size_t FACT_TABLE_SIZE = 1000;
    const size_t DIM_S_SIZE = 500;
    const size_t DIM_T_SIZE = 250;

    auto& registry = omnisketch::Registry::Get();
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

    auto combinator_s = std::make_shared<omnisketch::CombinedPredicateEstimator>(dim_s_att->MinHashSketchSize());
    combinator_s->AddPredicate(dim_s_att, omnisketch::PredicateConverter::ConvertRange(0, 249));
    auto combinator_t = std::make_shared<omnisketch::CombinedPredicateEstimator>(dim_t_att->MinHashSketchSize());
    combinator_t->AddPredicate(dim_t_att, omnisketch::PredicateConverter::ConvertRange(0, 124));
    auto combinator_fact = std::make_shared<omnisketch::CombinedPredicateEstimator>(fact_fk_s->MinHashSketchSize());
    combinator_fact->AddPredicate(fact_fk_s, combinator_s->ComputeResult(UINT64_MAX));
    combinator_fact->AddPredicate(fact_fk_t, combinator_t->ComputeResult(UINT64_MAX));
    auto result_2 = combinator_fact->ComputeResult(UINT64_MAX);
    EXPECT_EQ(result, result_2->RecordCount());
}
