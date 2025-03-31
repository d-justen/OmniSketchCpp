#include <gtest/gtest.h>

#include "combinator_test.hpp"

using UncorrelatedCombinator = CombinatorTestFixture<omnisketch::UncorrelatedCombinator>;
using ExhaustiveCombinator = CombinatorTestFixture<omnisketch::ExhaustiveCombinator>;

TEST_F(UncorrelatedCombinator, SingleJoinNoSampling) {
	combinator->AddPredicate(sketch_1, probe_result_1);
	auto result = combinator->Execute(SAMPLE_COUNT);
	EXPECT_EQ(result->RecordCount(), 20);
	EXPECT_GT(result->SampleCount(), 1);
	EXPECT_EQ(result->MaxSampleCount(), SAMPLE_COUNT);
}

TEST_F(ExhaustiveCombinator, SingleJoinNoSampling) {
	combinator->AddPredicate(sketch_1, probe_result_1);
	auto result = combinator->Execute(SAMPLE_COUNT);
	EXPECT_EQ(result->RecordCount(), 20);
	EXPECT_GT(result->SampleCount(), 1);
	EXPECT_EQ(result->MaxSampleCount(), SAMPLE_COUNT);
}

TEST_F(UncorrelatedCombinator, SingleJoinWithSampling) {
	combinator->AddPredicate(sketch_3, probe_result_3);
	auto result = combinator->Execute(SAMPLE_COUNT);
	EXPECT_EQ(result->RecordCount(), 32);
	EXPECT_GT(result->SampleCount(), 1);
	EXPECT_EQ(result->MaxSampleCount(), SAMPLE_COUNT);
}

TEST_F(ExhaustiveCombinator, SingleJoinWithSampling) {
	combinator->AddPredicate(sketch_3, probe_result_3);
	auto result = combinator->Execute(SAMPLE_COUNT);
	EXPECT_EQ(result->RecordCount(), 32);
	EXPECT_GT(result->SampleCount(), 1);
	EXPECT_EQ(result->MaxSampleCount(), SAMPLE_COUNT);
}

TEST_F(UncorrelatedCombinator, MultiJoin) {
	combinator->AddPredicate(sketch_1, probe_result_1);
	combinator->AddPredicate(sketch_2, probe_result_2);
	combinator->AddPredicate(sketch_3, probe_result_3);
	auto result = combinator->Execute(SAMPLE_COUNT);
	EXPECT_EQ(result->RecordCount(), 4);
	EXPECT_EQ(result->SampleCount(), 2);
	EXPECT_EQ(result->MaxSampleCount(), SAMPLE_COUNT);
}

TEST_F(ExhaustiveCombinator, MultiJoin) {
	combinator->AddPredicate(sketch_1, probe_result_1);
	combinator->AddPredicate(sketch_2, probe_result_2);
	combinator->AddPredicate(sketch_3, probe_result_3);
	auto result = combinator->Execute(SAMPLE_COUNT);
	// 8 = the actual result cardinality
	EXPECT_GE(result->RecordCount(), 8);
	EXPECT_EQ(result->SampleCount(), 2);
	EXPECT_EQ(result->MaxSampleCount(), SAMPLE_COUNT);
}