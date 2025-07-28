#include <gtest/gtest.h>

#include "combinator_test.hpp"
#include <random>

TEST_F(CombinatorTestFixture, SingleJoinNoSampling) {
	combinator->AddPredicate(sketch_1, probe_result_1);
	auto result = combinator->ComputeResult(SAMPLE_COUNT);
	EXPECT_EQ(result->RecordCount(), 20);
	EXPECT_GT(result->SampleCount(), 1);
	EXPECT_EQ(result->MaxSampleCount(), SAMPLE_COUNT);
}

TEST_F(CombinatorTestFixture, SingleJoinWithSampling) {
	combinator->AddPredicate(sketch_3, probe_result_3);
	auto result = combinator->ComputeResult(SAMPLE_COUNT);
	EXPECT_EQ(result->RecordCount(), 32);
	EXPECT_GT(result->SampleCount(), 1);
	EXPECT_EQ(result->MaxSampleCount(), SAMPLE_COUNT);
}

TEST_F(CombinatorTestFixture, MultiJoin) {
	combinator->AddPredicate(sketch_1, probe_result_1);
	combinator->AddPredicate(sketch_2, probe_result_2);
	combinator->AddPredicate(sketch_3, probe_result_3);
	auto result = combinator->ComputeResult(SAMPLE_COUNT);
	// 8 = the actual result cardinality
	EXPECT_GE(result->RecordCount(), 8);
	EXPECT_EQ(result->SampleCount(), 2);
	EXPECT_EQ(result->MaxSampleCount(), SAMPLE_COUNT);
}

TEST_F(CombinatorTestFixture, JoinCorrelation) {
	const size_t RECORD_COUNT = 10000;
	const size_t DOMAIN_1 = 200;
	const double SEL_1 = 0.5;
	auto omni_1 = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(64, 3, 32);
	const size_t DOMAIN_2 = 1000;
	const double SEL_2 = 0.2;
	auto omni_2 = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(64, 3, 32);

	std::vector<size_t> attributes_2;
	attributes_2.reserve(RECORD_COUNT);
	for (size_t i = 0; i < RECORD_COUNT; i++) {
		attributes_2.push_back((i % DOMAIN_2) + 1);
	}

	std::mt19937 g;
	std::shuffle(attributes_2.begin(), attributes_2.end(), g);

	for (size_t i = 0; i < RECORD_COUNT; i++) {
		omni_1->AddRecord((i % DOMAIN_1) + 1, i);
		omni_2->AddRecord(attributes_2[i], i);
	}

	std::vector<double> sampling_rates {1.0, 0.8, 0.6, 0.4, 0.2, 0.1};
	for (auto sampling_rate : sampling_rates) {
		size_t probe_size_1 = DOMAIN_1 * SEL_1 * sampling_rate;
		size_t probe_size_2 = DOMAIN_2 * SEL_2 * sampling_rate;
		auto set_1 = omnisketch::PredicateConverter::ConvertRange((size_t)1, probe_size_1);
		set_1->SetRecordCount(DOMAIN_1 * SEL_1);
		auto set_2 = omnisketch::PredicateConverter::ConvertRange((size_t)1, probe_size_2);
		set_2->SetRecordCount(DOMAIN_2 * SEL_2);

		auto comb = std::make_shared<omnisketch::CombinedPredicateEstimator>(32);
		comb->AddPredicate(omni_1, set_1);
		comb->AddPredicate(omni_2, set_2);
		comb->ComputeResult(SAMPLE_COUNT);
	}
}

TEST_F(CombinatorTestFixture, FilterProbeSet) {
	const size_t FK_SIDE_CARD = 1000;
	const size_t FK_SIDE_ATTRIBUTE_DOMAIN = 100;
	const size_t PK_SIDE_CARD = 200;
	auto omni_fk_side = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    64, 3, 32, std::make_shared<omnisketch::MurmurHashFunction<size_t>>(),
	    std::make_shared<omnisketch::ProbeAllSum>(), std::make_shared<omnisketch::BarrettModSplitHashMapper>(64));

	for (size_t i = 0; i < FK_SIDE_CARD; i++) {
		omni_fk_side->AddRecord(i % FK_SIDE_ATTRIBUTE_DOMAIN, i);
	}

	auto probe_set = std::make_shared<omnisketch::OmniSketchCell>(32);
	for (size_t i = 0; i < 32; i++) {
		if (i % 2 == 0) {
			probe_set->AddRecord(omnisketch::hash_functions::MurmurHash64(i));
		} else {
			probe_set->AddRecord(omnisketch::hash_functions::MurmurHash64(i + FK_SIDE_ATTRIBUTE_DOMAIN));
		}
	}
	probe_set->SetRecordCount(PK_SIDE_CARD);

	auto result = combinator->FilterProbeSet(omni_fk_side, probe_set);
	EXPECT_EQ(result->RecordCount(), FK_SIDE_CARD);
	EXPECT_EQ(result->SampleCount(), probe_set->SampleCount() / 2);

	auto omni_fk_side_2 = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    64, 3, 32, std::make_shared<omnisketch::MurmurHashFunction<size_t>>(),
	    std::make_shared<omnisketch::ProbeAllSum>(), std::make_shared<omnisketch::BarrettModSplitHashMapper>(64));

	for (size_t i = 0; i < FK_SIDE_CARD; i++) {
		omni_fk_side_2->AddRecord(i % 10, i);
	}

	combinator->AddPredicate(omni_fk_side_2, omnisketch::PredicateConverter::ConvertRange(0, 4));
	result = combinator->FilterProbeSet(omni_fk_side, probe_set);
	EXPECT_GE(result->RecordCount(), FK_SIDE_CARD / 2);
	EXPECT_LT(result->RecordCount(), FK_SIDE_CARD);
	EXPECT_GE(result->SampleCount(), probe_set->SampleCount() / 4);

	auto omni_fk_side_3 = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    64, 3, 32, std::make_shared<omnisketch::MurmurHashFunction<size_t>>(),
	    std::make_shared<omnisketch::ProbeAllSum>(), std::make_shared<omnisketch::BarrettModSplitHashMapper>(64));

	for (size_t i = 0; i < FK_SIDE_CARD; i++) {
		omni_fk_side_3->AddRecord(i % 8, i);
	}

	auto probe_range_3 = omnisketch::PredicateConverter::ConvertRange(2, 4);
	probe_range_3->SetRecordCount(6);
	combinator->AddPredicate(omni_fk_side_3, probe_range_3);
	result = combinator->FilterProbeSet(omni_fk_side, probe_set);
	EXPECT_GE(result->RecordCount(), ((FK_SIDE_CARD / 2.0) / 8.0) * 6.0);
	EXPECT_GE(result->SampleCount(), 1);
}
