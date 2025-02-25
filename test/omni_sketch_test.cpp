#include <gtest/gtest.h>

#include "omni_sketch.hpp"

TEST(OmniSketchTest, BasicEstimation) {
	omnisketch::OmniSketch<int, int, std::set<uint64_t>> sketch(4, 3, 8);
	sketch.AddRecord(1, 1);
	sketch.AddRecord(1, 2);
	EXPECT_EQ(sketch.EstimateCardinality(1).cardinality, 2);
	sketch.AddRecord(2, 3);
	sketch.AddRecord(3, 4);
	EXPECT_EQ(sketch.EstimateCardinality(1).cardinality, 2);
	EXPECT_EQ(sketch.EstimateCardinality(3).cardinality, 1);
	EXPECT_EQ(sketch.RecordCount(), 4);
	EXPECT_EQ(sketch.EstimateCardinality(0).cardinality, 0);
}

TEST(OmniSketchTest, NoMatches) {
	omnisketch::OmniSketch<int, int, std::set<uint64_t>> sketch(4, 3, 8);
	EXPECT_EQ(sketch.EstimateCardinality(17).cardinality, 0);
}

TEST(OmniSketchTest, FlattenEstimate) {
	omnisketch::OmniSketch<int, int, std::set<uint64_t>> sketch(4, 3, 8);
	for (size_t i = 0; i < 64; i++) {
		sketch.AddRecord(i, i);
	}

	auto card_est = sketch.EstimateCardinality(17);
	auto sketch_flattened = sketch.Flatten();

	std::vector<omnisketch::MinHashSketch<std::vector<uint64_t>>> results;
	auto card_est_2 = sketch_flattened.EstimateCardinality(17);
	EXPECT_EQ(card_est.cardinality, card_est_2.cardinality);
	EXPECT_EQ(card_est.min_hash_sketch.hashes, card_est_2.min_hash_sketch.hashes);
	EXPECT_EQ(card_est.min_hash_sketch.max_count, card_est_2.min_hash_sketch.max_count);
}

TEST(OmniSketchTest, BasicEstimationString) {
	omnisketch::OmniSketch<std::string, int, std::set<uint64_t>> sketch(4, 3, 8);
	sketch.AddRecord("String #1", 1);
	sketch.AddRecord("String #1", 2);
	EXPECT_EQ(sketch.EstimateCardinality("String #1").cardinality, 2);
	sketch.AddRecord("Another String", 3);
	sketch.AddRecord("String #2", 4);
	EXPECT_EQ(sketch.EstimateCardinality("String #1").cardinality, 2);
	EXPECT_EQ(sketch.EstimateCardinality("Another String").cardinality, 1);
	EXPECT_EQ(sketch.RecordCount(), 4);
	EXPECT_EQ(sketch.EstimateCardinality("String #3").cardinality, 0);
}