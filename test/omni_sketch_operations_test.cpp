#include <gtest/gtest.h>

#include "algorithm/omni_sketch_operations.hpp"
#include "omni_sketch.hpp"

TEST(OmniSketchOperationsTest, ConjunctPointQuery) {
	omnisketch::OmniSketch<size_t, uint64_t, std::set<uint64_t>> sketch_1(4, 3, 8);
	omnisketch::OmniSketch<size_t, uint64_t, std::set<uint64_t>> sketch_2(4, 3, 8);
	omnisketch::OmniSketch<size_t, uint64_t, std::set<uint64_t>> sketch_3(4, 3, 8);
	for (size_t i = 0; i < 8; i++) {
		sketch_1.AddRecord(i, i);
		sketch_2.AddRecord(8 + i, i);
		sketch_3.AddRecord(17 + i, i);
	}

	std::vector<omnisketch::OmniSketch<size_t, uint64_t, std::set<uint64_t>> *> sketch_vec {&sketch_1, &sketch_2,
	                                                                                        &sketch_3};

	for (size_t i = 0; i < 8; i++) {
		auto card_est = omnisketch::Algorithm::ConjunctPointQueries(sketch_vec, std::vector<size_t> {i, 8 + i, 17 + i});
		EXPECT_EQ(card_est.min_hash_sketch.max_count, 8);
		EXPECT_EQ(card_est.min_hash_sketch.hashes.size(), 1);
		EXPECT_NE(card_est.min_hash_sketch.hashes.find(omnisketch::Hash(i)), card_est.min_hash_sketch.hashes.end());
		EXPECT_EQ(card_est.cardinality, 1.0);
	}
}

TEST(OmniSketchOperationsTest, DisjunctPointQuery) {
	omnisketch::OmniSketch<size_t, uint64_t, std::set<uint64_t>> sketch_1(8, 3, 16);
	for (size_t i = 0; i < 16; i++) {
		sketch_1.AddRecord(i, i);
	}

	for (size_t i = 0; i < 16; i++) {
		auto card_est =
		    omnisketch::Algorithm::DisjunctPointQueries(sketch_1, std::vector<size_t> {i, (i + 1) % 16, +(i + 7) % 16});
		EXPECT_EQ(card_est.min_hash_sketch.max_count, 16);
		EXPECT_EQ(card_est.min_hash_sketch.hashes.size(), 3);
		EXPECT_NE(card_est.min_hash_sketch.hashes.find(omnisketch::Hash(i)), card_est.min_hash_sketch.hashes.end());
		EXPECT_EQ(card_est.cardinality, 3.0);
		card_est = omnisketch::Algorithm::DisjunctPointQueries(sketch_1, std::vector<size_t> {i, 23, +(i + 7) % 16});
		EXPECT_EQ(card_est.min_hash_sketch.hashes.size(), 2);
		EXPECT_NE(card_est.min_hash_sketch.hashes.find(omnisketch::Hash(i)), card_est.min_hash_sketch.hashes.end());
		EXPECT_EQ(card_est.cardinality, 2.0);
	}
}

TEST(OmniSketchOperationsTest, SetMembership) {
	omnisketch::OmniSketch<size_t, uint64_t, std::set<uint64_t>> sketch_1(4, 3, 16);
	for (size_t i = 0; i < 16; i++) {
		sketch_1.AddRecord(i, i);
	}

	for (size_t i = 0; i < 16; i++) {
		const auto values = std::vector<size_t> {i, (i + 1) % 16, +(i + 7) % 16};
		auto card_est_1 = omnisketch::Algorithm::DisjunctPointQueries(sketch_1, values);
		auto card_est_2 = omnisketch::Algorithm::SetMembership(sketch_1, values);
		EXPECT_EQ(card_est_1.min_hash_sketch.hashes, card_est_2.min_hash_sketch.hashes);
		EXPECT_EQ(card_est_1.min_hash_sketch.max_count, card_est_2.min_hash_sketch.max_count);
	}
}
