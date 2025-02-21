#include <gtest/gtest.h>

#include "algorithm/min_hash_set_operations.hpp"
#include "min_hash_sketch_test.hpp"
#include "min_hash_sketch.hpp"

using MinHashSketchSet64 = MinHashSketchTestFixture<std::set<uint64_t>>;
using MinHashSketchVec64 = MinHashSketchTestFixture<std::vector<uint64_t>>;

TEST_F(MinHashSketchSet64, Intersection) {
	FillSketches();
	std::vector<const omnisketch::MinHashSketch<std::set<uint64_t>> *> ptrs {&a, &b, &c};
	auto result = omnisketch::Algorithm::MultiwayIntersection(ptrs);
	EXPECT_EQ(result.max_count, SKETCH_SIZE);
	EXPECT_EQ(result.Size(), MATCH_COUNT);
	for (size_t i = 0; i < MATCH_COUNT; i++) {
		uint64_t hash = omnisketch::Hash(i);
		EXPECT_NE(result.hashes.find(hash), result.hashes.end());
	}
}

TEST_F(MinHashSketchSet64, Flatten) {
	FillSketches();
	auto flattened = a.Flatten();
	EXPECT_EQ(a.max_count, flattened.max_count);
	EXPECT_EQ(a.Size(), flattened.Size());
	for (size_t i = 0; i < a.Size(); i++) {
		uint64_t hash = omnisketch::Hash(i);
		EXPECT_NE(a.hashes.find(hash), a.hashes.end());
		if (i < a.Size() - 1) {
			EXPECT_LT(flattened.hashes[i], flattened.hashes[i + 1]);
		}
	}
}

TEST_F(MinHashSketchSet64, FlattenIntersection) {
	FillSketches();
	std::vector<omnisketch::MinHashSketch<std::vector<uint64_t>>> flat_sketches {a.Flatten(), b.Flatten(), c.Flatten()};
	std::vector<const omnisketch::MinHashSketch<std::vector<uint64_t>> *> ptrs {&flat_sketches[0], &flat_sketches[1],
	                                                                            &flat_sketches[2]};

	auto result = omnisketch::Algorithm::MultiwayIntersection(ptrs);
	EXPECT_EQ(result.max_count, SKETCH_SIZE);
	EXPECT_EQ(result.Size(), MATCH_COUNT);
	for (size_t i = 0; i < MATCH_COUNT; i++) {
		uint64_t hash = omnisketch::Hash(i);
		EXPECT_NE(result.hashes.find(hash), result.hashes.end());
	}
}

TEST_F(MinHashSketchVec64, Intersection) {
	FillSketches();
	std::vector<const omnisketch::MinHashSketch<std::vector<uint64_t>> *> ptrs {&a, &b, &c};
	auto result = omnisketch::Algorithm::MultiwayIntersection(ptrs);
	EXPECT_EQ(result.max_count, SKETCH_SIZE);
	EXPECT_EQ(result.Size(), MATCH_COUNT);
	for (size_t i = 0; i < MATCH_COUNT; i++) {
		uint64_t hash = omnisketch::Hash(i);
		EXPECT_NE(result.hashes.find(hash), result.hashes.end());
	}
}

TEST_F(MinHashSketchSet64, InsertBelowMaxHash) {
	for (size_t i = 0; i < SKETCH_SIZE; i++) {
		a.AddRecord(i + SKETCH_SIZE);
		EXPECT_EQ(a.Size(), i + 1);
	}

	EXPECT_EQ(a.Size(), SKETCH_SIZE);

	for (size_t i = 0; i < SKETCH_SIZE; i++) {
		a.AddRecord(i);
		EXPECT_EQ(a.Size(), SKETCH_SIZE);
		EXPECT_NE(a.hashes.find(i), a.hashes.end());
	}
}

TEST_F(MinHashSketchSet64, DoubleInsert) {
	a.AddRecord(1);
	EXPECT_EQ(a.Size(), 1);
	a.AddRecord(1);
	EXPECT_EQ(a.Size(), 1);
}

TEST_F(MinHashSketchSet64, UnionThisSideSurvives) {
	FillSketchesNoHash();
	auto a_copy = a;
	a_copy.Union(c);
	EXPECT_EQ(a_copy.Size(), a.Size());
	auto a_it = a.hashes.begin();
	auto a_copy_it = a_copy.hashes.begin();

	for (; a_it != a.hashes.end(); a_it++, a_copy_it++) {
		EXPECT_EQ(*a_it, *a_copy_it);
	}
}

TEST_F(MinHashSketchSet64, UnionOtherSideSurvives) {
	FillSketchesNoHash();
	c.Union(a);
	EXPECT_EQ(c.Size(), a.Size());
	auto a_it = a.hashes.begin();
	auto c_it = c.hashes.begin();

	for (; a_it != a.hashes.end(); a_it++, c_it++) {
		EXPECT_EQ(*a_it, *c_it);
	}
}

TEST_F(MinHashSketchSet64, UnionDuplicates) {
	FillSketchesNoHash();
	std::vector<const omnisketch::MinHashSketch<std::set<uint64_t>> *> intersect_ptrs {&b, &c};
	auto intersection = omnisketch::Algorithm::MultiwayIntersection(intersect_ptrs);

	b.max_count = 16;
	b.Union(c);
	// |A union B| = |A| + |B| - |A intersect B|
	EXPECT_EQ(b.Size(), SKETCH_SIZE * 2 - intersection.Size());
}
