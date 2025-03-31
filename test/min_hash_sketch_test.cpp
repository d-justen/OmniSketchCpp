#include <gtest/gtest.h>

#include "min_hash_sketch_test.hpp"
#include "include/min_hash_sketch.hpp"

using MinHashSketchSet = MinHashSketchTestFixture<omnisketch::MinHashSketchSet>;
using MinHashSketchVec = MinHashSketchTestFixture<omnisketch::MinHashSketchVector>;

TEST_F(MinHashSketchSet, Intersection) {
	FillSketches();
	auto result = a->Intersect({a, b, c});
	EXPECT_EQ(result->MaxCount(), SKETCH_SIZE);
	EXPECT_EQ(result->Size(), MATCH_COUNT);
	auto *typed_result = dynamic_cast<omnisketch::MinHashSketchSet *>(result.get());
	for (size_t i = 0; i < MATCH_COUNT; i++) {
		uint64_t hash = omnisketch::Hash(i);
		EXPECT_NE(typed_result->Data().find(hash), typed_result->Data().end());
	}
}

TEST_F(MinHashSketchSet, Flatten) {
	FillSketches();
	auto flattened = a->Flatten();
	EXPECT_EQ(a->MaxCount(), flattened->MaxCount());
	EXPECT_EQ(a->Size(), flattened->Size());
	auto *typed_a = dynamic_cast<omnisketch::MinHashSketchSet *>(a.get());
	auto *typed_flattened = dynamic_cast<omnisketch::MinHashSketchVector *>(flattened.get());

	for (size_t i = 0; i < a->Size(); i++) {
		uint64_t hash = omnisketch::Hash(i);
		EXPECT_NE(typed_a->Data().find(hash), typed_a->Data().end());
		if (i < a->Size() - 1) {
			EXPECT_LT(typed_flattened->Data()[i], typed_flattened->Data()[i + 1]);
		}
	}
}

TEST_F(MinHashSketchSet, FlattenIntersection) {
	FillSketches();
	std::vector<std::shared_ptr<omnisketch::MinHashSketch>> flat_sketches {a->Flatten(), b->Flatten(), c->Flatten()};

	auto result = flat_sketches.front()->Intersect(flat_sketches);
	EXPECT_EQ(result->MaxCount(), SKETCH_SIZE);
	EXPECT_EQ(result->Size(), MATCH_COUNT);
	auto *typed_result = dynamic_cast<omnisketch::MinHashSketchSet *>(result.get());
	for (size_t i = 0; i < MATCH_COUNT; i++) {
		uint64_t hash = omnisketch::Hash(i);
		EXPECT_NE(typed_result->Data().find(hash), typed_result->Data().end());
	}
}

TEST_F(MinHashSketchVec, Intersection) {
	FillSketches();
	auto result = a->Intersect({a, b, c});
	EXPECT_EQ(result->MaxCount(), SKETCH_SIZE);
	EXPECT_EQ(result->Size(), MATCH_COUNT);
	auto *typed_result = dynamic_cast<omnisketch::MinHashSketchSet *>(result.get());
	for (size_t i = 0; i < MATCH_COUNT; i++) {
		uint64_t hash = omnisketch::Hash(i);
		EXPECT_NE(typed_result->Data().find(hash), typed_result->Data().end());
	}
}

TEST_F(MinHashSketchSet, InsertBelowMaxHash) {
	for (size_t i = 0; i < SKETCH_SIZE; i++) {
		a->AddRecord(i + SKETCH_SIZE);
		EXPECT_EQ(a->Size(), i + 1);
	}

	EXPECT_EQ(a->Size(), SKETCH_SIZE);

	auto *typed_a = dynamic_cast<omnisketch::MinHashSketchSet *>(a.get());
	for (size_t i = 0; i < SKETCH_SIZE; i++) {
		a->AddRecord(i);
		EXPECT_EQ(a->Size(), SKETCH_SIZE);
		EXPECT_NE(typed_a->Data().find(i), typed_a->Data().end());
	}
}

TEST_F(MinHashSketchSet, DoubleInsert) {
	a->AddRecord(1);
	EXPECT_EQ(a->Size(), 1);
	a->AddRecord(1);
	EXPECT_EQ(a->Size(), 1);
}

TEST_F(MinHashSketchSet, UnionThisSideSurvives) {
	FillSketchesNoHash();
	auto a_copy = a->Copy();
	a_copy->Combine(*c);
	EXPECT_EQ(a_copy->Size(), a->Size());
	auto a_it = dynamic_cast<omnisketch::MinHashSketchSet *>(a.get())->Data().begin();
	auto a_end = dynamic_cast<omnisketch::MinHashSketchSet *>(a.get())->Data().end();
	auto a_copy_it = dynamic_cast<omnisketch::MinHashSketchSet *>(a_copy.get())->Data().begin();

	for (; a_it != a_end; a_it++, a_copy_it++) {
		EXPECT_EQ(*a_it, *a_copy_it);
	}
}

TEST_F(MinHashSketchSet, UnionOtherSideSurvives) {
	FillSketchesNoHash();
	c->Combine(*a);
	EXPECT_EQ(c->Size(), a->Size());
	auto a_it = dynamic_cast<omnisketch::MinHashSketchSet *>(a.get())->Data().begin();
	auto a_end = dynamic_cast<omnisketch::MinHashSketchSet *>(a.get())->Data().end();
	auto c_it = dynamic_cast<omnisketch::MinHashSketchSet *>(c.get())->Data().begin();

	for (; a_it != a_end; a_it++, c_it++) {
		EXPECT_EQ(*a_it, *c_it);
	}
}

TEST_F(MinHashSketchSet, UnionDuplicates) {
	FillSketchesNoHash();
	auto intersection = b->Intersect({b, c});

	auto b_resized = b->Resize(16);
	b_resized->Combine(*c);
	// |A union B| = |A| + |B| - |A intersect B|
	EXPECT_EQ(b_resized->Size(), SKETCH_SIZE * 2 - intersection->Size());
}

TEST_F(MinHashSketchSet, MultiwayCombine) {
	FillSketches();
	auto combine_result = a->Combine({a, b, c});
	EXPECT_EQ(combine_result->MaxCount(), a->MaxCount());
	EXPECT_EQ(combine_result->Size(), a->Size());
}

TEST_F(MinHashSketchVec, MultiwayCombine) {
	FillSketches();
	auto combine_result = a->Combine({a, b, c});
	EXPECT_EQ(combine_result->MaxCount(), a->MaxCount());
	EXPECT_EQ(combine_result->Size(), a->Size());
}