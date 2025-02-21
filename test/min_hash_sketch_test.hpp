#pragma once

#include <gtest/gtest.h>

#include "hash.hpp"
#include "min_hash_sketch.hpp"

static constexpr size_t SKETCH_SIZE = 8;
static constexpr size_t MATCH_COUNT = 3;

template <typename T>
class MinHashSketchTestFixture : public testing::Test {
protected:
	MinHashSketchTestFixture() {
		a = omnisketch::MinHashSketch<T>(SKETCH_SIZE);
		b = omnisketch::MinHashSketch<T>(SKETCH_SIZE);
		c = omnisketch::MinHashSketch<T>(SKETCH_SIZE);
	}

	void FillSketches() {
		for (size_t i = 0; i < MATCH_COUNT; i++) {
			typename T::value_type hash = omnisketch::Hash(i);
			a.AddRecord(hash);
			b.AddRecord(hash);
			c.AddRecord(hash);
		}
		for (size_t i = MATCH_COUNT; i < SKETCH_SIZE; i++) {
			typename T::value_type hash_1 = omnisketch::Hash(i);
			a.AddRecord(hash_1);
			typename T::value_type hash_2 = omnisketch::Hash(SKETCH_SIZE + i);
			b.AddRecord(i % 2 == 0 ? hash_1 : hash_2);
			c.AddRecord(hash_2);
		}
	}

	void FillSketchesNoHash() {
		for (size_t i = 0; i < MATCH_COUNT; i++) {
			a.AddRecord(i);
			b.AddRecord(i);
			c.AddRecord(i);
		}
		for (size_t i = MATCH_COUNT; i < SKETCH_SIZE; i++) {
			a.AddRecord(i);
			b.AddRecord(i % 2 == 0 ? i : SKETCH_SIZE + i);
			c.AddRecord(SKETCH_SIZE + i);
		}
	}

	void SetUp() override {
		// Code here will be called immediately after the constructor (right
		// before each test).
	}

	void TearDown() override {
		// Code here will be called immediately after each test (right
		// before the destructor).
	}

	omnisketch::MinHashSketch<T> a;
	omnisketch::MinHashSketch<T> b;
	omnisketch::MinHashSketch<T> c;
	// Class members declared here can be used by all tests in the test suite
	// for Foo.
};
