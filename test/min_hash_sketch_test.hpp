#pragma once

#include <gtest/gtest.h>

#include "min_hash_sketch/min_hash_sketch_set.hpp"
#include "min_hash_sketch/min_hash_sketch_vector.hpp"
#include "util/hash.hpp"

static constexpr size_t SKETCH_SIZE = 8;
static constexpr size_t MATCH_COUNT = 3;

template <typename T>
class MinHashSketchTestFixture : public testing::Test {
protected:
    MinHashSketchTestFixture() {
        a = std::make_shared<T>(SKETCH_SIZE);
        b = std::make_shared<T>(SKETCH_SIZE);
        c = std::make_shared<T>(SKETCH_SIZE);
        hf = std::make_shared<omnisketch::MurmurHashFunction<size_t>>();
    }

    void FillSketches() {
        for (size_t i = 0; i < MATCH_COUNT; i++) {
            auto hash = hf->HashRid(i);
            a->AddRecord(hash);
            b->AddRecord(hash);
            c->AddRecord(hash);
        }
        for (size_t i = MATCH_COUNT; i < SKETCH_SIZE; i++) {
            auto hash_1 = hf->HashRid(i);
            a->AddRecord(hash_1);
            auto hash_2 = hf->HashRid(SKETCH_SIZE + i);
            b->AddRecord(i % 2 == 0 ? hash_1 : hash_2);
            c->AddRecord(hash_2);
        }
    }

    void FillSketchesNoHash() {
        for (size_t i = 0; i < MATCH_COUNT; i++) {
            a->AddRecord(i);
            b->AddRecord(i);
            c->AddRecord(i);
        }
        for (size_t i = MATCH_COUNT; i < SKETCH_SIZE; i++) {
            a->AddRecord(i);
            b->AddRecord(i % 2 == 0 ? i : SKETCH_SIZE + i);
            c->AddRecord(SKETCH_SIZE + i);
        }
    }

    std::shared_ptr<omnisketch::MinHashSketch> a;
    std::shared_ptr<omnisketch::MinHashSketch> b;
    std::shared_ptr<omnisketch::MinHashSketch> c;
    std::shared_ptr<omnisketch::HashFunction<size_t>> hf;
};
