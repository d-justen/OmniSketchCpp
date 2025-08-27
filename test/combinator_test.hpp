#pragma once

#include <gtest/gtest.h>
#include "combinator.hpp"
#include "min_hash_sketch/min_hash_sketch_set.hpp"
#include "omni_sketch/standard_omni_sketch.hpp"

static constexpr size_t WIDTH = 16;
static constexpr size_t DEPTH = 3;
static constexpr size_t SAMPLE_COUNT = 16;

class CombinatorTestFixture : public testing::Test {
protected:
    CombinatorTestFixture() {
        combinator = std::make_unique<omnisketch::CombinedPredicateEstimator>(SAMPLE_COUNT);
        sketch_1 = std::make_shared<omnisketch::TypedPointOmniSketch<int>>(WIDTH, DEPTH, SAMPLE_COUNT);
        sketch_2 = std::make_shared<omnisketch::TypedPointOmniSketch<int>>(WIDTH, DEPTH, SAMPLE_COUNT);
        sketch_3 = std::make_shared<omnisketch::TypedPointOmniSketch<int>>(WIDTH, DEPTH, SAMPLE_COUNT);
        FillSketches();
        CreateProbeResults();
    }

    static constexpr size_t REC_COUNT = 100;
    static constexpr size_t ATT_COUNT_1 = 5;
    static constexpr size_t ATT_COUNT_2 = 10;
    static constexpr size_t ATT_COUNT_3 = 50;

    void FillSketches() {
        for (size_t rid = 0; rid < REC_COUNT; rid++) {
            sketch_1->AddRecord(rid % ATT_COUNT_1, rid);
            sketch_2->AddRecord(rid % ATT_COUNT_2, rid);
            sketch_3->AddRecord(rid % ATT_COUNT_3, rid);
        }
    }

    void CreateProbeResults() {
        auto hf = std::make_shared<omnisketch::MurmurHashFunction<size_t>>();
        auto probe_set_1 = std::make_shared<omnisketch::MinHashSketchSet>(8);
        probe_set_1->AddRecord(hf->Hash(3));
        probe_result_1 = std::make_shared<omnisketch::OmniSketchCell>(probe_set_1, 1);

        auto probe_set_2 = std::make_shared<omnisketch::MinHashSketchSet>(8);
        probe_set_2->AddRecord(hf->Hash(3));
        probe_set_2->AddRecord(hf->Hash(4));
        probe_set_2->AddRecord(hf->Hash(5));
        probe_result_2 = std::make_shared<omnisketch::OmniSketchCell>(probe_set_2, 6);

        auto probe_set_3 = std::make_shared<omnisketch::MinHashSketchSet>(8);
        for (size_t i = 3; i < 16 + 3; i++) {
            probe_set_3->AddRecord(hf->Hash(i));
        }

        probe_result_3 = std::make_shared<omnisketch::OmniSketchCell>(probe_set_3, 16);
    }

    std::unique_ptr<omnisketch::CombinedPredicateEstimator> combinator;
    std::shared_ptr<omnisketch::TypedPointOmniSketch<int>> sketch_1;
    std::shared_ptr<omnisketch::TypedPointOmniSketch<int>> sketch_2;
    std::shared_ptr<omnisketch::TypedPointOmniSketch<int>> sketch_3;

    std::shared_ptr<omnisketch::OmniSketchCell> probe_result_1;
    std::shared_ptr<omnisketch::OmniSketchCell> probe_result_2;
    std::shared_ptr<omnisketch::OmniSketchCell> probe_result_3;
};
