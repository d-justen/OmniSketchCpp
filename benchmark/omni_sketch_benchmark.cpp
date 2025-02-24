#include "omni_sketch_fixture.hpp"

constexpr size_t WIDTH = 512;
constexpr size_t DEPTH = 3;
constexpr size_t SAMPLE_COUNT = 16;
constexpr size_t INSERT_COUNT = 1024;

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, AddRecords, WIDTH, DEPTH, SAMPLE_COUNT)(::benchmark::State &state) {
	FillOmniSketch(0.5);

	for (auto _ : state) {
		for (size_t i = 0; i < INSERT_COUNT; i++) {
			omni_sketch->AddRecord(i, omni_sketch->RecordCount() + i);
		}
	}
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, PointQuery, WIDTH, DEPTH, SAMPLE_COUNT)(::benchmark::State &state) {
	FillOmniSketch(1);

	omnisketch::CardEstResult<uint64_t> card;
	for (auto _ : state) {
		card = omni_sketch->EstimateCardinality(17);
	}

	state.counters["Q-Error"] = card.cardinality / 1;
}

BENCHMARK_REGISTER_F(OmniSketchFixture, AddRecords);
BENCHMARK_REGISTER_F(OmniSketchFixture, PointQuery);

BENCHMARK_MAIN();
