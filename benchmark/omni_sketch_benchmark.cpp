#include "omni_sketch_fixture.hpp"

#include "algorithm/omni_sketch_operations.hpp"

constexpr size_t WIDTH = 512;
constexpr size_t DEPTH = 3;
constexpr size_t SAMPLE_COUNT = 19;
constexpr size_t INSERT_COUNT = 1024;
constexpr size_t ATTRIBUTE_COUNT = 10000;

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, AddRecords, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	for (auto _ : state) {
		for (size_t i = 0; i < INSERT_COUNT; i++) {
			omni_sketch->AddRecord(i, omni_sketch->RecordCount() + i);
		}
	}
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, PointQuery, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	const size_t multiplicity = 500;
	FillOmniSketch(ATTRIBUTE_COUNT, multiplicity);

	omnisketch::CardEstResult<uint64_t> card;
	for (auto _ : state) {
		card = omni_sketch->EstimateCardinality(17);
	}

	state.counters["Q-Error"] = card.cardinality / multiplicity;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, ConjunctPointQueries, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);
	auto omni_sketch_2 =
	    std::make_unique<omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>>>(WIDTH, DEPTH, SAMPLE_COUNT);
	FillOmniSketch(ATTRIBUTE_COUNT, 30, &*omni_sketch_2);
	auto omni_sketch_3 =
	    std::make_unique<omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>>>(WIDTH, DEPTH, SAMPLE_COUNT);
	FillOmniSketch(ATTRIBUTE_COUNT, 20, &*omni_sketch_3);

	std::vector<omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>> *> sketch_vec {
	    &*omni_sketch, &*omni_sketch_2, &*omni_sketch_3};
	std::vector<size_t> values {17, 17, 17};

	omnisketch::CardEstResult<uint64_t> card;
	for (auto _ : state) {
		card = omnisketch::Algorithm::ConjunctPointQueries(sketch_vec, values);
	}

	state.counters["Q-Error"] = card.cardinality / 20;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, ConjunctPointQueriesFlattened, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);
	auto omni_sketch_2 =
	    std::make_unique<omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>>>(WIDTH, DEPTH, SAMPLE_COUNT);
	FillOmniSketch(ATTRIBUTE_COUNT, 30, &*omni_sketch_2);
	auto omni_sketch_3 =
	    std::make_unique<omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>>>(WIDTH, DEPTH, SAMPLE_COUNT);
	FillOmniSketch(ATTRIBUTE_COUNT, 20, &*omni_sketch_3);

	std::vector<omnisketch::OmniSketch<size_t, size_t, std::vector<uint64_t>>> flattened {
	    omni_sketch->Flatten(), omni_sketch_2->Flatten(), omni_sketch_3->Flatten()};
	omni_sketch = nullptr;
	omni_sketch_2 = nullptr;
	omni_sketch_3 = nullptr;

	std::vector<omnisketch::OmniSketch<size_t, size_t, std::vector<uint64_t>> *> sketch_vec {
	    &flattened[0], &flattened[1], &flattened[2]};

	std::vector<size_t> values {17, 17, 17};

	omnisketch::CardEstResult<uint64_t> card;
	for (auto _ : state) {
		card = omnisketch::Algorithm::ConjunctPointQueries(sketch_vec, values);
	}

	state.counters["Q-Error"] = card.cardinality / 20;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, DisjunctPointQueries, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);

	std::vector<size_t> values(state.range());
	for (size_t i = 0; i < state.range(); i++) {
		values[i] = i + 1;
	}

	double records_per_cell = omni_sketch->RecordCount() / (double)omni_sketch->Width();
	double omni_sketch_granularity = records_per_cell / omni_sketch->MinHashSampleCount();

	omnisketch::CardEstResult<uint64_t> card;
	for (auto _ : state) {
		card = omnisketch::Algorithm::DisjunctPointQueries(*omni_sketch, values);
	}

	state.counters["Q-Error"] = card.cardinality / (50 * state.range());
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, DisjunctPointQueriesFlattened, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);
	auto flattened_omni_sketch = omni_sketch->Flatten();

	std::vector<size_t> values(state.range());
	for (size_t i = 0; i < state.range(); i++) {
		values[i] = i + 1;
	}

	omnisketch::CardEstResult<uint64_t> card;
	for (auto _ : state) {
		card = omnisketch::Algorithm::DisjunctPointQueries(flattened_omni_sketch, values);
	}

	state.counters["Q-Error"] = card.cardinality / (50 * state.range());
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, SetMembership, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);

	std::vector<size_t> values(state.range());
	for (size_t i = 0; i < state.range(); i++) {
		values[i] = i + 1;
	}

	omnisketch::CardEstResult<uint64_t> card;
	for (auto _ : state) {
		card = omnisketch::Algorithm::SetMembership(*omni_sketch, values);
	}

	state.counters["Q-Error"] = card.cardinality / (50 * state.range());
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, SetMembershipFlattened, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);
	auto flattened_omni_sketch = omni_sketch->Flatten();

	std::vector<size_t> values(state.range());
	for (size_t i = 0; i < state.range(); i++) {
		values[i] = i + 1;
	}

	omnisketch::CardEstResult<uint64_t> card;
	for (auto _ : state) {
		card = omnisketch::Algorithm::SetMembership(flattened_omni_sketch, values);
	}

	state.counters["Q-Error"] = card.cardinality / (50 * state.range());
}

BENCHMARK_REGISTER_F(OmniSketchFixture, AddRecords);
BENCHMARK_REGISTER_F(OmniSketchFixture, PointQuery);
BENCHMARK_REGISTER_F(OmniSketchFixture, ConjunctPointQueries);
BENCHMARK_REGISTER_F(OmniSketchFixture, ConjunctPointQueriesFlattened);
BENCHMARK_REGISTER_F(OmniSketchFixture, DisjunctPointQueries)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(OmniSketchFixture, DisjunctPointQueriesFlattened)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(OmniSketchFixture, SetMembership)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(OmniSketchFixture, SetMembershipFlattened)->RangeMultiplier(2)->Range(2, 4096);

BENCHMARK_MAIN();
