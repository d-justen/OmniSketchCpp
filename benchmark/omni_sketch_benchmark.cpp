#include "omni_sketch_fixture.hpp"

#include "combinator.hpp"

constexpr size_t WIDTH = 256;
constexpr size_t DEPTH = 3;
constexpr size_t BYTES_PER_UINT64_SAMPLE = sizeof(uint64_t) * WIDTH * DEPTH;
constexpr size_t BYTES_PER_MB = 1048576;
constexpr size_t SAMPLE_COUNT = 64;
constexpr size_t INSERT_COUNT = 1024;
constexpr size_t ATTRIBUTE_COUNT = 50000;

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, AddRecords, WIDTH, DEPTH, 10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE)
(::benchmark::State &state) {
	for (auto _ : state) {
		for (size_t i = 0; i < INSERT_COUNT; i++) {
			omni_sketch->AddRecord(i, omni_sketch->RecordCount() + i);
		}
	}
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, PointQuery, WIDTH, 3, 200)
(::benchmark::State &state) {
	const size_t multiplicity = 50;
	FillOmniSketch(ATTRIBUTE_COUNT, multiplicity);

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		card = omni_sketch->Probe(17);
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / multiplicity;
	state.counters["OmniSketchSizeMB"] = (double)omni_sketch->EstimateByteSize() / 1024.0 / 1024.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, ConjunctPointQueries, WIDTH, DEPTH,
                            10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);
	auto omni_sketch_2 = std::make_shared<omnisketch::PointOmniSketch<size_t>>(
	    WIDTH, DEPTH, 10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE);
	FillOmniSketch(ATTRIBUTE_COUNT, 30, &*omni_sketch_2);
	auto omni_sketch_3 = std::make_shared<omnisketch::PointOmniSketch<size_t>>(
	    WIDTH, DEPTH, 10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE);
	FillOmniSketch(ATTRIBUTE_COUNT, 20, &*omni_sketch_3);

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::ExhaustiveCombinator>();
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertPoint(17));
		combinator->AddPredicate(omni_sketch_2, omnisketch::PredicateConverter::ConvertPoint(17));
		combinator->AddPredicate(omni_sketch_3, omnisketch::PredicateConverter::ConvertPoint(17));
		card = combinator->Execute(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / 20.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, ConjunctPointQueriesFlattened, WIDTH, DEPTH,
                            10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);
	auto omni_sketch_2 = std::make_shared<omnisketch::PointOmniSketch<size_t>>(
	    WIDTH, DEPTH, 10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE);
	FillOmniSketch(ATTRIBUTE_COUNT, 30, &*omni_sketch_2);
	auto omni_sketch_3 = std::make_shared<omnisketch::PointOmniSketch<size_t>>(
	    WIDTH, DEPTH, 10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE);
	FillOmniSketch(ATTRIBUTE_COUNT, 20, &*omni_sketch_3);

	omni_sketch->Flatten();
	omni_sketch_2->Flatten();
	omni_sketch_3->Flatten();

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::ExhaustiveCombinator>();
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertPoint(17));
		combinator->AddPredicate(omni_sketch_2, omnisketch::PredicateConverter::ConvertPoint(17));
		combinator->AddPredicate(omni_sketch_3, omnisketch::PredicateConverter::ConvertPoint(17));
		card = combinator->Execute(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / 20.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, DisjunctPointQueries, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);

	std::vector<size_t> values(state.range());
	for (size_t i = 0; i < state.range(); i++) {
		values[i] = i + 1;
	}

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::ExhaustiveCombinator>();
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertSet(values));
		card = combinator->Execute(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / (50.0 * (double)state.range());
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, DisjunctPointQueriesFlattened, WIDTH, DEPTH,
                            10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);
	omni_sketch->Flatten();

	std::vector<size_t> values(state.range());
	for (size_t i = 0; i < state.range(); i++) {
		values[i] = i + 1;
	}

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::ExhaustiveCombinator>();
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertSet(values));
		card = combinator->Execute(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / (50.0 * (double)state.range());
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, SetMembership, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);

	std::vector<size_t> values(state.range());
	for (size_t i = 0; i < state.range(); i++) {
		values[i] = i + 1;
	}

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::ExhaustiveCombinator>();
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertSet(values));
		card = combinator->Execute(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / (50.0 * (double)state.range());
	state.counters["OmniSketchSizeMB"] = (double)omni_sketch->EstimateByteSize() / 1024.0 / 1024.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, SetMembershipFlattened, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);
	omni_sketch->Flatten();

	std::vector<size_t> values(state.range());
	for (size_t i = 0; i < state.range(); i++) {
		values[i] = i + 1;
	}

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::ExhaustiveCombinator>();
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertSet(values));
		card = combinator->Execute(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / (50.0 * (double)state.range());
	state.counters["OmniSketchSizeMB"] = (double)omni_sketch->EstimateByteSize() / 1024.0 / 1024.0;
}

BENCHMARK_REGISTER_F(OmniSketchFixture, AddRecords);
BENCHMARK_REGISTER_F(OmniSketchFixture, PointQuery);
BENCHMARK_REGISTER_F(OmniSketchFixture, ConjunctPointQueries);
BENCHMARK_REGISTER_F(OmniSketchFixture, ConjunctPointQueriesFlattened);
BENCHMARK_REGISTER_F(OmniSketchFixture, DisjunctPointQueries)->RangeMultiplier(2)->Range(2, 32768);
BENCHMARK_REGISTER_F(OmniSketchFixture, DisjunctPointQueriesFlattened)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(OmniSketchFixture, SetMembership)->RangeMultiplier(2)->Range(2, 32768);
BENCHMARK_REGISTER_F(OmniSketchFixture, SetMembershipFlattened)->RangeMultiplier(2)->Range(2, 4096);

BENCHMARK_MAIN();
