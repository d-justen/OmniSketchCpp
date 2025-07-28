#include "omni_sketch_fixture.hpp"

#include "include/combinator.hpp"

constexpr size_t WIDTH = 256;
constexpr size_t DEPTH = 3;
constexpr size_t BYTES_PER_UINT64_SAMPLE = sizeof(uint64_t) * WIDTH * DEPTH;
constexpr size_t BYTES_PER_MB = 1048576;
constexpr size_t SAMPLE_COUNT = 64;
constexpr size_t ATTRIBUTE_COUNT = 50000;

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, AddRecords, WIDTH, DEPTH, 1024)
(::benchmark::State &state) {
	size_t items_processed = 0;
	for (auto _ : state) {
		omni_sketch->AddRecord(current_repetition, current_repetition);
		current_repetition++;
		items_processed++;
	}
	state.SetItemsProcessed(items_processed);
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, PointQuery, 1, 1, 1)
(::benchmark::State &state) {
	auto sample_count = static_cast<size_t>(state.range());

	omnisketch::OmniSketchConfig config;
	config.sample_count = sample_count;
	config.SetWidth(WIDTH);
	config.depth = DEPTH;
	config.hash_processor = std::make_shared<omnisketch::BarrettModSplitHashMapper>(WIDTH);
	omni_sketch = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    config.width, config.depth, config.sample_count, std::make_shared<omnisketch::MurmurHashFunction<size_t>>(),
	    config.set_membership_algo, config.hash_processor);

	const size_t target_record_count = WIDTH * sample_count * 8;
	const size_t attribute_values = WIDTH * 64;
	const size_t multiplicity = target_record_count / attribute_values;
	FillOmniSketch(attribute_values, multiplicity);

	std::shared_ptr<omnisketch::OmniSketchCell> card;

	int64_t items_processed = 0;
	for (auto _ : state) {
		card = omni_sketch->Probe((1 + items_processed) % target_record_count);
		benchmark::DoNotOptimize(card);
		items_processed++;
	}

	state.SetItemsProcessed(items_processed);
	state.counters["OmniSketchSizeMB"] = (double)omni_sketch->EstimateByteSize() / 1024.0 / 1024.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, PointQueryFlattened, 1, 1, 1)
(::benchmark::State &state) {
	auto sample_count = static_cast<size_t>(state.range());

	omnisketch::OmniSketchConfig config;
	config.sample_count = sample_count;
	config.SetWidth(WIDTH);
	config.depth = DEPTH;
	config.hash_processor = std::make_shared<omnisketch::BarrettModSplitHashMapper>(WIDTH);
	omni_sketch = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    config.width, config.depth, config.sample_count, std::make_shared<omnisketch::MurmurHashFunction<size_t>>(),
	    config.set_membership_algo, config.hash_processor);

	omni_sketch->Flatten();

	const size_t target_record_count = WIDTH * sample_count * 8;
	const size_t attribute_values = WIDTH * 64;
	const size_t multiplicity = target_record_count / attribute_values;
	FillOmniSketch(attribute_values, multiplicity);

	std::shared_ptr<omnisketch::OmniSketchCell> card;

	int64_t items_processed = 0;
	for (auto _ : state) {
		card = omni_sketch->Probe((1 + items_processed) % target_record_count);
		benchmark::DoNotOptimize(card);
		items_processed++;
	}

	state.SetItemsProcessed(items_processed);
	state.counters["OmniSketchSizeMB"] = (double)omni_sketch->EstimateByteSize() / 1024.0 / 1024.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, ConjunctPointQueries, WIDTH, DEPTH,
                            10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);
	auto omni_sketch_2 = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    WIDTH, DEPTH, 10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE);
	FillOmniSketch(ATTRIBUTE_COUNT, 30, &*omni_sketch_2);
	auto omni_sketch_3 = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    WIDTH, DEPTH, 10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE);
	FillOmniSketch(ATTRIBUTE_COUNT, 20, &*omni_sketch_3);

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertPoint(17));
		combinator->AddPredicate(omni_sketch_2, omnisketch::PredicateConverter::ConvertPoint(17));
		combinator->AddPredicate(omni_sketch_3, omnisketch::PredicateConverter::ConvertPoint(17));
		card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / 20.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, ConjunctPointQueriesFlattened, WIDTH, DEPTH,
                            10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);
	auto omni_sketch_2 = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    WIDTH, DEPTH, 10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE);
	FillOmniSketch(ATTRIBUTE_COUNT, 30, &*omni_sketch_2);
	auto omni_sketch_3 = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    WIDTH, DEPTH, 10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE);
	FillOmniSketch(ATTRIBUTE_COUNT, 20, &*omni_sketch_3);

	omni_sketch->Flatten();
	omni_sketch_2->Flatten();
	omni_sketch_3->Flatten();

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertPoint(17));
		combinator->AddPredicate(omni_sketch_2, omnisketch::PredicateConverter::ConvertPoint(17));
		combinator->AddPredicate(omni_sketch_3, omnisketch::PredicateConverter::ConvertPoint(17));
		card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / 20.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, DisjunctPointQueries, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);

	std::vector<size_t> values(state.range());
	for (int64_t i = 0; i < state.range(); i++) {
		values[i] = i + 1;
	}

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertSet(values));
		card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / (50.0 * (double)state.range());
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, DisjunctPointQueriesFlattened, WIDTH, DEPTH,
                            10 * BYTES_PER_MB / BYTES_PER_UINT64_SAMPLE)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);
	omni_sketch->Flatten();

	std::vector<size_t> values(state.range());
	for (int64_t i = 0; i < state.range(); i++) {
		values[i] = i + 1;
	}

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertSet(values));
		card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / (50.0 * (double)state.range());
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, SetMembership, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);

	std::vector<size_t> values(state.range());
	for (int64_t i = 0; i < state.range(); i++) {
		values[i] = i + 1;
	}

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertSet(values));
		card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / (50.0 * (double)state.range());
	state.counters["OmniSketchSizeMB"] = (double)omni_sketch->EstimateByteSize() / 1024.0 / 1024.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, SetMembershipFlattened, WIDTH, DEPTH, SAMPLE_COUNT)
(::benchmark::State &state) {
	FillOmniSketch(ATTRIBUTE_COUNT, 50);
	omni_sketch->Flatten();

	std::vector<size_t> values(state.range());
	for (int64_t i = 0; i < state.range(); i++) {
		values[i] = i + 1;
	}

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertSet(values));
		card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = (double)card->RecordCount() / (50.0 * (double)state.range());
	state.counters["OmniSketchSizeMB"] = (double)omni_sketch->EstimateByteSize() / 1024.0 / 1024.0;
}

BENCHMARK_REGISTER_F(OmniSketchFixture, AddRecords)->Iterations(10000)->Repetitions(2000);
BENCHMARK_REGISTER_F(OmniSketchFixture, PointQuery)->RangeMultiplier(2)->Range(128, 4096);
BENCHMARK_REGISTER_F(OmniSketchFixture, PointQueryFlattened)->RangeMultiplier(2)->Range(128, 4096);
BENCHMARK_REGISTER_F(OmniSketchFixture, ConjunctPointQueries);
BENCHMARK_REGISTER_F(OmniSketchFixture, ConjunctPointQueriesFlattened);
BENCHMARK_REGISTER_F(OmniSketchFixture, DisjunctPointQueries)->RangeMultiplier(2)->Range(2, 32768);
BENCHMARK_REGISTER_F(OmniSketchFixture, DisjunctPointQueriesFlattened)->RangeMultiplier(2)->Range(2, 4096);
BENCHMARK_REGISTER_F(OmniSketchFixture, SetMembership)->RangeMultiplier(2)->Range(2, 32768);
BENCHMARK_REGISTER_F(OmniSketchFixture, SetMembershipFlattened)->RangeMultiplier(2)->Range(2, 4096);

BENCHMARK_MAIN();
