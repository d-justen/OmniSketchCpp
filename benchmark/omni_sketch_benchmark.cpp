#include "omni_sketch_fixture.hpp"

#include "include/combinator.hpp"

constexpr size_t kWidth = 256;
constexpr size_t kDepth = 3;
constexpr size_t kBytesPerUint64Sample = sizeof(uint64_t) * kWidth * kDepth;
constexpr size_t kBytesPerMB = 1048576;
constexpr size_t kSampleCount = 64;
constexpr size_t kAttributeCount = 50000;

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, AddRecords, kWidth, kDepth, 1024)
(::benchmark::State &state) {
	size_t itemsProcessed = 0;
	for (auto _ : state) {
		omni_sketch->AddRecord(current_repetition, current_repetition);
		current_repetition++;
		itemsProcessed++;
	}
	state.SetItemsProcessed(itemsProcessed);
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, PointQuery, 1, 1, 1)
(::benchmark::State &state) {
	const auto sampleCount = static_cast<size_t>(state.range());

	omnisketch::OmniSketchConfig config;
	config.sample_count = sampleCount;
	config.SetWidth(kWidth);
	config.depth = kDepth;
	config.hash_processor = std::make_shared<omnisketch::BarrettModSplitHashMapper>(kWidth);
	omni_sketch = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    config.width, config.depth, config.sample_count, std::make_shared<omnisketch::MurmurHashFunction<size_t>>(),
	    config.set_membership_algo, config.hash_processor);

	const size_t targetRecordCount = kWidth * sampleCount * 8;
	const size_t attributeValues = kWidth * 64;
	const size_t multiplicity = targetRecordCount / attributeValues;
	FillOmniSketch(attributeValues, multiplicity);

	std::shared_ptr<omnisketch::OmniSketchCell> card;

	int64_t itemsProcessed = 0;
	for (auto _ : state) {
		card = omni_sketch->Probe((1 + itemsProcessed) % targetRecordCount);
		benchmark::DoNotOptimize(card);
		itemsProcessed++;
	}

	state.SetItemsProcessed(itemsProcessed);
	state.counters["OmniSketchSizeMB"] = static_cast<double>(omni_sketch->EstimateByteSize()) / 1024.0 / 1024.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, PointQueryFlattened, 1, 1, 1)
(::benchmark::State &state) {
	const auto sampleCount = static_cast<size_t>(state.range());

	omnisketch::OmniSketchConfig config;
	config.sample_count = sampleCount;
	config.SetWidth(kWidth);
	config.depth = kDepth;
	config.hash_processor = std::make_shared<omnisketch::BarrettModSplitHashMapper>(kWidth);
	omni_sketch = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    config.width, config.depth, config.sample_count, std::make_shared<omnisketch::MurmurHashFunction<size_t>>(),
	    config.set_membership_algo, config.hash_processor);

	omni_sketch->Flatten();

	const size_t targetRecordCount = kWidth * sampleCount * 8;
	const size_t attributeValues = kWidth * 64;
	const size_t multiplicity = targetRecordCount / attributeValues;
	FillOmniSketch(attributeValues, multiplicity);

	std::shared_ptr<omnisketch::OmniSketchCell> card;

	int64_t itemsProcessed = 0;
	for (auto _ : state) {
		card = omni_sketch->Probe((1 + itemsProcessed) % targetRecordCount);
		benchmark::DoNotOptimize(card);
		itemsProcessed++;
	}

	state.SetItemsProcessed(itemsProcessed);
	state.counters["OmniSketchSizeMB"] = static_cast<double>(omni_sketch->EstimateByteSize()) / 1024.0 / 1024.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, ConjunctPointQueries, kWidth, kDepth,
                            10 * kBytesPerMB / kBytesPerUint64Sample)
(::benchmark::State &state) {
	FillOmniSketch(kAttributeCount, 50);
	auto omniSketch2 = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    kWidth, kDepth, 10 * kBytesPerMB / kBytesPerUint64Sample);
	FillOmniSketch(kAttributeCount, 30, omniSketch2.get());
	auto omniSketch3 = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    kWidth, kDepth, 10 * kBytesPerMB / kBytesPerUint64Sample);
	FillOmniSketch(kAttributeCount, 20, omniSketch3.get());

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertPoint(17));
		combinator->AddPredicate(omniSketch2, omnisketch::PredicateConverter::ConvertPoint(17));
		combinator->AddPredicate(omniSketch3, omnisketch::PredicateConverter::ConvertPoint(17));
		card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = static_cast<double>(card->RecordCount()) / 20.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, ConjunctPointQueriesFlattened, kWidth, kDepth,
                            10 * kBytesPerMB / kBytesPerUint64Sample)
(::benchmark::State &state) {
	FillOmniSketch(kAttributeCount, 50);
	auto omniSketch2 = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    kWidth, kDepth, 10 * kBytesPerMB / kBytesPerUint64Sample);
	FillOmniSketch(kAttributeCount, 30, omniSketch2.get());
	auto omniSketch3 = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
	    kWidth, kDepth, 10 * kBytesPerMB / kBytesPerUint64Sample);
	FillOmniSketch(kAttributeCount, 20, omniSketch3.get());

	omni_sketch->Flatten();
	omniSketch2->Flatten();
	omniSketch3->Flatten();

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertPoint(17));
		combinator->AddPredicate(omniSketch2, omnisketch::PredicateConverter::ConvertPoint(17));
		combinator->AddPredicate(omniSketch3, omnisketch::PredicateConverter::ConvertPoint(17));
		card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = static_cast<double>(card->RecordCount()) / 20.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, DisjunctPointQueries, kWidth, kDepth, kSampleCount)
(::benchmark::State &state) {
	FillOmniSketch(kAttributeCount, 50);

	std::vector<size_t> values;
	values.reserve(state.range());
	for (int64_t i = 0; i < state.range(); i++) {
		values.emplace_back(i + 1);
	}

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertSet(values));
		card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = static_cast<double>(card->RecordCount()) / (50.0 * static_cast<double>(state.range()));
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, DisjunctPointQueriesFlattened, kWidth, kDepth,
                            10 * kBytesPerMB / kBytesPerUint64Sample)
(::benchmark::State &state) {
	FillOmniSketch(kAttributeCount, 50);
	omni_sketch->Flatten();

	std::vector<size_t> values;
	values.reserve(state.range());
	for (int64_t i = 0; i < state.range(); i++) {
		values.emplace_back(i + 1);
	}

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertSet(values));
		card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = static_cast<double>(card->RecordCount()) / (50.0 * static_cast<double>(state.range()));
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, SetMembership, kWidth, kDepth, kSampleCount)
(::benchmark::State &state) {
	FillOmniSketch(kAttributeCount, 50);

	std::vector<size_t> values;
	values.reserve(state.range());
	for (int64_t i = 0; i < state.range(); i++) {
		values.emplace_back(i + 1);
	}

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertSet(values));
		card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = static_cast<double>(card->RecordCount()) / (50.0 * static_cast<double>(state.range()));
	state.counters["OmniSketchSizeMB"] = static_cast<double>(omni_sketch->EstimateByteSize()) / 1024.0 / 1024.0;
}

BENCHMARK_TEMPLATE_DEFINE_F(OmniSketchFixture, SetMembershipFlattened, kWidth, kDepth, kSampleCount)
(::benchmark::State &state) {
	FillOmniSketch(kAttributeCount, 50);
	omni_sketch->Flatten();

	std::vector<size_t> values;
	values.reserve(state.range());
	for (int64_t i = 0; i < state.range(); i++) {
		values.emplace_back(i + 1);
	}

	std::shared_ptr<omnisketch::OmniSketchCell> card;
	for (auto _ : state) {
		auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
		combinator->AddPredicate(omni_sketch, omnisketch::PredicateConverter::ConvertSet(values));
		card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
	}

	state.counters["Q-Error"] = static_cast<double>(card->RecordCount()) / (50.0 * static_cast<double>(state.range()));
	state.counters["OmniSketchSizeMB"] = static_cast<double>(omni_sketch->EstimateByteSize()) / 1024.0 / 1024.0;
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
