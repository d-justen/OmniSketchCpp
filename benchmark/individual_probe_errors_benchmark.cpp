#include <benchmark/benchmark.h>
#include "combinator.hpp"
#include "omni_sketch/standard_omni_sketch.hpp"

#include <random>

static constexpr size_t SKETCH_WIDTH = 256;
static constexpr size_t SKETCH_DEPTH = 3;
static constexpr size_t RECORD_COUNT = 1 << 20;
static constexpr size_t ITERATION_COUNT = 1;
static constexpr size_t MIN_PROBE_SET_SIZE = 128;
static constexpr double SKEW = 3.0;

template <bool IsUniform>
class ProbeErrorFixture : public benchmark::Fixture {
public:
	void SetUp(::benchmark::State& state) override {
		const auto param0 = static_cast<size_t>(state.range(0));
		const auto param1 = static_cast<size_t>(state.range(1));
		const auto param2 = static_cast<size_t>(state.range(2));

		if (param0 != attributeCount || param1 != minHashSampleSize) {
			omniSketch = nullptr;
			cardinalities.clear();
			allValues.clear();
		}

		const auto name = std::string(GetName());

		if (name.find("Join") != std::string::npos) {
			attributeCount = param1;
			minHashSampleSize = param2;
		} else {
			attributeCount = param0;
			minHashSampleSize = param1;
		}
		
		if (!omniSketch) {
			omniSketch = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(
				SKETCH_WIDTH, SKETCH_DEPTH, minHashSampleSize);
			cardinalities = FillOmniSketch(*omniSketch, IsUniform);
			allValues.resize(attributeCount);
			for (size_t valueIdx = 1; valueIdx < attributeCount + 1; ++valueIdx) {
				allValues[valueIdx - 1] = valueIdx;
			}
		}
	}

	std::unordered_map<size_t, size_t> FillOmniSketch(omnisketch::TypedPointOmniSketch<size_t>& sketch, bool uniform) {
		std::unordered_map<size_t, size_t> cards;
		cards.reserve(attributeCount);
		
		if (uniform) {
			for (size_t i = 1; i < RECORD_COUNT + 1; ++i) {
				cards[i % attributeCount]++;
				sketch.AddRecord(i % attributeCount, i);
			}
		} else {
			std::uniform_real_distribution<double> distribution(0.0, 1.0);
			for (size_t i = 1; i < RECORD_COUNT + 1; ++i) {
				const double uniformValue = distribution(randomGenerator);
				const double skewedValue = std::pow(uniformValue, SKEW);
				const size_t value = 1 + static_cast<size_t>(skewedValue * attributeCount);
				cards[value]++;
				sketch.AddRecord(value % attributeCount, i);
			}
		}
		return cards;
	}

	void ProbeSketch(::benchmark::State& state) {
		std::uniform_int_distribution<> distribution(1, static_cast<int>(attributeCount + 1));

		for (auto _ : state) {
			const size_t value = static_cast<size_t>(distribution(randomGenerator));
			const auto card = omniSketch->Probe(value);
			state.counters["QError"] = ComputeQError(card->RecordCount(), cardinalities[value]);
		}
	}

	void SetMembershipProbeSketch(::benchmark::State& state) {
		const size_t probeSetSize = std::min(attributeCount, MIN_PROBE_SET_SIZE);

		std::shuffle(allValues.begin(), allValues.end(), randomGenerator);
		std::vector<size_t> probeSet(allValues.begin(), allValues.begin() + probeSetSize);
		double actualCardinality = 0.0;
		for (const auto value : probeSet) {
			actualCardinality += cardinalities[value];
		}

		for (auto _ : state) {
			const auto card = omniSketch->ProbeSet(probeSet.data(), probeSet.size());
			SetCounters(state, ComputeQError(card->RecordCount(), actualCardinality));
		}
	}

	void JoinSketch(::benchmark::State& state, bool useApproximateJoin = false) {
		static const size_t JOIN_KEY_COUNT = allValues.size() / 4;
		std::shuffle(allValues.begin(), allValues.end(), randomGenerator);
		double actualCardinality = 0.0;
		for (auto it = allValues.begin(); it != allValues.begin() + JOIN_KEY_COUNT; ++it) {
			actualCardinality += cardinalities[*it];
		}

		const size_t probeSampleSize = static_cast<size_t>(state.range(0));
		auto probeSample = std::make_shared<omnisketch::OmniSketchCell>(probeSampleSize);
		auto hashFunction = std::make_shared<omnisketch::MurmurHashFunction<size_t>>();
		for (auto it = allValues.begin(); it != allValues.begin() + probeSampleSize; ++it) {
			probeSample->AddRecord(hashFunction->HashRid(*it));
		}
		probeSample->SetRecordCount(JOIN_KEY_COUNT);

		for (auto _ : state) {
			if (useApproximateJoin) {
				assert(false); // TODO: implement approximate join
			} else {
				auto combinator = std::make_shared<omnisketch::CombinedPredicateEstimator>(
					omniSketch->MinHashSketchSize());
				combinator->AddPredicate(omniSketch, probeSample);
				const auto card = combinator->ComputeResult(omniSketch->MinHashSketchSize());
				SetCounters(state, ComputeQError(card->RecordCount(), actualCardinality));
			}
		}
	}

	double ComputeQError(double estimate, double actual) const {
		if (actual == 0.0) {
			return estimate == 0.0 ? 1.0 : estimate;
		}
		return estimate / actual;
	}

	void SetCounters(::benchmark::State& state, double qError) const {
		state.counters["QError"] = qError;
		state.counters["SketchSize"] = omniSketch->EstimateByteSize();
	}

private:
	size_t attributeCount = 0;
	size_t minHashSampleSize = 0;
	std::shared_ptr<omnisketch::TypedPointOmniSketch<size_t>> omniSketch;
	std::vector<size_t> allValues;
	std::unordered_map<size_t, size_t> cardinalities;
	std::mt19937 randomGenerator;
};

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, ProbeErrorUniform, true)(benchmark::State& state) {
	ProbeSketch(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, ProbeErrorSkewed, false)(benchmark::State& state) {
	ProbeSketch(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, SetMembershipProbeErrorUniform, true)(benchmark::State& state) {
	SetMembershipProbeSketch(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, SetMembershipProbeErrorSkewed, false)(benchmark::State& state) {
	SetMembershipProbeSketch(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, JoinErrorUniform, true)(benchmark::State& state) {
	JoinSketch(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, JoinErrorSkewed, false)(benchmark::State& state) {
	JoinSketch(state);
}

BENCHMARK_REGISTER_F(ProbeErrorFixture, ProbeErrorUniform)
    ->Iterations(ITERATION_COUNT)
    ->ArgsProduct({benchmark::CreateRange(1 << 3, 1 << 18, 2), {64, 1024}});

BENCHMARK_REGISTER_F(ProbeErrorFixture, ProbeErrorSkewed)
    ->Iterations(ITERATION_COUNT)
    ->ArgsProduct({benchmark::CreateRange(1 << 3, 1 << 18, 2), {64, 1024}});

BENCHMARK_REGISTER_F(ProbeErrorFixture, SetMembershipProbeErrorUniform)
    ->Iterations(ITERATION_COUNT)
    ->ArgsProduct({benchmark::CreateRange(1 << 3, 1 << 18, 2), {64, 1024}});

BENCHMARK_REGISTER_F(ProbeErrorFixture, SetMembershipProbeErrorSkewed)
    ->Iterations(ITERATION_COUNT)
    ->ArgsProduct({benchmark::CreateRange(1 << 3, 1 << 18, 2), {64, 1024}});

/*
BENCHMARK_REGISTER_F(ProbeErrorFixture, ApproximateJoinErrorUniform)
    ->Iterations(ITERATION_COUNT)
    ->ArgsProduct({benchmark::CreateRange(1 << 2, 1 << 14, 2), {1 << 16}, {64, 1024}});

BENCHMARK_REGISTER_F(ProbeErrorFixture, ApproximateJoinErrorSkewed)
    ->Iterations(ITERATION_COUNT)
    ->ArgsProduct({benchmark::CreateRange(1 << 2, 1 << 14, 2), {1 << 16}, {64, 1024}});
*/

BENCHMARK_REGISTER_F(ProbeErrorFixture, JoinErrorUniform)
    ->Iterations(ITERATION_COUNT)
    ->ArgsProduct({benchmark::CreateRange(1 << 2, 1 << 14, 2), {1 << 16}, {64, 1024}});

BENCHMARK_REGISTER_F(ProbeErrorFixture, JoinErrorSkewed)
    ->Iterations(ITERATION_COUNT)
    ->ArgsProduct({benchmark::CreateRange(1 << 2, 1 << 14, 2), {1 << 16}, {64, 1024}});

BENCHMARK_MAIN();
