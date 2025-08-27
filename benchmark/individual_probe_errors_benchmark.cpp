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

        if (param0 != attribute_count || param1 != min_hash_sample_size) {
            omni_sketch = nullptr;
            cardinalities.clear();
            all_values.clear();
        }

        const auto name = std::string(GetName());

        if (name.find("Join") != std::string::npos) {
            attribute_count = param1;
            min_hash_sample_size = param2;
        } else {
            attribute_count = param0;
            min_hash_sample_size = param1;
        }

        if (!omni_sketch) {
            omni_sketch = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(SKETCH_WIDTH, SKETCH_DEPTH,
                                                                                    min_hash_sample_size);
            cardinalities = FillOmniSketch(*omni_sketch, IsUniform);
            all_values.resize(attribute_count);
            for (size_t valueIdx = 1; valueIdx < attribute_count + 1; ++valueIdx) {
                all_values[valueIdx - 1] = valueIdx;
            }
        }
    }

    std::unordered_map<size_t, size_t> FillOmniSketch(omnisketch::TypedPointOmniSketch<size_t>& sketch, bool uniform) {
        std::unordered_map<size_t, size_t> cards;
        cards.reserve(attribute_count);

        if (uniform) {
            for (size_t i = 1; i < RECORD_COUNT + 1; ++i) {
                cards[i % attribute_count]++;
                sketch.AddRecord(i % attribute_count, i);
            }
        } else {
            std::uniform_real_distribution<double> distribution(0.0, 1.0);
            for (size_t i = 1; i < RECORD_COUNT + 1; ++i) {
                const double uniform_value = distribution(random_generator);
                const double skewed_value = std::pow(uniform_value, SKEW);
                const size_t value = 1 + static_cast<size_t>(skewed_value * attribute_count);
                cards[value]++;
                sketch.AddRecord(value % attribute_count, i);
            }
        }
        return cards;
    }

    void ProbeSketch(::benchmark::State& state) {
        std::uniform_int_distribution<> distribution(1, static_cast<int>(attribute_count + 1));

        for (auto _ : state) {
            const size_t value = static_cast<size_t>(distribution(random_generator));
            const auto card = omni_sketch->Probe(value);
            state.counters["QError"] = ComputeQError(card->RecordCount(), cardinalities[value]);
        }
    }

    void SetMembershipProbeSketch(::benchmark::State& state) {
        const size_t probe_set_size = std::min(attribute_count, MIN_PROBE_SET_SIZE);

        std::shuffle(all_values.begin(), all_values.end(), random_generator);
        std::vector<size_t> probe_set(all_values.begin(), all_values.begin() + probe_set_size);
        double actualCardinality = 0.0;
        for (const auto value : probe_set) {
            actualCardinality += cardinalities[value];
        }

        for (auto _ : state) {
            const auto card = omni_sketch->ProbeSet(probe_set.data(), probe_set.size());
            SetCounters(state, ComputeQError(card->RecordCount(), actualCardinality));
        }
    }

    void JoinSketch(::benchmark::State& state, bool use_approximate_join = false) {
        static const size_t JOIN_KEY_COUNT = all_values.size() / 4;
        std::shuffle(all_values.begin(), all_values.end(), random_generator);
        double actual_cardinality = 0.0;
        for (auto it = all_values.begin(); it != all_values.begin() + JOIN_KEY_COUNT; ++it) {
            actual_cardinality += cardinalities[*it];
        }

        const auto probe_sample_size = static_cast<size_t>(state.range(0));
        auto probe_sample = std::make_shared<omnisketch::OmniSketchCell>(probe_sample_size);
        auto hash_function = std::make_shared<omnisketch::MurmurHashFunction<size_t>>();
        for (auto it = all_values.begin(); it != all_values.begin() + probe_sample_size; ++it) {
            probe_sample->AddRecord(hash_function->HashRid(*it));
        }
        probe_sample->SetRecordCount(JOIN_KEY_COUNT);

        for (auto _ : state) {
            if (use_approximate_join) {
                assert(false);  // TODO: implement approximate join
            } else {
                auto combinator =
                    std::make_shared<omnisketch::CombinedPredicateEstimator>(omni_sketch->MinHashSketchSize());
                combinator->AddPredicate(omni_sketch, probe_sample);
                const auto card = combinator->ComputeResult(omni_sketch->MinHashSketchSize());
                SetCounters(state, ComputeQError(card->RecordCount(), actual_cardinality));
            }
        }
    }

    double ComputeQError(double estimate, double actual) const {
        if (actual == 0.0) {
            return estimate == 0.0 ? 1.0 : estimate;
        }
        return estimate / actual;
    }

    void SetCounters(::benchmark::State& state, double q_error) const {
        state.counters["QError"] = q_error;
        state.counters["SketchSize"] = omni_sketch->EstimateByteSize();
    }

private:
    size_t attribute_count = 0;
    size_t min_hash_sample_size = 0;
    std::shared_ptr<omnisketch::TypedPointOmniSketch<size_t>> omni_sketch;
    std::vector<size_t> all_values;
    std::unordered_map<size_t, size_t> cardinalities;
    std::mt19937 random_generator;
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
