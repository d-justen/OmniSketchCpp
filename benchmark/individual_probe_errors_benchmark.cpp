#include <benchmark/benchmark.h>
#include "algorithm/omni_sketch_operations.hpp"
#include "algorithm/join_operations.hpp"
#include "omni_sketch.hpp"
#include <random>

static constexpr size_t SKETCH_WIDTH = 256;
static constexpr size_t SKETCH_DEPTH = 3;
static constexpr size_t RECORD_COUNT = 1 << 20;
static constexpr size_t ITERATION_COUNT = 1;
static constexpr size_t MIN_PROBE_SET_SIZE = 128;
static constexpr double SKEW = 3;

template <bool IsUniform>
class ProbeErrorFixture : public benchmark::Fixture {
public:
	void SetUp(::benchmark::State &state) override {
		if (state.range(0) != attribute_count || state.range(1) != min_hash_sample_size) {
			omni_sketch = nullptr;
			cardinalities.clear();
			all_values.clear();
		}

		const auto name = std::string(GetName());

		if (name.find("Join") != std::string::npos) {
			attribute_count = state.range(1);
			min_hash_sample_size = state.range(2);
		} else {
			attribute_count = state.range(0);
			min_hash_sample_size = state.range(1);
		}
		if (!omni_sketch) {
			omni_sketch = std::make_unique<omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>>>(
			    SKETCH_WIDTH, SKETCH_DEPTH, min_hash_sample_size);
			cardinalities = FillOmniSketch(*omni_sketch, IsUniform);
			all_values.resize(attribute_count);
			for (size_t val_idx = 1; val_idx < attribute_count + 1; val_idx++) {
				all_values[val_idx - 1] = val_idx;
			}
		}
	}

	std::unordered_map<size_t, size_t>
	FillOmniSketch(omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>> &sketch, bool uniform) {
		std::unordered_map<size_t, size_t> cards;
		cards.reserve(attribute_count);
		if (uniform) {
			for (size_t i = 1; i < RECORD_COUNT + 1; i++) {
				cards[i % attribute_count]++;
				sketch.AddRecord(i % attribute_count, i);
			}
		} else {
			std::uniform_real_distribution<double> dist(0.0, 1.0);
			for (int i = 1; i < RECORD_COUNT + 1; ++i) {
				double u = dist(gen);
				double skewed = pow(u, SKEW);
				size_t value = 1 + skewed * attribute_count;
				cards[value]++;
				sketch.AddRecord(value % attribute_count, i);
			}
		}
		return cards;
	}

	void ProbeSketch(::benchmark::State &state) {
		std::uniform_int_distribution<> dist(1, attribute_count + 1);

		for (auto _ : state) {
			const size_t value = dist(gen);
			const auto card = omni_sketch->EstimateCardinality(value);
			state.counters["QError"] = ComputeQError(card.cardinality, cardinalities[value]);
		}
	}

	void SetMembershipProbeSketch(::benchmark::State &state) {
		const size_t probe_set_size = std::min(attribute_count, MIN_PROBE_SET_SIZE);

		std::shuffle(all_values.begin(), all_values.end(), gen);
		std::vector<size_t> probe_set(all_values.begin(), all_values.begin() + probe_set_size);
		double actual_card = 0;
		for (const auto val : probe_set) {
			actual_card += cardinalities[val];
		}

		for (auto _ : state) {
			const auto card = omnisketch::Algorithm::DisjunctPointQueries(*omni_sketch, probe_set);
			SetCounters(state, ComputeQError(card.cardinality, actual_card));
		}
	}

	void JoinSketch(::benchmark::State &state, bool use_approximate_join = false) {
		static const size_t JOIN_KEY_COUNT = all_values.size() / 4;
		std::shuffle(all_values.begin(), all_values.end(), gen);
		double actual_card = 0;
		for (auto it = all_values.begin(); it != all_values.begin() + JOIN_KEY_COUNT; ++it) {
			actual_card += cardinalities[*it];
		}

		const size_t probe_sample_size = state.range(0);

		omnisketch::CardEstResult<uint64_t> probe_sample;
		probe_sample.min_hash_sketch.max_count = probe_sample_size;
		probe_sample.cardinality = (double)JOIN_KEY_COUNT;
		for (auto it = all_values.begin(); it != all_values.begin() + probe_sample_size; ++it) {
			probe_sample.min_hash_sketch.hashes.insert(omnisketch::Hash(*it));
		}

		for (auto _ : state) {
			if (use_approximate_join) {
				const auto card = omnisketch::Algorithm::ApproximateJoin(*omni_sketch, probe_sample);
				SetCounters(state, ComputeQError(card.cardinality, actual_card));
			} else {
				const auto card = omnisketch::Algorithm::Join(*omni_sketch, probe_sample);
				SetCounters(state, ComputeQError(card.cardinality, actual_card));
			}
		}
	}

	double ComputeQError(double estimate, double actual) {
		if (actual == 0) {
			return estimate == 0 ? 1 : estimate;
		} else {
			return estimate / actual;
		}
	}

	void SetCounters(::benchmark::State &state, double q_error) {
		state.counters["QError"] = q_error;
		state.counters["SketchSize"] = omni_sketch->EstimateByteSize();
	}

	size_t attribute_count;
	size_t min_hash_sample_size;
	std::unique_ptr<omnisketch::OmniSketch<size_t, size_t, std::set<uint64_t>>> omni_sketch;
	std::vector<size_t> all_values;
	std::unordered_map<size_t, size_t> cardinalities;
	std::mt19937 gen;
};

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, ProbeErrorUniform, true)(benchmark::State &state) {
	ProbeSketch(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, ProbeErrorSkewed, false)(benchmark::State &state) {
	ProbeSketch(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, SetMembershipProbeErrorUniform, true)(benchmark::State &state) {
	SetMembershipProbeSketch(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, SetMembershipProbeErrorSkewed, false)(benchmark::State &state) {
	SetMembershipProbeSketch(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, ApproximateJoinErrorUniform, true)(benchmark::State &state) {
	JoinSketch(state, true);
}

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, ApproximateJoinErrorSkewed, false)(benchmark::State &state) {
	JoinSketch(state, true);
}

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, JoinErrorUniform, true)(benchmark::State &state) {
	JoinSketch(state);
}

BENCHMARK_TEMPLATE_DEFINE_F(ProbeErrorFixture, JoinErrorSkewed, false)(benchmark::State &state) {
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

BENCHMARK_REGISTER_F(ProbeErrorFixture, ApproximateJoinErrorUniform)
    ->Iterations(ITERATION_COUNT)
    ->ArgsProduct({benchmark::CreateRange(1 << 2, 1 << 14, 2), {1 << 16}, {64, 1024}});

BENCHMARK_REGISTER_F(ProbeErrorFixture, ApproximateJoinErrorSkewed)
    ->Iterations(ITERATION_COUNT)
    ->ArgsProduct({benchmark::CreateRange(1 << 2, 1 << 14, 2), {1 << 16}, {64, 1024}});

BENCHMARK_REGISTER_F(ProbeErrorFixture, JoinErrorUniform)
    ->Iterations(ITERATION_COUNT)
    ->ArgsProduct({benchmark::CreateRange(1 << 2, 1 << 14, 2), {1 << 16}, {64, 1024}});

BENCHMARK_REGISTER_F(ProbeErrorFixture, JoinErrorSkewed)
    ->Iterations(ITERATION_COUNT)
    ->ArgsProduct({benchmark::CreateRange(1 << 2, 1 << 14, 2), {1 << 16}, {64, 1024}});

BENCHMARK_MAIN();
