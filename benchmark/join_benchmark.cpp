#include <benchmark/benchmark.h>

#include "csv_importer.hpp"
#include "plan_generator.hpp"

#include <numeric>

using namespace omnisketch;
enum class BenchmarkName : int64_t { SSB = 0, SSB_SKEW = 1, JOB_LIGHT = 2 };
enum class CombinatorType : int64_t { EXHAUSTIVE = 0, EXHAUSTIVE_CORRELATED = 1 };

std::vector<int64_t> AllSSBQueries() {
	return {11, 12, 13, 21, 22, 23, 31, 32, 33, 34, 41, 42, 43};
}

int64_t SSBQueryIdx(int64_t in) {
	switch (in) {
	case 11:
		return 0;
	case 12:
		return 1;
	case 13:
		return 2;
	case 21:
		return 3;
	case 22:
		return 4;
	case 23:
		return 5;
	case 31:
		return 6;
	case 32:
		return 7;
	case 33:
		return 8;
	case 34:
		return 9;
	case 41:
		return 10;
	case 42:
		return 11;
	case 43:
		return 12;
	default:
		return -1;
	}
}

std::vector<int64_t> AllSSBSubQueries(size_t join_count) {
	size_t offset = 0;
	size_t query_count = 0;
	if (join_count == 0) {
		query_count = 23;
	} else if (join_count == 1) {
		offset = 23;
		query_count = 21;
	} else if (join_count == 2) {
		offset = 44;
		query_count = 34;
	} else if (join_count == 3) {
		offset = 78;
		query_count = 18;
	} else if (join_count == 4) {
		offset = 96;
		query_count = 3;
	} else {
		throw std::logic_error("Too many joins.");
	}

	std::vector<int64_t> idxs(query_count);
	std::iota(idxs.begin(), idxs.end(), offset);
	return idxs;
}

std::vector<int64_t> AllJOBLightQueries() {
	std::vector<int64_t> idxs(70);
	std::iota(idxs.begin(), idxs.end(), 0);
	return idxs;
}

class SketchesSingleton {
public:
	SketchesSingleton(SketchesSingleton const &) = delete;
	void operator=(SketchesSingleton const &) = delete;
	static SketchesSingleton &GetInstance() {
		static SketchesSingleton instance;
		return instance;
	}

	std::vector<CountQuery> LoadSSBQueries() {
		if (!ssb_loaded) {
			CSVImporter::ImportTables("../data/ssb/definition.csv");
			ssb_loaded = true;
		}
		return CSVImporter::ImportQueries("../data/ssb/queries.csv");
	}

	std::vector<CountQuery> LoadSSBSkewQueries() {
		if (!ssb_skew_loaded) {
			CSVImporter::ImportTables("../data/ssb-skew-sf1/definition.csv");
			ssb_skew_loaded = true;
		}
		std::vector<CountQuery> queries;
		auto ssb_queries = CSVImporter::ImportQueries("../data/ssb-skew-sf1/queries.csv");
		queries.insert(queries.end(), ssb_queries.begin(), ssb_queries.end());
		ssb_queries = CSVImporter::ImportQueries("../data/ssb-skew-sf1/sub_queries_1.csv");
		queries.insert(queries.end(), ssb_queries.begin(), ssb_queries.end());
		ssb_queries = CSVImporter::ImportQueries("../data/ssb-skew-sf1/sub_queries_2.csv");
		queries.insert(queries.end(), ssb_queries.begin(), ssb_queries.end());
		ssb_queries = CSVImporter::ImportQueries("../data/ssb-skew-sf1/sub_queries_3.csv");
		queries.insert(queries.end(), ssb_queries.begin(), ssb_queries.end());
		ssb_queries = CSVImporter::ImportQueries("../data/ssb-skew-sf1/sub_queries_4.csv");
		queries.insert(queries.end(), ssb_queries.begin(), ssb_queries.end());
		ssb_queries = CSVImporter::ImportQueries("../data/ssb-skew-sf1/sub_queries_5.csv");
		queries.insert(queries.end(), ssb_queries.begin(), ssb_queries.end());

		return queries;
	}

	std::vector<CountQuery> LoadJOBLightQueries() {
		if (!job_light_loaded) {
			CSVImporter::ImportTables("../data/imdb/definition.csv");
			job_light_loaded = true;
		}
		return CSVImporter::ImportQueries("../data/imdb/queries.csv");
	}

private:
	SketchesSingleton() {
		ssb_loaded = false;
		ssb_skew_loaded = false;
		job_light_loaded = false;
	}

	bool ssb_loaded;
	bool ssb_skew_loaded;
	bool job_light_loaded;
};

template <typename Bench, typename Comb>
class JoinBenchmarkFixture : public benchmark::Fixture {
public:
	static constexpr auto BENCH_ID = Bench::value;
	static constexpr auto COMBINATOR_ID = Comb::value;

	JoinBenchmarkFixture() : singleton(SketchesSingleton::GetInstance()) {
	}

	void SetUp(const benchmark::State &) override {
		if (!loaded_queries.empty()) {
			return;
		}

		auto benchmark_name = static_cast<BenchmarkName>(BENCH_ID);

		if (benchmark_name == BenchmarkName::SSB) {
			loaded_queries = singleton.LoadSSBQueries();
		}
		if (benchmark_name == BenchmarkName::SSB_SKEW) {
			loaded_queries = singleton.LoadSSBSkewQueries();
		}
		if (benchmark_name == BenchmarkName::JOB_LIGHT) {
			loaded_queries = singleton.LoadJOBLightQueries();
		}
	}

	const CountQuery &GetQuery(size_t idx) {
		assert(idx < loaded_queries.size());
		return loaded_queries[idx];
	}

protected:
	SketchesSingleton &singleton;
	std::vector<CountQuery> loaded_queries;
};

using SSB = std::integral_constant<int64_t, static_cast<int64_t>(BenchmarkName::SSB)>;
using SSB_SKEW = std::integral_constant<int64_t, static_cast<int64_t>(BenchmarkName::SSB_SKEW)>;
using JOB_LIGHT = std::integral_constant<int64_t, static_cast<int64_t>(BenchmarkName::JOB_LIGHT)>;
using EXHAUSTIVE = std::integral_constant<int64_t, static_cast<int64_t>(CombinatorType::EXHAUSTIVE)>;
using EXHAUSTIVE_CORR = std::integral_constant<int64_t, static_cast<int64_t>(CombinatorType::EXHAUSTIVE_CORRELATED)>;

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, SSBE, SSB, EXHAUSTIVE)
(benchmark::State &state) {
	for (auto _ : state) {
		auto query = GetQuery(SSBQueryIdx(state.range()));
		auto result = query.plan.Estimate();
		state.counters["Card"] = (double)query.cardinality;
		state.counters["Est"] = result;
		state.counters["QErr"] = result / (double)query.cardinality;
		state.counters["MemoryUsageMB"] =
		    benchmark::Counter((double)Registry::Get().EstimateByteSize(), benchmark::Counter::kIsIterationInvariant,
		                       benchmark::Counter::OneK::kIs1024);
	}
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, SSBCE, SSB, EXHAUSTIVE_CORR)
(benchmark::State &state) {
	for (auto _ : state) {
		auto query = GetQuery(SSBQueryIdx(state.range()));
		auto result = query.plan.Estimate();
		state.counters["Card"] = (double)query.cardinality;
		state.counters["Est"] = result;
		state.counters["QErr"] = result / (double)query.cardinality;
		state.counters["MemoryUsageMB"] =
		    benchmark::Counter((double)Registry::Get().EstimateByteSize(), benchmark::Counter::kIsIterationInvariant,
		                       benchmark::Counter::OneK::kIs1024);
	}
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, SSBSkewE, SSB_SKEW, EXHAUSTIVE)
(benchmark::State &state) {
	for (auto _ : state) {
		auto query = GetQuery(SSBQueryIdx(state.range()));
		auto result = query.plan.Estimate();
		state.counters["Card"] = (double)query.cardinality;
		state.counters["Est"] = result;
		state.counters["QErr"] = result / (double)query.cardinality;
		state.counters["MemoryUsageMB"] =
		    benchmark::Counter((double)Registry::Get().EstimateByteSize(), benchmark::Counter::kIsIterationInvariant,
		                       benchmark::Counter::OneK::kIs1024);
	}
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, SSBSkewCE, SSB_SKEW, EXHAUSTIVE_CORR)
(benchmark::State &state) {
	for (auto _ : state) {
		auto query = GetQuery(SSBQueryIdx(state.range()));
		auto result = query.plan.Estimate();
		state.counters["Card"] = (double)query.cardinality;
		state.counters["Est"] = result;
		state.counters["QErr"] = result / (double)query.cardinality;
		state.counters["MemoryUsageMB"] =
		    benchmark::Counter((double)Registry::Get().EstimateByteSize(), benchmark::Counter::kIsIterationInvariant,
		                       benchmark::Counter::OneK::kIs1024);
	}
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, SSBSkewSubE, SSB_SKEW, EXHAUSTIVE)
(benchmark::State &state) {
	auto queries = AllSSBSubQueries(state.range());
	for (auto _ : state) {
		auto query = GetQuery(13 + queries[state.range(1)]);
		auto result = query.plan.Estimate();
		state.counters["Card"] = (double)query.cardinality;
		state.counters["Est"] = result;
		state.counters["QErr"] = result / (double)query.cardinality;
		state.counters["MemoryUsageMB"] =
		    benchmark::Counter((double)Registry::Get().EstimateByteSize(), benchmark::Counter::kIsIterationInvariant,
		                       benchmark::Counter::OneK::kIs1024);
	}
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, SSBSkewSubCE, SSB_SKEW, EXHAUSTIVE_CORR)
(benchmark::State &state) {
	auto queries = AllSSBSubQueries(state.range());
	for (auto _ : state) {
		auto query = GetQuery(13 + queries[state.range(1)]);
		auto result = query.plan.Estimate();
		state.counters["Card"] = (double)query.cardinality;
		state.counters["Est"] = result;
		state.counters["QErr"] = result / (double)query.cardinality;
		state.counters["MemoryUsageMB"] =
		    benchmark::Counter((double)Registry::Get().EstimateByteSize(), benchmark::Counter::kIsIterationInvariant,
		                       benchmark::Counter::OneK::kIs1024);
	}
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, JOBLightE, JOB_LIGHT, EXHAUSTIVE)
(benchmark::State &state) {
	for (auto _ : state) {
		auto query = GetQuery(state.range());
		auto result = query.plan.Estimate();
		state.counters["Card"] = (double)query.cardinality;
		state.counters["Est"] = result;
		state.counters["QErr"] = result / (double)query.cardinality;
		state.counters["MemoryUsageMB"] =
		    benchmark::Counter((double)Registry::Get().EstimateByteSize(), benchmark::Counter::kIsIterationInvariant,
		                       benchmark::Counter::OneK::kIs1024);
	}
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, JOBLightCE, JOB_LIGHT, EXHAUSTIVE_CORR)
(benchmark::State &state) {
	for (auto _ : state) {
		auto query = GetQuery(state.range());
		auto result = query.plan.Estimate();
		state.counters["Card"] = (double)query.cardinality;
		state.counters["Est"] = result;
		state.counters["QErr"] = result / (double)query.cardinality;
		state.counters["MemoryUsageMB"] =
		    benchmark::Counter((double)Registry::Get().EstimateByteSize(), benchmark::Counter::kIsIterationInvariant,
		                       benchmark::Counter::OneK::kIs1024);
	}
}

static void SSBSkewSubArgs(benchmark::internal::Benchmark *b) {
	for (size_t i = 0; i < 5; i++) {
		auto queries = AllSSBSubQueries(i);
		for (size_t j = 0; j < queries.size(); j++) {
			b->Args({static_cast<int>(i), static_cast<int>(j)});
		}
	}
}

BENCHMARK_REGISTER_F(JoinBenchmarkFixture, SSBE)->ArgsProduct({AllSSBQueries()})->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, SSBCE)->ArgsProduct({AllSSBQueries()})->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, SSBSkewE)->ArgsProduct({AllSSBQueries()})->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, SSBSkewCE)->ArgsProduct({AllSSBQueries()})->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, SSBSkewSubE)->Apply(SSBSkewSubArgs)->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, SSBSkewSubCE)->Apply(SSBSkewSubArgs)->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, JOBLightE)->ArgsProduct({AllJOBLightQueries()})->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, JOBLightCE)->ArgsProduct({AllJOBLightQueries()})->Iterations(5);

BENCHMARK_MAIN();
