#include <benchmark/benchmark.h>

#include "csv_importer.hpp"
#include "plan_generator.hpp"

#include <numeric>

using namespace omnisketch;

enum class BenchmarkName : int64_t { SSB = 0, SSB_SKEW = 1, JOB_LIGHT = 2 };

enum class CombinatorType : int64_t { EXHAUSTIVE = 0, EXHAUSTIVE_CORRELATED = 1 };

std::vector<int64_t> GetAllSSBQueries() {
    return {11, 12, 13, 21, 22, 23, 31, 32, 33, 34, 41, 42, 43};
}

int64_t GetSSBQueryIndex(int64_t query_id) {
    switch (query_id) {
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

std::vector<int64_t> GetAllSSBSubQueries(size_t join_count) {
    size_t offset = 0;
    size_t queryCount = 0;

    switch (join_count) {
        case 0:
            queryCount = 23;
            break;
        case 1:
            offset = 23;
            queryCount = 21;
            break;
        case 2:
            offset = 44;
            queryCount = 34;
            break;
        case 3:
            offset = 78;
            queryCount = 18;
            break;
        case 4:
            offset = 96;
            queryCount = 3;
            break;
        default:
            throw std::logic_error("Invalid join count: " + std::to_string(join_count));
    }

    std::vector<int64_t> indices(queryCount);
    std::iota(indices.begin(), indices.end(), static_cast<int64_t>(offset));
    return indices;
}

std::vector<int64_t> GetAllJOBLightQueries() {
    std::vector<int64_t> indices(70);
    std::iota(indices.begin(), indices.end(), 0);
    return indices;
}

class SketchesSingleton {
public:
    SketchesSingleton(const SketchesSingleton&) = delete;
    SketchesSingleton& operator=(const SketchesSingleton&) = delete;

    static SketchesSingleton& GetInstance() {
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
        queries.reserve(200);  // Reserve space for better performance

        const std::vector<std::string> query_files = {
            "../data/ssb-skew-sf1/queries.csv",       "../data/ssb-skew-sf1/sub_queries_1.csv",
            "../data/ssb-skew-sf1/sub_queries_2.csv", "../data/ssb-skew-sf1/sub_queries_3.csv",
            "../data/ssb-skew-sf1/sub_queries_4.csv", "../data/ssb-skew-sf1/sub_queries_5.csv"};

        for (const auto& query_file : query_files) {
            auto file_queries = CSVImporter::ImportQueries(query_file);
            queries.insert(queries.end(), std::make_move_iterator(file_queries.begin()),
                           std::make_move_iterator(file_queries.end()));
        }

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
    SketchesSingleton() = default;

    bool ssb_loaded = false;
    bool ssb_skew_loaded = false;
    bool job_light_loaded = false;
};

template <typename Bench, typename Comb>
class JoinBenchmarkFixture : public benchmark::Fixture {
public:
    static constexpr auto BENCH_ID = Bench::value;
    static constexpr auto COMBINATOR_ID = Comb::value;

    JoinBenchmarkFixture() : singleton(SketchesSingleton::GetInstance()) {
    }

    void SetUp(const benchmark::State&) override {
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

    const CountQuery& GetQuery(size_t idx) {
        assert(idx < loaded_queries.size());
        return loaded_queries[idx];
    }

protected:
    SketchesSingleton& singleton;
    std::vector<CountQuery> loaded_queries;
};

using SSB = std::integral_constant<int64_t, static_cast<int64_t>(BenchmarkName::SSB)>;
using SSB_SKEW = std::integral_constant<int64_t, static_cast<int64_t>(BenchmarkName::SSB_SKEW)>;
using JOB_LIGHT = std::integral_constant<int64_t, static_cast<int64_t>(BenchmarkName::JOB_LIGHT)>;
using EXHAUSTIVE = std::integral_constant<int64_t, static_cast<int64_t>(CombinatorType::EXHAUSTIVE)>;
using EXHAUSTIVE_CORR = std::integral_constant<int64_t, static_cast<int64_t>(CombinatorType::EXHAUSTIVE_CORRELATED)>;

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, SSBE, SSB, EXHAUSTIVE)
(benchmark::State& state) {
    for (auto _ : state) {
        auto query = GetQuery(GetSSBQueryIndex(state.range()));
        const auto result = query.plan.Estimate();
        state.counters["Card"] = static_cast<double>(query.cardinality);
        state.counters["Est"] = result;
        state.counters["QErr"] = result / static_cast<double>(query.cardinality);
        state.counters["MemoryUsageMB"] =
            benchmark::Counter(static_cast<double>(Registry::Get().EstimateByteSize()),
                               benchmark::Counter::kIsIterationInvariant, benchmark::Counter::OneK::kIs1024);
    }
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, SSBCE, SSB, EXHAUSTIVE_CORR)
(benchmark::State& state) {
    for (auto _ : state) {
        auto query = GetQuery(GetSSBQueryIndex(state.range()));
        const auto result = query.plan.Estimate();
        state.counters["Card"] = static_cast<double>(query.cardinality);
        state.counters["Est"] = result;
        state.counters["QErr"] = result / static_cast<double>(query.cardinality);
        state.counters["MemoryUsageMB"] =
            benchmark::Counter(static_cast<double>(Registry::Get().EstimateByteSize()),
                               benchmark::Counter::kIsIterationInvariant, benchmark::Counter::OneK::kIs1024);
    }
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, SSBSkewE, SSB_SKEW, EXHAUSTIVE)
(benchmark::State& state) {
    for (auto _ : state) {
        auto query = GetQuery(GetSSBQueryIndex(state.range()));
        const auto result = query.plan.Estimate();
        state.counters["Card"] = static_cast<double>(query.cardinality);
        state.counters["Est"] = result;
        state.counters["QErr"] = result / static_cast<double>(query.cardinality);
        state.counters["MemoryUsageMB"] =
            benchmark::Counter(static_cast<double>(Registry::Get().EstimateByteSize()),
                               benchmark::Counter::kIsIterationInvariant, benchmark::Counter::OneK::kIs1024);
    }
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, SSBSkewCE, SSB_SKEW, EXHAUSTIVE_CORR)
(benchmark::State& state) {
    for (auto _ : state) {
        auto query = GetQuery(GetSSBQueryIndex(state.range()));
        const auto result = query.plan.Estimate();
        state.counters["Card"] = static_cast<double>(query.cardinality);
        state.counters["Est"] = result;
        state.counters["QErr"] = result / static_cast<double>(query.cardinality);
        state.counters["MemoryUsageMB"] =
            benchmark::Counter(static_cast<double>(Registry::Get().EstimateByteSize()),
                               benchmark::Counter::kIsIterationInvariant, benchmark::Counter::OneK::kIs1024);
    }
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, SSBSkewSubE, SSB_SKEW, EXHAUSTIVE)
(benchmark::State& state) {
    const auto queries = GetAllSSBSubQueries(static_cast<size_t>(state.range()));
    for (auto _ : state) {
        auto query = GetQuery(13 + queries[state.range(1)]);
        const auto result = query.plan.Estimate();
        state.counters["Card"] = static_cast<double>(query.cardinality);
        state.counters["Est"] = result;
        state.counters["QErr"] = result / static_cast<double>(query.cardinality);
        state.counters["MemoryUsageMB"] =
            benchmark::Counter(static_cast<double>(Registry::Get().EstimateByteSize()),
                               benchmark::Counter::kIsIterationInvariant, benchmark::Counter::OneK::kIs1024);
    }
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, SSBSkewSubCE, SSB_SKEW, EXHAUSTIVE_CORR)
(benchmark::State& state) {
    const auto queries = GetAllSSBSubQueries(static_cast<size_t>(state.range()));
    for (auto _ : state) {
        auto query = GetQuery(13 + queries[state.range(1)]);
        const auto result = query.plan.Estimate();
        state.counters["Card"] = static_cast<double>(query.cardinality);
        state.counters["Est"] = result;
        state.counters["QErr"] = result / static_cast<double>(query.cardinality);
        state.counters["MemoryUsageMB"] =
            benchmark::Counter(static_cast<double>(Registry::Get().EstimateByteSize()),
                               benchmark::Counter::kIsIterationInvariant, benchmark::Counter::OneK::kIs1024);
    }
}

BENCHMARK_TEMPLATE_DEFINE_F(JoinBenchmarkFixture, JOBLightE, JOB_LIGHT, EXHAUSTIVE)
(benchmark::State& state) {
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
(benchmark::State& state) {
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

static void SSBSkewSubArgs(benchmark::internal::Benchmark* benchmark) {
    for (size_t i = 0; i < 5; ++i) {
        const auto queries = GetAllSSBSubQueries(i);
        for (size_t j = 0; j < queries.size(); ++j) {
            benchmark->Args({static_cast<int>(i), static_cast<int>(j)});
        }
    }
}

BENCHMARK_REGISTER_F(JoinBenchmarkFixture, SSBE)->ArgsProduct({GetAllSSBQueries()})->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, SSBCE)->ArgsProduct({GetAllSSBQueries()})->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, SSBSkewE)->ArgsProduct({GetAllSSBQueries()})->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, SSBSkewCE)->ArgsProduct({GetAllSSBQueries()})->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, SSBSkewSubE)->Apply(SSBSkewSubArgs)->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, SSBSkewSubCE)->Apply(SSBSkewSubArgs)->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, JOBLightE)->ArgsProduct({GetAllJOBLightQueries()})->Iterations(5);
BENCHMARK_REGISTER_F(JoinBenchmarkFixture, JOBLightCE)->ArgsProduct({GetAllJOBLightQueries()})->Iterations(5);

BENCHMARK_MAIN();
