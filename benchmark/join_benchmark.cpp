#include <benchmark/benchmark.h>

#include "include/combinator.hpp"
#include "include/csv_importer.hpp"
#include "include/plan_generator.hpp"

using namespace omnisketch;
using IntOmniSketch = TypedPointOmniSketch<size_t>;
using StringOmniSketch = TypedPointOmniSketch<std::string>;
static constexpr size_t OUTPUT_SIZE = 1024;

enum class SSBQuery : int64_t {
	Q1_1 = 11,
	Q1_2 = 12,
	Q1_3 = 13,
	Q2_1 = 21,
	Q2_2 = 22,
	Q2_3 = 23,
	Q3_1 = 31,
	Q3_2 = 32,
	Q3_3 = 33,
	Q3_4 = 34,
	Q4_1 = 41,
	Q4_2 = 42,
	Q4_3 = 43
};

size_t GetCardinality(SSBQuery q) {
	switch (q) {
	case SSBQuery::Q1_1:
		return 123856;
	case SSBQuery::Q1_2:
		return 4251;
	case SSBQuery::Q1_3:
		return 470;
	case SSBQuery::Q2_1:
		return 46026;
	case SSBQuery::Q2_2:
		return 10577;
	case SSBQuery::Q2_3:
		return 1222;
	case SSBQuery::Q3_1:
		return 246821;
	case SSBQuery::Q3_2:
		return 8606;
	case SSBQuery::Q3_3:
		return 329;
	case SSBQuery::Q3_4:
		return 5;
	case SSBQuery::Q4_1:
		return 90353;
	case SSBQuery::Q4_2:
		return 21803;
	case SSBQuery::Q4_3:
		return 447;
	}
}

class SketchesSingleton {
public:
	SketchesSingleton(SketchesSingleton const &) = delete;
	void operator=(SketchesSingleton const &) = delete;
	static SketchesSingleton &GetInstance() {
		static SketchesSingleton instance;
		return instance;
	}

private:
	SketchesSingleton() {
		OmniSketchConfig config;
		config.SetWidth(512);
		config.SetSampleCount(1 << 11);

		CSVImporter::ReadFile("../data/lineorder_f.csv", "lineorder",
		                      {"lo_discount", "lo_quantity", "lo_orderdate", "lo_partkey", "lo_custkey", "lo_suppkey"},
		                      std::vector<ColumnType>(6, ColumnType::UINT), config, true, 1);

		config.SetWidth(256);
		config.SetSampleCount(1 << 10);

		CSVImporter::ReadFile(
		    "../data/date_f.csv", "date", {"d_datekey", "d_year", "d_yearmonthnum", "d_weeknuminyear", "d_yearmonth"},
		    {ColumnType::UINT, ColumnType::UINT, ColumnType::UINT, ColumnType::UINT, ColumnType::VARCHAR}, config);

		CSVImporter::ReadFile("../data/part_f.csv", "part", {"p_partkey", "p_category", "p_brand", "p_mfgr"},
		                      {ColumnType::UINT, ColumnType::VARCHAR, ColumnType::VARCHAR, ColumnType::VARCHAR},
		                      config);

		CSVImporter::ReadFile("../data/supplier_f.csv", "supplier", {"s_suppkey", "s_region", "s_nation", "s_city"},
		                      {ColumnType::UINT, ColumnType::VARCHAR, ColumnType::VARCHAR, ColumnType::VARCHAR},
		                      config);

		CSVImporter::ReadFile("../data/customer_f.csv", "customer", {"c_custkey", "c_region", "c_nation", "c_city"},
		                      {ColumnType::UINT, ColumnType::VARCHAR, ColumnType::VARCHAR, ColumnType::VARCHAR},
		                      config);
	}
};

class SSBFixture : public benchmark::Fixture {
public:
	SSBFixture() : singleton(SketchesSingleton::GetInstance()) {
	}

	template <typename CombinatorType>
	double ExecuteQuery(SSBQuery q) {
		switch (q) {
		case SSBQuery::Q1_1:
			return Q1_1<CombinatorType>();
		case SSBQuery::Q1_2:
			return Q1_2<CombinatorType>();
		case SSBQuery::Q1_3:
			return Q1_3<CombinatorType>();
		case SSBQuery::Q2_1:
			return Q2_1<CombinatorType>();
		case SSBQuery::Q2_2:
			return Q2_2<CombinatorType>();
		case SSBQuery::Q2_3:
			return Q2_3<CombinatorType>();
		case SSBQuery::Q3_1:
			return Q3_1<CombinatorType>();
		case SSBQuery::Q3_2:
			return Q3_2<CombinatorType>();
		case SSBQuery::Q3_3:
			return Q3_3<CombinatorType>();
		case SSBQuery::Q3_4:
			return Q3_4<CombinatorType>();
		case SSBQuery::Q4_1:
			return Q4_1<CombinatorType>();
		case SSBQuery::Q4_2:
			return Q4_2<CombinatorType>();
		case SSBQuery::Q4_3:
			return Q4_3<CombinatorType>();
		}
	}

	template <typename CombinatorType>
	double Q1_1() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate("date", "d_year", PredicateConverter::ConvertPoint(1993));
		gen.AddJoin("lineorder", "lo_orderdate", "date");
		gen.AddPredicate("lineorder", "lo_discount", PredicateConverter::ConvertRange(1, 3));
		gen.AddPredicate("lineorder", "lo_quantity", PredicateConverter::ConvertRange(1, 25));

		return gen.EstimateCardinality();
	}

	template <typename CombinatorType>
	double Q1_2() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate("date", "d_yearmonthnum", PredicateConverter::ConvertPoint(199401));
		gen.AddJoin("lineorder", "lo_orderdate", "date");
		gen.AddPredicate("lineorder", "lo_discount", PredicateConverter::ConvertRange(4, 6));
		gen.AddPredicate("lineorder", "lo_quantity", PredicateConverter::ConvertRange(26, 35));

		return gen.EstimateCardinality();
	}

	template <typename CombinatorType>
	double Q1_3() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate("date", "d_year", PredicateConverter::ConvertPoint(1994));
		gen.AddPredicate("date", "d_weeknuminyear", PredicateConverter::ConvertPoint(6));
		gen.AddJoin("lineorder", "lo_orderdate", "date");
		gen.AddPredicate("lineorder", "lo_discount", PredicateConverter::ConvertRange(5, 7));
		gen.AddPredicate("lineorder", "lo_quantity", PredicateConverter::ConvertRange(36, 40));

		return gen.EstimateCardinality();
	}

	template <typename CombinatorType>
	double Q2_1() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate("part", "p_category", PredicateConverter::ConvertPoint<std::string>("MFGR#12"));
		gen.AddPredicate("supplier", "s_region", PredicateConverter::ConvertPoint<std::string>("AMERICA"));
		gen.AddJoin("lineorder", "lo_partkey", "part");
		gen.AddJoin("lineorder", "lo_suppkey", "supplier");
		gen.AddJoin("lineorder", "lo_orderdate", "date");

		return gen.EstimateCardinality();
	}

	template <typename CombinatorType>
	double Q2_2() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate(
		    "part", "p_brand",
		    PredicateConverter::ConvertSet<std::string>({"MFGR#2221", "MFGR#2222", "MFGR#2223", "MFGR#2224",
		                                                 "MFGR#2225", "MFGR#2226", "MFGR#2227", "MFGR#2228"}));
		gen.AddPredicate("supplier", "s_region", PredicateConverter::ConvertPoint<std::string>("ASIA"));
		gen.AddJoin("lineorder", "lo_partkey", "part");
		gen.AddJoin("lineorder", "lo_suppkey", "supplier");
		gen.AddJoin("lineorder", "lo_orderdate", "date");

		return gen.EstimateCardinality();
	}

	template <typename CombinatorType>
	double Q2_3() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate("part", "p_brand", PredicateConverter::ConvertPoint<std::string>("MFGR#2339"));
		gen.AddPredicate("supplier", "s_region", PredicateConverter::ConvertPoint<std::string>("EUROPE"));
		gen.AddJoin("lineorder", "lo_partkey", "part");
		gen.AddJoin("lineorder", "lo_suppkey", "supplier");
		gen.AddJoin("lineorder", "lo_orderdate", "date");

		return gen.EstimateCardinality();
	}

	template <typename CombinatorType>
	double Q3_1() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate("customer", "c_region", PredicateConverter::ConvertPoint<std::string>("ASIA"));
		gen.AddPredicate("supplier", "s_region", PredicateConverter::ConvertPoint<std::string>("ASIA"));
		gen.AddPredicate("date", "d_year", PredicateConverter::ConvertRange(1992, 1997));
		gen.AddJoin("lineorder", "lo_custkey", "customer");
		gen.AddJoin("lineorder", "lo_suppkey", "supplier");
		gen.AddJoin("lineorder", "lo_orderdate", "date");

		return gen.EstimateCardinality();
	}

	template <typename CombinatorType>
	double Q3_2() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate("customer", "c_nation", PredicateConverter::ConvertPoint<std::string>("UNITED STATES"));
		gen.AddPredicate("supplier", "s_nation", PredicateConverter::ConvertPoint<std::string>("UNITED STATES"));
		gen.AddPredicate("date", "d_year", PredicateConverter::ConvertRange(1992, 1997));
		gen.AddJoin("lineorder", "lo_custkey", "customer");
		gen.AddJoin("lineorder", "lo_suppkey", "supplier");
		gen.AddJoin("lineorder", "lo_orderdate", "date");

		return gen.EstimateCardinality();
	}

	template <typename CombinatorType>
	double Q3_3() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate("customer", "c_city",
		                 PredicateConverter::ConvertSet<std::string>({"UNITED KI1", "UNITED KI5"}));
		gen.AddPredicate("supplier", "s_city",
		                 PredicateConverter::ConvertSet<std::string>({"UNITED KI1", "UNITED KI5"}));
		gen.AddPredicate("date", "d_year", PredicateConverter::ConvertRange(1992, 1997));
		gen.AddJoin("lineorder", "lo_custkey", "customer");
		gen.AddJoin("lineorder", "lo_suppkey", "supplier");
		gen.AddJoin("lineorder", "lo_orderdate", "date");

		return gen.EstimateCardinality();
	}

	template <typename CombinatorType>
	double Q3_4() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate("customer", "c_city",
		                 PredicateConverter::ConvertSet<std::string>({"UNITED KI1", "UNITED KI5"}));
		gen.AddPredicate("supplier", "s_city",
		                 PredicateConverter::ConvertSet<std::string>({"UNITED KI1", "UNITED KI5"}));
		gen.AddPredicate("date", "d_yearmonth", PredicateConverter::ConvertPoint<std::string>("Dec1997"));
		gen.AddJoin("lineorder", "lo_custkey", "customer");
		gen.AddJoin("lineorder", "lo_suppkey", "supplier");
		gen.AddJoin("lineorder", "lo_orderdate", "date");

		return gen.EstimateCardinality();
	}

	template <typename CombinatorType>
	double Q4_1() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate("customer", "c_region", PredicateConverter::ConvertPoint<std::string>("AMERICA"));
		gen.AddPredicate("supplier", "s_region", PredicateConverter::ConvertPoint<std::string>("AMERICA"));
		gen.AddPredicate("part", "p_mfgr", PredicateConverter::ConvertSet<std::string>({"MFGR#1", "MFGR#2"}));
		gen.AddJoin("lineorder", "lo_custkey", "customer");
		gen.AddJoin("lineorder", "lo_suppkey", "supplier");
		gen.AddJoin("lineorder", "lo_partkey", "part");
		gen.AddJoin("lineorder", "lo_orderdate", "date");

		return gen.EstimateCardinality();
	}

	template <typename CombinatorType>
	double Q4_2() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate("customer", "c_region", PredicateConverter::ConvertPoint<std::string>("AMERICA"));
		gen.AddPredicate("supplier", "s_region", PredicateConverter::ConvertPoint<std::string>("AMERICA"));
		gen.AddPredicate("part", "p_mfgr", PredicateConverter::ConvertSet<std::string>({"MFGR#1", "MFGR#2"}));
		gen.AddPredicate("date", "d_year", PredicateConverter::ConvertRange(1997, 1998));
		gen.AddJoin("lineorder", "lo_custkey", "customer");
		gen.AddJoin("lineorder", "lo_suppkey", "supplier");
		gen.AddJoin("lineorder", "lo_partkey", "part");
		gen.AddJoin("lineorder", "lo_orderdate", "date");

		return gen.EstimateCardinality();
	}

	template <typename CombinatorType>
	double Q4_3() {
		PlanGenerator gen(std::make_shared<CombinatorType>());
		gen.AddPredicate("customer", "c_region", PredicateConverter::ConvertPoint<std::string>("AMERICA"));
		gen.AddPredicate("supplier", "s_nation", PredicateConverter::ConvertPoint<std::string>("UNITED STATES"));
		gen.AddPredicate("part", "p_category", PredicateConverter::ConvertPoint<std::string>("MFGR#14"));
		gen.AddPredicate("date", "d_year", PredicateConverter::ConvertRange(1997, 1998));
		gen.AddJoin("lineorder", "lo_custkey", "customer");
		gen.AddJoin("lineorder", "lo_suppkey", "supplier");
		gen.AddJoin("lineorder", "lo_partkey", "part");
		gen.AddJoin("lineorder", "lo_orderdate", "date");

		return gen.EstimateCardinality();
	}

protected:
	const SketchesSingleton &singleton;
};

static const std::vector<int64_t> query_idxs {11, 12, 13, 21, 22, 23, 31, 32, 33, 34, 41, 42, 43};

BENCHMARK_DEFINE_F(SSBFixture, UncorrelatedCombination)(::benchmark::State &state) {
	auto query = static_cast<SSBQuery>(state.range());
	for (auto _ : state) {
		auto result = ExecuteQuery<UncorrelatedCombinator>(query);
		state.counters["Card"] = (double)GetCardinality(query);
		state.counters["Est"] = result;
		state.counters["QErr"] = result / (double)GetCardinality(query);
	}
}

BENCHMARK_REGISTER_F(SSBFixture, UncorrelatedCombination)->ArgsProduct({query_idxs});

BENCHMARK_DEFINE_F(SSBFixture, ExhaustiveCombination)(::benchmark::State &state) {
	auto query = static_cast<SSBQuery>(state.range());
	for (auto _ : state) {
		auto result = ExecuteQuery<ExhaustiveCombinator>(query);
		state.counters["Card"] = (double)GetCardinality(query);
		state.counters["Est"] = result;
		state.counters["QErr"] = result / (double)GetCardinality(query);
	}
}

BENCHMARK_REGISTER_F(SSBFixture, ExhaustiveCombination)->ArgsProduct({query_idxs});

BENCHMARK_MAIN();
