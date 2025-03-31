#include <benchmark/benchmark.h>

#include "include/combinator.hpp"

#include <iostream>
#include <fstream>
#include <sstream>

using namespace omnisketch;
using IntOmniSketch = PointOmniSketch<size_t>;
using StringOmniSketch = PointOmniSketch<std::string>;
static constexpr size_t OUTPUT_SIZE = 1024;

enum class SSBQuery : uint64_t { Q1_1, Q1_2, Q1_3, Q2_1, Q2_2, Q2_3, Q3_1, Q3_2, Q3_3, Q3_4, Q4_1, Q4_2, Q4_3 };

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

	std::shared_ptr<IntOmniSketch> lo_discount;
	std::shared_ptr<IntOmniSketch> lo_quantity;
	std::shared_ptr<IntOmniSketch> lo_orderdate;
	std::shared_ptr<IntOmniSketch> lo_partkey;
	std::shared_ptr<IntOmniSketch> lo_custkey;
	std::shared_ptr<IntOmniSketch> lo_suppkey;
	std::shared_ptr<IntOmniSketch> d_year;
	std::shared_ptr<IntOmniSketch> d_yearmonthnum;
	std::shared_ptr<IntOmniSketch> d_weeknuminyear;
	std::shared_ptr<StringOmniSketch> d_yearmonth;
	std::shared_ptr<StringOmniSketch> p_category;
	std::shared_ptr<StringOmniSketch> p_brand;
	std::shared_ptr<StringOmniSketch> p_mfgr;
	std::shared_ptr<StringOmniSketch> s_region;
	std::shared_ptr<StringOmniSketch> s_nation;
	std::shared_ptr<StringOmniSketch> s_city;
	std::shared_ptr<StringOmniSketch> c_region;
	std::shared_ptr<StringOmniSketch> c_nation;
	std::shared_ptr<StringOmniSketch> c_city;

private:
	SketchesSingleton() {
		lo_discount = std::make_shared<IntOmniSketch>(256, 3, 1 << 10);
		lo_quantity = std::make_shared<IntOmniSketch>(256, 3, 1 << 10);
		lo_orderdate = std::make_shared<IntOmniSketch>(256, 3, 1 << 10);
		lo_partkey = std::make_shared<IntOmniSketch>(256, 3, 1 << 10);
		lo_custkey = std::make_shared<IntOmniSketch>(256, 3, 1 << 10);
		lo_suppkey = std::make_shared<IntOmniSketch>(256, 3, 1 << 10);

		d_year = std::make_shared<IntOmniSketch>(256, 3, 1 << 10);
		d_yearmonthnum = std::make_shared<IntOmniSketch>(256, 3, 1 << 10);
		d_weeknuminyear = std::make_shared<IntOmniSketch>(256, 3, 1 << 10);
		d_yearmonth = std::make_shared<StringOmniSketch>(256, 3, 1 << 10);

		p_category = std::make_shared<StringOmniSketch>(256, 3, 1 << 10);
		p_brand = std::make_shared<StringOmniSketch>(256, 3, 1 << 10);
		p_mfgr = std::make_shared<StringOmniSketch>(256, 3, 1 << 10);

		s_region = std::make_shared<StringOmniSketch>(256, 3, 1 << 10);
		s_nation = std::make_shared<StringOmniSketch>(256, 3, 1 << 10);
		s_city = std::make_shared<StringOmniSketch>(256, 3, 1 << 10);

		c_region = std::make_shared<StringOmniSketch>(256, 3, 1 << 10);
		c_nation = std::make_shared<StringOmniSketch>(256, 3, 1 << 10);
		c_city = std::make_shared<StringOmniSketch>(256, 3, 1 << 10);

		std::cout << "Loading lineorder table..." << std::endl;
		std::ifstream lineorder("../data/lineorder_f.csv");
		std::string line;
		size_t lineorder_id = 0;
		while (std::getline(lineorder, line)) {
			auto tokens = Split(line);
			size_t id = ++lineorder_id;

			lo_discount->AddRecord(std::stoul(tokens[1]), id);
			lo_quantity->AddRecord(std::stoul(tokens[2]), id);
			lo_orderdate->AddRecord(std::stoul(tokens[3]), id);
			lo_partkey->AddRecord(std::stoul(tokens[4]), id);
			lo_custkey->AddRecord(std::stoul(tokens[5]), id);
			lo_suppkey->AddRecord(std::stoul(tokens[6]), id);
		}
		lineorder.close();

		std::cout << "Loading date table..." << std::endl;
		std::ifstream date("../data/date_f.csv");
		while (std::getline(date, line)) {
			auto tokens = Split(line);
			size_t id = std::stoul(tokens[0]);
			d_year->AddRecord(std::stoul(tokens[1]), id);
			d_yearmonthnum->AddRecord(std::stoul(tokens[2]), id);
			d_weeknuminyear->AddRecord(std::stoul(tokens[3]), id);
			d_yearmonth->AddRecord(tokens[4], id);
		}
		date.close();

		std::cout << "Loading part table..." << std::endl;
		std::ifstream part("../data/part_f.csv");
		while (std::getline(part, line)) {
			auto tokens = Split(line);
			size_t id = std::stoul(tokens[0]);
			p_category->AddRecord(tokens[1], id);
			p_brand->AddRecord(tokens[2], id);
			p_mfgr->AddRecord(tokens[3], id);
		}
		part.close();

		std::cout << "Loading supplier table..." << std::endl;
		std::ifstream supplier("../data/supplier_f.csv");
		while (std::getline(supplier, line)) {
			auto tokens = Split(line);
			size_t id = std::stoul(tokens[0]);
			s_region->AddRecord(tokens[1], id);
			s_nation->AddRecord(tokens[2], id);
			s_city->AddRecord(tokens[3], id);
		}
		supplier.close();

		std::cout << "Loading customer table..." << std::endl;
		std::ifstream customer("../data/customer_f.csv");
		while (std::getline(customer, line)) {
			auto tokens = Split(line);
			size_t id = std::stoul(tokens[0]);
			c_region->AddRecord(tokens[1], id);
			c_nation->AddRecord(tokens[2], id);
			c_city->AddRecord(tokens[3], id);
		}
	}

	static std::vector<std::string> Split(const std::string &line) {
		std::vector<std::string> tokens;
		std::stringstream ss(line);
		std::string token;

		while (std::getline(ss, token, ',')) {
			tokens.push_back(token);
		}

		return tokens;
	}
};

class SSBFixture : public benchmark::Fixture {
public:
	SSBFixture() {
		auto &sketches = SketchesSingleton::GetInstance();
		lo_discount = sketches.lo_discount;
		lo_quantity = sketches.lo_quantity;
		lo_orderdate = sketches.lo_orderdate;
		lo_partkey = sketches.lo_partkey;
		lo_custkey = sketches.lo_custkey;
		lo_suppkey = sketches.lo_suppkey;
		d_year = sketches.d_year;
		d_yearmonthnum = sketches.d_yearmonthnum;
		d_weeknuminyear = sketches.d_weeknuminyear;
		d_yearmonth = sketches.d_yearmonth;
		p_category = sketches.p_category;
		p_brand = sketches.p_brand;
		p_mfgr = sketches.p_mfgr;
		s_region = sketches.s_region;
		s_nation = sketches.s_nation;
		s_city = sketches.s_city;
		c_region = sketches.c_region;
		c_nation = sketches.c_nation;
		c_city = sketches.c_city;
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> ExecuteQuery(SSBQuery q) {
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
	std::shared_ptr<OmniSketchCell> Q1_1() {
		auto combinator = std::make_unique<CombinatorType>();
		combinator->AddPredicate(lo_orderdate, d_year->Probe(1993));
		combinator->AddPredicate(lo_discount, PredicateConverter::ConvertRange(1, 3));
		combinator->AddPredicate(lo_quantity, PredicateConverter::ConvertRange(1, 25));
		return combinator->Execute(OUTPUT_SIZE);
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> Q1_2() {
		auto combinator = std::make_unique<CombinatorType>();
		combinator->AddPredicate(lo_orderdate, d_yearmonthnum->Probe(199401));
		combinator->AddPredicate(lo_discount, PredicateConverter::ConvertRange(4, 6));
		combinator->AddPredicate(lo_quantity, PredicateConverter::ConvertRange(26, 35));
		return combinator->Execute(OUTPUT_SIZE);
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> Q1_3() {
		auto date_combinator = std::make_unique<CombinatorType>();
		date_combinator->AddPredicate(d_year, PredicateConverter::ConvertPoint(1994));
		date_combinator->AddPredicate(d_weeknuminyear, PredicateConverter::ConvertPoint(6));

		auto combinator = std::make_unique<CombinatorType>();
		combinator->AddPredicate(lo_orderdate, date_combinator->Execute(OUTPUT_SIZE));
		combinator->AddPredicate(lo_discount, PredicateConverter::ConvertRange(5, 7));
		combinator->AddPredicate(lo_quantity, PredicateConverter::ConvertRange(36, 40));
		return combinator->Execute(OUTPUT_SIZE);
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> Q2_1() {
		auto combinator = std::make_unique<CombinatorType>();
		combinator->AddPredicate(lo_partkey, p_category->Probe("MFGR#12"));
		combinator->AddPredicate(lo_suppkey, s_region->Probe("AMERICA"));
		return combinator->Execute(OUTPUT_SIZE);
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> Q2_2() {
		auto combinator = std::make_unique<CombinatorType>();
		std::vector<std::string> probe_set {"MFGR#2221", "MFGR#2222", "MFGR#2223", "MFGR#2224",
		                                    "MFGR#2225", "MFGR#2226", "MFGR#2227", "MFGR#2228"};
		combinator->AddPredicate(lo_partkey, p_brand->ProbeSet(probe_set.data(), probe_set.size()));
		combinator->AddPredicate(lo_suppkey, s_region->Probe("ASIA"));
		return combinator->Execute(OUTPUT_SIZE);
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> Q2_3() {
		auto combinator = std::make_unique<CombinatorType>();
		combinator->AddPredicate(lo_partkey, p_brand->Probe("MFGR#2339"));
		combinator->AddPredicate(lo_suppkey, s_region->Probe("EUROPE"));
		return combinator->Execute(OUTPUT_SIZE);
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> Q3_1() {
		auto combinator = std::make_unique<CombinatorType>();
		combinator->AddPredicate(lo_custkey, c_region->Probe("ASIA"));
		combinator->AddPredicate(lo_suppkey, s_region->Probe("ASIA"));
		combinator->AddPredicate(lo_orderdate, d_year->ProbeRange(1992, 1997));
		return combinator->Execute(OUTPUT_SIZE);
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> Q3_2() {
		auto combinator = std::make_unique<CombinatorType>();
		combinator->AddPredicate(lo_custkey, c_nation->Probe("UNITED STATES"));
		combinator->AddPredicate(lo_suppkey, s_nation->Probe("UNITED STATES"));
		combinator->AddPredicate(lo_orderdate, d_year->ProbeRange(1992, 1997));
		return combinator->Execute(OUTPUT_SIZE);
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> Q3_3() {
		auto combinator = std::make_unique<CombinatorType>();
		std::vector<std::string> probe_set {"UNITED KI1", "UNITED KI5"};
		combinator->AddPredicate(lo_custkey, c_city->ProbeSet(probe_set.data(), probe_set.size()));
		combinator->AddPredicate(lo_suppkey, s_city->ProbeSet(probe_set.data(), probe_set.size()));
		combinator->AddPredicate(lo_orderdate, d_year->ProbeRange(1992, 1997));
		return combinator->Execute(OUTPUT_SIZE);
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> Q3_4() {
		auto combinator = std::make_unique<CombinatorType>();
		std::vector<std::string> probe_set {"UNITED KI1", "UNITED KI5"};
		combinator->AddPredicate(lo_custkey, c_city->ProbeSet(probe_set.data(), probe_set.size()));
		combinator->AddPredicate(lo_suppkey, s_city->ProbeSet(probe_set.data(), probe_set.size()));
		combinator->AddPredicate(lo_orderdate, d_yearmonth->Probe("Dec1997"));
		return combinator->Execute(OUTPUT_SIZE);
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> Q4_1() {
		auto combinator = std::make_unique<CombinatorType>();
		combinator->AddPredicate(lo_custkey, c_region->Probe("AMERICA"));
		combinator->AddPredicate(lo_suppkey, s_region->Probe("AMERICA"));
		std::vector<std::string> probe_set {"MFGR#1", "MFGR#2"};
		combinator->AddPredicate(lo_partkey, p_mfgr->ProbeSet(probe_set.data(), probe_set.size()));
		return combinator->Execute(OUTPUT_SIZE);
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> Q4_2() {
		auto combinator = std::make_unique<CombinatorType>();
		combinator->AddPredicate(lo_custkey, c_region->Probe("AMERICA"));
		combinator->AddPredicate(lo_suppkey, s_region->Probe("AMERICA"));
		std::vector<std::string> probe_set {"MFGR#1", "MFGR#2"};
		combinator->AddPredicate(lo_partkey, p_mfgr->ProbeSet(probe_set.data(), probe_set.size()));
		combinator->AddPredicate(lo_orderdate, d_year->ProbeRange(1997, 1998));
		return combinator->Execute(OUTPUT_SIZE);
	}

	template <typename CombinatorType>
	std::shared_ptr<OmniSketchCell> Q4_3() {
		auto combinator = std::make_unique<CombinatorType>();
		combinator->AddPredicate(lo_custkey, c_region->Probe("AMERICA"));
		combinator->AddPredicate(lo_suppkey, s_nation->Probe("UNITED STATES"));
		combinator->AddPredicate(lo_partkey, p_category->Probe("MFGR#14"));
		combinator->AddPredicate(lo_orderdate, d_year->ProbeRange(1997, 1998));
		return combinator->Execute(OUTPUT_SIZE);
	}

protected:
	std::shared_ptr<IntOmniSketch> lo_discount;
	std::shared_ptr<IntOmniSketch> lo_quantity;
	std::shared_ptr<IntOmniSketch> lo_orderdate;
	std::shared_ptr<IntOmniSketch> lo_partkey;
	std::shared_ptr<IntOmniSketch> lo_custkey;
	std::shared_ptr<IntOmniSketch> lo_suppkey;
	std::shared_ptr<IntOmniSketch> d_year;
	std::shared_ptr<IntOmniSketch> d_yearmonthnum;
	std::shared_ptr<IntOmniSketch> d_weeknuminyear;
	std::shared_ptr<StringOmniSketch> d_yearmonth;
	std::shared_ptr<StringOmniSketch> p_category;
	std::shared_ptr<StringOmniSketch> p_brand;
	std::shared_ptr<StringOmniSketch> p_mfgr;
	std::shared_ptr<StringOmniSketch> s_region;
	std::shared_ptr<StringOmniSketch> s_nation;
	std::shared_ptr<StringOmniSketch> s_city;
	std::shared_ptr<StringOmniSketch> c_region;
	std::shared_ptr<StringOmniSketch> c_nation;
	std::shared_ptr<StringOmniSketch> c_city;
};

BENCHMARK_DEFINE_F(SSBFixture, UncorrelatedCombination)(::benchmark::State &state) {
	auto query = static_cast<SSBQuery>(state.range());
	for (auto _ : state) {
		auto result = ExecuteQuery<UncorrelatedCombinator>(query);
		state.counters["Card"] = (double)GetCardinality(query);
		state.counters["Est"] = (double)result->RecordCount();
		state.counters["QErr"] = (double)result->RecordCount() / (double)GetCardinality(query);
	}
}

BENCHMARK_REGISTER_F(SSBFixture, UncorrelatedCombination)->DenseRange(0, 12, 1);

BENCHMARK_DEFINE_F(SSBFixture, ExhaustiveCombination)(::benchmark::State &state) {
	auto query = static_cast<SSBQuery>(state.range());
	for (auto _ : state) {
		auto result = ExecuteQuery<ExhaustiveCombinator>(query);
		state.counters["Card"] = (double)GetCardinality(query);
		state.counters["Est"] = (double)result->RecordCount();
		state.counters["QErr"] = (double)result->RecordCount() / (double)GetCardinality(query);
	}
}

BENCHMARK_REGISTER_F(SSBFixture, ExhaustiveCombination)->DenseRange(0, 12, 1);

BENCHMARK_MAIN();
