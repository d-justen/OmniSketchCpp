#include <benchmark/benchmark.h>

#include "algorithm/expression.hpp"
#include "algorithm/join_operations.hpp"

#include <iostream>
#include <fstream>
#include <sstream>

using namespace omnisketch;
using IntOmniSketch = OmniSketch<size_t, size_t, std::set<uint64_t>>;
using StringOmniSketch = OmniSketch<std::string, size_t, std::set<uint64_t>>;

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
		lo_discount = std::make_shared<IntOmniSketch>(256, 3, 1 << 11);
		lo_quantity = std::make_shared<IntOmniSketch>(256, 3, 1 << 11);
		lo_orderdate = std::make_shared<IntOmniSketch>(256, 3, 1 << 11);
		lo_partkey = std::make_shared<IntOmniSketch>(256, 3, 1 << 11);
		lo_custkey = std::make_shared<IntOmniSketch>(256, 3, 1 << 11);
		lo_suppkey = std::make_shared<IntOmniSketch>(256, 3, 1 << 11);

		d_year = std::make_shared<IntOmniSketch>(256, 3, 1 << 11);
		d_yearmonthnum = std::make_shared<IntOmniSketch>(256, 3, 1 << 11);
		d_weeknuminyear = std::make_shared<IntOmniSketch>(256, 3, 1 << 11);
		d_yearmonth = std::make_shared<StringOmniSketch>(256, 3, 1 << 11);

		p_category = std::make_shared<StringOmniSketch>(256, 3, 1 << 11);
		p_brand = std::make_shared<StringOmniSketch>(256, 3, 1 << 11);
		p_mfgr = std::make_shared<StringOmniSketch>(256, 3, 1 << 11);

		s_region = std::make_shared<StringOmniSketch>(256, 3, 1 << 11);
		s_nation = std::make_shared<StringOmniSketch>(256, 3, 1 << 11);
		s_city = std::make_shared<StringOmniSketch>(256, 3, 1 << 11);

		c_region = std::make_shared<StringOmniSketch>(256, 3, 1 << 11);
		c_nation = std::make_shared<StringOmniSketch>(256, 3, 1 << 11);
		c_city = std::make_shared<StringOmniSketch>(256, 3, 1 << 11);

		std::cout << "Loading lineorder table..." << std::endl;
		std::ifstream lineorder("lineorder_f.csv");
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
		std::ifstream date("date_f.csv");
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
		std::ifstream part("part_f.csv");
		while (std::getline(part, line)) {
			auto tokens = Split(line);
			size_t id = std::stoul(tokens[0]);
			p_category->AddRecord(tokens[1], id);
			p_brand->AddRecord(tokens[2], id);
			p_mfgr->AddRecord(tokens[3], id);
		}
		part.close();

		std::cout << "Loading supplier table..." << std::endl;
		std::ifstream supplier("supplier_f.csv");
		while (std::getline(supplier, line)) {
			auto tokens = Split(line);
			size_t id = std::stoul(tokens[0]);
			s_region->AddRecord(tokens[1], id);
			s_nation->AddRecord(tokens[2], id);
			s_city->AddRecord(tokens[3], id);
		}
		supplier.close();

		std::cout << "Loading customer table..." << std::endl;
		std::ifstream customer("customer_f.csv");
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

	std::vector<double> Q1_1() {
		std::vector<double> q_errs;

		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		conditions.push_back(Join(*lo_orderdate, Equals(*d_year, (size_t)1993)));
		q_errs.push_back(conditions.back()->Cardinality() / 908288);
		conditions.push_back(Or(*lo_discount, {1, 2, 3}));
		q_errs.push_back(conditions.back()->Cardinality() / 1636870);
		conditions.push_back(Or(
		    *lo_quantity, {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25}));
		q_errs.push_back(conditions.back()->Cardinality() / 3000182);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 123856);
		return q_errs;
	}

	std::vector<double> Q1_2() {
		std::vector<double> q_errs;

		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		conditions.push_back(Join(*lo_orderdate, Equals(*d_yearmonthnum, (size_t)199401)));
		q_errs.push_back(conditions.back()->Cardinality() / 78532);
		conditions.push_back(Or(*lo_discount, {4, 5, 6}));
		q_errs.push_back(conditions.back()->Cardinality() / 1636834);
		conditions.push_back(Or(*lo_quantity, {26, 27, 28, 29, 30, 31, 32, 33, 34, 35}));
		q_errs.push_back(conditions.back()->Cardinality() / 1200596);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 4251);
		return q_errs;
	}

	std::vector<double> Q1_3() {
		std::vector<double> q_errs;

		std::vector<std::unique_ptr<ExpressionResult>> join_conds;
		join_conds.push_back(Equals(*d_year, (size_t)1994));
		join_conds.push_back(Equals(*d_weeknuminyear, (size_t)6));

		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		auto and_expr = And<uint64_t>(join_conds);
		conditions.push_back(Join(*lo_orderdate, and_expr));
		q_errs.push_back(conditions.back()->Cardinality() / 17317);
		conditions.push_back(Or(*lo_discount, {5, 6, 7}));
		q_errs.push_back(conditions.back()->Cardinality() / 1637846);
		conditions.push_back(Or(*lo_quantity, {36, 37, 38, 39, 40}));
		q_errs.push_back(conditions.back()->Cardinality() / 599734);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 470);
		return q_errs;
	}

	std::vector<double> Q2_1() {
		std::vector<double> q_errs;
		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		// TODO: Join in date even if it doesn't do anything. Requires filterless OmniSketch intersection
		conditions.push_back(Join(*lo_partkey, Equals(*p_category, std::string("MFGR#12"))));
		q_errs.push_back(conditions.back()->Cardinality() / 242241);
		conditions.push_back(Join(*lo_suppkey, Equals(*s_region, std::string("AMERICA"))));
		q_errs.push_back(conditions.back()->Cardinality() / 1134621);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 46026);
		return q_errs;
	}

	std::vector<double> Q2_2() {
		std::vector<double> q_errs;
		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		// TODO: Join in date even if it doesn't do anything. Requires filterless OmniSketch intersection
		conditions.push_back(Join(
		    *lo_partkey, Or(*p_brand, std::vector<std::string> {"MFGR#2221", "MFGR#2222", "MFGR#2223", "MFGR#2224",
		                                                        "MFGR#2225", "MFGR#2226", "MFGR#2227", "MFGR#2228"})));
		q_errs.push_back(conditions.back()->Cardinality() / 47481);
		conditions.push_back(Join(*lo_suppkey, Equals(*s_region, std::string("ASIA"))));
		q_errs.push_back(conditions.back()->Cardinality() / 1347093);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 10577);
		return q_errs;
	}

	std::vector<double> Q2_3() {
		std::vector<double> q_errs;

		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		// TODO: Join in date even if it doesn't do anything. Requires filterless OmniSketch intersection
		conditions.push_back(Join(*lo_partkey, Equals(*p_brand, std::string("MFGR#2339"))));
		q_errs.push_back(conditions.back()->Cardinality() / 6330);
		conditions.push_back(Join(*lo_suppkey, Equals(*s_region, std::string("EUROPE"))));
		q_errs.push_back(conditions.back()->Cardinality() / 1140656);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 1222);
		return q_errs;
	}

	std::vector<double> Q3_1() {
		std::vector<double> q_errs;

		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		conditions.push_back(Join(*lo_custkey, Equals(*c_region, std::string("ASIA"))));
		q_errs.push_back(conditions.back()->Cardinality() / 1207529);
		conditions.push_back(Join(*lo_suppkey, Equals(*s_region, std::string("ASIA"))));
		q_errs.push_back(conditions.back()->Cardinality() / 1347093);
		conditions.push_back(
		    Join(*lo_orderdate, Or(*d_year, std::vector<size_t> {1992, 1993, 1994, 1995, 1996, 1997})));
		q_errs.push_back(conditions.back()->Cardinality() / 5466180);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 246821);
		return q_errs;
	}

	std::vector<double> Q3_2() {
		std::vector<double> q_errs;

		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		conditions.push_back(Join(*lo_custkey, Equals(*c_nation, std::string("UNITED STATES"))));
		q_errs.push_back(conditions.back()->Cardinality() / 246734);
		conditions.push_back(Join(*lo_suppkey, Equals(*s_nation, std::string("UNITED STATES"))));
		q_errs.push_back(conditions.back()->Cardinality() / 228297);

		conditions.push_back(
		    Join(*lo_orderdate, Or(*d_year, std::vector<size_t> {1992, 1993, 1994, 1995, 1996, 1997})));
		q_errs.push_back(conditions.back()->Cardinality() / 5466180);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 8606);
		return q_errs;
	}

	std::vector<double> Q3_3() {
		std::vector<double> q_errs;

		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		conditions.push_back(Join(*lo_custkey, Or(*c_city, std::vector<std::string> {"UNITED KI1", "UNITED KI5"})));
		q_errs.push_back(conditions.back()->Cardinality() / 41743);
		conditions.push_back(Join(*lo_suppkey, Or(*s_city, std::vector<std::string> {"UNITED KI1", "UNITED KI5"})));
		q_errs.push_back(conditions.back()->Cardinality() / 53874);
		conditions.push_back(
		    Join(*lo_orderdate, Or(*d_year, std::vector<size_t> {1992, 1993, 1994, 1995, 1996, 1997})));
		q_errs.push_back(conditions.back()->Cardinality() / 5466180);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 339);
		return q_errs;
	}

	std::vector<double> Q3_4() {
		std::vector<double> q_errs;

		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		conditions.push_back(Join(*lo_custkey, Or(*c_city, std::vector<std::string> {"UNITED KI1", "UNITED KI5"})));
		q_errs.push_back(conditions.back()->Cardinality() / 41743);
		conditions.push_back(Join(*lo_suppkey, Or(*s_city, std::vector<std::string> {"UNITED KI1", "UNITED KI5"})));
		q_errs.push_back(conditions.back()->Cardinality() / 53874);
		conditions.push_back(Join(*lo_orderdate, Equals(*d_yearmonth, std::string("Dec1997"))));
		q_errs.push_back(conditions.back()->Cardinality() / 77451);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 5);
		return q_errs;
	}

	std::vector<double> Q4_1() {
		std::vector<double> q_errs;

		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		// TODO: Join in date even if it doesn't do anything. Requires filterless OmniSketch intersection
		conditions.push_back(Join(*lo_custkey, Equals(*c_region, std::string("AMERICA"))));
		q_errs.push_back(conditions.back()->Cardinality() / 1192672);
		conditions.push_back(Join(*lo_suppkey, Equals(*s_region, std::string("AMERICA"))));
		q_errs.push_back(conditions.back()->Cardinality() / 1134621);
		conditions.push_back(Join(*lo_partkey, Or(*p_mfgr, std::vector<std::string> {"MFGR#1", "MFGR#2"})));
		q_errs.push_back(conditions.back()->Cardinality() / 2391945);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 90353);
		return q_errs;
	}

	std::vector<double> Q4_2() {
		std::vector<double> q_errs;

		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		conditions.push_back(Join(*lo_custkey, Equals(*c_region, std::string("AMERICA"))));
		q_errs.push_back(conditions.back()->Cardinality() / 1192672);
		conditions.push_back(Join(*lo_suppkey, Equals(*s_region, std::string("AMERICA"))));
		q_errs.push_back(conditions.back()->Cardinality() / 1134621);
		conditions.push_back(Join(*lo_partkey, Or(*p_mfgr, std::vector<std::string> {"MFGR#1", "MFGR#2"})));
		q_errs.push_back(conditions.back()->Cardinality() / 2391945);
		conditions.push_back(Join(*lo_orderdate, Or(*d_year, std::vector<size_t> {1997, 1998})));
		q_errs.push_back(conditions.back()->Cardinality() / 1444989);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 21803);
		return q_errs;
	}

	std::vector<double> Q4_3() {
		std::vector<double> q_errs;

		std::vector<std::unique_ptr<ExpressionResult>> conditions;
		conditions.push_back(Join(*lo_custkey, Equals(*c_region, std::string("AMERICA"))));
		q_errs.push_back(conditions.back()->Cardinality() / 1192672);
		conditions.push_back(Join(*lo_suppkey, Equals(*s_nation, std::string("UNITED STATES"))));
		q_errs.push_back(conditions.back()->Cardinality() / 228297);
		conditions.push_back(Join(*lo_partkey, Equals(*p_category, std::string("MFGR#14"))));
		q_errs.push_back(conditions.back()->Cardinality() / 240848);
		conditions.push_back(Join(*lo_orderdate, Or(*d_year, std::vector<size_t> {1997, 1998})));
		q_errs.push_back(conditions.back()->Cardinality() / 1444989);
		q_errs.push_back(And<uint64_t>(conditions)->Cardinality() / 447);
		return q_errs;
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

BENCHMARK_F(SSBFixture, Query1_1)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q1_1();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 123856;
	}
}

BENCHMARK_F(SSBFixture, Query1_2)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q1_2();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 4251;
	}
}

BENCHMARK_F(SSBFixture, Query1_3)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q1_3();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 470;
	}
}

BENCHMARK_F(SSBFixture, Query2_1)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q2_1();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 46026;
	}
}

BENCHMARK_F(SSBFixture, Query2_2)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q2_2();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 10577;
	}
}

BENCHMARK_F(SSBFixture, Query2_3)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q2_3();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 1222;
	}
}

BENCHMARK_F(SSBFixture, Query3_1)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q3_1();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 246821;
	}
}

BENCHMARK_F(SSBFixture, Query3_2)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q3_2();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 8606;
	}
}

BENCHMARK_F(SSBFixture, Query3_3)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q3_3();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 329;
	}
}

BENCHMARK_F(SSBFixture, Query3_4)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q3_4();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 5;
	}
}

BENCHMARK_F(SSBFixture, Query4_1)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q4_1();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 90353;
	}
}

BENCHMARK_F(SSBFixture, Query4_2)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q4_2();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 21803;
	}
}

BENCHMARK_F(SSBFixture, Query4_3)(::benchmark::State &state) {
	for (auto _ : state) {
		auto card_ests = Q4_3();
		for (size_t i = 0; i < card_ests.size(); i++) {
			state.counters["QErr" + std::to_string(i)] = card_ests[i];
		}
		state.counters["CardEst"] = card_ests.back() * 447;
	}
}

BENCHMARK_MAIN();
