#include <benchmark/benchmark.h>

#include "in_memory_table.hpp"
#include "registry.hpp"

class SSBIngestionFixture : public benchmark::Fixture {
public:
	void SetUp(::benchmark::State &) override {
		omnisketch::OmniSketchConfig lineorder_config;
		lineorder_config.SetWidth(512);
		lineorder_config.sample_count = 2048;

		lineorder = omnisketch::InMemoryTable(
		    "../data/ssb/lineorder_f.csv", "lineorder",
		    std::vector<omnisketch::ColumnType>(7, omnisketch::ColumnType::UINT),
		    {"id", "lo_discount", "lo_quantity", "lo_orderdate", "lo_partkey", "lo_custkey", "lo_suppkey"},
		    std::vector<omnisketch::OmniSketchConfig>(7, lineorder_config));

		omnisketch::OmniSketchConfig dimension_config;
		dimension_config.SetWidth(128);
		dimension_config.sample_count = 512;

		date = omnisketch::InMemoryTable("../data/ssb/date_f.csv", "date",
		                                 {omnisketch::ColumnType::UINT, omnisketch::ColumnType::UINT,
		                                  omnisketch::ColumnType::UINT, omnisketch::ColumnType::VARCHAR},
		                                 {"d_datekey", "d_year", "d_yearmonthnum", "d_weeknuminyear", "d_yearmonth"},
		                                 std::vector<omnisketch::OmniSketchConfig>(5, dimension_config));
		customer = omnisketch::InMemoryTable("../data/ssb/customer_f.csv", "customer",
		                                 {omnisketch::ColumnType::UINT, omnisketch::ColumnType::VARCHAR,
		                                  omnisketch::ColumnType::VARCHAR, omnisketch::ColumnType::VARCHAR},
		                                 {"c_custkey", "c_region", "c_nation", "c_city"},
		                                 std::vector<omnisketch::OmniSketchConfig>(4, dimension_config));
		supplier = omnisketch::InMemoryTable("../data/ssb/supplier_f.csv", "supplier",
											 {omnisketch::ColumnType::UINT, omnisketch::ColumnType::VARCHAR,
											  omnisketch::ColumnType::VARCHAR, omnisketch::ColumnType::VARCHAR},
											 {"s_suppkey", "s_region", "s_nation", "s_city"},
											 std::vector<omnisketch::OmniSketchConfig>(4, dimension_config));
		part = omnisketch::InMemoryTable("../data/ssb/part_f.csv", "part",
		                                 {omnisketch::ColumnType::UINT, omnisketch::ColumnType::VARCHAR,
		                                  omnisketch::ColumnType::VARCHAR, omnisketch::ColumnType::VARCHAR},
		                                 {"p_partkey", "p_category", "p_brand", "p_mfgr"},
		                                 std::vector<omnisketch::OmniSketchConfig>(4, dimension_config));
		std::cout << "fill lo..." << std::endl;
		lineorder.FillTable();
		std::cout << "fill cu..." << std::endl;
		customer.FillTable();
		std::cout << "fill su..." << std::endl;
		supplier.FillTable();
		std::cout << "fill pa..." << std::endl;
		part.FillTable();
		std::cout << "fill da..." << std::endl;
		date.FillTable();
	}

	omnisketch::InMemoryTable lineorder;
	omnisketch::InMemoryTable date;
	omnisketch::InMemoryTable customer;
	omnisketch::InMemoryTable supplier;
	omnisketch::InMemoryTable part;
};

BENCHMARK_F(SSBIngestionFixture, IngestWithSecondarySketches)(benchmark::State& state) {
	for (auto _ : state) {
		for (const auto& column : lineorder.columns) {
			std::cout << "ingest lo..." << std::endl;
			column->IngestDataRegular();
		}
		for (const auto& column : customer.columns) {
			std::cout << "ingest cu..." << std::endl;
			column->IngestDataRegular();
			column->IngestDataSecondary("lineorder", "lo_custkey");
		}
		for (const auto& column : supplier.columns) {
			std::cout << "ingest su..." << std::endl;
			column->IngestDataRegular();
			column->IngestDataSecondary("lineorder", "lo_suppkey");
		}
		for (const auto& column : part.columns) {
			std::cout << "ingest pa regular..." << std::endl;
			column->IngestDataRegular();
			std::cout << "ingest pa secondary..." << std::endl;
			column->IngestDataSecondary("lineorder", "lo_partkey");
		}
		for (const auto& column : date.columns) {
			std::cout << "ingest da..." << std::endl;
			column->IngestDataRegular();
			column->IngestDataSecondary("lineorder", "lo_orderdate");
		}
	}
}

BENCHMARK_MAIN();
