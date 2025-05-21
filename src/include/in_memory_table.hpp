#pragma once

#include "csv_importer.hpp"

namespace omnisketch {

class AllTypeColumn {
public:
	virtual ~AllTypeColumn() = default;
	virtual void IngestDataRegular() = 0;
	virtual void IngestDataSecondary(const std::string &ref_table_name, const std::string &ref_column_name) = 0;
	virtual void LoadValue(const std::string &v) = 0;
};

template <typename T>
class InMemoryColumn : public AllTypeColumn {
public:
	InMemoryColumn(const std::string &table_name_p, const std::string &column_name_p, const OmniSketchConfig &config_p,
	               std::shared_ptr<std::vector<size_t>> primary_keys_p)
	    : table_name(table_name_p), column_name(column_name_p), config(config_p),
	      primary_keys(std::move(primary_keys_p)) {
		std::cout << typeid(T).name() << std::endl;
	}

	void IngestDataRegular() override {
		auto &registry = Registry::Get();
		auto sketch = registry.CreateOmniSketch<T>(table_name, column_name, config);
		auto &pk_ref = *primary_keys;

		for (size_t i = 0; i < values.size(); i++) {
			sketch->AddRecord(values[i], pk_ref[i]);
		}
	}

	void IngestDataSecondary(const std::string &ref_table_name, const std::string &ref_column_name) override {
		auto &registry = Registry::Get();
		auto sketch = registry.CreateExtendingOmniSketch<PreJoinedOmniSketch<T>, T>(
		    table_name, column_name, ref_table_name, ref_column_name, config);
		auto &pk_ref = *primary_keys;

		for (size_t i = 0; i < values.size(); i++) {
			sketch->AddRecord(values[i], pk_ref[i]);
		}
	}

	void LoadValue(const std::string &v) override {
		values.push_back(CSVImporter::ConvertString<T>(v));
	}

	std::string table_name;
	std::string column_name;
	OmniSketchConfig config;
	std::vector<T> values;
	std::shared_ptr<std::vector<size_t>> primary_keys;
};

class InMemoryTable {
public:
	InMemoryTable() = default;

	InMemoryTable(std::string path_p, const std::string &table_name, const std::vector<ColumnType> &types,
	              const std::vector<std::string> &names, const std::vector<OmniSketchConfig> &configs)
	    : path(std::move(path_p)) {
		primary_key_column = std::make_shared<std::vector<size_t>>();
		columns.reserve(types.size() - 1);
		for (size_t i = 1; i < types.size(); i++) {
			switch (types[i]) {
			case ColumnType::INT:
				columns.push_back(
				    std::make_unique<InMemoryColumn<int32_t>>(table_name, names[i], configs[i], primary_key_column));
				break;
			case ColumnType::UINT:
				columns.push_back(
				    std::make_unique<InMemoryColumn<size_t>>(table_name, names[i], configs[i], primary_key_column));
				break;
			case ColumnType::DOUBLE:
				columns.push_back(
				    std::make_unique<InMemoryColumn<double>>(table_name, names[i], configs[i], primary_key_column));
				break;
			case ColumnType::VARCHAR:
				columns.push_back(std::make_unique<InMemoryColumn<std::string>>(table_name, names[i], configs[i],
				                                                                primary_key_column));
				break;
			}
		}
	}

	void FillTable() {
		std::ifstream table_stream(path);
		std::string line;
		auto &pk_ref = *primary_key_column;
		while (std::getline(table_stream, line)) {
			auto tokens = CSVImporter::Split(line);
			pk_ref.push_back(std::stoul(tokens[0]));
			for (size_t i = 0; i < columns.size(); i++) {
				columns[i]->LoadValue(tokens[i + 1]);
			}
		}

		table_stream.close();
	}

	std::string path;
	std::shared_ptr<std::vector<size_t>> primary_key_column;
	std::vector<std::unique_ptr<AllTypeColumn>> columns;
};

} // namespace omnisketch
