#pragma once

#include "registry.hpp"

#include <iostream>
#include <fstream>
#include <sstream>

namespace omnisketch {

enum class ColumnType { INT, UINT, DOUBLE, VARCHAR };

class CSVImporter {
public:
	static std::vector<std::string> Split(const std::string &line) {
		std::vector<std::string> tokens;
		std::stringstream ss(line);
		std::string token;

		while (std::getline(ss, token, ',')) {
			tokens.push_back(token);
		}

		return tokens;
	}

	static void ReadFile(const std::string &path, const std::string &table_name,
	                     const std::vector<std::string> &column_names, std::vector<ColumnType> types,
	                     const OmniSketchConfig config = OmniSketchConfig(), bool create_rids = false,
	                     size_t column_offset = 0) {
		std::vector<OmniSketchConfig> configs(column_names.size(), config);
		ReadFile(path, table_name, column_names, types, configs, create_rids, column_offset);
	}

	static void ReadFile(const std::string &path, const std::string &table_name,
	                     const std::vector<std::string> &column_names, std::vector<ColumnType> types,
	                     std::vector<OmniSketchConfig> configs, bool create_rids = false, size_t column_offset = 0) {
		auto &registry = Registry::Get();
		assert(types.size() == column_names.size());
		assert(types.size() == configs.size());
		assert(types.size() > 1);
		assert(types.front() == ColumnType::UINT);
		std::cout << "Reading " << table_name << " table file..." << std::endl;

		std::vector<std::function<void(const std::string &, const size_t)>> insert_funcs;
		insert_funcs.reserve(column_names.size());

		for (size_t i = 0; i < column_names.size(); i++) {
			switch (types[i]) {
			case ColumnType::INT: {
				auto sketch = registry.CreateOmniSketch<int32_t>(table_name, column_names[i], configs[i]);
				insert_funcs.push_back(
				    [sketch](const std::string &val, const size_t rid) { sketch->AddRecord(std::stoi(val), rid); });
				break;
			}
			case ColumnType::UINT: {
				auto sketch = registry.CreateOmniSketch<size_t>(table_name, column_names[i], configs[i]);
				insert_funcs.push_back(
				    [sketch](const std::string &val, const size_t rid) { sketch->AddRecord(std::stoul(val), rid); });
				break;
			}
			case ColumnType::DOUBLE: {
				auto sketch = registry.CreateOmniSketch<double>(table_name, column_names[i], configs[i]);
				insert_funcs.push_back(
				    [sketch](const std::string &val, const size_t rid) { sketch->AddRecord(std::stod(val), rid); });
				break;
			}
			case ColumnType::VARCHAR: {
				auto sketch = registry.CreateOmniSketch<std::string>(table_name, column_names[i], configs[i]);
				insert_funcs.push_back(
				    [sketch](const std::string &val, const size_t rid) { sketch->AddRecord(val, rid); });
				break;
			}
			}
		}

		std::ifstream table_stream(path);
		std::string line;
		if (create_rids) {
			size_t id = 0;
			while (std::getline(table_stream, line)) {
				auto tokens = Split(line);
				id++;
				for (size_t i = 0; i < column_names.size(); i++) {
					insert_funcs[i](tokens[column_offset + i], id);
				}
			}
		} else {
			while (std::getline(table_stream, line)) {
				auto tokens = Split(line);
				for (size_t i = 1; i < column_names.size(); i++) {
					insert_funcs[i](tokens[column_offset + i], std::stoul(tokens[0]));
				}
			}
		}

		table_stream.close();
	}
};

} // namespace omnisketch
