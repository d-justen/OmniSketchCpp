#pragma once

#include "execution/query_graph.hpp"
#include "omni_sketch/foreign_sorted_omni_sketch.hpp"
#include "omni_sketch/pre_joined_omni_sketch.hpp"
#include "registry.hpp"
#include "plan_generator.hpp"

#include <iostream>
#include <fstream>
#include <sstream>

namespace omnisketch {

enum class ColumnType { INT, UINT, DOUBLE, VARCHAR };

struct CountQuery {
	QueryGraph plan;
	size_t cardinality = 0;
};

class CSVImporter {
public:
	static void ImportTable(const std::string &path, const std::string &table_name,
	                        const std::vector<std::string> &column_names,
	                        const std::vector<std::string> &referencing_table_names,
	                        const std::vector<std::string> &referencing_column_names,
	                        const std::vector<ColumnType> &types, const OmniSketchConfig &config = OmniSketchConfig());
	static void ImportTable(const std::string &path, const std::string &table_name,
	                        const std::vector<std::string> &column_names,
	                        const std::vector<std::string> &referencing_table_names,
	                        const std::vector<std::string> &referencing_column_names,
	                        const std::vector<ColumnType> &types, std::vector<OmniSketchConfig> configs);
	static void ImportTables(const std::string &path_to_definition_file);
	static std::vector<CountQuery> ImportQueries(const std::string &path_to_query_file,
	                                             const std::shared_ptr<OmniSketchCombinator> &combinator,
	                                             bool use_ref_sketches);

	static std::pair<std::vector<std::string>, std::vector<std::string>>
	ExtractReferencingTables(const std::string &input);
	static std::vector<std::string> Split(const std::string &line, char sep = ',');

	template <typename T>
	static T ConvertString(const std::string &) {
		throw std::logic_error("Not implemented");
	}

	template <typename T>
	static std::function<void(const std::string &, const size_t)> CreateInsertFunc(const std::string &table_name,
	                                                                               const std::string &column_name) {
		auto &registry = Registry::Get();
		auto sketch = registry.GetOmniSketchTyped<T>(table_name, column_name);

		return [sketch](const std::string &val, const size_t rid) {
			if (val.empty()) {
				sketch->AddNullValues(1);
			} else {
				sketch->AddRecord(ConvertString<T>(val), rid);
			}
		};
	}

	template <typename T, typename U>
	static std::function<void(const std::string &, const size_t)>
	CreateInsertFunc(const std::string &table_name, const std::string &column_name,
	                 const std::vector<std::string> &ref_tbl_names) {
		auto &registry = Registry::Get();
		auto sketch = registry.GetOmniSketchTyped<T>(table_name, column_name);
		std::vector<std::shared_ptr<U>> ref_sketches;
		for (const auto &ref_tbl_name : ref_tbl_names) {
			auto ref_sketch = registry.FindReferencingOmniSketchTyped<U>(table_name, column_name, ref_tbl_name);
			assert(ref_sketch);
			ref_sketches.push_back(ref_sketch);
		}

		return [sketch, ref_sketches](const std::string &val, const size_t rid) {
			if (val.empty()) {
				sketch->AddNullValues(1);
				for (auto &rs : ref_sketches) {
					rs->AddNullValues(1);
				}
			}
			sketch->AddRecord(ConvertString<T>(val), rid);
			for (auto &rs : ref_sketches) {
				rs->AddRecord(ConvertString<T>(val), rid);
			}
		};
	}
};

} // namespace omnisketch
