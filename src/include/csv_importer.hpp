#pragma once

#include "registry.hpp"
#include "plan_generator.hpp"

#include <iostream>
#include <fstream>
#include <sstream>

namespace omnisketch {

enum class ColumnType { INT, UINT, DOUBLE, VARCHAR };

struct CountQuery {
	PlanGenerator plan;
	size_t cardinality;
};

class CSVImporter {
public:
	static void ImportTable(const std::string &path, const std::string &table_name,
	                     const std::vector<std::string> &column_names, std::vector<ColumnType> types,
	                     OmniSketchConfig config = OmniSketchConfig());
	static void ImportTable(const std::string &path, const std::string &table_name,
	                     const std::vector<std::string> &column_names, std::vector<ColumnType> types,
	                     std::vector<OmniSketchConfig> configs);
	static void ImportTables(const std::string& path_to_definition_file);
	static std::vector<CountQuery> ImportQueries(const std::string& path_to_query_file, const std::shared_ptr<OmniSketchCombinator>& combinator);
protected:
	static std::vector<std::string> Split(const std::string &line, char sep = ',');
};

} // namespace omnisketch
