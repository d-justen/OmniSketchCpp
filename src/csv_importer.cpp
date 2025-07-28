#include "csv_importer.hpp"

#include "registry.hpp"

namespace omnisketch {

void CSVImporter::ImportTable(const std::string &path, const std::string &table_name,
                              const std::vector<std::string> &column_names,
                              const std::vector<std::string> &referencing_table_names,
                              const std::vector<std::string> &referencing_column_names,
                              const std::vector<ColumnType> &types, const OmniSketchConfig &config) {
	std::vector<OmniSketchConfig> configs(column_names.size(), config);
	ImportTable(path, table_name, column_names, referencing_table_names, referencing_column_names, types, configs);
}

template <typename T>
std::function<void(const std::string &, const size_t)>
CreateExtendingSketchAndFunc(const std::string &table_name, const std::string &column_name,
                             const std::vector<std::string> &referencing_table_names,
                             const std::vector<std::string> &referencing_column_names, const OmniSketchConfig &config) {
	auto &registry = Registry::Get();

	registry.CreateOmniSketch<T>(table_name, column_name, config);
	if (!config.referencing_type) {
		return CSVImporter::CreateInsertFunc<T>(table_name, column_name);
	} else {
		assert(*config.referencing_type == OmniSketchType::PRE_JOINED);
		for (size_t i = 0; i < referencing_table_names.size(); i++) {
			registry.CreateExtendingOmniSketch<PreJoinedOmniSketch<T>, T>(
			    table_name, column_name, referencing_table_names[i], referencing_column_names[i], config);
		}
		return CSVImporter::CreateInsertFunc<T, PreJoinedOmniSketch<T>>(table_name, column_name,
		                                                                referencing_table_names);
	}
}

void CSVImporter::ImportTable(const std::string &path, const std::string &table_name,
                              const std::vector<std::string> &column_names,
                              const std::vector<std::string> &referencing_table_names,
                              const std::vector<std::string> &referencing_column_names,
                              const std::vector<ColumnType> &types, std::vector<OmniSketchConfig> configs) {
	assert(types.size() == column_names.size());
	assert(types.size() == configs.size());
	assert(types.size() > 1);
	assert(types.front() == ColumnType::UINT);
	std::cout << "Reading " << table_name << " table file..." << std::endl;

	std::vector<std::function<void(const std::string &, const size_t)>> insert_funcs;
	insert_funcs.reserve(column_names.size());

	for (size_t i = 1; i < column_names.size(); i++) {
		switch (types[i]) {
		case ColumnType::INT: {
			insert_funcs.emplace_back(CreateExtendingSketchAndFunc<int32_t>(
			    table_name, column_names[i], referencing_table_names, referencing_column_names, configs[i]));
			break;
		}
		case ColumnType::UINT: {
			insert_funcs.emplace_back(CreateExtendingSketchAndFunc<size_t>(
			    table_name, column_names[i], referencing_table_names, referencing_column_names, configs[i]));
			break;
		}
		case ColumnType::DOUBLE: {
			insert_funcs.emplace_back(CreateExtendingSketchAndFunc<double>(
			    table_name, column_names[i], referencing_table_names, referencing_column_names, configs[i]));
			break;
		}
		case ColumnType::VARCHAR: {
			insert_funcs.emplace_back(CreateExtendingSketchAndFunc<std::string>(
			    table_name, column_names[i], referencing_table_names, referencing_column_names, configs[i]));
			break;
		}
		}
	}

	std::ifstream table_stream(path);
	std::string line;
	while (std::getline(table_stream, line)) {
		auto tokens = Split(line);
		for (size_t i = 0; i < column_names.size() - 1; i++) {
			insert_funcs[i](tokens[i + 1], std::stoul(tokens[0]));
		}
	}

	table_stream.close();
	for (size_t i = 1; i < column_names.size(); i++) {
		Registry::Get().GetOmniSketch(table_name, column_names[i])->Flatten();
		for (auto &referencing_tbl_name : referencing_table_names) {
			Registry::Get().FindReferencingOmniSketch(table_name, column_names[i], referencing_tbl_name)->Flatten();
		}
		// TODO: What about referencing OmniSketches??
	}
}

std::pair<std::vector<std::string>, std::vector<std::string>>
CSVImporter::ExtractReferencingTables(const std::string &input) {
	if (input.empty()) {
		return {};
	}
	const auto referencing_table_identifiers = Split(input);
	std::vector<std::string> referencing_table_names;
	std::vector<std::string> referencing_column_names;
	referencing_table_names.reserve(referencing_table_identifiers.size());
	referencing_column_names.reserve(referencing_column_names.size());
	for (auto &id : referencing_table_identifiers) {
		auto tbl_compound = Split(id, '.');
		assert(tbl_compound.size() == 2);
		referencing_table_names.push_back(tbl_compound[0]);
		referencing_column_names.push_back(tbl_compound[1]);
	}
	return {referencing_table_names, referencing_column_names};
}

void CSVImporter::ImportTables(const std::string &path_to_definition_file) {
	std::ifstream table_stream(path_to_definition_file);
	std::string line;

	while (std::getline(table_stream, line)) {
		auto tokens = Split(line, '|');
		assert(tokens.size() == 3 || tokens.size() == 5);
		auto vals = Split(tokens[0]);
		const auto data_path = vals[0];
		const size_t width = std::stoul(vals[1]);
		const size_t max_sample_count = std::stoul(vals[2]);
		const auto table_name = vals[3];

		const auto column_names = Split(tokens[1]);
		vals = Split(tokens[2]);
		assert(vals.size() == column_names.size());

		std::vector<ColumnType> data_types;
		data_types.reserve(vals.size());
		for (const auto &val : vals) {
			if (val == "int") {
				data_types.push_back(ColumnType::INT);
			} else if (val == "uint") {
				data_types.push_back(ColumnType::UINT);
			} else if (val == "double") {
				data_types.push_back(ColumnType::DOUBLE);
			} else if (val == "varchar") {
				data_types.push_back(ColumnType::VARCHAR);
			} else {
				throw std::logic_error("Unkown data type.");
			}
		}

		std::pair<std::vector<std::string>, std::vector<std::string>> referencing_table_ids;
		std::shared_ptr<OmniSketchType> reference_type;
		if (tokens.size() == 5) {
			referencing_table_ids = ExtractReferencingTables(tokens[4]);
		} else {
			assert(tokens[3] == "P");
			reference_type = std::make_shared<OmniSketchType>(OmniSketchType::PRE_JOINED);
		}

		OmniSketchConfig config;
		config.SetWidth(width);
		config.sample_count = max_sample_count;
		config.referencing_type = reference_type;
		ImportTable(data_path, table_name, column_names, referencing_table_ids.first, referencing_table_ids.second,
		            data_types, config);
	}
}

std::shared_ptr<OmniSketchCell> ConvertPoint(const std::string &table_name, const std::string &column_name,
                                             const std::string &val) {
	auto sketch = Registry::Get().GetOmniSketch(table_name, column_name);
	if (std::dynamic_pointer_cast<TypedPointOmniSketch<int32_t>>(sketch)) {
		return PredicateConverter::ConvertPoint<int32_t>(std::stoi(val));
	}
	if (std::dynamic_pointer_cast<TypedPointOmniSketch<size_t>>(sketch)) {
		return PredicateConverter::ConvertPoint<size_t>(std::stoul(val));
	}
	if (std::dynamic_pointer_cast<TypedPointOmniSketch<double>>(sketch)) {
		return PredicateConverter::ConvertPoint<double>(std::stod(val));
	}
	if (std::dynamic_pointer_cast<TypedPointOmniSketch<std::string>>(sketch)) {
		return PredicateConverter::ConvertPoint<std::string>(val);
	}
	throw std::logic_error("Data type not supported");
}

std::shared_ptr<OmniSketchCell> ConvertRange(const std::string &table_name, const std::string &column_name,
                                             const std::string &lb, const std::string &ub, bool lb_excl = true,
                                             bool ub_excl = true) {
	auto sketch = Registry::Get().GetOmniSketch(table_name, column_name);
	if (std::dynamic_pointer_cast<TypedPointOmniSketch<int32_t>>(sketch)) {
		int32_t lower, upper;
		if (lb.empty()) {
			lower = std::dynamic_pointer_cast<TypedPointOmniSketch<int32_t>>(sketch)->GetMin();
		} else {
			lower = std::stoi(lb);
		}
		lower = lb_excl ? lower + 1 : lower;
		if (ub.empty()) {
			upper = std::dynamic_pointer_cast<TypedPointOmniSketch<int32_t>>(sketch)->GetMax();
		} else {
			upper = std::stoi(ub);
		}
		upper = ub_excl ? upper - 1 : upper;
		return PredicateConverter::ConvertRange<int32_t>(lower, upper);
	}
	if (std::dynamic_pointer_cast<TypedPointOmniSketch<size_t>>(sketch)) {
		size_t lower, upper;
		if (lb.empty()) {
			lower = std::dynamic_pointer_cast<TypedPointOmniSketch<size_t>>(sketch)->GetMin();
		} else {
			lower = std::stoi(lb);
		}
		lower = lb_excl ? lower + 1 : lower;
		if (ub.empty()) {
			upper = std::dynamic_pointer_cast<TypedPointOmniSketch<size_t>>(sketch)->GetMax();
		} else {
			upper = std::stoi(ub);
		}
		upper = ub_excl ? upper - 1 : upper;
		return PredicateConverter::ConvertRange<size_t>(lower, upper);
	}
	throw std::logic_error("Data type not supported");
}

std::shared_ptr<OmniSketchCell> ConvertSet(const std::string &table_name, const std::string &column_name,
                                           const std::vector<std::string> &vals) {
	auto sketch = Registry::Get().GetOmniSketch(table_name, column_name);
	if (std::dynamic_pointer_cast<TypedPointOmniSketch<int32_t>>(sketch)) {
		std::vector<int32_t> v;
		v.reserve(vals.size());
		for (const auto &val : vals) {
			v.push_back(std::stoi(val));
		}
		return PredicateConverter::ConvertSet<int32_t>(v);
	}
	if (std::dynamic_pointer_cast<TypedPointOmniSketch<size_t>>(sketch)) {
		std::vector<size_t> v;
		v.reserve(vals.size());
		for (const auto &val : vals) {
			v.push_back(std::stoul(val));
		}
		return PredicateConverter::ConvertSet<size_t>(v);
	}
	if (std::dynamic_pointer_cast<TypedPointOmniSketch<double>>(sketch)) {
		std::vector<double> v;
		v.reserve(vals.size());
		for (const auto &val : vals) {
			v.push_back(std::stod(val));
		}
		return PredicateConverter::ConvertSet<double>(v);
	}
	if (std::dynamic_pointer_cast<TypedPointOmniSketch<std::string>>(sketch)) {
		return PredicateConverter::ConvertSet<std::string>(vals);
	}
	throw std::logic_error("Data type not supported");
}

std::vector<CountQuery> CSVImporter::ImportQueries(const std::string &path_to_query_file,
                                                   bool use_ref_sketches) {
	std::ifstream query_stream(path_to_query_file);
	std::string line;

	std::vector<CountQuery> queries;

	while (std::getline(query_stream, line)) {
		auto vals = Split(line, '|');
		assert(vals.size() == 3);
		CountQuery query;
		query.plan = PlanGenerator(use_ref_sketches);
		query.cardinality = std::stoul(vals[2]);

		const auto joins = Split(vals[0]);
		for (auto &join : joins) {
			const auto join_sides = Split(join, '=');
			assert(join_sides.size() == 2);
			const auto left_side = Split(join_sides[0], '.');
			const auto right_side = Split(join_sides[1], '.');
			assert(left_side.size() == 2 && right_side.size() == 2);
			const bool left_is_fk_side = Registry::Get().HasOmniSketch(left_side[0], left_side[1]);
			const bool right_is_fk_side = Registry::Get().HasOmniSketch(right_side[0], right_side[1]);
			assert((left_is_fk_side && !right_is_fk_side) || (!left_is_fk_side && right_is_fk_side));
			if (left_is_fk_side) {
				query.plan.AddJoin(left_side[0], left_side[1], right_side[0]);
			} else {
				query.plan.AddJoin(right_side[0], right_side[1], left_side[0]);
			}
		}

		const auto predicates = Split(vals[1]);
		assert(predicates.size() % 3 == 0);

		std::unordered_map<std::string, std::pair<std::string, bool>> gt_predicates;
		std::unordered_map<std::string, std::pair<std::string, bool>> lt_predicates;

		for (size_t predicate_idx = 0; predicate_idx < predicates.size(); predicate_idx += 3) {
			const auto &column_id = predicates[predicate_idx];
			assert(Split(predicates[predicate_idx], '.').size() == 2);
			const auto table_name = Split(predicates[predicate_idx], '.')[0];
			const auto column_name = Split(predicates[predicate_idx], '.')[1];
			const auto &operand = predicates[predicate_idx + 1];
			const auto &value = predicates[predicate_idx + 2];

			if (operand == "=") {
				query.plan.AddPredicate(table_name, column_name, ConvertPoint(table_name, column_name, value));
			} else if (operand == ">" || operand == ">=") {
				const bool lb_excl = operand == ">";
				if (lt_predicates.find(column_id) != lt_predicates.end()) {
					const auto &lt_predicate = lt_predicates[column_id];
					query.plan.AddPredicate(
					    table_name, column_name,
					    ConvertRange(table_name, column_name, value, lt_predicate.first, lb_excl, lt_predicate.second));
					lt_predicates.erase(column_id);
				} else {
					gt_predicates[column_id] = std::make_pair(value, lb_excl);
				}
			} else if (operand == "<" || operand == "<=") {
				const bool ub_excl = operand == ">";
				if (gt_predicates.find(column_id) != gt_predicates.end()) {
					auto &gt_predicate = gt_predicates[column_id];
					query.plan.AddPredicate(
					    table_name, column_name,
					    ConvertRange(table_name, column_name, gt_predicate.first, value, gt_predicate.second, ub_excl));
					gt_predicates.erase(column_id);
				} else {
					lt_predicates[column_id] = std::make_pair(value, ub_excl);
				}
			} else if (operand == "E") {
				const auto values = Split(value, ';');
				query.plan.AddPredicate(table_name, column_name, ConvertSet(table_name, column_name, values));
			} else {
				throw std::logic_error("Unknown operand");
			}
		}

		for (auto &gt_predicate : gt_predicates) {
			const auto table_column = Split(gt_predicate.first, '.');
			query.plan.AddPredicate(table_column[0], table_column[1],
			                        ConvertRange(table_column[0], table_column[1], gt_predicate.second.first, "",
			                                     gt_predicate.second.second, false));
		}

		for (auto &lt_predicate : lt_predicates) {
			const auto table_column = Split(lt_predicate.first, '.');
			query.plan.AddPredicate(table_column[0], table_column[1],
			                        ConvertRange(table_column[0], table_column[1], "", lt_predicate.second.first, false,
			                                     lt_predicate.second.second));
		}

		queries.push_back(query);
	}
	return queries;
}

std::vector<std::string> CSVImporter::Split(const std::string &line, char sep) {
	if (line.empty()) {
		return {};
	}
	std::vector<std::string> tokens;
	std::stringstream ss(line);
	std::string token;

	while (std::getline(ss, token, sep)) {
		tokens.push_back(token);
	}

	return tokens;
}

template <>
std::string CSVImporter::ConvertString<std::string>(const std::string &in) {
	return in;
}

template <>
int32_t CSVImporter::ConvertString<int32_t>(const std::string &in) {
	return std::stoi(in);
}

template <>
size_t CSVImporter::ConvertString<size_t>(const std::string &in) {
	return std::stoul(in);
}

template <>
double CSVImporter::ConvertString<double>(const std::string &in) {
	return std::stod(in);
}

template <>
uint64_t CSVImporter::ConvertString<uint64_t>(const std::string &in) {
	return std::stoull(in);
}

} // namespace omnisketch
