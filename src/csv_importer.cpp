#include "csv_importer.hpp"

#include "execution/plan_node.hpp"
#include "execution/query_graph.hpp"
#include "registry.hpp"

namespace omnisketch {

void CSVImporter::ImportTable(const std::string& path, const std::string& table_name,
                              const std::vector<std::string>& column_names,
                              const std::vector<std::string>& referencing_table_names,
                              const std::vector<std::string>& referencing_column_names,
                              const std::vector<ColumnType>& types, const OmniSketchConfig& config) {
    std::vector<OmniSketchConfig> configs(column_names.size(), config);
    ImportTable(path, table_name, column_names, referencing_table_names, referencing_column_names, types,
                std::move(configs));
}

template <typename T>
std::function<void(const std::string&, const size_t)> CreateExtendingSketchAndFunc(
    const std::string& table_name, const std::string& column_name,
    const std::vector<std::string>& referencing_table_names, const std::vector<std::string>& referencing_column_names,
    const OmniSketchConfig& config) {
    auto& registry = Registry::Get();

    registry.CreateOmniSketch<T>(table_name, column_name, config);
    if (!config.referencing_type) {
        return CSVImporter::CreateInsertFunc<T>(table_name, column_name);
    }

    if (*config.referencing_type == OmniSketchType::PRE_JOINED) {
        for (size_t i = 0; i < referencing_table_names.size(); ++i) {
            registry.CreateExtendingOmniSketch<PreJoinedOmniSketch<T>, T>(
                table_name, column_name, referencing_table_names[i], referencing_column_names[i], config);
        }
        return CSVImporter::CreateInsertFunc<T, PreJoinedOmniSketch<T>>(table_name, column_name,
                                                                        referencing_table_names);
    }

    throw std::runtime_error("Referencing type not supported.");
}

bool ReadLogicalLine(std::ifstream& in, std::string& line_out) {
    line_out.clear();
    bool escaping = false;

    char c;
    while (in.get(c)) {
        if (escaping) {
            line_out += '\\';  // preserve the backslash
            line_out += c;     // preserve escaped char (e.g., 'n')
            escaping = false;
        } else if (c == '\\') {
            escaping = true;
        } else if (c == '\n') {
            // Unescaped newline: end of logical line
            return true;
        } else {
            line_out += c;
        }
    }

    // If we reached EOF and accumulated content, return the final line
    return !line_out.empty();
}

void CSVImporter::ImportTable(const std::string& path, const std::string& table_name,
                              const std::vector<std::string>& column_names,
                              const std::vector<std::string>& referencing_table_names,
                              const std::vector<std::string>& referencing_column_names,
                              const std::vector<ColumnType>& types, std::vector<OmniSketchConfig> configs) {
    assert(types.size() == column_names.size());
    assert(types.size() == configs.size());
    assert(types.size() > 1);
    assert(types.front() == ColumnType::UINT);
    std::cout << "Reading " << table_name << " table file..." << std::endl;

    std::vector<std::function<void(const std::string&, const size_t)>> insert_funcs;
    insert_funcs.reserve(column_names.size());

    auto rids = Registry::Get().CreateRidSketch(table_name, configs.front().sample_count);
    insert_funcs.emplace_back(
        [&rids](const std::string&, const size_t rid) { rids->AddRecord(hash_functions::MurmurHash64(rid)); });

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
    while (ReadLogicalLine(table_stream, line)) {
        auto tokens = Split(line);
        size_t rid = std::stoul(tokens[0]);
        insert_funcs.front()({}, rid);
        for (size_t i = 1; i < column_names.size(); i++) {
            insert_funcs[i](tokens[i], rid);
        }
    }

    table_stream.close();
    for (size_t i = 1; i < column_names.size(); i++) {
        Registry::Get().GetOmniSketch(table_name, column_names[i])->Flatten();
    }
}

std::pair<std::vector<std::string>, std::vector<std::string>> CSVImporter::ExtractReferencingTables(
    const std::string& input) {
    if (input.empty()) {
        return {};
    }
    const auto referencing_table_identifiers = Split(input);
    std::vector<std::string> referencing_table_names;
    std::vector<std::string> referencing_column_names;
    referencing_table_names.reserve(referencing_table_identifiers.size());
    referencing_column_names.reserve(referencing_column_names.size());
    for (auto& id : referencing_table_identifiers) {
        auto table_compound = Split(id, '.');
        assert(table_compound.size() == 2);
        referencing_table_names.push_back(table_compound[0]);
        referencing_column_names.push_back(table_compound[1]);
    }
    return {referencing_table_names, referencing_column_names};
}

void CSVImporter::ImportTables(const std::string& path_to_definition_file) {
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
        for (const auto& val : vals) {
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
            if (tokens[3] == "P") {
                reference_type = std::make_shared<OmniSketchType>(OmniSketchType::PRE_JOINED);
            } else {
                throw std::runtime_error("Foreign sorted is deprecated and not supported anymore.");
            }
        }

        OmniSketchConfig config;
        config.SetWidth(width);
        config.sample_count = max_sample_count;
        config.referencing_type = reference_type;
        ImportTable(data_path, table_name, column_names, referencing_table_ids.first, referencing_table_ids.second,
                    data_types, config);
    }
}

std::shared_ptr<OmniSketchCell> ConvertPoint(const std::string& table_name, const std::string& column_name,
                                             const std::string& val) {
    auto& registry = Registry::Get();
    auto sketch = registry.GetOmniSketch(table_name, column_name);
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

std::shared_ptr<OmniSketchCell> ConvertRange(const std::string& table_name, const std::string& column_name,
                                             const std::string& lb, const std::string& ub, bool lb_excl = true,
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

std::shared_ptr<OmniSketchCell> ConvertSet(const std::string& table_name, const std::string& column_name,
                                           const std::vector<std::string>& vals) {
    auto sketch = Registry::Get().GetOmniSketch(table_name, column_name);
    if (std::dynamic_pointer_cast<TypedPointOmniSketch<int32_t>>(sketch)) {
        std::vector<int32_t> v;
        v.reserve(vals.size());
        for (const auto& val : vals) {
            v.push_back(std::stoi(val));
        }
        return PredicateConverter::ConvertSet<int32_t>(v);
    }
    if (std::dynamic_pointer_cast<TypedPointOmniSketch<size_t>>(sketch)) {
        std::vector<size_t> v;
        v.reserve(vals.size());
        for (const auto& val : vals) {
            v.push_back(std::stoul(val));
        }
        return PredicateConverter::ConvertSet<size_t>(v);
    }
    if (std::dynamic_pointer_cast<TypedPointOmniSketch<double>>(sketch)) {
        std::vector<double> v;
        v.reserve(vals.size());
        for (const auto& val : vals) {
            v.push_back(std::stod(val));
        }
        return PredicateConverter::ConvertSet<double>(v);
    }
    if (std::dynamic_pointer_cast<TypedPointOmniSketch<std::string>>(sketch)) {
        return PredicateConverter::ConvertSet<std::string>(vals);
    }
    throw std::logic_error("Data type not supported");
}

bool IsStringColumn(Registry& registry, const std::string& table_name, const std::string& column_name) {
    return (std::dynamic_pointer_cast<TypedPointOmniSketch<std::string>>(
                registry.GetOmniSketch(table_name, column_name)) != nullptr);
}

std::vector<CountQuery> CSVImporter::ImportQueries(const std::string& path_to_query_file) {
    std::ifstream query_stream(path_to_query_file);
    if (!query_stream.is_open()) {
        throw std::runtime_error("Failed to open query file: " + path_to_query_file);
    }

    std::string line;
    std::vector<CountQuery> queries;

    while (std::getline(query_stream, line)) {
        queries.push_back(ParseSingleQuery(line));
    }

    return queries;
}

CountQuery CSVImporter::ParseSingleQuery(const std::string& line) {
    auto vals = Split(line, '|');
    if (vals.size() != 3) {
        throw std::runtime_error("Invalid query format: expected 3 parts separated by '|'");
    }

    CountQuery query;
    query.plan = QueryGraph();
    query.cardinality = std::stoul(vals[2]);

    ProcessJoins(vals[0], query);
    ProcessPredicates(vals[1], query);

    return query;
}

void CSVImporter::ProcessJoins(const std::string& join_string, CountQuery& query) {
    const auto joins = Split(join_string);
    for (const auto& join : joins) {
        const auto join_sides = Split(join, '=');
        if (join_sides.size() != 2) {
            throw std::runtime_error("Invalid join format: " + join);
        }

        const auto left_side = Split(join_sides[0], '.');
        const auto right_side = Split(join_sides[1], '.');

        if (left_side.size() != 2 || right_side.size() != 2) {
            throw std::runtime_error("Invalid table.column format in join: " + join);
        }

        const bool left_is_fk_side = Registry::Get().HasOmniSketch(left_side[0], left_side[1]);
        const bool right_is_fk_side = Registry::Get().HasOmniSketch(right_side[0], right_side[1]);

        if (left_is_fk_side && !right_is_fk_side) {
            query.plan.AddPkFkJoin(left_side[0], left_side[1], right_side[0]);
        } else if (right_is_fk_side && !left_is_fk_side) {
            query.plan.AddPkFkJoin(right_side[0], right_side[1], left_side[0]);
        } else {
            query.plan.AddFkFkJoin(left_side[0], left_side[1], right_side[0], right_side[1]);
        }

        query.rel_info[left_side[0]].join_conditions[right_side[0]] = join;
        query.rel_info[right_side[0]].join_conditions[left_side[0]] = join;
    }
}

void CSVImporter::ProcessPredicates(const std::string& predicate_string, CountQuery& query) {
    const auto predicates = Split(predicate_string);
    if (predicates.size() % 3 != 0) {
        throw std::runtime_error("Invalid predicate format: predicates must be in groups of 3");
    }

    std::unordered_map<std::string, std::pair<std::string, bool>> gt_predicates;
    std::unordered_map<std::string, std::pair<std::string, bool>> lt_predicates;

    for (size_t i = 0; i < predicates.size(); i += 3) {
        ProcessSinglePredicate(predicates[i], predicates[i + 1], predicates[i + 2], query, gt_predicates, lt_predicates);
    }

    ProcessRemainingRangePredicates(gt_predicates, lt_predicates, query);
}

void CSVImporter::ProcessSinglePredicate(const std::string& column_id, const std::string& operand,
                                         const std::string& value, CountQuery& query,
                                         std::unordered_map<std::string, std::pair<std::string, bool>>& gt_predicates,
                                         std::unordered_map<std::string, std::pair<std::string, bool>>& lt_predicates) {
    const auto column_parts = Split(column_id, '.');
    if (column_parts.size() != 2) {
        throw std::runtime_error("Invalid column format: " + column_id);
    }

    const auto& table_name = column_parts[0];
    const auto& column_name = column_parts[1];
    const bool is_string_column = IsStringColumn(Registry::Get(), table_name, column_name);

    // Add to relation info for display purposes
    if (operand != "E") {
        std::ostringstream predicate_stream;
        predicate_stream << table_name << "." << column_name << " " << operand << " ";
        if (is_string_column) {
            predicate_stream << "'" << value << "'";
        } else {
            predicate_stream << value;
        }
        query.rel_info[table_name].predicates.emplace_back(predicate_stream.str());
    }

    // Process the predicate based on operand type
    if (operand == "=") {
        query.plan.AddConstantPredicate(table_name, column_name, ConvertPoint(table_name, column_name, value));
    } else if (operand == ">" || operand == ">=") {
        ProcessGreaterThanPredicate(column_id, operand, value, table_name, column_name, query, gt_predicates, lt_predicates);
    } else if (operand == "<" || operand == "<=") {
        ProcessLessThanPredicate(column_id, operand, value, table_name, column_name, query, gt_predicates, lt_predicates);
    } else if (operand == "E") {
        ProcessInPredicate(table_name, column_name, value, is_string_column, query);
    } else {
        throw std::logic_error("Unknown operand: " + operand);
    }
}

void CSVImporter::ProcessGreaterThanPredicate(
    const std::string& column_id, const std::string& operand, const std::string& value, const std::string& table_name,
    const std::string& column_name, CountQuery& query,
    std::unordered_map<std::string, std::pair<std::string, bool>>& gt_predicates,
    std::unordered_map<std::string, std::pair<std::string, bool>>& lt_predicates) {
    const bool lower_bound_exclusive = (operand == ">");
    auto lt_it = lt_predicates.find(column_id);

    if (lt_it != lt_predicates.end()) {
        // We have both bounds, create range predicate
        const auto& lt_predicate = lt_it->second;
        query.plan.AddConstantPredicate(
            table_name, column_name,
            ConvertRange(table_name, column_name, value, lt_predicate.first, lower_bound_exclusive, lt_predicate.second));
        lt_predicates.erase(lt_it);
    } else {
        // Store for potential later combination
        gt_predicates[column_id] = std::make_pair(value, lower_bound_exclusive);
    }
}

void CSVImporter::ProcessLessThanPredicate(
    const std::string& column_id, const std::string& operand, const std::string& value, const std::string& table_name,
    const std::string& column_name, CountQuery& query,
    std::unordered_map<std::string, std::pair<std::string, bool>>& gt_predicates,
    std::unordered_map<std::string, std::pair<std::string, bool>>& lt_predicates) {
    const bool upper_bound_exclusive = (operand == "<");
    auto gt_it = gt_predicates.find(column_id);

    if (gt_it != gt_predicates.end()) {
        // We have both bounds, create range predicate
        const auto& gt_predicate = gt_it->second;
        query.plan.AddConstantPredicate(
            table_name, column_name,
            ConvertRange(table_name, column_name, gt_predicate.first, value, gt_predicate.second, upper_bound_exclusive));
        gt_predicates.erase(gt_it);
    } else {
        // Store for potential later combination
        lt_predicates[column_id] = std::make_pair(value, upper_bound_exclusive);
    }
}

void CSVImporter::ProcessInPredicate(const std::string& table_name, const std::string& column_name,
                                     const std::string& value, bool is_string_column, CountQuery& query) {
    const auto values = Split(value, ';');

    // Use string stream for more efficient string concatenation
    std::ostringstream concat_stream;
    concat_stream << "(";

    for (size_t i = 0; i < values.size(); ++i) {
        if (is_string_column) {
            concat_stream << "'" << values[i] << "'";
        } else {
            concat_stream << values[i];
        }
        if (i < values.size() - 1) {
            concat_stream << ", ";
        }
    }
    concat_stream << ")";

    query.rel_info[table_name].predicates.emplace_back(table_name + "." + column_name + " IN " + concat_stream.str());
    query.plan.AddConstantPredicate(table_name, column_name, ConvertSet(table_name, column_name, values));
}

void CSVImporter::ProcessRemainingRangePredicates(
    const std::unordered_map<std::string, std::pair<std::string, bool>>& gt_predicates,
    const std::unordered_map<std::string, std::pair<std::string, bool>>& lt_predicates, CountQuery& query) {
    // Process remaining greater-than predicates (no upper bound)
    for (const auto& gt_predicate : gt_predicates) {
        const auto table_column = Split(gt_predicate.first, '.');
        query.plan.AddConstantPredicate(table_column[0], table_column[1],
                                        ConvertRange(table_column[0], table_column[1], gt_predicate.second.first, "",
                                                     gt_predicate.second.second, false));
    }

    // Process remaining less-than predicates (no lower bound)
    for (const auto& lt_predicate : lt_predicates) {
        const auto table_column = Split(lt_predicate.first, '.');
        query.plan.AddConstantPredicate(table_column[0], table_column[1],
                                        ConvertRange(table_column[0], table_column[1], "", lt_predicate.second.first,
                                                     false, lt_predicate.second.second));
    }
}

std::vector<std::string> CSVImporter::Split(const std::string& line, char sep) {
    if (line.empty()) {
        return {};
    }

    std::vector<std::string> tokens;
    tokens.reserve(8);  // Reserve space for common case to avoid reallocations

    std::string token;
    token.reserve(64);  // Reserve space to avoid frequent reallocations

    bool in_escape = false;

    for (char c : line) {
        if (c == '"') {
            if (!line.empty() && line.back() == '"') {
                // Double escape
                token += c;
            }
            in_escape = !in_escape;
        } else if (c == sep) {
            if (in_escape) {
                token += c;
            } else {
                tokens.emplace_back(std::move(token));
                token.clear();
                token.reserve(64);  // Re-reserve after move
            }
        } else {
            token += c;
        }
    }
    tokens.emplace_back(std::move(token));

    return tokens;
}

template <>
std::string CSVImporter::ConvertString<std::string>(const std::string& in) {
    return in;
}

template <>
int32_t CSVImporter::ConvertString<int32_t>(const std::string& in) {
    return std::stoi(in);
}

template <>
size_t CSVImporter::ConvertString<size_t>(const std::string& in) {
    return std::stoul(in);
}

template <>
double CSVImporter::ConvertString<double>(const std::string& in) {
    return std::stod(in);
}

template <>
uint64_t CSVImporter::ConvertString<uint64_t>(const std::string& in) {
    return std::stoull(in);
}

}  // namespace omnisketch
