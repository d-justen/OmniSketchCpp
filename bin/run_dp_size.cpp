#include <chrono>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>

#include "csv_importer.hpp"

void PrintUsage(const std::string& program_name) {
    std::cout << "Usage: " << program_name
              << " --sketches=path/to/sketches --queries=query_file [--out=path/to/out_file] [--help]\n";
    std::cout << "Options:\n";
    std::cout << "  --sketches=path/to/sketches     Directory with JSON-serialized sketches\n";
    std::cout << "  --queries=query_file            File containing the query in OmniCpp-Format\n";
    std::cout << "  --out=path/to/out_file          Target file for results\n";
    std::cout << "  --help                          Display this help message\n";
}

int main(int argc, char* argv[]) {
    std::unordered_map<std::string, std::string> options;
    const std::string program_name = argv[0];

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];

        if (arg == "--help") {
            PrintUsage(program_name);
            return 0;
        } else if (arg.rfind("--", 0) == 0) {
            const size_t eq_pos = arg.find('=');
            if (eq_pos != std::string::npos) {
                const std::string key = arg.substr(2, eq_pos - 2);
                const std::string value = arg.substr(eq_pos + 1);
                options[key] = value;
            } else {
                std::cerr << "Invalid option format: " << arg << std::endl;
                return 1;
            }
        } else {
            std::cerr << "Unrecognized argument: " << arg << std::endl;
            return 1;
        }
    }

    assert(options.find("sketches") != options.end());
    assert(options.find("queries") != options.end());

    auto& registry = omnisketch::Registry::Get();
    registry.SetSketchDirectory(options["sketches"]);
    std::cout << "Estimated sketch size: " << registry.EstimateByteSize() << " B\n";

    omnisketch::CSVImporter csvImporter;
    auto queries = csvImporter.ImportQueries(options["queries"]);
    for (size_t i = 0; i < queries.size(); ++i) {
        std::cout << "##### Query " << i + 1 << " #####\n";
        const auto begin = std::chrono::steady_clock::now();
        auto dp_size_results = queries[i].plan.RunDpSizeAlgo();
        const auto end = std::chrono::steady_clock::now();
        const auto duration = std::chrono::duration<double, std::milli>(end - begin);
        std::cout << "Total: " << duration.count() << " ms\n\n";
        std::cout << "## Details ##\n";
        std::cout << "relations,sql,card_est,duration_ns\n";

        std::set<std::string> visited_queries;
        for (size_t j = 0; j < dp_size_results.size(); ++j) {
            const auto& dp_size_result = dp_size_results[j];
            std::ostringstream sql_query;
            sql_query << "\"SELECT count(*) FROM ";
            size_t current_idx = 0;

            for (const auto& relName : dp_size_result.relations) {
                sql_query << relName;
                ++current_idx;
                if (current_idx < dp_size_result.relations.size()) {
                    sql_query << ", ";
                }
            }

            std::set<std::string> conditions;
            for (const auto& relation : dp_size_result.relations) {
                const auto& rel_info = queries[i].rel_info.at(relation);
                conditions.insert(rel_info.predicates.begin(), rel_info.predicates.end());
                for (const auto& join_cond : rel_info.join_conditions) {
                    if (dp_size_result.relations.find(join_cond.first) != dp_size_result.relations.end()) {
                        conditions.insert(join_cond.second);
                    }
                }
            }

            sql_query << " WHERE ";
            current_idx = 0;
            for (const auto& cond : conditions) {
                sql_query << cond;
                ++current_idx;
                if (current_idx < conditions.size()) {
                    sql_query << " AND ";
                }
            }

            const std::string sql_query_str = sql_query.str();
            if (visited_queries.find(sql_query_str) == visited_queries.end()) {
                visited_queries.insert(sql_query_str);
                std::cout << dp_size_result.relations.size() << "," << sql_query_str << ";\"," << std::fixed
                          << static_cast<size_t>(dp_size_result.card_est) << "," << dp_size_result.duration_ns << "\n";
            }

            if (j == dp_size_results.size() - 1) {
                std::cout << "\n";
                queries[i].plan.RunDpSizeAlgo();
                std::cout << "|";
                std::cout << duration.count() << "|";
                std::cout << sql_query_str;
            }
        }

        std::cout << "\n\n";
    }

    return 0;
}
