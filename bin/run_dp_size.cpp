#include <iostream>
#include <string>
#include <sstream>
#include <unordered_map>
#include <chrono>
#include <set>

#include "csv_importer.hpp"

void PrintUsage(const std::string &programName) {
	std::cout << "Usage: " << programName
	          << " --sketches=path/to/sketches --queries=query_file [--out=path/to/out_file] [--help]\n";
	std::cout << "Options:\n";
	std::cout << "  --sketches=path/to/sketches     Directory with JSON-serialized sketches\n";
	std::cout << "  --queries=query_file            File containing the query in OmniCpp-Format\n";
	std::cout << "  --out=path/to/out_file          Target file for results\n";
	std::cout << "  --help                          Display this help message\n";
}

int main(int argc, char *argv[]) {
	std::unordered_map<std::string, std::string> options;
	const std::string programName = argv[0];

	for (int i = 1; i < argc; ++i) {
		const std::string arg = argv[i];

		if (arg == "--help") {
			PrintUsage(programName);
			return 0;
		} else if (arg.rfind("--", 0) == 0) {
			const size_t eqPos = arg.find('=');
			if (eqPos != std::string::npos) {
				const std::string key = arg.substr(2, eqPos - 2);
				const std::string value = arg.substr(eqPos + 1);
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

	auto &registry = omnisketch::Registry::Get();
	registry.SetSketchDirectory(options["sketches"]);
	std::cout << "Estimated sketch size: " << registry.EstimateByteSize() << " B\n";

	omnisketch::CSVImporter csvImporter;
	auto queries = csvImporter.ImportQueries(options["queries"]);
	for (size_t i = 0; i < queries.size(); ++i) {
		std::cout << "##### Query " << i + 1 << " #####\n";
		const auto begin = std::chrono::steady_clock::now();
		auto dpSizeResults = queries[i].plan.RunDpSizeAlgo();
		const auto end = std::chrono::steady_clock::now();
		const auto duration = std::chrono::duration<double, std::milli>(end - begin);
		std::cout << "Total: " << duration.count() << " ms\n\n";
		std::cout << "## Details ##\n";
		std::cout << "relations,sql,card_est,duration_ns\n";

		std::set<std::string> visitedQueries;
		for (size_t j = 0; j < dpSizeResults.size(); ++j) {
			const auto &dpSizeResult = dpSizeResults[j];
			std::ostringstream sqlQuery;
			sqlQuery << "\"SELECT count(*) FROM ";
			size_t currentIdx = 0;

			for (const auto &relName : dpSizeResult.relations) {
				sqlQuery << relName;
				++currentIdx;
				if (currentIdx < dpSizeResult.relations.size()) {
					sqlQuery << ", ";
				}
			}

			std::set<std::string> conditions;
			for (const auto &relation : dpSizeResult.relations) {
				const auto &relInfo = queries[i].rel_info.at(relation);
				conditions.insert(relInfo.predicates.begin(), relInfo.predicates.end());
				for (const auto &joinCond : relInfo.join_conditions) {
					if (dpSizeResult.relations.find(joinCond.first) != dpSizeResult.relations.end()) {
						conditions.insert(joinCond.second);
					}
				}
			}

			sqlQuery << " WHERE ";
			currentIdx = 0;
			for (const auto &cond : conditions) {
				sqlQuery << cond;
				++currentIdx;
				if (currentIdx < conditions.size()) {
					sqlQuery << " AND ";
				}
			}

			const std::string sqlQueryStr = sqlQuery.str();
			if (visitedQueries.find(sqlQueryStr) == visitedQueries.end()) {
				visitedQueries.insert(sqlQueryStr);
				std::cout << dpSizeResult.relations.size() << "," << sqlQueryStr << ";\"," << std::fixed
				          << static_cast<size_t>(dpSizeResult.card_est) << "," << dpSizeResult.duration_ns << "\n";
			}

			if (j == dpSizeResults.size() - 1) {
				std::cout << "\n";
				queries[i].plan.RunDpSizeAlgo();
				std::cout << "|";
				std::cout << duration.count() << "|";
				std::cout << sqlQueryStr;
			}
		}

		std::cout << "\n\n";
	}

	return 0;
}
