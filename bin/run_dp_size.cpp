#include <iostream>
#include <string>
#include <unordered_map>

#include "csv_importer.hpp"

void print_usage(const std::string &program_name) {
	std::cout << "Usage: " << program_name
	          << " --sketches=path/to/sketches --queries=query_file [--out=path/to/out_file] [--help]\n";
	std::cout << "Options:\n";
	std::cout << "  --sketches=path/to/sketches     Directory with JSON-serialized sketches\n";
	std::cout << "  --queries=query_file            File containing the query in OmniCpp-Format\n";
	std::cout << "  --out=path/to/out_file          Target file for results\n";
	std::cout << "  --help                          Display this help message\n";
}

int main(int argc, char *argv[]) {
	std::unordered_map<std::string, std::string> options;
	std::string program_name = argv[0];

	for (int i = 1; i < argc; ++i) {
		std::string arg = argv[i];

		if (arg == "--help") {
			print_usage(program_name);
			return 0;
		} else if (arg.rfind("--", 0) == 0) {
			size_t eq_pos = arg.find('=');
			if (eq_pos != std::string::npos) {
				std::string key = arg.substr(2, eq_pos - 2);
				std::string value = arg.substr(eq_pos + 1);
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

	omnisketch::CSVImporter csv_importer;
	auto queries = csv_importer.ImportQueries(options["queries"]);
	for (size_t i = 0; i < queries.size(); i++) {
		std::cout << "##### Query " << i + 1 << " #####\n";
		auto begin = std::chrono::steady_clock::now();
		queries[i].plan.RunDpSizeAlgo();
		auto end = std::chrono::steady_clock::now();
		auto duration = std::chrono::duration<double, std::milli>(end - begin);
		std::cout << "Total: " << duration.count() << " ms\n\n";
	}

	return 0;
}
