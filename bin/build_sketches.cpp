#include <iostream>
#include <string>
#include <unordered_map>

#include "csv_importer.hpp"

constexpr size_t kDefaultWidth = 16;
constexpr size_t kDefaultDepth = 3;
constexpr size_t kDefaultCellSize = 32;

void PrintUsage(const std::string& programName) {
    std::cout << "Usage: " << programName
              << " --in=some_file.csv --table_name=some_name --column_names=col1,..,coln --data_types=uint,..,varchar "
                 "--out=some/path [--width=16] [--depth=3] [--cell_size=32] [--ref_sketch=some_sketch.json] [--help]\n";
    std::cout << "Options:\n";
    std::cout << "  --in=some_file.csv              Location of the table CSV file\n";
    std::cout << "  --table_name=some_name          Table name\n";
    std::cout << "  --column_names=col1,..,coln     Comma-separated column names\n";
    std::cout
        << "  --data_types=uint,..,varchar    Comma-separated data types: allowed are uint, int, double, and varchar\n";
    std::cout << "  --out=some/path                 Target location for sketches as JSON\n";
    std::cout << "  --width=16                      OmniSketch width\n";
    std::cout << "  --depth=3                       OmniSketch depth\n";
    std::cout << "  --cell_size=32                  Min-Hash Sketch size per cell\n";
    std::cout << "  --ref_sketch=some_sketch.json   Location of OmniSketch to be referenced\n";
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

    // TODO: Validate parameters
    const std::vector<std::string> column_names = omnisketch::CSVImporter::Split(options["column_names"]);
    const std::vector<std::string> data_types_str = omnisketch::CSVImporter::Split(options["data_types"]);
    std::vector<omnisketch::ColumnType> data_types;
    data_types.reserve(data_types_str.size());

    for (const auto& data_type_str : data_types_str) {
        if (data_type_str == "uint") {
            data_types.emplace_back(omnisketch::ColumnType::UINT);
        } else if (data_type_str == "int") {
            data_types.emplace_back(omnisketch::ColumnType::INT);
        } else if (data_type_str == "double") {
            data_types.emplace_back(omnisketch::ColumnType::DOUBLE);
        } else if (data_type_str == "varchar") {
            data_types.emplace_back(omnisketch::ColumnType::VARCHAR);
        }
    }

    omnisketch::OmniSketchConfig config;
    if (options.find("width") != options.end()) {
        config.SetWidth(std::stoul(options["width"]));
    } else {
        config.SetWidth(kDefaultWidth);
    }

    if (options.find("depth") != options.end()) {
        config.depth = std::stoul(options["depth"]);
    } else {
        config.depth = kDefaultDepth;
    }

    if (options.find("cell_size") != options.end()) {
        config.sample_count = std::stoul(options["cell_size"]);
    } else {
        config.sample_count = kDefaultCellSize;
    }

    auto& registry = omnisketch::Registry::Get();

    std::string referencing_table_name;
    if (options.find("ref_sketch") != options.end()) {
        config.referencing_type = std::make_shared<omnisketch::OmniSketchType>(omnisketch::OmniSketchType::PRE_JOINED);
        registry.Deserialize(options["ref_sketch"]);
        nlohmann::json jsonObj;
        std::ifstream file;
        file.open(options["ref_sketch"]);
        file >> jsonObj;
        file.close();
        referencing_table_name = jsonObj["table_name"];
        const std::string referencingColumnName = jsonObj["column_name"];

        omnisketch::CSVImporter::ImportTable(options["in"], options["table_name"], column_names, {referencing_table_name},
                                             {referencingColumnName}, data_types, config);
    } else {
        omnisketch::CSVImporter::ImportTable(options["in"], options["table_name"], column_names, {}, {}, data_types,
                                             config);
    }

    for (size_t i = 1; i < column_names.size(); ++i) {
        const auto& columnName = column_names[i];
        std::string outputFilePath = options["out"] + "/";
        outputFilePath += options["table_name"] + "__" + columnName;
        if (!referencing_table_name.empty()) {
            outputFilePath += "__" + referencing_table_name;
        }
        outputFilePath += ".json";
        omnisketch::Registry::Serialize(options["table_name"], columnName, referencing_table_name, outputFilePath);
    }

    omnisketch::Registry::Serialize(options["table_name"], {}, {},
                                    options["out"] + "/" + options["table_name"] + "__RIDS.json");

    return 0;
}
