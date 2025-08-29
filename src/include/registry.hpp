#pragma once

#include <dirent.h>
#include <fstream>
#include <iostream>
#include <variant>

#include "json/json.hpp"
#include "omni_sketch/pre_joined_omni_sketch.hpp"
#include "omni_sketch/standard_omni_sketch.hpp"

namespace omnisketch {

struct OmniSketchConfig {
    void SetWidth(size_t width_p) {
        width = width_p;
        hash_processor = std::make_shared<BarrettModSplitHashMapper>(width);
    }

    size_t width = 256;
    size_t depth = 3;
    size_t sample_count = 64;
    std::shared_ptr<SetMembershipAlgorithm> set_membership_algo = std::make_shared<ProbeAllSum>();
    std::shared_ptr<CellIdxMapper> hash_processor = std::make_shared<BarrettModSplitHashMapper>(width);
    std::shared_ptr<OmniSketchType> referencing_type;
};

struct OmniSketchEntry {
    std::shared_ptr<PointOmniSketch> main_sketch;
    std::unordered_map<std::string, std::shared_ptr<PointOmniSketch>> referencing_sketches;
};

using TableEntry = std::unordered_map<std::string, OmniSketchEntry>;

class Registry {
public:
    Registry(Registry const&) = delete;
    void operator=(Registry const&) = delete;
    static Registry& Get();

    template <typename T>
    std::shared_ptr<TypedPointOmniSketch<T>> CreateOmniSketch(const std::string& table_name,
                                                              const std::string& column_name,
                                                              const OmniSketchConfig& config = OmniSketchConfig{}) {
        assert(!HasOmniSketch(table_name, column_name));
        std::shared_ptr<PointOmniSketch> sketch = std::make_shared<TypedPointOmniSketch<T>>(
            config.width, config.depth, config.sample_count, std::make_shared<MurmurHashFunction<T>>(),
            config.set_membership_algo, config.hash_processor);
        sketches[table_name][column_name] = OmniSketchEntry{sketch, {}};
        return std::dynamic_pointer_cast<TypedPointOmniSketch<T>>(sketch);
    }

    std::shared_ptr<OmniSketchCell> CreateRidSketch(const std::string& table_name, size_t size) {
        rid_sketches[table_name] = std::make_shared<OmniSketchCell>(size);
        return rid_sketches[table_name];
    }

    std::shared_ptr<OmniSketchCell> GetRidSample(const std::string& table_name) {
        return rid_sketches[table_name];
    }

    template <typename T, typename U>
    std::shared_ptr<T> CreateExtendingOmniSketch(const std::string& table_name, const std::string& column_name,
                                                 const std::string& referencing_table_name,
                                                 const std::string& referencing_column_name,
                                                 const OmniSketchConfig& config = OmniSketchConfig{}) {
        assert(HasOmniSketch(table_name, column_name));
        auto& entry = sketches[table_name][column_name];

        std::shared_ptr<PointOmniSketch> sketch =
            std::make_shared<T>(GetOmniSketch(referencing_table_name, referencing_column_name), config.width,
                                config.depth, config.sample_count, std::make_shared<MurmurHashFunction<U>>(),
                                config.set_membership_algo, config.hash_processor);
        entry.referencing_sketches[referencing_table_name] = sketch;

        return std::dynamic_pointer_cast<T>(sketch);
    }

    template <typename T>
    std::shared_ptr<TypedPointOmniSketch<T>> GetOmniSketchTyped(const std::string& table_name,
                                                                const std::string& column_name) {
        return std::dynamic_pointer_cast<TypedPointOmniSketch<T>>(GetOmniSketch(table_name, column_name));
    }

    std::shared_ptr<PointOmniSketch> GetOmniSketch(const std::string& table_name, const std::string& column_name) {
        assert(HasOmniSketch(table_name, column_name));
        return sketches[table_name][column_name].main_sketch;
    }

    template <typename T>
    std::shared_ptr<T> FindReferencingOmniSketchTyped(const std::string& table_name, const std::string& column_name,
                                                      const std::string& referencing_table_name) {
        return std::dynamic_pointer_cast<T>(FindReferencingOmniSketch(table_name, column_name, referencing_table_name));
    }

    std::shared_ptr<PointOmniSketch> FindReferencingOmniSketch(const std::string& table_name,
                                                               const std::string& column_name,
                                                               const std::string& referencing_table_name) {
        assert(HasOmniSketch(table_name, column_name));

        auto entry = sketches[table_name][column_name];
        if (entry.referencing_sketches.find(referencing_table_name) != entry.referencing_sketches.end()) {
            return entry.referencing_sketches[referencing_table_name];
        }
        return nullptr;
    }

    bool HasOmniSketch(const std::string& table_name, const std::string& column_name) {
        auto table_entry = sketches.find(table_name);
        return table_entry != sketches.end() && table_entry->second.find(column_name) != table_entry->second.end();
    }

    std::shared_ptr<OmniSketchCell> ProduceRidSample(const std::string& table_name) {
        assert(sketches.find(table_name) != sketches.end());
        return sketches[table_name].begin()->second.main_sketch->GetRids();
    }

    std::shared_ptr<OmniSketchCell> TryProduceReferencingRidSample(const std::string& table_name,
                                                                   const std::string& referencing_table_name) {
        auto entry = sketches.find(table_name);
        assert(entry != sketches.end());
        if (!entry->second.empty()) {
            const auto& referencing_sketches = entry->second.begin()->second.referencing_sketches;
            auto referencing_sketch = referencing_sketches.find(referencing_table_name);
            if (referencing_sketch != referencing_sketches.end()) {
                return referencing_sketch->second->GetRids();
            }
        }
        return nullptr;
    }

    size_t GetBaseTableCard(const std::string& table_name) const {
        const auto table_entry_it = sketches.find(table_name);
        assert(table_entry_it != sketches.end());
        assert(!table_entry_it->second.empty());
        return table_entry_it->second.begin()->second.main_sketch->RecordCount();
    }

    size_t GetMinHashSketchSize(const std::string& table_name) const {
        const auto table_entry_it = sketches.find(table_name);
        assert(table_entry_it != sketches.end());
        assert(!table_entry_it->second.empty());
        return table_entry_it->second.begin()->second.main_sketch->MinHashSketchSize();
    }

    size_t EstimateByteSize() const {
        size_t result = 0;
        for (auto& table : sketches) {
            for (auto& column : table.second) {
                result += column.second.main_sketch->EstimateByteSize();
                for (auto& ref_sketch : column.second.referencing_sketches) {
                    result += ref_sketch.second->EstimateByteSize();
                }
            }
        }
        for (auto& rid_sketch : rid_sketches) {
            result += rid_sketch.second->EstimateByteSize();
        }
        return result;
    }

    static void Serialize(const std::string& table_name, const std::string& column_name,
                          const std::string& referencing_table_name, const std::string& path) {
        auto& registry = Registry::Get();
        nlohmann::json json_obj;

        if (column_name.empty()) {
            nlohmann::json mhs_obj = nlohmann::json::array();
            auto cell = registry.rid_sketches[table_name];
            for (auto it = cell->GetMinHashSketch()->Iterator(); !it->IsAtEnd(); it->Next()) {
                mhs_obj.push_back(it->Current());
            }
            json_obj["table_name"] = table_name;
            json_obj["type"] = "rid_sample";
            json_obj["hashes"] = mhs_obj;
            json_obj["max_sample_count"] = cell->MaxSampleCount();
            json_obj["record_count"] = cell->RecordCount();
            std::ofstream file;
            file.open(path);
            file << json_obj;
            file.close();
            return;
        }

        std::shared_ptr<PointOmniSketch> sketch;
        if (referencing_table_name.empty()) {
            sketch = registry.GetOmniSketch(table_name, column_name);
            json_obj["type"] = "standard";
        } else {
            sketch = registry.FindReferencingOmniSketch(table_name, column_name, referencing_table_name);
            json_obj["type"] = "prejoined";
        }

        json_obj["table_name"] = table_name;
        json_obj["column_name"] = column_name;
        json_obj["width"] = sketch->Width();
        json_obj["depth"] = sketch->Depth();
        json_obj["min_hash_sketch_size"] = sketch->MinHashSketchSize();
        json_obj["record_count"] = sketch->RecordCount();

        nlohmann::json rows;
        for (size_t i = 0; i < sketch->Depth(); i++) {
            nlohmann::json row_obj;
            for (size_t j = 0; j < sketch->Width(); j++) {
                auto& cell = sketch->GetCell(i, j);
                nlohmann::json cell_obj;
                cell_obj["record_count"] = cell.RecordCount();
                cell_obj["max_sample_count"] = cell.MaxSampleCount();
                nlohmann::json mhs_obj = nlohmann::json::array();
                for (auto it = cell.GetMinHashSketch()->Iterator(); !it->IsAtEnd(); it->Next()) {
                    mhs_obj.push_back(it->Current());
                }
                cell_obj["hashes"] = mhs_obj;
                row_obj.emplace_back(cell_obj);
            }
            rows.push_back(row_obj);
        }
        json_obj["rows"] = rows;

        if (std::dynamic_pointer_cast<TypedPointOmniSketch<size_t>>(sketch)) {
            json_obj["data_type"] = "uint";
            json_obj["min"] = std::dynamic_pointer_cast<TypedPointOmniSketch<size_t>>(sketch)->GetMin();
            json_obj["max"] = std::dynamic_pointer_cast<TypedPointOmniSketch<size_t>>(sketch)->GetMax();
        }
        if (std::dynamic_pointer_cast<TypedPointOmniSketch<double>>(sketch)) {
            json_obj["data_type"] = "double";
            json_obj["min"] = std::dynamic_pointer_cast<TypedPointOmniSketch<double>>(sketch)->GetMin();
            json_obj["max"] = std::dynamic_pointer_cast<TypedPointOmniSketch<double>>(sketch)->GetMax();
        }
        if (std::dynamic_pointer_cast<TypedPointOmniSketch<int32_t>>(sketch)) {
            json_obj["data_type"] = "int";
            json_obj["min"] = std::dynamic_pointer_cast<TypedPointOmniSketch<int32_t>>(sketch)->GetMin();
            json_obj["max"] = std::dynamic_pointer_cast<TypedPointOmniSketch<int32_t>>(sketch)->GetMax();
        }
        if (std::dynamic_pointer_cast<TypedPointOmniSketch<std::string>>(sketch)) {
            json_obj["data_type"] = "varchar";
            json_obj["min"] = std::dynamic_pointer_cast<TypedPointOmniSketch<std::string>>(sketch)->GetMin();
            json_obj["max"] = std::dynamic_pointer_cast<TypedPointOmniSketch<std::string>>(sketch)->GetMax();
        }
        if (std::dynamic_pointer_cast<PreJoinedOmniSketch<size_t>>(sketch)) {
            json_obj["data_type"] = "uint";
            json_obj["min"] = std::dynamic_pointer_cast<PreJoinedOmniSketch<size_t>>(sketch)->GetMin();
            json_obj["max"] = std::dynamic_pointer_cast<PreJoinedOmniSketch<size_t>>(sketch)->GetMax();
            json_obj["referencing_table_name"] = referencing_table_name;
        }
        if (std::dynamic_pointer_cast<PreJoinedOmniSketch<double>>(sketch)) {
            json_obj["data_type"] = "double";
            json_obj["min"] = std::dynamic_pointer_cast<PreJoinedOmniSketch<double>>(sketch)->GetMin();
            json_obj["max"] = std::dynamic_pointer_cast<PreJoinedOmniSketch<double>>(sketch)->GetMax();
            json_obj["referencing_table_name"] = referencing_table_name;
        }
        if (std::dynamic_pointer_cast<PreJoinedOmniSketch<int32_t>>(sketch)) {
            json_obj["data_type"] = "int";
            json_obj["min"] = std::dynamic_pointer_cast<PreJoinedOmniSketch<int32_t>>(sketch)->GetMin();
            json_obj["max"] = std::dynamic_pointer_cast<PreJoinedOmniSketch<int32_t>>(sketch)->GetMax();
            json_obj["referencing_table_name"] = referencing_table_name;
        }
        if (std::dynamic_pointer_cast<PreJoinedOmniSketch<std::string>>(sketch)) {
            json_obj["data_type"] = "varchar";
            json_obj["min"] = std::dynamic_pointer_cast<PreJoinedOmniSketch<std::string>>(sketch)->GetMin();
            json_obj["max"] = std::dynamic_pointer_cast<PreJoinedOmniSketch<std::string>>(sketch)->GetMax();
            json_obj["referencing_table_name"] = referencing_table_name;
        }
        std::ofstream file;
        file.open(path);
        file << json_obj;
        file.close();
    }

    void Deserialize(const std::string& path) {
        nlohmann::json json_obj;
        std::ifstream file;
        file.open(path);
        file >> json_obj;
        file.close();

        if (json_obj["type"] == "rid_sample") {
            std::vector<uint64_t> hashes = json_obj["hashes"];
            auto mhs = std::make_shared<MinHashSketchVector>(hashes, json_obj["max_sample_count"]);
            rid_sketches[json_obj["table_name"]] = std::make_shared<OmniSketchCell>(mhs, json_obj["record_count"]);
            return;
        }

        std::shared_ptr<PointOmniSketch> sketch;
        if (json_obj["type"] == "standard") {
            if (json_obj["data_type"] == "uint") {
                auto typed_sketch = std::make_shared<TypedPointOmniSketch<size_t>>(json_obj["width"], json_obj["depth"],
                                                                                   json_obj["min_hash_sketch_size"]);
                size_t max = json_obj["max"];
                typed_sketch->SetMax(max);
                size_t min = json_obj["min"];
                typed_sketch->SetMin(min);
                sketch = typed_sketch;
                sketches[json_obj["table_name"]][json_obj["column_name"]].main_sketch = typed_sketch;
            } else if (json_obj["data_type"] == "int") {
                auto typed_sketch = std::make_shared<TypedPointOmniSketch<int32_t>>(
                    json_obj["width"], json_obj["depth"], json_obj["min_hash_sketch_size"]);
                int32_t max = json_obj["max"];
                typed_sketch->SetMax(max);
                int32_t min = json_obj["min"];
                typed_sketch->SetMin(min);
                sketch = typed_sketch;
                sketches[json_obj["table_name"]][json_obj["column_name"]].main_sketch = typed_sketch;
            } else if (json_obj["data_type"] == "double") {
                auto typed_sketch = std::make_shared<TypedPointOmniSketch<double>>(json_obj["width"], json_obj["depth"],
                                                                                   json_obj["min_hash_sketch_size"]);
                double max = json_obj["max"];
                typed_sketch->SetMax(max);
                double min = json_obj["min"];
                typed_sketch->SetMin(min);
                sketch = typed_sketch;
                sketches[json_obj["table_name"]][json_obj["column_name"]].main_sketch = typed_sketch;
            } else if (json_obj["data_type"] == "varchar") {
                auto typed_sketch = std::make_shared<TypedPointOmniSketch<std::string>>(
                    json_obj["width"], json_obj["depth"], json_obj["min_hash_sketch_size"]);
                std::string max = json_obj["max"];
                typed_sketch->SetMax(max);
                std::string min = json_obj["min"];
                typed_sketch->SetMin(min);
                sketch = typed_sketch;
                sketches[json_obj["table_name"]][json_obj["column_name"]].main_sketch = typed_sketch;
            }
        } else {
            assert(json_obj["type"] == "prejoined");
            if (json_obj["data_type"] == "uint") {
                auto typed_sketch = std::make_shared<PreJoinedOmniSketch<size_t>>(
                    nullptr, json_obj["width"], json_obj["depth"], json_obj["min_hash_sketch_size"]);
                size_t max = json_obj["max"];
                typed_sketch->SetMax(max);
                size_t min = json_obj["min"];
                typed_sketch->SetMin(min);
                sketch = typed_sketch;
                sketches[json_obj["table_name"]][json_obj["column_name"]]
                    .referencing_sketches[json_obj["referencing_table_name"]] = typed_sketch;
            } else if (json_obj["data_type"] == "int") {
                auto typed_sketch = std::make_shared<PreJoinedOmniSketch<int32_t>>(
                    nullptr, json_obj["width"], json_obj["depth"], json_obj["min_hash_sketch_size"]);
                int32_t max = json_obj["max"];
                typed_sketch->SetMax(max);
                int32_t min = json_obj["min"];
                typed_sketch->SetMin(min);
                sketch = typed_sketch;
                sketches[json_obj["table_name"]][json_obj["column_name"]]
                    .referencing_sketches[json_obj["referencing_table_name"]] = typed_sketch;
            } else if (json_obj["data_type"] == "double") {
                auto typed_sketch = std::make_shared<PreJoinedOmniSketch<double>>(
                    nullptr, json_obj["width"], json_obj["depth"], json_obj["min_hash_sketch_size"]);
                double max = json_obj["max"];
                typed_sketch->SetMax(max);
                double min = json_obj["min"];
                typed_sketch->SetMin(min);
                sketch = typed_sketch;
                sketches[json_obj["table_name"]][json_obj["column_name"]]
                    .referencing_sketches[json_obj["referencing_table_name"]] = typed_sketch;
            } else if (json_obj["data_type"] == "varchar") {
                auto typed_sketch = std::make_shared<PreJoinedOmniSketch<std::string>>(
                    nullptr, json_obj["width"], json_obj["depth"], json_obj["min_hash_sketch_size"]);
                std::string max = json_obj["max"];
                typed_sketch->SetMax(max);
                std::string min = json_obj["min"];
                typed_sketch->SetMin(min);
                sketch = typed_sketch;
                sketches[json_obj["table_name"]][json_obj["column_name"]]
                    .referencing_sketches[json_obj["referencing_table_name"]] = typed_sketch;
            }
        }

        sketch->SetRecordCount(json_obj["record_count"]);
        for (size_t i = 0; i < sketch->Depth(); i++) {
            auto& row = json_obj["rows"][i];
            for (size_t j = 0; j < sketch->Width(); j++) {
                auto& cell_json = row[j];
                std::vector<uint64_t> hashes = cell_json["hashes"];
                auto mhs = std::make_shared<MinHashSketchVector>(hashes, cell_json["max_sample_count"]);
                sketch->SetCell(i, j, std::make_shared<OmniSketchCell>(mhs, cell_json["record_count"]));
            }
        }
    }

    void SetSketchDirectory(const std::string& path) {
        std::vector<std::string> json_files;
        DIR* dir = opendir(path.c_str());

        if (dir == nullptr) {
            std::cerr << "Could not open directory: " << path << std::endl;
            return;
        }

        sketches.clear();

        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            if (entry->d_type == DT_REG || entry->d_type == DT_UNKNOWN) {
                std::string filename(entry->d_name);
                if (HasJsonExtension(filename)) {
                    std::string fullPath = path;
                    if (fullPath.back() != '/') {
                        fullPath += '/';
                    }
                    fullPath += filename;
                    json_files.push_back(fullPath);
                }
            }
        }

        for (auto& json_file : json_files) {
            Deserialize(json_file);
        }
    }

private:
    Registry();
    std::unordered_map<std::string, TableEntry> sketches;
    std::unordered_map<std::string, std::shared_ptr<OmniSketchCell>> rid_sketches;

    bool HasJsonExtension(const std::string& filename) {
        const std::string extension = ".json";
        if (filename.length() >= extension.length()) {
            return (0 == filename.compare(filename.length() - extension.length(), extension.length(), extension));
        }
        return false;
    }
};

}  // namespace omnisketch
