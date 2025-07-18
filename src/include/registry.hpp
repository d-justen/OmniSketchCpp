#pragma once

#include <variant>

#include "omni_sketch/standard_omni_sketch.hpp"
#include "omni_sketch/foreign_sorted_omni_sketch.hpp"
#include "omni_sketch/pre_joined_omni_sketch.hpp"

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
	Registry(Registry const &) = delete;
	void operator=(Registry const &) = delete;
	static Registry &Get();

	template <typename T>
	std::shared_ptr<TypedPointOmniSketch<T>> CreateOmniSketch(const std::string &table_name,
	                                                          const std::string &column_name,
	                                                          const OmniSketchConfig &config = OmniSketchConfig {}) {
		assert(!HasOmniSketch(table_name, column_name));
		std::shared_ptr<PointOmniSketch> sketch = std::make_shared<TypedPointOmniSketch<T>>(
		    config.width, config.depth, config.sample_count, std::make_shared<MurmurHashFunction<T>>(),
		    config.set_membership_algo, config.hash_processor);
		sketches[table_name][column_name] = OmniSketchEntry {sketch, {}};
		return std::dynamic_pointer_cast<TypedPointOmniSketch<T>>(sketch);
	}

	template <typename T, typename U>
	std::shared_ptr<T> CreateExtendingOmniSketch(const std::string &table_name, const std::string &column_name,
	                                             const std::string &referencing_table_name,
	                                             const std::string &referencing_column_name,
	                                             const OmniSketchConfig &config = OmniSketchConfig {}) {
		assert(HasOmniSketch(table_name, column_name));
		auto &entry = sketches[table_name][column_name];

		std::shared_ptr<PointOmniSketch> sketch =
		    std::make_shared<T>(GetOmniSketch(referencing_table_name, referencing_column_name), config.width,
		                        config.depth, config.sample_count, std::make_shared<MurmurHashFunction<U>>(),
		                        config.set_membership_algo, config.hash_processor);
		entry.referencing_sketches[referencing_table_name] = sketch;

		return std::dynamic_pointer_cast<T>(sketch);
	}

	template <typename T>
	std::shared_ptr<TypedPointOmniSketch<T>> GetOmniSketchTyped(const std::string &table_name,
	                                                            const std::string &column_name) {
		return std::dynamic_pointer_cast<TypedPointOmniSketch<T>>(GetOmniSketch(table_name, column_name));
	}

	std::shared_ptr<PointOmniSketch> GetOmniSketch(const std::string &table_name, const std::string &column_name) {
		assert(HasOmniSketch(table_name, column_name));
		return sketches[table_name][column_name].main_sketch;
	}

	template <typename T>
	std::shared_ptr<T> FindReferencingOmniSketchTyped(const std::string &table_name, const std::string &column_name,
	                                                  const std::string &referencing_table_name) {
		return std::dynamic_pointer_cast<T>(FindReferencingOmniSketch(table_name, column_name, referencing_table_name));
	}

	std::shared_ptr<PointOmniSketch> FindReferencingOmniSketch(const std::string &table_name,
	                                                           const std::string &column_name,
	                                                           const std::string &referencing_table_name) {
		assert(HasOmniSketch(table_name, column_name));

		auto entry = sketches[table_name][column_name];
		if (entry.referencing_sketches.find(referencing_table_name) != entry.referencing_sketches.end()) {
			return entry.referencing_sketches[referencing_table_name];
		}
		return nullptr;
	}

	bool HasOmniSketch(const std::string &table_name, const std::string &column_name) {
		auto table_entry = sketches.find(table_name);
		return table_entry != sketches.end() && table_entry->second.find(column_name) != table_entry->second.end();
	}

	// TODO: Taking any sketch for a rid sample could be a bad idea - what if it has many nulls? -> Create OS on rids!
	std::shared_ptr<OmniSketchCell> ProduceRidSample(const std::string &table_name) {
		assert(sketches.find(table_name) != sketches.end());
		for (auto& column_sketch : sketches[table_name]) {
			if (column_sketch.first.find("__translator") != std::string::npos) {
				continue;
			}
			return column_sketch.second.main_sketch->GetRids();
		}
		return sketches[table_name].begin()->second.main_sketch->GetRids();
	}

	size_t GetNextBestSampleCount(const std::string &table_name) {
		assert(sketches.find(table_name) != sketches.end());
		for (auto& column_sketch : sketches[table_name]) {
			if (column_sketch.first.find("__translator") != std::string::npos) {
				continue;
			}
			return column_sketch.second.main_sketch->MinHashSketchSize();
		}
		return sketches[table_name].begin()->second.main_sketch->MinHashSketchSize();
	}

	std::shared_ptr<OmniSketchCell> TryProduceReferencingRidSample(const std::string &table_name,
	                                                               const std::string &referencing_table_name) {
		auto entry = sketches.find(table_name);
		assert(entry != sketches.end());
		if (!entry->second.empty()) {
			const auto &referencing_sketches = entry->second.begin()->second.referencing_sketches;
			auto referencing_sketch = referencing_sketches.find(referencing_table_name);
			if (referencing_sketch != referencing_sketches.end()) {
				return referencing_sketch->second->GetRids();
			}
		}
		return nullptr;
	}

	size_t GetBaseTableCard(const std::string &table_name) const {
		const auto table_entry_it = sketches.find(table_name);
		assert(table_entry_it != sketches.end());
		assert(!table_entry_it->second.empty());
		return table_entry_it->second.begin()->second.main_sketch->RecordCount();
	}

	size_t EstimateByteSize() const {
		size_t result = 0;
		for (auto &table : sketches) {
			for (auto &column : table.second) {
				result += column.second.main_sketch->EstimateByteSize();
				for (auto &ref_sketch : column.second.referencing_sketches) {
					result += ref_sketch.second->EstimateByteSize();
				}
			}
		}
		return result;
	}

	void Clear() {
		sketches.clear();
	}

private:
	Registry();
	std::unordered_map<std::string, TableEntry> sketches;
};

} // namespace omnisketch
