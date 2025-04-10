#pragma once

#include <variant>

#include "omni_sketch.hpp"

namespace omnisketch {

struct OmniSketchConfig {
	void SetWidth(size_t width_p) {
		width = width_p;
		hash_processor->width = width_p;
	}

	void SetSampleCount(size_t sample_count_p) {
		sample_count = sample_count_p;
		min_hash_sketch_factory->SetMaxSampleCount(sample_count_p);
	}

	size_t width = 256;
	size_t depth = 3;
	size_t sample_count = 64;
	std::shared_ptr<MinHashSketchFactory> min_hash_sketch_factory =
	    std::make_shared<MinHashSketchSetFactory>(sample_count);
	std::shared_ptr<SetMembershipAlgorithm> set_membership_algo = std::make_shared<ProbeAllSum>();
	std::shared_ptr<CellIdxMapper> hash_processor = std::make_shared<BarrettModSplitHashMapper>(width);
};

class Registry {
public:
	Registry(Registry const &) = delete;
	void operator=(Registry const &) = delete;
	static Registry &Get();

	template <class T>
	std::shared_ptr<TypedPointOmniSketch<T>> CreateOmniSketch(const std::string &table_name, const std::string &column_name,
	                      const OmniSketchConfig &config = OmniSketchConfig {}) {
		auto sketch = std::make_shared<TypedPointOmniSketch<T>>(
		    config.width, config.depth, std::make_shared<MurmurHashFunction<T>>(), config.min_hash_sketch_factory,
		    config.set_membership_algo, config.hash_processor);
		sketches[table_name][column_name] = sketch;
		return sketch;
	}

	template <class T>
	std::shared_ptr<TypedPointOmniSketch<T>> GetOmniSketchTyped(const std::string &table_name,
	                                                       const std::string &column_name) {
		return std::dynamic_pointer_cast<TypedPointOmniSketch<T>>(sketches[table_name][column_name]);
	}

	std::shared_ptr<PointOmniSketch> GetOmniSketch(const std::string &table_name,
	                                                            const std::string &column_name) {
		return sketches[table_name][column_name];
	}

	std::shared_ptr<OmniSketchCell> ProduceRidSample(const std::string& table_name) {
		return sketches[table_name].begin()->second->GetRids();
	}

private:
	Registry();
	std::unordered_map<std::string, std::unordered_map<std::string, std::shared_ptr<PointOmniSketch>>> sketches;
};

} // namespace omnisketch
