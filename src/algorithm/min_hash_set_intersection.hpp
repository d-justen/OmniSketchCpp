#pragma once

#include "min_hash_sketch.hpp"

namespace omnisketch {

namespace Algorithm {

enum class StepResult : uint8_t {
	MATCH,
	NO_MATCH,
	DONE
};

template <typename T>
static StepResult MultiWayIntersectionStep(const std::vector<const MinHashSketch<T> *> &sketches,
                                     std::vector<typename T::const_iterator> &offsets, typename T::value_type &match) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");

	const size_t sketch_count = sketches.size();
	const auto &max_rid_first_sketch = sketches[0]->hashes.cend();

	if (offsets[0] != max_rid_first_sketch) {
		match = *offsets[0];

		for (size_t sketch_idx = 1; sketch_idx < sketch_count; sketch_idx++) {
			auto &other_offset = offsets[sketch_idx];
			auto other_end = sketches[sketch_idx]->hashes.cend();

			while (other_offset != other_end && *other_offset < match) {
				++other_offset;
			}

			if (other_offset == other_end) {
				// There can be no other matches
				return StepResult::DONE;
			}
			if (match == *other_offset) {
				// It's a match
				++other_offset;
			} else {
				// No match, skip to next rid geq the mismatch
				match = *other_offset;
				while (offsets[0] != max_rid_first_sketch && *offsets[0] < match) {
					++offsets[0];
				}
				if (offsets[0] != max_rid_first_sketch) {
					match = *offsets[0];
					return StepResult::NO_MATCH;
				} else {
					return StepResult::DONE;
				}
			}
		}

		++offsets[0];
		return StepResult::MATCH;
	}
	return StepResult::DONE;
}

template <typename T>
static MinHashSketch<std::set<typename T::value_type>> MultiwayIntersection(const std::vector<const MinHashSketch<T> *> &sketches) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");

	const size_t output_sketch_size = sketches.front()->max_count;

	std::vector<typename T::const_iterator> offsets;
	offsets.reserve(sketches.size());
	for (size_t sketch_idx = 0; sketch_idx < sketches.size(); sketch_idx++) {
		offsets.push_back(sketches[sketch_idx]->hashes.cbegin());
	}

	MinHashSketch<std::set<typename T::value_type>> result(output_sketch_size);
	const typename T::value_type max_hash = std::numeric_limits<typename T::value_type>::max();

	typename T::value_type current_hash;
	StepResult step_result;
	for (size_t i = 0; i < output_sketch_size; i++) {
		step_result = MultiWayIntersectionStep(sketches, offsets, current_hash);
		if (step_result == StepResult::MATCH) {
			result.hashes.insert(current_hash);
		} else if (step_result == StepResult::DONE) {
			return result;
		}
	}

	return result;
}

} // namespace Algorithm

} // namespace omnisketch