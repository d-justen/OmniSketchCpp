#pragma once

#include "min_hash_sketch.hpp"

#include <queue>

namespace omnisketch {

namespace Algorithm {

enum class StepResult : uint8_t { MATCH, NO_MATCH, DONE };

template <typename HashContainerType>
static StepResult MultiwayIntersectionStep(const std::vector<const MinHashSketch<HashContainerType> *> &sketches,
                                           std::vector<typename HashContainerType::const_iterator> &offsets,
                                           typename HashContainerType::value_type &match) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");

	const auto &max_rid_first_sketch = sketches[0]->hashes.cend();

	if (offsets[0] != max_rid_first_sketch) {
		match = *offsets[0];

		for (size_t sketch_idx = 1; sketch_idx < sketches.size(); sketch_idx++) {
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

template <typename HashContainerType>
static MinHashSketch<std::set<typename HashContainerType::value_type>>
MultiwayIntersection(const std::vector<const MinHashSketch<HashContainerType> *> &sketches) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");

	const size_t output_sketch_size = sketches.front()->max_count;

	std::vector<typename HashContainerType::const_iterator> offsets;
	offsets.reserve(sketches.size());
	for (size_t sketch_idx = 0; sketch_idx < sketches.size(); sketch_idx++) {
		offsets.push_back(sketches[sketch_idx]->hashes.cbegin());
	}

	MinHashSketch<std::set<typename HashContainerType::value_type>> result(output_sketch_size);
	const typename HashContainerType::value_type max_hash =
	    std::numeric_limits<typename HashContainerType::value_type>::max();

	typename HashContainerType::value_type current_hash;
	StepResult step_result;
	for (size_t i = 0; i < output_sketch_size; i++) {
		step_result = MultiwayIntersectionStep(sketches, offsets, current_hash);
		if (step_result == StepResult::MATCH) {
			result.hashes.insert(current_hash);
		} else if (step_result == StepResult::DONE) {
			return result;
		}
	}

	return result;
}

template <typename HashContainerType>
static MinHashSketch<std::set<typename HashContainerType::value_type>>
MultiwayUnion(const std::vector<const MinHashSketch<HashContainerType> *> &sketches, size_t output_size = 0) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");

	if (output_size == 0) {
		output_size = sketches.front()->max_count;
	}

	using IterTuple = std::tuple<typename HashContainerType::value_type, typename HashContainerType::const_iterator,
	                             typename HashContainerType::const_iterator>;
	static auto CompareIterTuples = [](const IterTuple &a, const IterTuple &b) {
		return std::get<0>(a) > std::get<0>(b);
	};
	std::priority_queue<IterTuple, std::vector<IterTuple>, decltype(CompareIterTuples)> iterator_queue(
	    CompareIterTuples);

	for (size_t sketch_idx = 0; sketch_idx < sketches.size(); sketch_idx++) {
		const auto &hashes = sketches[sketch_idx]->hashes;
		auto it = hashes.begin();
		if (it != hashes.end()) {
			iterator_queue.emplace(*it, it, hashes.end());
		}
	}

	MinHashSketch<std::set<typename HashContainerType::value_type>> result;
	result.max_count = output_size;

	while (!iterator_queue.empty()) {
		if (result.hashes.size() == output_size) {
			break;
		}

		auto current_iter_pair = iterator_queue.top();
		auto hash_value = std::get<0>(current_iter_pair);
		auto &it_begin = std::get<1>(current_iter_pair);
		auto &it_end = std::get<2>(current_iter_pair);

		result.hashes.insert(hash_value);
		++it_begin;
		if (it_begin != it_end) {
			iterator_queue.emplace(*it_begin, it_begin, it_end);
		}

		iterator_queue.pop();
	}

	return result;
}

} // namespace Algorithm

} // namespace omnisketch