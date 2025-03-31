#include "min_hash_sketch.hpp"

#include <cassert>
#include <queue>

namespace omnisketch {

void MinHashSketchSet::AddRecord(uint64_t hash) {
	if (data.size() < max_count) {
		data.insert(hash);
	} else {
		if (hash < *data.crbegin()) {
			data.insert(hash);
			if (data.size() > max_count) {
				data.erase(std::prev(data.cend()));
			}
		}
	}
}

size_t MinHashSketchSet::Size() const {
	return data.size();
}

size_t MinHashSketchSet::MaxCount() const {
	return max_count;
}

void MinHashSketchSet::Combine(const MinHashSketch &other) {
	auto it = other.Iterator();

	for (size_t val_idx = 0; val_idx < other.Size(); val_idx++) {
		const uint64_t hash = it->Next();
		if (data.size() < max_count) {
			data.insert(hash);
			continue;
		}
		if (hash < *data.crbegin()) {
			data.insert(hash);
			if (data.size() > max_count) {
				data.erase(std::prev(data.cend()));
			}
			continue;
		}
		return;
	}
}

std::shared_ptr<MinHashSketch> MinHashSketchSet::Resize(size_t size) const {
	auto result = std::make_shared<MinHashSketchSet>(size);
	for (const auto &hash : data) {
		result->AddRecord(hash);
	}
	return result;
}

std::shared_ptr<MinHashSketch> MinHashSketchSet::Flatten() const {
	auto flattened = std::make_shared<MinHashSketchVector>(max_count);
	flattened->Data().insert(flattened->Data().end(), data.cbegin(), data.cend());
	return flattened;
}

enum class StepResult : uint8_t { MATCH, NO_MATCH, DONE };

StepResult MultiwayIntersectionStep(const std::vector<MinHashSketchSet *> &sketches,
                                    std::vector<std::set<uint64_t>::const_iterator> &offsets, uint64_t &current_val,
                                    size_t &first_it_offset) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");

	const auto &first_sketch_end = sketches.front()->Data().cend();

	if (offsets[0] != first_sketch_end) {
		current_val = *offsets[0];

		for (size_t sketch_idx = 1; sketch_idx < sketches.size(); sketch_idx++) {
			auto &other_offset = offsets[sketch_idx];
			auto other_end = sketches[sketch_idx]->Data().cend();

			while (other_offset != other_end && *other_offset < current_val) {
				++other_offset;
			}

			if (other_offset == other_end) {
				// There can be no other matches
				return StepResult::DONE;
			}
			if (current_val == *other_offset) {
				// It's a match
				++other_offset;
			} else {
				// No match, skip to next rid geq the mismatch
				current_val = *other_offset;
				while (offsets[0] != first_sketch_end && *offsets[0] < current_val) {
					++offsets[0];
					++first_it_offset;
				}
				if (offsets[0] != first_sketch_end) {
					current_val = *offsets[0];
					return StepResult::NO_MATCH;
				} else {
					return StepResult::DONE;
				}
			}
		}

		++offsets[0];
		++first_it_offset;
		return StepResult::MATCH;
	}
	return StepResult::DONE;
}

std::shared_ptr<MinHashSketch>
MinHashSketchSet::Intersect(const std::vector<std::shared_ptr<MinHashSketch>> &sketches) const {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");

	std::vector<std::set<uint64_t>::const_iterator> offsets;
	offsets.reserve(sketches.size());
	std::vector<MinHashSketchSet *> typed_sketches;
	typed_sketches.reserve(sketches.size());

	for (const auto &sketch : sketches) {
		auto *typed = dynamic_cast<MinHashSketchSet *>(sketch.get());
		assert(typed);
		offsets.push_back(typed->Data().cbegin());
		typed_sketches.push_back(typed);
	}

	auto result = std::make_shared<MinHashSketchSet>(max_count);

	const size_t step_count = sketches.front()->Size();
	uint64_t current_hash;
	StepResult step_result;
	size_t dummy_offset = 0;

	auto &result_data = result->Data();
	for (size_t i = 0; i < step_count; i++) {
		step_result = MultiwayIntersectionStep(typed_sketches, offsets, current_hash, dummy_offset);
		if (step_result == StepResult::MATCH) {
			result_data.insert(current_hash);
		} else if (step_result == StepResult::DONE) {
			return result;
		}
	}

	return result;
}

std::shared_ptr<MinHashSketch>
MinHashSketchSet::Combine(const std::vector<std::shared_ptr<MinHashSketch>> &others) const {
	auto result = std::make_shared<MinHashSketchSet>(max_count);

	size_t active_sketch_count = others.size();
	std::vector<std::unique_ptr<MinHashSketch::SketchIterator>> sketch_iterators(active_sketch_count);
	std::vector<size_t> active_sketch_idxs(active_sketch_count);

	for (size_t sketch_idx = 0; sketch_idx < active_sketch_idxs.size(); sketch_idx++) {
		sketch_iterators[sketch_idx] = others[sketch_idx]->Iterator();
		active_sketch_idxs[sketch_idx] = sketch_idx;
	}

	while (active_sketch_count > 0) {
		size_t active_count_next_round = 0;
		for (size_t sketch_idx = 0; sketch_idx < active_sketch_count; sketch_idx++) {
			auto &current_sketch_it = sketch_iterators[active_sketch_idxs[sketch_idx]];
			if (!current_sketch_it->HasNext()) {
				continue;
			}

			auto next_hash = current_sketch_it->Next();
			if (result->Data().size() < result->max_count) {
				result->Data().insert(next_hash);
				active_sketch_idxs[active_count_next_round++] = sketch_idx;
				continue;
			}
			if (next_hash < *result->Data().crbegin()) {
				result->Data().insert(next_hash);
				if (result->Data().size() > result->max_count) {
					result->Data().erase(std::prev(result->Data().cend()));
				}
				active_sketch_idxs[active_count_next_round++] = sketch_idx;
			}
		}
		active_sketch_count = active_count_next_round;
	}
	return result;
}

std::shared_ptr<MinHashSketch> MinHashSketchSet::Copy() const {
	auto result = std::make_shared<MinHashSketchSet>(max_count);
	result->Data() = data;
	return result;
}

size_t MinHashSketchSet::EstimateByteSize() const {
	const size_t max_count_size = sizeof(size_t);
	const size_t set_overhead = 16;
	const size_t per_item_size = 32 + sizeof(uint64_t);
	return max_count_size + set_overhead + data.size() * per_item_size;
}

std::unique_ptr<MinHashSketch::SketchIterator> MinHashSketchSet::Iterator() const {
	return std::make_unique<SketchIterator>(data.begin(), data.size());
}

std::set<uint64_t> &MinHashSketchSet::Data() {
	return data;
}

const std::set<uint64_t> &MinHashSketchSet::Data() const {
	return data;
}

void MinHashSketchVector::AddRecord(uint64_t hash) {
	data.insert(std::upper_bound(data.begin(), data.end(), hash), hash);
	if (data.size() > max_count) {
		data.pop_back();
	}
}

size_t MinHashSketchVector::Size() const {
	return data.size();
}

size_t MinHashSketchVector::MaxCount() const {
	return max_count;
}

void MinHashSketchVector::Combine(const MinHashSketch &other) {
	MinHashSketchSet tmp(max_count);
	tmp.Data().insert(data.begin(), data.end());
	tmp.Combine(other);
	auto flattened = tmp.Flatten();
	data = std::move(dynamic_cast<MinHashSketchVector *>(flattened.get())->data);
}

std::shared_ptr<MinHashSketch> MinHashSketchVector::Resize(size_t size) const {
	auto result = std::make_shared<MinHashSketchVector>(size);
	for (size_t hash_idx = 0; hash_idx < size; hash_idx++) {
		result->AddRecord(data[hash_idx]);
	}
	return result;
}

std::shared_ptr<MinHashSketch> MinHashSketchVector::Flatten() const {
	auto flattened = std::make_shared<MinHashSketchVector>(max_count);
	flattened->Data().insert(flattened->Data().end(), data.cbegin(), data.cend());
	return flattened;
}

StepResult MultiwayIntersectionStep(const std::vector<MinHashSketchVector *> &sketches, std::vector<size_t> &offsets,
                                    uint64_t &current_val, size_t &first_it_offset) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");
	auto &this_data = sketches.front()->Data();
	const size_t first_sketch_size = this_data.size();

	if (offsets[0] < first_sketch_size) {
		current_val = this_data[offsets[0]];

		for (size_t sketch_idx = 1; sketch_idx < sketches.size(); sketch_idx++) {
			auto &other_offset = offsets[sketch_idx];
			auto &other_data = sketches[sketch_idx]->Data();

			while (other_offset < other_data.size() && other_data[other_offset] < current_val) {
				++other_offset;
			}

			if (other_offset == other_data.size()) {
				// There can be no other matches
				return StepResult::DONE;
			}
			if (current_val == other_data[other_offset]) {
				// It's a match
				++other_offset;
				continue;
			}
			// No match, skip to next rid geq the mismatch
			current_val = other_data[other_offset];
			while (offsets[0] < first_sketch_size && this_data[offsets[0]] < current_val) {
				++offsets[0];
				++first_it_offset;
			}
			if (offsets[0] < first_sketch_size) {
				current_val = this_data[offsets[0]];
				return StepResult::NO_MATCH;
			} else {
				return StepResult::DONE;
			}
		}

		++offsets[0];
		++first_it_offset;
		return StepResult::MATCH;
	}
	return StepResult::DONE;
}

std::shared_ptr<MinHashSketch>
MinHashSketchVector::Intersect(const std::vector<std::shared_ptr<MinHashSketch>> &sketches) const {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");

	std::vector<size_t> offsets(sketches.size());
	std::vector<MinHashSketchVector *> typed_sketches;
	typed_sketches.reserve(sketches.size());

	for (const auto &sketch : sketches) {
		auto *typed = dynamic_cast<MinHashSketchVector *>(sketch.get());
		assert(typed);
		typed_sketches.push_back(typed);
	}

	auto result = std::make_shared<MinHashSketchSet>(max_count);

	const size_t step_count = sketches.front()->Size();
	uint64_t current_hash;
	StepResult step_result;
	size_t dummy_offset = 0;

	auto &result_data = result->Data();
	for (size_t i = 0; i < step_count; i++) {
		step_result = MultiwayIntersectionStep(typed_sketches, offsets, current_hash, dummy_offset);
		if (step_result == StepResult::MATCH) {
			result_data.insert(current_hash);
		} else if (step_result == StepResult::DONE) {
			return result;
		}
	}

	return result;
}

std::shared_ptr<MinHashSketch>
MinHashSketchVector::Combine(const std::vector<std::shared_ptr<MinHashSketch>> &others) const {
	MinHashSketchSet dummy(max_count);
	return dummy.Combine(others);
}

std::shared_ptr<MinHashSketch> MinHashSketchVector::Copy() const {
	auto result = std::make_shared<MinHashSketchVector>(max_count);
	result->Data() = data;
	return result;
}

size_t MinHashSketchVector::EstimateByteSize() const {
	const size_t max_count_size = sizeof(size_t);
	const size_t vector_overhead = sizeof(std::vector<uint64_t>);
	const size_t per_item_size = sizeof(uint64_t);
	return max_count_size + vector_overhead + data.size() * per_item_size;
}

std::unique_ptr<MinHashSketch::SketchIterator> MinHashSketchVector::Iterator() const {
	return std::make_unique<SketchIterator>(data.begin(), data.size());
}

std::vector<uint64_t> &MinHashSketchVector::Data() {
	return data;
}

const std::vector<uint64_t> &MinHashSketchVector::Data() const {
	return data;
}

} // namespace omnisketch
