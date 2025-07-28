#include "min_hash_sketch/min_hash_sketch_set.hpp"

#include "min_hash_sketch/min_hash_sketch_vector.hpp"

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
		const uint64_t hash = it->Current();
		it->Next();
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

std::shared_ptr<MinHashSketch> MinHashSketchSet::Intersect(const std::vector<std::shared_ptr<MinHashSketch>> &sketches,
                                                           size_t max_sample_count) {
	return MinHashSketchVector::ComputeIntersection(sketches, nullptr, max_sample_count);
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
		for (size_t active_sketch_idx = 0; active_sketch_idx < active_sketch_count; active_sketch_idx++) {
			const size_t current_sketch_idx = active_sketch_idxs[active_sketch_idx];
			auto &current_sketch_it = sketch_iterators[current_sketch_idx];
			if (current_sketch_it->IsAtEnd()) {
				continue;
			}

			auto next_hash = current_sketch_it->Current();
			current_sketch_it->Next();
			if (result->Data().size() < result->max_count) {
				result->Data().insert(next_hash);
				active_sketch_idxs[active_count_next_round++] = current_sketch_idx;
				continue;
			}
			if (next_hash < *result->Data().crbegin()) {
				result->Data().insert(next_hash);
				if (result->Data().size() > result->max_count) {
					result->Data().erase(std::prev(result->Data().cend()));
				}
				active_sketch_idxs[active_count_next_round++] = active_sketch_idx;
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

std::unique_ptr<MinHashSketch::SketchIterator> MinHashSketchSet::Iterator(size_t max_sample_count) const {
	return std::make_unique<SketchIterator>(data.begin(), std::min(data.size(), max_sample_count));
}

std::set<uint64_t> &MinHashSketchSet::Data() {
	return data;
}

const std::set<uint64_t> &MinHashSketchSet::Data() const {
	return data;
}

void MinHashSketchSet::EraseRecord(uint64_t hash) {
	data.erase(hash);
}

} // namespace omnisketch
