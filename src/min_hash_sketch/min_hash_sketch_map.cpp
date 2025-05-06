#include "min_hash_sketch/min_hash_sketch_map.hpp"

#include "min_hash_sketch/intersect_min_hash_sketches.hpp"
#include "min_hash_sketch/min_hash_sketch_vector.hpp"

namespace omnisketch {

void MinHashSketchMap::AddRecord(uint64_t order_key, uint64_t hash) {
	if (hash_index.find(order_key) != hash_index.end()) {
		return;
	}
	if (data.size() < max_count) {
		auto hash_it = data.insert(hash).first;
		hash_index[order_key] = hash_it;
	} else {
		auto index_it = std::prev(hash_index.end());
		const uint64_t max_hash = index_it->first;

		if (order_key < max_hash) {
			hash_index[order_key] = data.insert(hash).first;
			if (data.size() > max_count) {
				auto data_it = index_it->second;
				hash_index.erase(index_it);
				data.erase(data_it);
			} else {
				hash_index.erase(order_key);
			}
		}
	}
}

void MinHashSketchMap::AddRecord(uint64_t) {
	throw std::logic_error("MinHashSketchMap needs to be called with a hash pair.");
}

size_t MinHashSketchMap::Size() const {
	return data.size();
}

size_t MinHashSketchMap::MaxCount() const {
	return max_count;
}

std::shared_ptr<MinHashSketch> MinHashSketchMap::Resize(size_t size) const {
	auto result = std::make_shared<MinHashSketchMap>(size);
	auto index_it = hash_index.begin();
	for (size_t i = 0; i < size; i++) {
		result->AddRecord(index_it->first, *index_it->second);
		index_it++;
	}
	return result;
}

std::shared_ptr<MinHashSketch> MinHashSketchMap::Flatten() const {
	std::vector<uint64_t> result_vec(data.begin(), data.end());
	return std::make_shared<MinHashSketchVector>(std::move(result_vec), ValidityMask(data.size()));
}

std::shared_ptr<MinHashSketch>
MinHashSketchMap::Intersect(const std::vector<std::shared_ptr<MinHashSketch>> &sketches) {
	return ComputeIntersection<MinHashSketchMap, std::set<uint64_t>>(sketches);
}

void MinHashSketchMap::Combine(const MinHashSketch &other) {
	auto other_it = other.Iterator();
	auto other_map_it = dynamic_cast<MinHashSketchMap::SketchIterator *>(other_it.get());
	assert(other_map_it);
	while (!other_map_it->IsAtEnd()) {
		const auto &kv_pair = other_map_it->CurrentKeyVal();
		if (data.size() == max_count && kv_pair.first > hash_index.rbegin()->first) {
			return;
		}
		AddRecord(kv_pair.first, kv_pair.second);
		other_map_it->NextKeyVal();
	}
}

std::shared_ptr<MinHashSketch>
MinHashSketchMap::Combine(const std::vector<std::shared_ptr<MinHashSketch>> &others) const {
	auto result = std::make_shared<MinHashSketchMap>(others.front()->MaxCount());
	for (const auto &sketch : others) {
		result->Combine(*sketch);
	}
	return result;
}

std::shared_ptr<MinHashSketch> MinHashSketchMap::Copy() const {
	auto result = std::make_shared<MinHashSketchMap>(max_count);
	result->data = data;
	result->hash_index = hash_index;
	return result;
}

size_t MinHashSketchMap::EstimateByteSize() const {
	// TODO
	const size_t max_count_size = sizeof(size_t);
	const size_t set_overhead = 16 + 16;
	const size_t per_item_size = 2 * (32 + sizeof(uint64_t));
	return max_count_size + set_overhead + data.size() * per_item_size;
}

std::unique_ptr<MinHashSketch::SketchIterator> MinHashSketchMap::Iterator() const {
	return std::make_unique<MinHashSketchMap::SketchIterator>(hash_index.cbegin(), data.cbegin(), data.size());
}

std::unique_ptr<MinHashSketch::SketchIterator> MinHashSketchMap::Iterator(size_t max_sample_count) const {
	return std::make_unique<MinHashSketchMap::SketchIterator>(hash_index.cbegin(), data.cbegin(),
	                                                          std::min(data.size(), max_sample_count));
}

std::set<uint64_t> &MinHashSketchMap::Data() {
	return data;
}

const std::set<uint64_t> &MinHashSketchMap::Data() const {
	return data;
}

void MinHashSketchMap::EraseRecord(uint64_t hash) {
	for (auto it = hash_index.begin(); it != hash_index.end(); ++it) {
		if (*it->second == hash) {
			data.erase(it->second);
			hash_index.erase(it);
			return;
		}
	}
}

} // namespace omnisketch
