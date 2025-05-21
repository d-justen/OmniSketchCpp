#include "min_hash_sketch/min_hash_sketch_pair.hpp"

#include "min_hash_sketch/intersect_min_hash_sketches.hpp"
#include "min_hash_sketch/min_hash_sketch_vector.hpp"

namespace omnisketch {

void MinHashSketchPair::AddRecord(uint64_t primary_rid, uint64_t secondary_rid) {
	if (data.find(primary_rid) != data.end()) {
		return;
	}
	if (data.size() < max_count) {
		data[primary_rid] = secondary_rid;
	} else {
		const auto data_it = std::prev(data.end());
		const uint64_t max_hash = data_it->first;

		if (primary_rid < max_hash) {
			data[primary_rid] = secondary_rid;
			data.erase(data_it);
		}
	}
}

void MinHashSketchPair::AddRecord(uint64_t) {
	throw std::logic_error("MinHashSketchPair needs to be called with a hash pair.");
}

size_t MinHashSketchPair::Size() const {
	return data.size();
}

size_t MinHashSketchPair::MaxCount() const {
	return max_count;
}

std::shared_ptr<MinHashSketch> MinHashSketchPair::Resize(size_t size) const {
	auto result = std::make_shared<MinHashSketchPair>(size, nullptr);
	auto index_it = data.begin();
	for (size_t i = 0; i < size; i++) {
		if (!validity || validity->IsValid(i)) {
			result->AddRecord(index_it->first, index_it->second);
		}

		index_it++;
	}
	return result;
}

std::shared_ptr<MinHashSketch> MinHashSketchPair::Flatten() const {
	std::vector<uint64_t> result_vec;
	result_vec.reserve(data.size());
	for (auto it : data) {
		result_vec.push_back(it.first);
	}

	return std::make_shared<MinHashSketchVector>(std::move(result_vec));
}

std::shared_ptr<MinHashSketch> MinHashSketchPair::Intersect(const std::vector<std::shared_ptr<MinHashSketch>> &sketches,
                                                            size_t max_sample_count) {
	return ComputeIntersection(sketches, validity.get(), max_sample_count);
}

void MinHashSketchPair::Combine(const MinHashSketch &other) {
	auto other_it = other.Iterator();
	auto other_pair_it = dynamic_cast<MinHashSketchPair::SketchIterator *>(other_it.get());
	assert(other_pair_it);
	while (!other_pair_it->IsAtEnd()) {
		if (data.size() == max_count && other_pair_it->Current() > data.rbegin()->first) {
			return;
		}
		data[other_pair_it->Current()] = other_pair_it->CurrentVal();
		data.erase(std::prev(data.end()));
		other_pair_it->Next();
	}
}

void MinHashSketchPair::CombineWithSecondaryHash(const MinHashSketch &other, uint64_t secondary_hash) {
	auto other_it = other.Iterator();
	while (!other_it->IsAtEnd()) {
		if (data.size() == max_count && other_it->Current() > data.rbegin()->first) {
			return;
		}
		data[other_it->Current()] = secondary_hash;
		if (data.size() > max_count) {
			data.erase(std::prev(data.end()));
		}
		other_it->Next();
	}
}

std::shared_ptr<MinHashSketch>
MinHashSketchPair::Combine(const std::vector<std::shared_ptr<MinHashSketch>> &others) const {
	auto result = std::make_shared<MinHashSketchPair>(others.front()->MaxCount(), nullptr);
	for (const auto &sketch : others) {
		result->Combine(*sketch);
	}
	return result;
}

std::shared_ptr<MinHashSketch> MinHashSketchPair::Copy() const {
	auto result = std::make_shared<MinHashSketchPair>(max_count, nullptr);
	if (validity) {
		result->validity = std::make_unique<ValidityMask>(*validity);
	}
	result->data = data;
	return result;
}

size_t MinHashSketchPair::EstimateByteSize() const {
	// TODO
	const size_t max_count_size = sizeof(size_t);
	const size_t set_overhead = 16 + 16;
	const size_t per_item_size = 2 * (32 + sizeof(uint64_t));
	return max_count_size + set_overhead + data.size() * per_item_size;
}

std::unique_ptr<MinHashSketch::SketchIterator> MinHashSketchPair::Iterator() const {
	return std::make_unique<MinHashSketchPair::SketchIterator>(data.cbegin(), validity.get(), data.size());
}

std::unique_ptr<MinHashSketch::SketchIterator> MinHashSketchPair::Iterator(size_t max_sample_count) const {
	return std::make_unique<MinHashSketchPair::SketchIterator>(data.cbegin(), validity.get(),
	                                                           std::min(data.size(), max_sample_count));
}

std::map<uint64_t, uint64_t> &MinHashSketchPair::Data() {
	return data;
}

const std::map<uint64_t, uint64_t> &MinHashSketchPair::Data() const {
	return data;
}

std::set<uint64_t> MinHashSketchPair::PrimaryRids() const {
	std::set<uint64_t> result;
	for (auto it = Iterator(); !it->IsAtEnd(); it->Next()) {
		result.insert(it->Current());
	}
	return result;
}

std::set<uint64_t> MinHashSketchPair::SecondaryRids() const {
	std::set<uint64_t> result;
	auto it = Iterator();
	auto *typed_it = dynamic_cast<MinHashSketchPair::SketchIterator *>(it.get());

	for (; !typed_it->IsAtEnd(); typed_it->Next()) {
		result.insert(typed_it->CurrentVal());
	}
	return result;
}

void MinHashSketchPair::EraseRecord(uint64_t hash) {
	assert(validity);
	validity->SetInvalid(std::distance(data.begin(), data.find(hash)));
}

} // namespace omnisketch
