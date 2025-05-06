#include "min_hash_sketch/min_hash_sketch_vector.hpp"

#include "min_hash_sketch/intersect_min_hash_sketches.hpp"

namespace omnisketch {

void MinHashSketchVector::AddRecord(uint64_t hash) {
	data.insert(std::upper_bound(data.begin(), data.end(), hash), hash);
	if (data.size() > max_count) {
		data.pop_back();
	}
}

size_t MinHashSketchVector::Size() const {
	return data.size() - validity.InvalidCount();
}

size_t MinHashSketchVector::MaxCount() const {
	return max_count;
}

void MinHashSketchVector::Combine(const MinHashSketch &other) {
	if (other.Size() == 0) {
		return;
	}

	if (data.empty()) {
		data.reserve(other.Size());
		auto other_it = other.Iterator();
		while (!other_it->IsAtEnd()) {
			data.push_back(other_it->Current());
			other_it->Next();
		}
		return;
	}

	const size_t result_size = std::min(data.size() + other.Size(), max_count);
	std::vector<uint64_t> result;
	result.reserve(result_size);

	auto other_it = other.Iterator();
	size_t this_idx = 0;
	while (result.size() < result_size) {
		if (this_idx < data.size() && (other_it->IsAtEnd() || data[this_idx] <= other_it->Current())) {
			result.push_back(data[this_idx++]);
		} else {
			result.push_back(other_it->Current());
			other_it->Next();
		}
	}

	data = std::move(result);
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

std::shared_ptr<MinHashSketch>
MinHashSketchVector::Intersect(const std::vector<std::shared_ptr<MinHashSketch>> &sketches) {
	auto result = ComputeIntersection<MinHashSketchVector, std::vector<uint64_t>>(sketches, &validity);
	ShrinkToFit();
	return result;
}

std::shared_ptr<MinHashSketch>
MinHashSketchVector::Combine(const std::vector<std::shared_ptr<MinHashSketch>> &others) const {
	auto result = std::make_shared<MinHashSketchVector>(max_count);
	result->Combine(*this);
	for (const auto &other : others) {
		result->Combine(*other);
	}
	return result;
}

std::shared_ptr<MinHashSketch> MinHashSketchVector::Copy() const {
	auto result = std::make_shared<MinHashSketchVector>(max_count);
	result->Data() = data;
	result->validity = validity;
	return result;
}

size_t MinHashSketchVector::EstimateByteSize() const {
	const size_t max_count_size = sizeof(size_t);
	const size_t vector_overhead = sizeof(std::vector<uint64_t>);
	const size_t per_item_size = sizeof(uint64_t);
	return max_count_size + vector_overhead + data.size() * per_item_size;
}

std::unique_ptr<MinHashSketch::SketchIterator> MinHashSketchVector::Iterator() const {
	return std::make_unique<SketchIterator>(data.begin(), &validity, data.size());
}

std::unique_ptr<MinHashSketch::SketchIterator> MinHashSketchVector::Iterator(size_t max_sample_count) const {
	return std::make_unique<SketchIterator>(data.begin(), &validity, std::min(max_sample_count, data.size()));
}

std::vector<uint64_t> &MinHashSketchVector::Data() {
	return data;
}

const std::vector<uint64_t> &MinHashSketchVector::Data() const {
	return data;
}

void MinHashSketchVector::EraseRecord(uint64_t hash) {
	validity.SetInvalid(std::lower_bound(data.begin(), data.end(), hash) - data.begin());
}
void MinHashSketchVector::ShrinkToFit() {
	const size_t valid_count = data.size() - validity.InvalidCount();
	if ((double)valid_count / (double)data.size() > SHRINK_TO_FIT_THRESHOLD && valid_count > 0) {
		return;
	}
	std::vector<uint64_t> new_data;
	new_data.reserve(valid_count);

	for (auto it = Iterator(); !it->IsAtEnd(); it->Next()) {
		new_data.push_back(it->Current());
	}

	data = std::move(new_data);
	validity = ValidityMask(data.size());
}

} // namespace omnisketch
