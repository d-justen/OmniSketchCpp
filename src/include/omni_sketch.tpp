#include "omni_sketch.hpp"

namespace omnisketch {

template <typename T>
PointOmniSketch<T>::PointOmniSketch(size_t width_p, size_t depth_p,
                                    std::shared_ptr<MinHashSketchFactory> min_hash_sketch_factory_p,
                                    std::shared_ptr<SetMembershipAlgorithm> set_membership_algo_p,
                                    std::shared_ptr<HashProcessor> hash_processor_p)
    : width(width_p), depth(depth_p), min_hash_sketch_factory(std::move(min_hash_sketch_factory_p)),
      set_membership_algo(std::move(set_membership_algo_p)), hash_processor(std::move(hash_processor_p)) {
	cells.resize(depth);
	for (auto &row : cells) {
		row.reserve(width);
		for (size_t col_idx = 0; col_idx < width; col_idx++) {
			row.emplace_back(std::make_shared<OmniSketchCell>(min_hash_sketch_factory->Create()));
		}
	}
}

template <typename T>
PointOmniSketch<T>::PointOmniSketch(size_t width, size_t depth, size_t max_sample_count_p)
    : PointOmniSketch(width, depth, std::make_shared<MinHashSketchSetFactory>(max_sample_count_p),
                      std::make_shared<ProbeAllSum>(), std::make_shared<BarrettModSplitHashProcessor>(width)) {
}

template <typename T>
void PointOmniSketch<T>::AddRecord(const T &value, const uint64_t record_id) {
	const uint64_t value_hash = Hash(value);
	const uint64_t record_id_hash = Hash(record_id);
	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		const size_t col_idx = hash_processor->ComputeCellIdx(value_hash, row_idx);
		cells[row_idx][col_idx]->AddRecord(record_id_hash);
	}
	record_count++;
}

template <typename T>
size_t PointOmniSketch<T>::RecordCount() const {
	return record_count;
}

template <typename T>
std::shared_ptr<OmniSketchCell> PointOmniSketch<T>::Probe(const T &value) const {
	std::vector<std::shared_ptr<OmniSketchCell>> matches(depth);

	return ProbeHash(Hash(value), matches);
}

template <typename T>
std::shared_ptr<OmniSketchCell>
PointOmniSketch<T>::ProbeHash(uint64_t hash, std::vector<std::shared_ptr<OmniSketchCell>> &matches) const {
	assert(matches.size() == depth);
	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		const size_t col_idx = hash_processor->ComputeCellIdx(hash, row_idx);
		matches[row_idx] = cells[row_idx][col_idx];
	}

	return OmniSketchCell::Intersect(matches);
}

template <typename T>
std::shared_ptr<OmniSketchCell> PointOmniSketch<T>::ProbeSet(const std::shared_ptr<MinHashSketch> &values) const {
	std::vector<std::vector<std::shared_ptr<OmniSketchCell>>> matches(
	    values->Size(), std::vector<std::shared_ptr<OmniSketchCell>>(depth));

	auto value_it = values->Iterator();
	for (size_t value_idx = 0; value_idx < values->Size(); value_idx++) {
		const uint64_t value = value_it->Next();
		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			const size_t col_idx = hash_processor->ComputeCellIdx(value, row_idx);
			matches[value_idx][row_idx] = cells[row_idx][col_idx];
		}
	}

	return set_membership_algo->Execute(min_hash_sketch_factory, matches);
}

template <typename T>
std::shared_ptr<OmniSketchCell> PointOmniSketch<T>::ProbeSet(const T *values, const size_t count) const {
	auto hashed = std::make_shared<MinHashSketchVector>(count);
	auto &hash_data = hashed->Data();
	for (size_t value_idx = 0; value_idx < count; value_idx++) {
		const uint64_t hash = Hash(values[value_idx]);
		hash_data.push_back(hash);
	}
	return ProbeSet(hashed);
}

template <typename T>
std::shared_ptr<OmniSketchCell> PointOmniSketch<T>::ProbeRange(const T &lower_bound, const T &upper_bound) const {
	assert(lower_bound < upper_bound);

	std::vector<T> values;
	values.reserve((upper_bound - lower_bound) + 1);
	for (T value = lower_bound; value <= upper_bound; value++) {
		values.push_back(value);
	}

	return ProbeSet(values.data(), values.size());
}

template <>
inline std::shared_ptr<OmniSketchCell> PointOmniSketch<std::string>::ProbeRange(const std::string &lower_bound,
                                                                                const std::string &upper_bound) const {
	throw std::logic_error("Range probing on strings is not allowed.");
}

template <typename T>
void PointOmniSketch<T>::Flatten() {
	for (auto &row : cells) {
		for (auto &cell : row) {
			cell = cell->Flatten();
		}
	}
}

template <typename T>
size_t PointOmniSketch<T>::EstimateByteSize() const {
	const size_t members = 3 * sizeof(size_t) + sizeof(hash_processor) + sizeof(cells);
	size_t cell_size = 0;
	for (const auto &row : cells) {
		cell_size += sizeof(row);
		for (const auto &cell : row) {
			cell_size += cell->EstimateByteSize();
		}
	}

	return members + cell_size;
}

template <typename T>
size_t PointOmniSketch<T>::Depth() const {
	return depth;
}

template <typename T>
size_t PointOmniSketch<T>::Width() const {
	return width;
}

template <typename T>
size_t PointOmniSketch<T>::MinHashSketchSize() const {
	return min_hash_sketch_factory->MaxSampleCount();
}

template <typename T>
void PointOmniSketch<T>::Combine(const std::shared_ptr<OmniSketchBase> &other) {
	assert(other->Depth() == depth);
	assert(other->Width() == width);
	assert(other->MinHashSketchSize() == min_hash_sketch_factory->MaxSampleCount());

	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		for (size_t col_idx = 0; col_idx < width; col_idx++) {
			cells[row_idx][col_idx]->Combine(other->GetCell(row_idx, col_idx));
		}
	}

	record_count += other->RecordCount();
}

template <typename T>
const OmniSketchCell &PointOmniSketch<T>::GetCell(size_t row_idx, size_t col_idx) const {
	return *cells[row_idx][col_idx];
}

} // namespace omnisketch
