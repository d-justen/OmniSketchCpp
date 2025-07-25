#include "omni_sketch/omni_sketch.hpp"

#include "omni_sketch/omni_sketch_cell.hpp"
#include "util/hash.hpp"

namespace omnisketch {

PointOmniSketch::PointOmniSketch(size_t width_p, size_t depth_p, size_t max_sample_count_p,
                                 std::shared_ptr<SetMembershipAlgorithm> set_membership_algo_p,
                                 std::shared_ptr<CellIdxMapper> hash_processor_p,
                                 const std::shared_ptr<MinHashSketch::SketchFactory> &factory)
    : width(width_p), depth(depth_p), max_sample_count(max_sample_count_p),
      set_membership_algo(std::move(set_membership_algo_p)), hash_processor(std::move(hash_processor_p)) {
	cells.resize(depth);
	for (auto &row : cells) {
		row.reserve(width);
		for (size_t col_idx = 0; col_idx < width; col_idx++) {
			row.emplace_back(std::make_shared<OmniSketchCell>(factory->Create(max_sample_count)));
		}
	}
}

PointOmniSketch::PointOmniSketch(size_t width, size_t depth, size_t max_sample_count_p)
    : PointOmniSketch(width, depth, max_sample_count_p, std::make_shared<ProbeAllSum>(),
                      std::make_shared<BarrettModSplitHashMapper>(width)) {
}

void PointOmniSketch::AddValueRecord(const Value &value, uint64_t record_id) {
	const uint64_t value_hash = value.GetHash();
	const uint64_t record_id_hash = hash_functions::Hash(record_id);
	AddRecordHashed(value_hash, record_id_hash);
}

size_t PointOmniSketch::RecordCount() const {
	return record_count;
}

std::shared_ptr<OmniSketchCell> PointOmniSketch::ProbeValue(const Value &value) const {
	std::vector<std::shared_ptr<OmniSketchCell>> matches(depth);

	return ProbeHash(value.GetHash(), matches);
}

// TODO(perf): use raw pointers for matches -> no more shared_ptr count increment
std::shared_ptr<OmniSketchCell>
PointOmniSketch::ProbeHash(uint64_t hash, std::vector<std::shared_ptr<OmniSketchCell>> &matches) const {
	assert(matches.size() == depth);
	assert(width == hash_processor->Width());
	hash_processor->SetHash(hash);
	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		const size_t col_idx = hash_processor->ComputeCellIdx(row_idx);
		matches[row_idx] = cells[row_idx][col_idx];
	}

	return OmniSketchCell::Intersect(matches);
}

std::shared_ptr<OmniSketchCell> PointOmniSketch::ProbeHashedSet(const std::shared_ptr<MinHashSketch> &values) const {
	std::vector<std::vector<std::shared_ptr<OmniSketchCell>>> matches(
	    values->Size(), std::vector<std::shared_ptr<OmniSketchCell>>(depth));

	auto value_it = values->Iterator();
	for (size_t value_idx = 0; value_idx < values->Size(); value_idx++) {
		const uint64_t hash = value_it->Current();
		value_it->Next();
		hash_processor->SetHash(hash);
		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			const size_t col_idx = hash_processor->ComputeCellIdx(row_idx);
			matches[value_idx][row_idx] = cells[row_idx][col_idx];
		}
	}

	return set_membership_algo->Execute(max_sample_count, matches);
}

std::shared_ptr<OmniSketchCell> PointOmniSketch::ProbeHashedSet(const std::shared_ptr<OmniSketchCell> &values) const {
	return ProbeHashedSet(values->GetMinHashSketch());
}

std::shared_ptr<OmniSketchCell> PointOmniSketch::ProbeValueSet(const ValueSet &values) const {
	const auto &hashes = values.GetHashes();
	std::vector<std::vector<std::shared_ptr<OmniSketchCell>>> matches(
	    hashes.size(), std::vector<std::shared_ptr<OmniSketchCell>>(depth));

	for (size_t value_idx = 0; value_idx < hashes.size(); value_idx++) {
		hash_processor->SetHash(hashes[value_idx]);
		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			const size_t col_idx = hash_processor->ComputeCellIdx(row_idx);
			matches[value_idx][row_idx] = cells[row_idx][col_idx];
		}
	}

	return set_membership_algo->Execute(max_sample_count, matches);
}

void PointOmniSketch::Flatten() {
	for (auto &row : cells) {
		for (auto &cell : row) {
			cell = cell->Flatten();
		}
	}
}

size_t PointOmniSketch::EstimateByteSize() const {
	/*const size_t members = 3 * sizeof(size_t) + sizeof(hash_processor) + sizeof(cells);
	size_t cell_size = 0;
	for (const auto &row : cells) {
	    cell_size += sizeof(row);
	    for (const auto &cell : row) {
	        cell_size += cell->EstimateByteSize();
	    }
	}

	return members + cell_size;*/
	size_t hash_count = 0;
	for (const auto &row : cells) {
		for (const auto &cell : row) {
			hash_count += cell->SampleCount();
		}
	}
	return hash_count * sizeof(uint64_t);
}

size_t PointOmniSketch::Depth() const {
	return depth;
}

size_t PointOmniSketch::Width() const {
	return width;
}

size_t PointOmniSketch::MinHashSketchSize() const {
	return max_sample_count;
}

std::shared_ptr<OmniSketchCell> PointOmniSketch::GetRids() const {
	return OmniSketchCell::Combine(cells.front());
}

void PointOmniSketch::Combine(const std::shared_ptr<OmniSketch> &other) {
	assert(other->Depth() == depth);
	assert(other->Width() == width);
	assert(other->MinHashSketchSize() == max_sample_count);

	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		for (size_t col_idx = 0; col_idx < width; col_idx++) {
			cells[row_idx][col_idx]->Combine(other->GetCell(row_idx, col_idx));
		}
	}

	record_count += other->RecordCount();
}

const OmniSketchCell &PointOmniSketch::GetCell(size_t row_idx, size_t col_idx) const {
	return *cells[row_idx][col_idx];
}

void PointOmniSketch::AddRecordHashed(uint64_t value_hash, uint64_t record_id_hash) {
	hash_processor->SetHash(value_hash);
	for (size_t row_idx = 0; row_idx < depth; row_idx++) {
		const size_t col_idx = hash_processor->ComputeCellIdx(row_idx);
		cells[row_idx][col_idx]->AddRecord(record_id_hash);
	}
	record_count++;
}

void PointOmniSketch::AddNullValues(size_t count) {
	record_count += count;
	null_count += count;
}

size_t PointOmniSketch::CountNulls() const {
	return null_count;
}

void PointOmniSketch::SetRecordCount(size_t record_count_p) {
	record_count = record_count_p;
}

void PointOmniSketch::SetCell(size_t row_idx, size_t col_idx, std::shared_ptr<OmniSketchCell> cell) {
	cells[row_idx][col_idx] = cell;
}

} // namespace omnisketch
