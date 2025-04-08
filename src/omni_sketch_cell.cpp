#include "include/omni_sketch_cell.hpp"

namespace omnisketch {

OmniSketchCell::OmniSketchCell(std::shared_ptr<MinHashSketch> min_hash_sketch_p, size_t record_count_p)
    : min_hash_sketch(std::move(min_hash_sketch_p)), record_count(record_count_p) {
}

OmniSketchCell::OmniSketchCell(std::shared_ptr<MinHashSketch> min_hash_sketch_p)
    : OmniSketchCell(std::move(min_hash_sketch_p), 0) {
}

OmniSketchCell::OmniSketchCell(size_t max_sample_count)
    : OmniSketchCell(std::make_shared<MinHashSketchSet>(max_sample_count)) {
}

OmniSketchCell::OmniSketchCell() : OmniSketchCell(std::make_shared<MinHashSketchSet>(0)) {
}

void OmniSketchCell::AddRecord(uint64_t hash) {
	min_hash_sketch->AddRecord(hash);
	record_count++;
}

size_t OmniSketchCell::RecordCount() const {
	return record_count;
}

size_t OmniSketchCell::SampleCount() const {
	return min_hash_sketch->Size();
}

size_t OmniSketchCell::MaxSampleCount() const {
	return min_hash_sketch->MaxCount();
}

void OmniSketchCell::Combine(const OmniSketchCell &other) {
	record_count += other.record_count;
	min_hash_sketch->Combine(*other.min_hash_sketch);
}

const std::shared_ptr<MinHashSketch> &OmniSketchCell::GetMinHashSketch() const {
	return min_hash_sketch;
}

std::shared_ptr<MinHashSketch> &OmniSketchCell::GetMinHashSketch() {
	return min_hash_sketch;
}

std::shared_ptr<OmniSketchCell> OmniSketchCell::Flatten() {
	return std::make_shared<OmniSketchCell>(min_hash_sketch->Flatten(), record_count);
}

void OmniSketchCell::SetRecordCount(size_t count) {
	record_count = count;
}

void OmniSketchCell::SetMinHashSketch(std::shared_ptr<MinHashSketch> sketch) {
	min_hash_sketch = std::move(sketch);
}

size_t OmniSketchCell::EstimateByteSize() const {
	if (min_hash_sketch) {
		return sizeof(size_t) + sizeof(std::shared_ptr<MinHashSketch>) + min_hash_sketch->EstimateByteSize();
	}
	return sizeof(size_t) + sizeof(std::shared_ptr<MinHashSketch>);
}

std::shared_ptr<OmniSketchCell> OmniSketchCell::Intersect(const std::vector<std::shared_ptr<OmniSketchCell>> &cells) {
	assert(!cells.empty());
	std::vector<std::shared_ptr<MinHashSketch>> sketches;
	sketches.reserve(cells.size());

	size_t n_max = 0;
	size_t n_min = UINT64_MAX;
	size_t sample_count = 0;
	for (const auto &cell : cells) {
		if (cell->RecordCount() > n_max) {
			n_max = cell->RecordCount();
			sample_count = cell->SampleCount();
		}
		n_max = std::max(n_max, cell->RecordCount());
		n_min = std::min(n_min, cell->RecordCount());
		sketches.push_back(cell->GetMinHashSketch());
	}

	auto result = std::make_shared<OmniSketchCell>(sketches.front()->Intersect(sketches));
	const double card_est =
	    std::min((double)n_min, (double)n_max / (double)sample_count * (double)result->SampleCount());
	result->SetRecordCount((size_t)std::round(card_est));

	return result;
}

std::shared_ptr<OmniSketchCell> OmniSketchCell::Combine(const std::vector<std::shared_ptr<OmniSketchCell>> &cells) {
	assert(!cells.empty());
	std::vector<std::shared_ptr<MinHashSketch>> sketches;
	sketches.reserve(cells.size());
	size_t combined_card = 0;
	for (const auto &cell : cells) {
		sketches.push_back(cell->GetMinHashSketch());
		combined_card += cell->RecordCount();
	}
	auto result = std::make_shared<OmniSketchCell>(sketches.front()->Combine(sketches));
	result->SetRecordCount(combined_card);
	return result;
}
double OmniSketchCell::SamplingProbability() const {
	return (double)min_hash_sketch->Size() / (double)record_count;
}

} // namespace omnisketch
