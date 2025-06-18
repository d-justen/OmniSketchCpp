#pragma once

#include "omni_sketch.hpp"
#include "min_hash_sketch/min_hash_sketch_map.hpp"
#include "min_hash_sketch/min_hash_sketch_vector.hpp"

namespace omnisketch {

template <typename T>
class ForeignSortedOmniSketch : public PointOmniSketch {
public:
	ForeignSortedOmniSketch(std::shared_ptr<OmniSketch> sketch, size_t width_p, size_t depth_p,
	                        size_t max_sample_count_p, std::shared_ptr<HashFunction<T>> hash_function_p,
	                        std::shared_ptr<SetMembershipAlgorithm> set_membership_algo_p,
	                        std::shared_ptr<CellIdxMapper> hash_processor_p)
	    : PointOmniSketch(width_p, depth_p, max_sample_count_p, std::move(set_membership_algo_p),
	                      std::move(hash_processor_p), std::make_shared<MinHashSketchMap::SketchFactory>()),
	      referenced_sketch(std::move(sketch)), hf(std::move(hash_function_p)) {
		probe_buffer.resize(referenced_sketch->Depth());
	}

	ForeignSortedOmniSketch(std::shared_ptr<OmniSketch> sketch, size_t width, size_t depth, size_t max_sample_count_p)
	    : ForeignSortedOmniSketch(std::move(sketch), width, depth, max_sample_count_p,
	                              std::make_shared<MurmurHashFunction<T>>(), std::make_shared<ProbeAllSum>(),
	                              std::make_shared<BarrettModSplitHashMapper>(width)) {
	}

	void AddValueRecord(const Value &value, uint64_t record_id) override {
		AddRecordHashed(value.GetHash(), hf->HashRid(record_id));
	}

	void AddRecordHashed(uint64_t value_hash, uint64_t record_id_hash) override {
		auto probe_result = referenced_sketch->ProbeHash(record_id_hash, probe_buffer);
		hash_processor->SetHash(value_hash);
		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			const size_t col_idx = hash_processor->ComputeCellIdx(row_idx);
			const auto &cell = cells[row_idx][col_idx];
			auto *sketch_map = dynamic_cast<MinHashSketchMap *>(cell->GetMinHashSketch().get());
			if (probe_result->RecordCount() > 0) {
				sketch_map->AddRecord(probe_result->GetMinHashSketch()->Iterator()->Current(), record_id_hash);
			} else {
				// TODO: try other ways? Here: If PK is not in Ref-OS, assign random* hash to represent non-present PKs
				sketch_map->AddRecord(value_hash, record_id_hash);
			}
			cell->SetRecordCount(cell->RecordCount() + 1);
		}
		record_count += probe_result->RecordCount();
	}

	void AddRecord(const T &value, uint64_t record_id) {
		min = std::min(min, value);
		max = std::max(max, value);
		AddRecordHashed(hf->Hash(value), hf->HashRid(record_id));
	}

	std::shared_ptr<OmniSketchCell> Probe(const T &value) const {
		std::vector<std::shared_ptr<OmniSketchCell>> matches(depth);
		return PointOmniSketch::ProbeHash(hf->Hash(value), matches);
	}

	std::shared_ptr<OmniSketchCell> ProbeSet(const T *values, size_t count) const {
		std::vector<uint64_t> hashes;
		hashes.reserve(count);

		for (size_t value_idx = 0; value_idx < count; value_idx++) {
			hashes.push_back(hf->Hash(values[value_idx]));
		}

		return PointOmniSketch::ProbeHashedSet(std::make_shared<MinHashSketchVector>(hashes));
	}

	std::shared_ptr<OmniSketchCell> ProbeRange(const T &lower_bound, const T &upper_bound) const {
		std::vector<uint64_t> hashes;
		hashes.reserve((upper_bound - lower_bound) + 1);
		for (T value = lower_bound; value <= upper_bound; value++) {
			hashes.push_back(hf->Hash(value));
		}

		return PointOmniSketch::ProbeHashedSet(std::make_shared<MinHashSketchVector>(hashes));
	}

	T GetMin() const {
		return min;
	}

	T GetMax() const {
		return max;
	}

	OmniSketchType Type() const override {
		return OmniSketchType::FOREIGN_SORTED;
	}

	std::shared_ptr<OmniSketch> MultiplyRecordCounts(const std::shared_ptr<OmniSketch> &other,
	                                                 double multiplier) const override {
		auto result = std::make_shared<ForeignSortedOmniSketch<T>>(referenced_sketch, width, depth, max_sample_count,
		                                                           hf, set_membership_algo, hash_processor);
		result->record_count = UINT64_MAX;
		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			size_t current_record_count = 0;
			for (size_t col_idx = 0; col_idx < width; col_idx++) {
				auto& this_cell = PointOmniSketch::GetCell(row_idx, col_idx);
				auto& other_cell = other->GetCell(row_idx, col_idx);
				if (this_cell.RecordCount() > 0 && other_cell.RecordCount() > 0) {
					auto& result_cell = result->cells[row_idx][col_idx];
					result_cell->GetMinHashSketch() = this_cell.GetMinHashSketch();
					result_cell->SetRecordCount(std::max((double)this_cell.RecordCount(), this_cell.RecordCount() * other_cell.RecordCount() * multiplier));
					current_record_count += result_cell->RecordCount();
				}
			}
			result->record_count = std::min(result->record_count, current_record_count);
		}
		return result;
	}

protected:
	std::shared_ptr<OmniSketch> referenced_sketch;
	std::vector<std::shared_ptr<OmniSketchCell>> probe_buffer;
	std::shared_ptr<HashFunction<T>> hf;
	T min = std::numeric_limits<T>::max();
	T max = std::numeric_limits<T>::min();
};

} // namespace omnisketch
