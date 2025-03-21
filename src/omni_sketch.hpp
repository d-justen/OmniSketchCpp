#pragma once

#include "hash.hpp"
#include "omni_sketch_cell.hpp"
#include "algorithm/min_hash_set_operations.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>

namespace omnisketch {

template <typename HashType>
struct CardEstResult {
	CardEstResult() = default;

	explicit CardEstResult(const size_t max_sample_count) : min_hash_sketch(max_sample_count) {
	}

	MinHashSketch<std::set<HashType>> min_hash_sketch;
	double cardinality = 0;
};

template <typename ValueType, typename RecordIdType, typename HashContainerType>
class OmniSketch {
public:
	using OmniSketchCellVector = std::vector<OmniSketchCell<HashContainerType>>;
	using FlattenedContainerType = std::vector<typename HashContainerType::value_type>;

	OmniSketch(size_t width_p, size_t depth_p, size_t min_hash_sample_count_p, bool is_flattened_p = false)
	    : width(width_p), depth(depth_p), min_hash_sample_count(min_hash_sample_count_p), record_count(0),
	      is_flattened(is_flattened_p) {
		cells.resize(depth);
		for (auto &row : cells) {
			row.reserve(width);
			for (size_t col_idx = 0; col_idx < width; col_idx++) {
				row.emplace_back(OmniSketchCell<HashContainerType>(min_hash_sample_count));
			}
		}
	}

	explicit OmniSketch(std::vector<OmniSketchCellVector> cells_p)
	    : cells(std::move(cells_p)), depth(cells.size()), width(cells.empty() ? 0 : cells.front().size()),
	      min_hash_sample_count(cells.empty() || cells.front().empty() ? 0 : cells.front().front().MaxSampleCount()),
	      record_count(0) {
	}

	virtual ~OmniSketch() = default;

	void AddRecord(const ValueType &value, const RecordIdType &rid) {
		auto hash_pair = SplitHash(Hash(value));
		typename HashContainerType::value_type rid_hash = Hash(rid);

		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			size_t col_idx = ComputeCellIdx(hash_pair.first, hash_pair.second, row_idx, width);
			cells[row_idx][col_idx].AddRecord(rid_hash);
		}
		record_count++;
	}

	CardEstResult<typename HashContainerType::value_type> EstimateCardinality(const ValueType &value) const {
		std::vector<size_t> cell_idxs(depth);
		FindCellIndices(value, cell_idxs);

		std::vector<const MinHashSketch<HashContainerType> *> min_hash_sketches(depth);
		size_t n_max = FindMinHashSketches(cell_idxs, min_hash_sketches);

		return EstimateCardinality(n_max, min_hash_sketches);
	}

	static CardEstResult<typename HashContainerType::value_type>
	EstimateCardinality(size_t n_max, std::vector<const MinHashSketch<HashContainerType> *> &min_hash_sketches) {
		assert(min_hash_sketches.size() > 0);
		auto min_hash_sample_count = min_hash_sketches.front()->max_count;

		CardEstResult<typename HashContainerType::value_type> result(min_hash_sample_count);
		result.min_hash_sketch = Algorithm::MultiwayIntersection(min_hash_sketches);

		if (n_max < min_hash_sample_count) {
			// Record ids are fully contained in sketch -> no scaling by sample size needed
			result.cardinality = result.min_hash_sketch.Size();
			return result;
		}

		result.cardinality = MinHashSketch<std::set<typename HashContainerType::value_type>>::EstimateCardinality(
		    result.min_hash_sketch, n_max);
		return result;
	}

	void FindCellIndices(const ValueType &value, std::vector<size_t> &result) const {
		FindCellIndicesWithHash(Hash(value), result);
	}

	void FindCellIndicesWithHash(const typename HashContainerType::value_type &hash,
	                             std::vector<size_t> &result) const {
		assert(result.size() >= depth);
		auto hash_pair = SplitHash(hash);

		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			result[row_idx] = ComputeCellIdx(hash_pair.first, hash_pair.second, row_idx, width);
		}
	}

	size_t FindMinHashSketches(std::vector<size_t> &cell_idxs,
	                           std::vector<const MinHashSketch<HashContainerType> *> &min_hash_sketches) const {
		size_t n_max = 0;
		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			const size_t cell_idx = cell_idxs[row_idx];
			const auto &cell = cells[row_idx][cell_idx];
			min_hash_sketches[row_idx] = &cell.GetMinHashSketch();
			n_max = std::max(n_max, cell.RecordCount());
		}
		return n_max;
	}

	inline size_t RecordCount() const {
		return record_count;
	}

	inline size_t MinHashSampleCount() const {
		return min_hash_sample_count;
	}

	inline size_t Width() const {
		return width;
	}

	inline size_t Depth() const {
		return depth;
	}

	size_t EstimateByteSize() const {
		// Vector overhead
		size_t size = sizeof(cells) + sizeof(*cells.begin()) * cells.capacity();
		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			// OmniSketchCell object overhead
			size += cells[row_idx].capacity() * sizeof(OmniSketchCell<HashContainerType>);
			for (size_t col_idx = 0; col_idx < width; col_idx++) {
				const auto &cell = cells[row_idx][col_idx];
				const auto &min_hash_sketch = cell.GetMinHashSketch();
				// MinHashSketch object overhead
				size += sizeof(min_hash_sketch);
				// Record Id Hash Container
				size +=
				    sizeof(min_hash_sketch.hashes) + min_hash_sketch.Size() * sizeof(*min_hash_sketch.hashes.begin());
			}
		}
		return size;
	}

	void Combine(const OmniSketch &other) {
		assert(other.depth == depth);
		assert(other.width == width);
		assert(other.min_hash_sample_count == min_hash_sample_count);

		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			for (size_t col_idx = 0; col_idx < width; col_idx++) {
				cells[row_idx][col_idx].Combine(other.cells[row_idx][col_idx], min_hash_sample_count);
			}
		}

		record_count += other.record_count;
	}

	OmniSketch<ValueType, RecordIdType, FlattenedContainerType> Flatten() {
		std::vector<std::vector<OmniSketchCell<FlattenedContainerType>>> result_cells(depth);
		for (size_t row_idx = 0; row_idx < depth; row_idx++) {
			auto &row = result_cells[row_idx];
			row.reserve(width);
			for (size_t col_idx = 0; col_idx < width; col_idx++) {
				row.emplace_back(cells[row_idx][col_idx].Flatten());
			}
		}

		return OmniSketch<ValueType, RecordIdType, FlattenedContainerType>(result_cells);
	}

	std::vector<OmniSketchCellVector> &GetCells() {
		return cells;
	}

	const std::vector<OmniSketchCellVector> &GetCells() const {
		return cells;
	}

protected:
	std::vector<OmniSketchCellVector> cells;
	const size_t width;
	const size_t depth;
	const size_t min_hash_sample_count;
	size_t record_count;
	bool is_flattened = false;
};

} // namespace omnisketch
