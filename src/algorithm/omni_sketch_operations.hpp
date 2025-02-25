#pragma once

#include "omni_sketch.hpp"

namespace omnisketch {

namespace Algorithm {

template <typename ValueType, typename RecordIdType, typename ContainerType>
CardEstResult<typename ContainerType::value_type>
ConjunctPointQueries(const std::vector<OmniSketch<ValueType, RecordIdType, ContainerType> *> &omni_sketches,
                     const std::vector<ValueType> &values) {
	assert(omni_sketches.size() == values.size());
	const size_t cell_count = omni_sketches.front()->Depth();
	std::vector<const MinHashSketch<ContainerType> *> all_min_hash_sketches;
	all_min_hash_sketches.reserve(cell_count * omni_sketches.size());
	std::vector<size_t> cell_idxs(cell_count);
	std::vector<const MinHashSketch<ContainerType> *> min_hash_sketches(cell_count);

	size_t n_max = 0;
	for (size_t query_idx = 0; query_idx < omni_sketches.size(); query_idx++) {
		auto &omni_sketch = *omni_sketches[query_idx];
		assert(omni_sketch.Depth() <= cell_count);

		omni_sketch.FindCellIndices(values[query_idx], cell_idxs);
		size_t n = omni_sketch.FindMinHashSketches(cell_idxs, min_hash_sketches);
		n_max = std::max(n, n_max);
		all_min_hash_sketches.insert(all_min_hash_sketches.end(), min_hash_sketches.begin(), min_hash_sketches.end());
	}

	const auto card_est =
	    OmniSketch<ValueType, RecordIdType, ContainerType>::EstimateCardinality(n_max, all_min_hash_sketches);
	return card_est;
}

template <typename ValueType, typename RecordIdType, typename ContainerType>
CardEstResult<typename ContainerType::value_type>
DisjunctPointQueries(const OmniSketch<ValueType, RecordIdType, ContainerType> &omni_sketch,
                     const std::vector<ValueType> &values) {
	std::vector<size_t> cell_idxs(omni_sketch.Depth());
	std::vector<const MinHashSketch<ContainerType> *> min_hash_sketches(omni_sketch.Depth());
	std::vector<CardEstResult<typename ContainerType::value_type>> intersect_results(values.size());
	std::vector<const MinHashSketch<std::set<typename ContainerType::value_type>> *> intersect_ptrs(values.size());

	CardEstResult<typename ContainerType::value_type> result;

	for (size_t query_idx = 0; query_idx < values.size(); query_idx++) {
		omni_sketch.FindCellIndices(values[query_idx], cell_idxs);
		size_t n_max = omni_sketch.FindMinHashSketches(cell_idxs, min_hash_sketches);
		intersect_results[query_idx] =
		    OmniSketch<ValueType, RecordIdType, ContainerType>::EstimateCardinality(n_max, min_hash_sketches);
		intersect_ptrs[query_idx] = &intersect_results[query_idx].min_hash_sketch;
		result.cardinality += intersect_results[query_idx].cardinality;
	}

	result.min_hash_sketch = MultiwayUnion(intersect_ptrs, omni_sketch.MinHashSampleCount());
	return result;
}

template <typename ContainerType>
struct SketchSet {
	SketchSet(size_t sketch_count) : sketches(sketch_count), iterators(sketch_count) {
	}

	void AddCell(const size_t idx, const OmniSketchCell<ContainerType> &cell) {
		sketches[idx] = &cell.GetMinHashSketch();
		iterators[idx] = sketches[idx]->hashes.cbegin();
		n_max = std::max(n_max, cell.RecordCount());
		max_hash = std::max(max_hash, *iterators[idx]);
	}

	static std::function<bool(const SketchSet &, const SketchSet &)> GetCompareFunction() {
		return [](const SketchSet &a, const SketchSet &b) {
			return a.max_hash > b.max_hash;
		};
	}

	std::vector<const MinHashSketch<ContainerType> *> sketches;
	std::vector<typename ContainerType::const_iterator> iterators;
	size_t n_max;
	typename ContainerType::value_type max_hash;
	size_t matches = 0;
};

template <typename ValueType, typename RecordIdType, typename ContainerType>
CardEstResult<typename ContainerType::value_type>
SetMembership(const OmniSketch<ValueType, RecordIdType, ContainerType> &sketch, const std::vector<ValueType> &values) {
	std::vector<SketchSet<ContainerType>> sketches_to_intersect(values.size(), sketch.Depth());

	CardEstResult<typename ContainerType::value_type> union_result;
	union_result.min_hash_sketch.max_count = sketch.MinHashSampleCount();
	const auto &cells = sketch.GetCells();

	for (size_t predicate_idx = 0; predicate_idx < values.size(); predicate_idx++) {
		auto hash = SplitHash(Hash(values[predicate_idx]));

		for (size_t row_idx = 0; row_idx < sketch.Depth(); row_idx++) {
			size_t col_idx = ComputeCellIdx(hash.first, hash.second, row_idx, sketch.Width());
			sketches_to_intersect[predicate_idx].AddCell(row_idx, cells[row_idx][col_idx]);
		}
	}

	std::vector<size_t> sketch_idxs(sketches_to_intersect.size());
	for (size_t i = 0; i < sketch_idxs.size(); i++) {
		sketch_idxs[i] = i;
	}

	uint64_t max_hash = std::numeric_limits<uint64_t>::max();
	size_t count = sketch_idxs.size();
	uint64_t current_hash_value;
	StepResult step_result;
	size_t steps = 0;

	while (count > 0) {
		size_t new_count = 0;
		for (size_t i = 0; i < count; i++) {
			const size_t sketch_idx = sketch_idxs[i];
			auto &min_hash_sketches = sketches_to_intersect[sketch_idx];
			step_result = Algorithm::MultiwayIntersectionStep(min_hash_sketches.sketches, min_hash_sketches.iterators,
			                                                  current_hash_value);

			if (step_result == StepResult::MATCH) {
				min_hash_sketches.matches++;
				union_result.min_hash_sketch.AddRecord(current_hash_value);
				if (union_result.min_hash_sketch.Size() == union_result.min_hash_sketch.max_count) {
					max_hash = *union_result.min_hash_sketch.hashes.crbegin();
				}
			}

			if (current_hash_value < max_hash && step_result != StepResult::DONE) {
				sketch_idxs[new_count++] = sketch_idx;
			}
		}
		count = new_count;
		steps++;
	}

	for (const auto &sketches : sketches_to_intersect) {
		if (sketches.matches > 0) {
			union_result.cardinality += (double)sketches.n_max / (double)steps * (double)sketches.matches;
		}
	}

	return union_result;
}

} // namespace Algorithm

} // namespace omnisketch
