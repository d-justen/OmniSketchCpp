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
	typename ContainerType::value_type max_hash;
	size_t n_max = 0;
	size_t first_it_offset = 0;
};

static constexpr double EXCLUSION_THRESHOLD = 0.1;

template <typename ValueType, typename RecordIdType, typename ContainerType>
CardEstResult<typename ContainerType::value_type>
SetMembership(const OmniSketch<ValueType, RecordIdType, ContainerType> &sketch, const std::vector<ValueType> &values) {
	const size_t intersection_count = values.size();
	const size_t min_hash_sample_count = sketch.MinHashSampleCount();

	std::vector<SketchSet<ContainerType>> intersect_sketches(intersection_count, sketch.Depth());
	const auto &cells = sketch.GetCells();

	for (size_t predicate_idx = 0; predicate_idx < values.size(); predicate_idx++) {
		auto hash = SplitHash(Hash(values[predicate_idx]));

		for (size_t row_idx = 0; row_idx < sketch.Depth(); row_idx++) {
			size_t col_idx = ComputeCellIdx(hash.first, hash.second, row_idx, sketch.Width());
			intersect_sketches[predicate_idx].AddCell(row_idx, cells[row_idx][col_idx]);
		}
	}

	CardEstResult<typename ContainerType::value_type> union_result;
	union_result.min_hash_sketch.max_count = min_hash_sample_count;

	std::vector<size_t> cardinality_estimates(min_hash_sample_count);
	std::vector<size_t> exclusion_offset_counts(min_hash_sample_count);

	size_t count = intersection_count;
	std::vector<size_t> sketch_idxs(count);
	for (size_t i = 0; i < count; i++) {
		sketch_idxs[i] = i;
	}

	auto max_hash = std::numeric_limits<typename ContainerType::value_type>::max();

	while (count > 0) {
		size_t next_count = 0;
		for (size_t intersect_idx = 0; intersect_idx < count; intersect_idx++) {
			const size_t sketch_idx = sketch_idxs[intersect_idx];
			auto &min_hash_sketches = intersect_sketches[sketch_idx];
			const size_t offset_before = min_hash_sketches.first_it_offset;
			typename ContainerType::value_type current_hash_value;
			const auto step_result =
			    Algorithm::MultiwayIntersectionStep(min_hash_sketches.sketches, min_hash_sketches.iterators,
			                                        current_hash_value, min_hash_sketches.first_it_offset);

			if (step_result == StepResult::MATCH) {
				union_result.min_hash_sketch.AddRecord(current_hash_value);
				if (union_result.min_hash_sketch.Size() == union_result.min_hash_sketch.max_count) {
					max_hash = *union_result.min_hash_sketch.hashes.crbegin();
				}
				cardinality_estimates[offset_before] += min_hash_sketches.n_max;
			}

			if (step_result != StepResult::DONE) {
				if (current_hash_value < max_hash) {
					// There could be another match, look at this offset next round
					sketch_idxs[next_count++] = sketch_idx;
				} else if (min_hash_sketches.first_it_offset < exclusion_offset_counts.size()) {
					// There might be another match, but it would be > max_hash, so we exclude it
					exclusion_offset_counts[min_hash_sketches.first_it_offset]++;
				}
			}
		}
		count = next_count;
	}

	size_t denom = 0;
	size_t current_exclusion_count = 0;
	for (size_t intersection_idx = 0; intersection_idx < cardinality_estimates.size(); intersection_idx++) {
		current_exclusion_count += exclusion_offset_counts[intersection_idx];
		if ((double)current_exclusion_count > intersection_count * EXCLUSION_THRESHOLD) {
			assert(intersection_idx > 0);
			break;
		}
		denom++;
		double scale_up = intersection_count / ((double)intersection_count - (double)current_exclusion_count);
		double scaled_card_est = (double)cardinality_estimates[intersection_idx] * scale_up;
		union_result.cardinality += scaled_card_est;
	}

	if (union_result.cardinality > 0) {
		union_result.cardinality /= (double)denom;
	}

	return union_result;
}

} // namespace Algorithm

} // namespace omnisketch
