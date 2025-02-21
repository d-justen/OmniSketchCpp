#include "algorithm.hpp"

#include <numeric>
#include <queue>

namespace omnisketch {

size_t Algorithm::GetResultCellCount(const std::vector<const OmniSketch *> &sketches) {
	size_t count = 0;
	for (const auto *sketch : sketches) {
		count += sketch->Depth();
	}
	return count;
}

CardEstResult Algorithm::Conjunction(const std::vector<const OmniSketch *> &sketches,
                                     const std::vector<uint64_t> &hashes) {
	assert(sketches.size() == hashes.size());
	assert(!sketches.empty());

	const size_t result_cell_count = GetResultCellCount(sketches);
	std::vector<const OmniSketchCell *> cells;
	cells.reserve(result_cell_count);
	std::vector<const MinHashSketch<uint64_t> *> min_hash_sketches;
	min_hash_sketches.reserve(result_cell_count);

	for (size_t predicate_idx = 0; predicate_idx < sketches.size(); predicate_idx++) {
		sketches[predicate_idx]->GetCells(hashes[predicate_idx], cells);
	}

	size_t n_max = 0, sample_count = 0;

	for (const auto *cell : cells) {
		min_hash_sketches.push_back(&cell->GetMinHashSketch());
		if (cell->RecordCount() > n_max) {
			n_max = cell->RecordCount();
			sample_count = cell->SampleCount();
		}
	}

	CardEstResult result;
	result.max_sample_size = sample_count;
	result.min_hash_sketch = MinHashSketch<uint64_t>::Intersect(min_hash_sketches);
	result.cardinality = OmniSketch::EstimateCardinality(result.min_hash_sketch, n_max, result.max_sample_size);
	return result;
}

CardEstResult Algorithm::Disjunction(const std::vector<const OmniSketch *> &sketches,
                                     const std::vector<uint64_t> &hashes) {
	assert(sketches.size() == hashes.size());
	assert(!sketches.empty());
	const size_t omni_sketch_depth = sketches.front()->Depth();

	std::vector<CardEstResult> intersections;
	intersections.reserve(sketches.size());

	std::vector<const MinHashSketch<uint64_t> *> min_hash_sketches;
	min_hash_sketches.reserve(sketches.size());

	CardEstResult union_result;
	std::vector<const MinHashSketch<uint64_t> *> sketches_to_intersect(omni_sketch_depth);

	for (size_t predicate_idx = 0; predicate_idx < sketches.size(); predicate_idx++) {
		const auto *omni_sketch = sketches[predicate_idx];
		auto hash = SplitHash(hashes[predicate_idx]);

		size_t n_max = 0;

		assert(omni_sketch_depth == omni_sketch->Depth());
		for (size_t row_idx = 0; row_idx < omni_sketch->Depth(); row_idx++) {
			size_t col_idx = ComputeCellIdx(hash.first, hash.second, row_idx, omni_sketch->Width());
			const auto &cell = omni_sketch->cells[row_idx][col_idx];
			n_max = std::max(n_max, cell.RecordCount());
			sketches_to_intersect[row_idx] = &cell.GetMinHashSketch();
		}

		CardEstResult result;
		result.min_hash_sketch = MinHashSketch<uint64_t>::Intersect(sketches_to_intersect);
		result.cardinality =
		    OmniSketch::EstimateCardinality(result.min_hash_sketch, n_max, omni_sketch->MinHashSampleCount());
		result.max_sample_size = omni_sketch->MinHashSampleCount();

		intersections.push_back(std::move(result));
		auto &intersection = intersections.back();
		min_hash_sketches.push_back(&intersection.min_hash_sketch);
		union_result.max_sample_size = std::max(intersection.max_sample_size, union_result.max_sample_size);
		union_result.cardinality += intersection.cardinality;
	}

	union_result.min_hash_sketch = MinHashSketch<uint64_t>::Union(min_hash_sketches);
	union_result.max_sample_size = hashes.size() * sketches.front()->MinHashSampleCount();

	return union_result;
}

CardEstResult Algorithm::UnionByRow(const OmniSketch &sketch, const std::vector<uint64_t> &hashes) {
	const size_t probe_count = hashes.size();
	const size_t union_sample_count = 32;
	std::vector<CardEstResult> unions(sketch.Depth());

	std::vector<const MinHashSketch *> union_sketches;
	union_sketches.reserve(sketch.Depth());

	std::vector<std::pair<uint32_t, uint32_t>> hash_pairs;
	hash_pairs.reserve(probe_count);

	for (auto hash : hashes) {
		hash_pairs.emplace_back(SplitHash(hash));
	}

	size_t n_max = 0;
	std::vector<const MinHashSketch *> min_hash_sketches(probe_count);

	for (size_t row_idx = 0; row_idx < sketch.Depth(); row_idx++) {
		for (size_t hash_idx = 0; hash_idx < hash_pairs.size(); hash_idx++) {
			const auto &hash_pair = hash_pairs[hash_idx];
			size_t col_idx = ComputeCellIdx(hash_pair.first, hash_pair.second, row_idx, sketch.Width());
			const auto &cell = sketch.cells[row_idx][col_idx];
			min_hash_sketches[hash_idx] = &cell.GetMinHashSketch();
			unions[row_idx].cardinality += cell.RecordCount();
		}

		n_max = std::max(n_max, (size_t)unions[row_idx].cardinality);
		unions[row_idx].min_hash_sketch = MinHashSketch::Union(min_hash_sketches, union_sample_count);
		union_sketches.push_back(&unions[row_idx].min_hash_sketch);
	}

	CardEstResult intersect_result;
	intersect_result.max_sample_size = union_sample_count;
	intersect_result.min_hash_sketch = MinHashSketch::Intersect(union_sketches);
	intersect_result.cardinality =
	    OmniSketch::EstimateCardinality(intersect_result.min_hash_sketch, n_max, union_sample_count);

	return intersect_result;
}

CardEstResult Algorithm::SegmentedUnionByRow(const OmniSketch &sketch, const std::vector<uint64_t> &hashes) {
	const size_t batch_count = hashes.size() / 64;
	const size_t probe_count = hashes.size() / batch_count;

	std::vector<CardEstResult> unions(batch_count);
	std::vector<const MinHashSketch *> union_sketches;
	union_sketches.reserve(batch_count);

	CardEstResult result;

	std::vector<uint64_t> hash_batch(probe_count);
	for (size_t batch_idx = 0; batch_idx < batch_count; batch_idx++) {
		for (size_t hash_idx = 0; hash_idx < probe_count; hash_idx++) {
			hash_batch[hash_idx] = hashes[batch_idx * probe_count + hash_idx];
		}

		unions[batch_idx] = UnionByRow(sketch, hash_batch);
		union_sketches.push_back(&unions[batch_idx].min_hash_sketch);
		result.cardinality += unions[batch_idx].cardinality;
	}

	// result.min_hash_sketch = MinHashSketch::Union(union_sketches, probe_count);
	// result.max_sample_size = probe_count

	return result;
}

CardEstResult Algorithm::DisjunctionEarlyExit(const std::vector<const OmniSketch *> &sketches,
                                              const std::vector<uint64_t> &hashes) {
	assert(sketches.size() == hashes.size());
	assert(!sketches.empty());
	const size_t omni_sketch_depth = sketches.front()->Depth();

	CardEstResult union_result;
	union_result.max_sample_size = sketches.front()->MinHashSampleCount();

	std::vector<const MinHashSketch *> sketches_to_intersect(omni_sketch_depth);

	for (size_t predicate_idx = 0; predicate_idx < sketches.size(); predicate_idx++) {
		const auto *omni_sketch = sketches[predicate_idx];
		auto hash = SplitHash(hashes[predicate_idx]);

		size_t n_max = 0;
		assert(omni_sketch_depth == omni_sketch->Depth());
		for (size_t row_idx = 0; row_idx < omni_sketch->Depth(); row_idx++) {
			size_t col_idx = ComputeCellIdx(hash.first, hash.second, row_idx, omni_sketch->Width());
			const auto &cell = omni_sketch->cells[row_idx][col_idx];
			n_max = std::max(n_max, cell.RecordCount());
			sketches_to_intersect[row_idx] = &cell.GetMinHashSketch();
		}

		CardEstResult result;
		if (union_result.min_hash_sketch.Size() == union_result.max_sample_size) {
			result.min_hash_sketch =
			    MinHashSketch::Intersect(sketches_to_intersect, *union_result.min_hash_sketch.GetRids().crbegin());
			// Take the average cardinality so far
			result.cardinality = union_result.cardinality / double(predicate_idx + 1);
		} else {
			result.min_hash_sketch = MinHashSketch::Intersect(sketches_to_intersect);
			result.cardinality =
			    OmniSketch::EstimateCardinality(result.min_hash_sketch, n_max, union_result.max_sample_size);
		}

		union_result.cardinality += result.cardinality;
		union_result.min_hash_sketch.Combine(result.min_hash_sketch, union_result.max_sample_size);
	}

	return union_result;
}

struct SketchSet {
	SketchSet(size_t sketch_count) : sketches(sketch_count), iterators(sketch_count) {
	}

	void AddCell(const size_t idx, const OmniSketchCell &cell) {
		sketches[idx] = &cell.GetMinHashSketch();
		iterators[idx] = sketches[idx]->GetRids().cbegin();
		n_max = std::max(n_max, cell.RecordCount());
		max_hash = std::max(max_hash, *iterators[idx]);
	}

	static std::function<bool(const SketchSet &, const SketchSet &)> GetCompareFunction() {
		return [](const SketchSet &a, const SketchSet &b) {
			return a.max_hash > b.max_hash;
		};
	}

	std::vector<const MinHashSketch *> sketches;
	std::vector<std::set<uint64_t>::iterator> iterators;
	size_t n_max;
	uint64_t max_hash;
	size_t matches = 0;
};

CardEstResult Algorithm::PartialIntersectUnion(const OmniSketch &sketch, const std::vector<uint64_t> &hashes) {
	std::vector<SketchSet> sketches_to_intersect(hashes.size(), sketch.Depth());

	CardEstResult union_result;
	union_result.max_sample_size = sketch.MinHashSampleCount();

	for (size_t predicate_idx = 0; predicate_idx < hashes.size(); predicate_idx++) {
		auto hash = SplitHash(hashes[predicate_idx]);

		for (size_t row_idx = 0; row_idx < sketch.Depth(); row_idx++) {
			size_t col_idx = ComputeCellIdx(hash.first, hash.second, row_idx, sketch.Width());
			sketches_to_intersect[predicate_idx].AddCell(row_idx, sketch.cells[row_idx][col_idx]);
		}
	}
/*
	std::priority_queue<SketchSet, std::vector<SketchSet>, decltype(SketchSet::GetCompareFunction())>
	    iterator_queue(SketchSet::GetCompareFunction(), sketches_to_intersect);

	uint64_t max_hash = std::numeric_limits<uint64_t>::max();
	uint64_t current_hash_value;
	bool found_match;

	while (union_result.min_hash_sketch.Size() < union_result.max_sample_size && !iterator_queue.empty()) {
		auto sketch_set = iterator_queue.top();
		iterator_queue.pop();
		found_match = MinHashSketch::IntersectOneStep(sketch_set.sketches, max_hash,
		                                              sketch_set.iterators, current_hash_value);

		if (found_match) {
			sketch_set.matches++;
			union_result.min_hash_sketch.rids.insert(current_hash_value);
		}

		if (current_hash_value < max_hash) {
			sketch_set.max_hash = *sketch_set.iterators[0];
			iterator_queue.push(sketch_set);
		}
	}
*/
	std::vector<size_t> sketch_idxs(sketches_to_intersect.size());
	for (size_t i = 0; i < sketch_idxs.size(); i++) {
		sketch_idxs[i] = i;
	}

	uint64_t max_hash = std::numeric_limits<uint64_t>::max();
	size_t count = sketch_idxs.size();
	uint64_t current_hash_value;
	bool found_match;
	size_t steps = 0;

	while (count > 0) {
		size_t new_count = 0;
		for (size_t i = 0; i < count; i++) {
			const size_t sketch_idx = sketch_idxs[i];
			auto &min_hash_sketches = sketches_to_intersect[sketch_idx];
			found_match = MinHashSketch::IntersectOneStep(min_hash_sketches.sketches, max_hash,
			                                              min_hash_sketches.iterators, current_hash_value);

			if (found_match) {
				min_hash_sketches.matches++;
				union_result.min_hash_sketch.AddRecord(current_hash_value, union_result.max_sample_size);
				if (union_result.min_hash_sketch.Size() == union_result.max_sample_size) {
					max_hash = *union_result.min_hash_sketch.rids.crbegin();
				}
			}

			if (current_hash_value < max_hash) {
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

} // namespace omnisketch
