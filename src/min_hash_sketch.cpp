#include "min_hash_sketch.hpp"

#include <cassert>

namespace omnisketch {

void MinHashSketch::AddRecord(uint64_t hash, size_t max_sample_size) {
	if (rids.size() < max_sample_size) {
		rids.insert(hash);
	} else {
		if (hash < *rids.crbegin()) {
			rids.erase(std::prev(rids.cend()));
			rids.insert(hash);
		}
	}
}

size_t MinHashSketch::Size() const {
	return rids.size();
}

MinHashSketch MinHashSketch::ReduceSampleSize(const size_t new_sample_size) const {
	assert(rids.size() >= new_sample_size && "Reducing sample size requires rid count >= target samples.");
	MinHashSketch result;
	result.rids.insert(rids.cbegin(), std::next(rids.cbegin(), new_sample_size));
	return result;
}

MinHashSketch MinHashSketch::Intersect(const std::vector<const MinHashSketch *> &sketches) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");
	if (sketches.size() == 1) {
		return *sketches[0];
	}

	MinHashSketch result;
	const size_t sketch_count = sketches.size();

	std::vector<std::set<uint64_t>::iterator> offsets;
	offsets.reserve(sketch_count);
	for (size_t sketch_idx = 0; sketch_idx < sketch_count; sketch_idx++) {
		offsets.push_back(sketches[sketch_idx]->rids.cbegin());
	}

	const auto &max_rid_first_sketch = sketches[0]->rids.cend();
	while (offsets[0] != max_rid_first_sketch) {
		bool found = true;
		auto this_val = *offsets[0];
		for (size_t sketch_idx = 1; sketch_idx < sketch_count; sketch_idx++) {
			auto &other_offset = offsets[sketch_idx];
			auto other_end = sketches[sketch_idx]->rids.cend();

			while (other_offset != other_end) {
				if (*other_offset < this_val)
					++other_offset;
				else {
					break;
				}
			}

			if (other_offset == other_end) {
				// There can be no other matches
				return result;
			}
			if (this_val == *other_offset) {
				// It's a match
				++other_offset;
				continue;
			} else {
				// No match, skip to next rid geq the mismatch
				found = false;
				auto this_offset_end = sketches[0]->rids.cend();
				auto other_val = *other_offset;

				while (offsets[0] != this_offset_end) {
					if (*offsets[0] < other_val)
						++offsets[0];
					else {
						break;
					}
				}
				break;
			}
		}

		if (found) {
			result.rids.insert(this_val);
			++offsets[0];
		}
	}
	return result;
}
void MinHashSketch::Combine(const MinHashSketch &other, const size_t max_sample_size) {
	for (const auto hash : other.rids) {
		AddRecord(hash, max_sample_size);
	}
}

} // namespace omnisketch
