#include "min_hash_sketch/intersect_min_hash_sketches.hpp"
#include "min_hash_sketch/min_hash_sketch_vector.hpp"

namespace omnisketch {

using ValiditySetter = void (*)(ValidityMask *, size_t);
inline void set_invalid(ValidityMask *mask, size_t i) {
	mask->SetInvalid(i);
}
inline void do_nothing(ValidityMask *, size_t) {
}

std::shared_ptr<MinHashSketch> ComputeIntersection(const std::vector<std::shared_ptr<MinHashSketch>> &sketches,
                                                   ValidityMask *mask, size_t max_sample_size) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");

	ValiditySetter setter = mask ? set_invalid : do_nothing;

	if (max_sample_size == 0) {
		max_sample_size = UINT64_MAX;
		for (const auto &sketch : sketches) {
			max_sample_size = std::min(max_sample_size, sketch->MaxCount());
		}
	}

	std::vector<std::unique_ptr<MinHashSketch::SketchIterator>> offsets;
	offsets.reserve(sketches.size());

	for (const auto &sketch : sketches) {
		offsets.push_back(sketch->Iterator(max_sample_size));
	}

	auto result =
	    std::make_shared<MinHashSketchVector>(max_sample_size, std::make_unique<ValidityMask>(max_sample_size));
	auto &result_data = result->Data();

	while (!offsets[0]->IsAtEnd()) {
		uint64_t current_hash = offsets[0]->Current();

		bool found_match = true;
		for (size_t sketch_idx = 1; sketch_idx < offsets.size(); sketch_idx++) {
			uint64_t other_hash = offsets[sketch_idx]->Current();
			while (!offsets[sketch_idx]->IsAtEnd() && other_hash < current_hash) {
				offsets[sketch_idx]->Next();
				other_hash = offsets[sketch_idx]->Current();
			}

			if (offsets[sketch_idx]->IsAtEnd()) {
				// There can be no other matches
				return result;
			}

			if (current_hash == other_hash) {
				// We have a match
				continue;
			}

			// No match, start from the beginning
			found_match = false;
			while (!offsets[0]->IsAtEnd() && offsets[0]->Current() < other_hash) {
				offsets[0]->Next();
			}
			// Back to the start
			break;
		}
		if (found_match) {
			result_data.push_back(current_hash);
			setter(mask, offsets[0]->CurrentIdx());
			offsets[0]->Next();
		}
	}

	return result;
}

} // namespace omnisketch
