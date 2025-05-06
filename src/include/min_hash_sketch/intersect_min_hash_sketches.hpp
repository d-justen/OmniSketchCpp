#pragma once

#include "min_hash_sketch_vector.hpp"

namespace omnisketch {
/*
enum class StepResult : uint8_t { MATCH, NO_MATCH, DONE };

template <typename T>
StepResult MultiwayIntersectionStep(const std::vector<typename T::const_iterator> &hash_set_ends,
                                    std::vector<typename T::const_iterator> &offsets, uint64_t &current_val,
                                    size_t &first_it_offset) {
    assert(!hash_set_ends.empty() && "Sketch vector to intersect must not be empty.");

    const auto &first_sketch_end = hash_set_ends.front();

    if (offsets[0] != first_sketch_end) {
        current_val = *offsets[0];

        for (size_t sketch_idx = 1; sketch_idx < offsets.size(); sketch_idx++) {
            auto &other_offset = offsets[sketch_idx];
            auto other_end = hash_set_ends[sketch_idx];

            while (other_offset != other_end && *other_offset < current_val) {
                ++other_offset;
            }

            if (other_offset == other_end) {
                // There can be no other matches
                return StepResult::DONE;
            }
            if (current_val == *other_offset) {
                // It's a match
                ++other_offset;
            } else {
                // No match, skip to next rid geq the mismatch
                current_val = *other_offset;
                while (offsets[0] != first_sketch_end && *offsets[0] < current_val) {
                    ++offsets[0];
                    ++first_it_offset;
                }
                if (offsets[0] != first_sketch_end) {
                    current_val = *offsets[0];
                    return StepResult::NO_MATCH;
                } else {
                    return StepResult::DONE;
                }
            }
        }

        ++offsets[0];
        ++first_it_offset;
        return StepResult::MATCH;
    }
    return StepResult::DONE;
}

template <typename T, typename U>
std::shared_ptr<MinHashSketch> ComputeIntersection(const std::vector<std::shared_ptr<MinHashSketch>> &sketches,
                                                   size_t max_match_count = 0) {
    assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");

    std::vector<typename U::const_iterator> offsets;
    offsets.reserve(sketches.size());
    std::vector<typename U::const_iterator> set_ends;
    set_ends.reserve(sketches.size());

    for (const auto &sketch : sketches) {
        auto *typed = dynamic_cast<T *>(sketch.get());
        assert(typed);
        offsets.push_back(typed->Data().cbegin());
        set_ends.push_back(typed->Data().cend());
    }

    auto result = std::make_shared<MinHashSketchSet>(sketches.front()->MaxCount());
    if (max_match_count == 0) {
        max_match_count = result->MaxCount();
    }

    const size_t step_count = sketches.front()->Size();
    uint64_t current_hash;
    StepResult step_result;
    size_t dummy_offset = 0;

    auto &result_data = result->Data();
    for (size_t i = 0; i < step_count; i++) {
        step_result = MultiwayIntersectionStep<U>(set_ends, offsets, current_hash, dummy_offset);
        if (step_result == StepResult::MATCH) {
            result_data.insert(current_hash);
            if (result_data.size() == max_match_count) {
                break;
            }
        } else if (step_result == StepResult::DONE) {
            break;
        }
    }

    return result;
}*/

using ValiditySetter = void (*)(ValidityMask *, size_t);
inline void set_invalid(ValidityMask *mask, size_t i) {
	mask->SetInvalid(i);
}
inline void do_nothing(ValidityMask *, size_t) {
}

template <typename T, typename U>
std::shared_ptr<MinHashSketch> ComputeIntersection(const std::vector<std::shared_ptr<MinHashSketch>> &sketches,
                                                   ValidityMask *mask = nullptr, size_t max_sample_size = 0) {
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

	auto result = std::make_shared<MinHashSketchVector>(max_sample_size);
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
