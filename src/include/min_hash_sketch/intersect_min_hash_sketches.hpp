#pragma once

#include "min_hash_sketch.hpp"
#include "validity_mask.hpp"

namespace omnisketch {

std::shared_ptr<MinHashSketch> ComputeIntersection(const std::vector<std::shared_ptr<MinHashSketch>> &sketches,
                                                   ValidityMask *mask = nullptr, size_t max_sample_size = 0);

} // namespace omnisketch
