#pragma once

#include "hash.hpp"
#include "omni_sketch.hpp"

namespace omnisketch {

class Algorithm {
public:
	static CardEstResult Conjunction(const std::vector<const OmniSketch *> &sketches,
	                                 const std::vector<uint64_t> &hashes);

	static CardEstResult Disjunction(const std::vector<const OmniSketch *> &sketches,
	                                 const std::vector<uint64_t> &hashes);

	static CardEstResult UnionByRow(const OmniSketch &sketch, const std::vector<uint64_t> &hashes);
	static CardEstResult SegmentedUnionByRow(const OmniSketch &sketch, const std::vector<uint64_t> &hashes);
	static CardEstResult DisjunctionEarlyExit(const std::vector<const OmniSketch *> &sketches,
	                                          const std::vector<uint64_t> &hashes);
	static CardEstResult PartialIntersectUnion(const OmniSketch &sketch, const std::vector<uint64_t> &hashes);

protected:
	static size_t GetResultCellCount(const std::vector<const OmniSketch *> &sketches);
};

} // namespace omnisketch
