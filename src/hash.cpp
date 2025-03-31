#include "include/hash.hpp"

namespace omnisketch {

BasicSplitHashProcessor::BasicSplitHashProcessor(size_t width_p) : width(width_p) {
}

size_t BasicSplitHashProcessor::ComputeCellIdx(uint64_t hash, size_t row_idx) {
	uint32_t h1 = hash;
	uint32_t h2 = hash >> 32;
	return (h1 + row_idx * h2) % width;
}

BarrettModSplitHashProcessor::BarrettModSplitHashProcessor(size_t width_p) : width(width_p) {
}

size_t BarrettModSplitHashProcessor::ComputeCellIdx(uint64_t hash, size_t row_idx) {
	uint32_t h1 = hash;
	uint32_t h2 = hash >> 32;
	uint32_t combined = h1 + (uint32_t)std::pow(row_idx + 1, 2) * h2;
	return BarrettReduction(combined) % width;
}

uint32_t BarrettModSplitHashProcessor::BarrettReduction(uint32_t x) {
	static constexpr uint32_t prime = (1 << 19) - 1; // largest prime in uint32_t
	static constexpr uint64_t mu = UINT64_MAX / prime;

	uint32_t q = ((uint64_t)x * mu) >> 32;
	uint32_t remainder = x - q * prime;
	if (remainder >= prime) {
		remainder -= prime;
	}

	return remainder;
}

} // namespace omnisketch
