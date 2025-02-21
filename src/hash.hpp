#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

namespace omnisketch {

// From https://nullprogram.com/blog/2018/07/31/

inline uint64_t MurmurHash64(uint64_t x) {
	x ^= x >> 32;
	x *= 0xd6e8feb86659fd93U;
	x ^= x >> 32;
	x *= 0xd6e8feb86659fd93U;
	x ^= x >> 32;
	return x;
}

inline size_t ComputeCellIdx(uint64_t h1, uint64_t h2, size_t i, size_t width) {
	return (h1 + i * h2) % width;
}

template <class T>
uint64_t Hash(const T &value) {
	return MurmurHash64(static_cast<uint64_t>(value));
}

inline std::pair<uint32_t, uint32_t> SplitHash(const uint64_t hash) {
	uint32_t h1 = hash;
	uint32_t h2 = hash >> 32;
	return std::make_pair(h1, h2);
}

// TODO: Implement other types

} // namespace omnisketch
