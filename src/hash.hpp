#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
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

inline uint32_t BarrettReduction(uint64_t x) {
	static constexpr uint32_t m = (1ULL << 31) - 1;
	static constexpr uint32_t mu = (1ULL << 62) / m;

	uint32_t d = ((uint64_t)x * mu) >> 62;
	x -= d * m;
	if (x >= m) {
		x -= m;
	}
	return x;
}

inline size_t ComputeCellIdx(uint64_t h1, uint64_t h2, size_t i, size_t width) {
	return BarrettReduction(h1 + i * h2) % width;
}

template <class T>
inline uint64_t Hash(const T &value) {
	return MurmurHash64(static_cast<uint64_t>(value));
}

inline std::pair<uint32_t, uint32_t> SplitHash(const uint64_t hash) {
	uint32_t h1 = hash;
	uint32_t h2 = hash >> 32;
	return std::make_pair(h1, h2);
}

template <>
inline uint64_t Hash(const std::string &value) {
	static const std::hash<std::string> string_hasher;
	return string_hasher(value);
}

// TODO: Implement other types

} // namespace omnisketch
