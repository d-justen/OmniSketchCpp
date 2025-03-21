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

inline uint32_t BarrettReduction(uint32_t x) {
	static constexpr uint32_t prime = (1 << 19) - 1; // largest prime in uint32_t
	static constexpr uint64_t mu = UINT64_MAX / prime;

	uint32_t q = ((uint64_t)x * mu) >> 32;
	uint32_t remainder = x - q * prime;
	if (remainder >= prime) {
		remainder -= prime;
	}

	return remainder;
}

inline size_t ComputeCellIdx(uint32_t h1, uint32_t h2, size_t i, size_t width) {
	uint32_t new_hash = h1 + ((uint32_t)std::pow(i + 1, 2)) * h2;
	return BarrettReduction(new_hash) % width;
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
