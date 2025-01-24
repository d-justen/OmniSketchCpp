#pragma once

#include <cstdint>

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

template <class T>
uint64_t Hash(const T &value) {
	return MurmurHash64(static_cast<uint64_t>(value));
}

// TODO: Implement other types

} // namespace omnisketch
