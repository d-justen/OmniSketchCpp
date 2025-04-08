#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <utility>

namespace omnisketch {

namespace hash_functions {

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

} // namespace hash_functions

// TODO: Implement other types
template <typename T>
class HashFunction {
public:
	virtual uint64_t Hash(const T &value) const = 0;
	virtual uint64_t HashRid(uint64_t rid) const = 0;
};

template <typename T>
class MurmurHashFunction : public HashFunction<T> {
public:
	uint64_t Hash(const T &value) const override {
		return hash_functions::Hash(value);
	}

	uint64_t HashRid(uint64_t rid) const override {
		return hash_functions::MurmurHash64(rid);
	}
};

template <typename T>
class IdentityHashFunction : public HashFunction<T> {
public:
	uint64_t Hash(const T &value) const override {
		return static_cast<uint64_t>(value);
	}
	uint64_t HashRid(uint64_t rid) const override {
		return rid;
	}
};

class CellIdxMapper {
public:
	virtual ~CellIdxMapper() = default;
	CellIdxMapper(size_t width_p) : width(width_p), h1(0), h2(0) {
	}
	virtual void SetHash(uint64_t hash) = 0;
	virtual size_t ComputeCellIdx(size_t row_idx) = 0;
	size_t width;
	uint32_t h1;
	uint32_t h2;
};

class BasicSplitHashMapper : public CellIdxMapper {
public:
	explicit BasicSplitHashMapper(size_t width_p) : CellIdxMapper(width_p) {
	}

	void SetHash(uint64_t hash) override {
		h1 = hash;
		h2 = hash >> 32;
	}

	size_t ComputeCellIdx(size_t row_idx) override {
		return (h1 + row_idx * h2) % width;
	}
};

class BarrettModSplitHashMapper : public CellIdxMapper {
public:
	explicit BarrettModSplitHashMapper(size_t width_p) : CellIdxMapper(width_p) {
	}

	void SetHash(uint64_t hash) override {
		h1 = hash;
		h2 = hash >> 32;
	}

	size_t ComputeCellIdx(size_t row_idx) override {
		uint32_t combined = h1 + (uint32_t)std::pow(row_idx + 1, 2) * h2;
		return BarrettReduction(combined) % width;
	}

protected:
	static uint32_t BarrettReduction(uint32_t x) {
		static constexpr uint32_t prime = (1 << 19) - 1; // largest prime in uint32_t
		static constexpr uint64_t mu = UINT64_MAX / prime;

		uint32_t q = ((uint64_t)x * mu) >> 32;
		uint32_t remainder = x - q * prime;
		if (remainder >= prime) {
			remainder -= prime;
		}

		return remainder;
	}
};

class IdentitySplitMapper : public CellIdxMapper {
public:
	explicit IdentitySplitMapper(size_t width_p) : CellIdxMapper(width_p) {
	}

	void SetHash(uint64_t value) override {
		uint64_t hash = hash_functions::Hash(value);
		h1 = hash;
		h2 = hash >> 32;
	}

	size_t ComputeCellIdx(size_t row_idx) override {
		return (h1 + row_idx * h2) % width;
	}
};

} // namespace omnisketch
