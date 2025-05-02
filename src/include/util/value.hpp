#pragma once

#include "hash.hpp"

namespace omnisketch {

class Value {
public:
	template <typename T>
	static Value From(const T &value) {
		return Value(hash_functions::Hash<T>(value));
	}
	inline uint64_t GetHash() const {
		return hash;
	}

private:
	const uint64_t hash;
	explicit Value(uint64_t hash_p) : hash(hash_p) {
	}
};

class ValueSet {
public:
	template <typename T>
	static ValueSet FromRange(const T &lower_bound, const T &upper_bound) {
		std::vector<uint64_t> hashes;
		hashes.reserve((upper_bound - lower_bound) + 1);
		for (T value = lower_bound; value <= upper_bound; value++) {
			hashes.push_back(hash_functions::Hash(value));
		}
		return ValueSet(hashes);
	}

	template <typename T>
	static ValueSet FromValues(const T *values, size_t count) {
		std::vector<uint64_t> hashes;
		hashes.reserve(count);

		for (size_t value_idx = 0; value_idx < count; value_idx++) {
			hashes.push_back(hash_functions::Hash(values[value_idx]));
		}

		return ValueSet(hashes);
	}

	inline const std::vector<uint64_t> &GetHashes() const {
		return hashes;
	}

private:
	const std::vector<uint64_t> hashes;
	explicit ValueSet(std::vector<uint64_t> hashes_p) : hashes(std::move(hashes_p)) {
	}
};

} // namespace omnisketch
