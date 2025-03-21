#pragma once

#include <cassert>
#include <set>
#include <stdexcept>
#include <vector>

namespace omnisketch {

template <typename T>
class MinHashSketch {
public:
	MinHashSketch() = default;
	explicit MinHashSketch(size_t max_count_p) : max_count(max_count_p) {
	}

	void AddRecord(typename T::value_type hash) {
		if (hashes.size() < max_count) {
			hashes.insert(hash);
		} else {
			if (hash < *hashes.crbegin()) {
				hashes.erase(std::prev(hashes.cend()));
				hashes.insert(hash);
			}
		}
	}

	inline size_t Size() const {
		return hashes.size();
	}

	void Union(const MinHashSketch &other) {
		for (const auto hash : other.hashes) {
			if (hashes.size() < max_count) {
				hashes.insert(hash);
			} else {
				if (hash < *hashes.crbegin()) {
					hashes.insert(hash);
					// Size may stay the same if hash was contained already
					if (hashes.size() > max_count) {
						hashes.erase(std::prev(hashes.cend()));
					}
				} else {
					return;
				}
			}
		}
	}

	MinHashSketch<T> Shrink(size_t size) const {
		assert(size < max_count);
		MinHashSketch<T> result(size);
		const size_t result_sample_count = std::min(size, hashes.size());
		result.hashes.insert(hashes.begin(), std::next(hashes.begin(), result_sample_count));
		return result;
	}

	MinHashSketch<std::vector<typename T::value_type>> Flatten() const {
		MinHashSketch<std::vector<typename T::value_type>> result(max_count);
		result.hashes.insert(result.hashes.end(), hashes.begin(), hashes.end());
		return result;
	}

	static double EstimateCardinality(const MinHashSketch<T> &min_hash_sketch, size_t n_max) {
		return ((double)n_max / (double)min_hash_sketch.max_count) * (double)min_hash_sketch.Size();
	}

	T hashes;
	size_t max_count;
};

template <>
void MinHashSketch<std::vector<uint16_t>>::AddRecord(uint16_t);

template <>
void MinHashSketch<std::vector<uint32_t>>::AddRecord(uint32_t);

template <>
void MinHashSketch<std::vector<uint64_t>>::AddRecord(uint64_t);

template <>
void MinHashSketch<std::vector<uint16_t>>::Union(const MinHashSketch &other);

template <>
void MinHashSketch<std::vector<uint32_t>>::Union(const MinHashSketch &other);

template <>
void MinHashSketch<std::vector<uint64_t>>::Union(const MinHashSketch &other);

} // namespace omnisketch
