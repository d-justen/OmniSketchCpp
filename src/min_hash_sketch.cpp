#include "min_hash_sketch.hpp"

#include <cassert>
#include <queue>

namespace omnisketch {

template <>
void MinHashSketch<std::vector<uint16_t>>::AddRecord(uint16_t hash) {
	MinHashSketch<std::set<uint16_t>> temp(max_count);
	temp.hashes.insert(hashes.begin(), hashes.end());
	temp.AddRecord(hash);
	hashes = std::vector<uint16_t>(temp.hashes.begin(), temp.hashes.end());
}

template <>
void MinHashSketch<std::vector<uint32_t>>::AddRecord(uint32_t hash) {
	MinHashSketch<std::set<uint32_t>> temp(max_count);
	temp.hashes.insert(hashes.begin(), hashes.end());
	temp.AddRecord(hash);
	hashes = std::vector<uint32_t>(temp.hashes.begin(), temp.hashes.end());
}

template <>
void MinHashSketch<std::vector<uint64_t>>::AddRecord(uint64_t hash) {
	MinHashSketch<std::set<uint64_t>> temp(max_count);
	temp.hashes.insert(hashes.begin(), hashes.end());
	temp.AddRecord(hash);
	hashes = std::vector<uint64_t>(temp.hashes.begin(), temp.hashes.end());
}

template <>
void MinHashSketch<std::vector<uint16_t>>::Union(const MinHashSketch &other) {
	throw std::logic_error("Union on flat MinHashSketch is not allowed.");
}

template <>
void MinHashSketch<std::vector<uint32_t>>::Union(const MinHashSketch &other) {
	throw std::logic_error("Union on flat MinHashSketch is not allowed.");
}

template <>
void MinHashSketch<std::vector<uint64_t>>::Union(const MinHashSketch &other) {
	throw std::logic_error("Union on flat MinHashSketch is not allowed.");
}

} // namespace omnisketch
