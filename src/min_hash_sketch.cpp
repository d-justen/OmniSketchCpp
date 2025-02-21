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

/*
template <class T>
void MinHashSketch<T>::AddRecord(T hash, size_t max_sample_size) {
	if (rids.size() < max_sample_size) {
		rids.insert(hash);
	} else {
		if (hash < *rids.crbegin()) {
			rids.erase(std::prev(rids.cend()));
			rids.insert(hash);
		}
	}
}

template <class T>
size_t MinHashSketch<T>::Size() const {
	return rids.size();
}

void MinHashSketch::Combine(const MinHashSketch &other, const size_t max_sample_size) {
	for (const auto hash : other.rids) {
		if (rids.size() < max_sample_size) {
			rids.insert(hash);
		} else {
			if (hash < *rids.crbegin()) {
				rids.erase(std::prev(rids.cend()));
				rids.insert(hash);
			} else {
				return;
			}
		}
	}
}

const std::set<uint64_t> &MinHashSketch::GetRids() const {
	return rids;
}

MinHashSketch MinHashSketch::ReduceSampleSize(const size_t new_sample_size) const {
	assert(rids.size() >= new_sample_size && "Reducing sample size requires rid count >= target samples.");
	MinHashSketch result;
	result.rids.insert(rids.cbegin(), std::next(rids.cbegin(), new_sample_size));
	return result;
}

MinHashSketch MinHashSketch::Intersect(const std::vector<const MinHashSketch *> &sketches) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");
	if (sketches.size() == 1) {
		return *sketches[0];
	}

	MinHashSketch result;
	const size_t sketch_count = sketches.size();

	std::vector<std::set<uint64_t>::iterator> offsets;
	offsets.reserve(sketch_count);
	for (size_t sketch_idx = 0; sketch_idx < sketch_count; sketch_idx++) {
		if (sketches[sketch_idx]->rids.empty()) {
			// Intersection cannot be > 0
			return {};
		}
		offsets.push_back(sketches[sketch_idx]->rids.cbegin());
	}

	const auto &max_rid_first_sketch = sketches[0]->rids.cend();
	while (offsets[0] != max_rid_first_sketch) {
		bool found = true;
		auto this_val = *offsets[0];
		for (size_t sketch_idx = 1; sketch_idx < sketch_count; sketch_idx++) {
			auto &other_offset = offsets[sketch_idx];
			auto other_end = sketches[sketch_idx]->rids.cend();

			while (other_offset != other_end) {
				if (*other_offset < this_val)
					++other_offset;
				else {
					break;
				}
			}

			if (other_offset == other_end) {
				// There can be no other matches
				return result;
			}
			if (this_val == *other_offset) {
				// It's a match
				++other_offset;
				continue;
			} else {
				// No match, skip to next rid geq the mismatch
				found = false;
				auto this_offset_end = sketches[0]->rids.cend();
				auto other_val = *other_offset;

				while (offsets[0] != this_offset_end) {
					if (*offsets[0] < other_val)
						++offsets[0];
					else {
						break;
					}
				}
				break;
			}
		}

		if (found) {
			result.rids.insert(this_val);
			++offsets[0];
		}
	}
	return result;
}

MinHashSketch MinHashSketch::Union(const std::vector<const MinHashSketch *> &sketches, const size_t sample_count) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");
	if (sketches.size() == 1) {
		return *sketches[0];
	}

	using IterTuple = std::tuple<uint64_t, std::set<uint64_t>::iterator, std::set<uint64_t>::iterator>;
	auto compare = [](const IterTuple &a, const IterTuple &b) {
		return std::get<0>(a) > std::get<0>(b);
	};
	std::priority_queue<IterTuple, std::vector<IterTuple>, decltype(compare)> iterator_queue(compare);

	MinHashSketch result;
	const size_t sketch_count = sketches.size();

	for (size_t sketch_idx = 0; sketch_idx < sketch_count; sketch_idx++) {
		const auto &rids = sketches[sketch_idx]->rids;
		auto it = rids.begin();
		if (it != rids.end()) {
			iterator_queue.emplace(*it, it, rids.end());
		}
	}

	while (!iterator_queue.empty()) {
		if (result.rids.size() == sample_count) {
			break;
		}

		auto current_iter_pair = iterator_queue.top();
		auto hash_value = std::get<0>(current_iter_pair);
		auto &it_begin = std::get<1>(current_iter_pair);
		auto &it_end = std::get<2>(current_iter_pair);

		result.rids.insert(hash_value);
		++it_begin;
		if (it_begin != it_end) {
			iterator_queue.emplace(*it_begin, it_begin, it_end);
		}

		iterator_queue.pop();
	}

	return result;
}

MinHashSketch MinHashSketch::Union(const std::vector<const MinHashSketch *> &sketches) {
	MinHashSketch result;
	for (const auto &sketch : sketches) {
		result.rids.insert(sketch->rids.begin(), sketch->rids.end());
	}
	return result;
}

MinHashSketch MinHashSketch::Intersect(const std::vector<const MinHashSketch *> &sketches, uint64_t max_hash) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");
	if (sketches.size() == 1) {
		return *sketches[0];
	}

	MinHashSketch result;
	const size_t sketch_count = sketches.size();

	std::vector<std::set<uint64_t>::iterator> offsets;
	offsets.reserve(sketch_count);
	for (size_t sketch_idx = 0; sketch_idx < sketch_count; sketch_idx++) {
		if (sketches[sketch_idx]->rids.empty()) {
			// Intersection cannot be > 0
			return {};
		}
		offsets.push_back(sketches[sketch_idx]->rids.cbegin());
	}

	const auto &max_rid_first_sketch = sketches[0]->rids.cend();
	while (offsets[0] != max_rid_first_sketch) {
		bool found = true;
		auto this_val = *offsets[0];
		if (this_val >= max_hash) {
			// No more matches < max_hash possible
			return result;
		}
		for (size_t sketch_idx = 1; sketch_idx < sketch_count; sketch_idx++) {
			auto &other_offset = offsets[sketch_idx];
			auto other_end = sketches[sketch_idx]->rids.cend();

			while (other_offset != other_end) {
				if (*other_offset < this_val)
					++other_offset;
				else {
					break;
				}
			}

			if (other_offset == other_end) {
				// There can be no other matches
				return result;
			}
			if (this_val == *other_offset) {
				// It's a match
				++other_offset;
				continue;
			} else {
				// No match, skip to next rid geq the mismatch
				found = false;
				auto this_offset_end = sketches[0]->rids.cend();
				auto other_val = *other_offset;

				if (other_val >= max_hash) {
					// No more matches < max_hash possible
					return result;
				}

				while (offsets[0] != this_offset_end) {
					if (*offsets[0] < other_val)
						++offsets[0];
					else {
						break;
					}
				}
				break;
			}
		}

		if (found) {
			result.rids.insert(this_val);
			++offsets[0];
		}
	}
	return result;
}

MinHashSketch MinHashSketch::Intersect(const std::vector<const MinHashSketch *> &sketches,
                                       std::vector<std::set<uint64_t>::iterator> &offsets) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");
	if (sketches.size() == 1) {
		return *sketches[0];
	}

	MinHashSketch result;
	const size_t sketch_count = sketches.size();

	for (size_t sketch_idx = 0; sketch_idx < sketch_count; sketch_idx++) {
		if (sketches[sketch_idx]->rids.empty()) {
			// Intersection cannot be > 0
			return {};
		}
		offsets[sketch_idx] = sketches[sketch_idx]->rids.cbegin();
	}

	const auto &max_rid_first_sketch = sketches[0]->rids.cend();
	while (offsets[0] != max_rid_first_sketch) {
		bool found = true;
		auto this_val = *offsets[0];
		for (size_t sketch_idx = 1; sketch_idx < sketch_count; sketch_idx++) {
			auto &other_offset = offsets[sketch_idx];
			auto other_end = sketches[sketch_idx]->rids.cend();

			while (other_offset != other_end && *other_offset < this_val) {
				++other_offset;
			}

			if (other_offset == other_end) {
				// There can be no other matches
				return result;
			}
			if (this_val == *other_offset) {
				// It's a match
				++other_offset;
				continue;
			} else {
				// No match, skip to next rid geq the mismatch
				found = false;
				auto other_val = *other_offset;

				while (offsets[0] != max_rid_first_sketch && *offsets[0] < other_val) {
					++offsets[0];
				}
				break;
			}
		}

		if (found) {
			result.rids.insert(this_val);
			++offsets[0];
		}
	}
	return result;
}

bool MinHashSketch::IntersectOneStep(const std::vector<const MinHashSketch *> &sketches, uint64_t max_hash,
                                     std::vector<std::set<uint64_t>::iterator> &offsets, uint64_t &match) {
	assert(!sketches.empty() && "Sketch vector to intersect must not be empty.");
	if (*offsets[0] > max_hash) {
		return false;
	}

	const size_t sketch_count = sketches.size();

	const auto &max_rid_first_sketch = sketches[0]->rids.cend();
	while (offsets[0] != max_rid_first_sketch) {
		match = *offsets[0];
		if (match >= max_hash) {
			return false;
		}

		for (size_t sketch_idx = 1; sketch_idx < sketch_count; sketch_idx++) {
			auto &other_offset = offsets[sketch_idx];
			auto other_end = sketches[sketch_idx]->rids.cend();

			while (other_offset != other_end && *other_offset < match) {
				++other_offset;
			}

			if (other_offset == other_end) {
				// There can be no other matches
				match = max_hash;
				return false;
			}
			if (match == *other_offset) {
				// It's a match
				++other_offset;
				continue;
			} else {
				// No match, skip to next rid geq the mismatch
				match = *other_offset;

				while (offsets[0] != max_rid_first_sketch && *offsets[0] < match) {
					++offsets[0];
				}
				return false;
			}
		}

		++offsets[0];
		return true;
	}
	match = max_hash;
	return false;
}

*/
} // namespace omnisketch
