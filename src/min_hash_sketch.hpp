#pragma once

#include <set>
#include <vector>

namespace omnisketch {

class MinHashSketch {
public:
	void AddRecord(uint64_t hash, size_t max_sample_size);
	size_t Size() const;
	MinHashSketch ReduceSampleSize(size_t new_sample_size) const;
	static MinHashSketch Intersect(const std::vector<const MinHashSketch *> &sketches);

protected:
	std::set<uint64_t> rids;
};

} // namespace omnisketch
