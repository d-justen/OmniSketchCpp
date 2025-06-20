#pragma once

#include <map>

namespace omnisketch {

template <typename ValueType>
class MinHashValueSample {
public:
	explicit MinHashValueSample(size_t max_count_p) : max_count(max_count_p) {
	}

	void AddRecord(uint64_t rid_hash, const ValueType &value) {
		if (data.size() < max_count) {
			data.emplace(rid_hash, value);
		} else {
			if (rid_hash < data.crbegin()->first) {
				data.emplace(rid_hash, value);
				if (data.size() > max_count) {
					data.erase(std::prev(data.cend()));
				}
			}
		}
	}

	const std::map<uint64_t, ValueType> &Data() const {
		return data;
	};

	size_t MaxCount() const {
		return max_count;
	}

protected:
	size_t max_count;
	std::map<uint64_t, ValueType> data;
};

} // namespace omnisketch
