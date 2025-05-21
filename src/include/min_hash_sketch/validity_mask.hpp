#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace omnisketch {

class ValidityMask {
public:
	explicit ValidityMask(size_t size);
	ValidityMask(const ValidityMask &other);
	void SetValid(size_t index);
	void SetInvalid(size_t index);
	bool IsValid(size_t index) const;
	size_t InvalidCount() const;

private:
	std::vector<std::uint8_t> mask_;
	size_t invalid_counter = 0;
};

} // namespace omnisketch
