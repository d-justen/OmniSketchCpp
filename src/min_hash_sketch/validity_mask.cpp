#include "min_hash_sketch/validity_mask.hpp"

namespace omnisketch {

ValidityMask::ValidityMask(size_t size) : mask_((size + 7) / 8, 0xFF) {
}

ValidityMask::ValidityMask(const ValidityMask &other) {
	mask_ = other.mask_;
	invalid_counter = other.invalid_counter;
}

void ValidityMask::SetValid(size_t index) {
	size_t byte_idx = index / 8;
	size_t bit_idx = index % 8;
	mask_[byte_idx] |= (1 << bit_idx);
}

void ValidityMask::SetInvalid(size_t index) {
	size_t byte_idx = index / 8;
	size_t bit_idx = index % 8;
	mask_[byte_idx] &= ~(1 << bit_idx);
	// TODO: implement a more robust method to track invalids. Repeated SetInvalid calls would be tracked twice
	invalid_counter++;
}

bool ValidityMask::IsValid(size_t index) const {
	size_t byte_idx = index / 8;
	size_t bit_idx = index % 8;
	return (mask_[byte_idx] >> bit_idx) & 1;
}

size_t ValidityMask::InvalidCount() const {
	return invalid_counter;
}

} // namespace omnisketch
