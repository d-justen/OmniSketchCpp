#pragma once

#include "omni_sketch/omni_sketch_cell.hpp"

namespace omnisketch {

class SetMembershipAlgorithm {
public:
	virtual ~SetMembershipAlgorithm() = default;
	virtual std::shared_ptr<OmniSketchCell>
	Execute(size_t max_sample_count, const std::vector<std::vector<std::shared_ptr<OmniSketchCell>>> &cells) const = 0;
};

class ProbeAllSum : public SetMembershipAlgorithm {
	std::shared_ptr<OmniSketchCell>
	Execute(size_t max_sample_count,
	        const std::vector<std::vector<std::shared_ptr<OmniSketchCell>>> &cells) const override {
		auto result = std::make_shared<OmniSketchCell>(max_sample_count);

		for (const auto &probe_set : cells) {
			result->Combine(*OmniSketchCell::Intersect(probe_set));
		}
		return result;
	}
};

} // namespace omnisketch
