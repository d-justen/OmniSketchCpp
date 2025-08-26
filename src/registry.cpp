#include "registry.hpp"

namespace omnisketch {

Registry::Registry() = default;

Registry& Registry::Get() {
	static Registry instance;
	return instance;
}

} // namespace omnisketch
