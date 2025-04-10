#include "registry.hpp"

namespace omnisketch {

Registry::Registry() {
}

Registry &Registry::Get() {
	static Registry instance;
	return instance;
}

} // namespace omnisketch
