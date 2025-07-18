#include "min_hash_sketch/min_hash_sample.hpp"

#include <regex>

namespace omnisketch {

std::unique_ptr<MinHashSample> MinHashSample::Create(size_t size, AllTypeVariant::Type type) {
	switch (type) {
	case AllTypeVariant::Type::INT:
		return std::make_unique<TypedMinHashSample<int32_t>>(size, type);
	case AllTypeVariant::Type::UINT:
		return std::make_unique<TypedMinHashSample<size_t>>(size, type);
	case AllTypeVariant::Type::DOUBLE:
		return std::make_unique<TypedMinHashSample<double>>(size, type);
	case AllTypeVariant::Type::STRING:
		return std::make_unique<TypedMinHashSample<std::string>>(size, type);
	case AllTypeVariant::Type::NIL:
		throw std::runtime_error("Null type not allowed.");
	}
}

std::regex StringToPattern(const std::string &input) {
	static const std::string regex_special_chars = ".^$|()[]{}*+?\\";
	std::string regex_pattern = "^";

	for (char ch : input) {
		if (ch == '%')
			regex_pattern += ".*";
		else if (ch == '_')
			regex_pattern += ".";
		else {
			if (regex_special_chars.find(ch) != std::string::npos) {
				regex_pattern += std::string("\\") + ch;
			} else {
				regex_pattern += ch;
			}
		}
	}

	regex_pattern += "$";
	std::regex pattern(regex_pattern);
	return pattern;
}

template <>
std::unique_ptr<MinHashSample> TypedMinHashSample<std::string>::LIKE(const omnisketch::AllTypeVariant &v) {
	auto pattern = StringToPattern(v.GetValue<std::string>());
	return ProcessPredicate([&pattern](const std::string &current) { return std::regex_match(current, pattern); });
}

template <>
std::unique_ptr<MinHashSample> TypedMinHashSample<std::string>::NOT_LIKE(const omnisketch::AllTypeVariant &v) {
	auto pattern = StringToPattern(v.GetValue<std::string>());
	return ProcessPredicate([&pattern](const std::string &current) { return !std::regex_match(current, pattern); });
}

} // namespace omnisketch
