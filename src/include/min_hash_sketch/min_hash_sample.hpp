#pragma once

#include "min_hash_sketch_vector.hpp"
#include "util/hash.hpp"

#include <map>
#include <memory>
#include <unordered_set>

namespace omnisketch {

class AllTypeVariant {
public:
	enum class Type { INT, UINT, DOUBLE, STRING, NIL };

	AllTypeVariant() : type(Type::NIL) {
	}

	explicit AllTypeVariant(std::nullptr_t) : type(Type::NIL) {
	}

	explicit AllTypeVariant(size_t v) : type(Type::UINT) {
		data.size_t_val = v;
	}

	explicit AllTypeVariant(int32_t v) : type(Type::INT) {
		data.int32_val = v;
	}

	explicit AllTypeVariant(double v) : type(Type::DOUBLE) {
		data.double_val = v;
	}

	explicit AllTypeVariant(const std::string &v) : type(Type::STRING) {
		new (&data.str_val) std::string(v);
	}

	explicit AllTypeVariant(std::string &&v) : type(Type::STRING) {
		new (&data.str_val) std::string(std::move(v));
	}

	AllTypeVariant(const AllTypeVariant &other) : type(other.type) {
		switch (type) {
		case Type::UINT:
			data.size_t_val = other.data.size_t_val;
			break;
		case Type::INT:
			data.int32_val = other.data.int32_val;
			break;
		case Type::DOUBLE:
			data.double_val = other.data.double_val;
			break;
		case Type::STRING:
			new (&data.str_val) std::string(other.data.str_val);
			break;
		case Type::NIL:
			break;
		}
	}

	AllTypeVariant &operator=(const AllTypeVariant &other) {
		if (this != &other) {
			this->~AllTypeVariant();          // Destroy existing value
			new (this) AllTypeVariant(other); // Copy-construct
		}
		return *this;
	}

	~AllTypeVariant() {
		if (type == Type::STRING) {
			using std::string;
			data.str_val.~string();
		}
	}

	Type GetType() const {
		return type;
	}

	template <typename T>
	T GetValue() const {
		return ValueGetter<T>::get(data);
	}

private:
	Type type;
	union Data {
		size_t size_t_val;
		int32_t int32_val;
		double double_val;
		std::string str_val;

		Data() {
		}
		~Data() {
		}
	} data;

	template <typename U>
	struct ValueGetter;

	template <>
	struct ValueGetter<size_t> {
		static size_t get(const Data &data) {
			return data.size_t_val;
		}
	};

	template <>
	struct ValueGetter<int32_t> {
		static int32_t get(const Data &data) {
			return data.int32_val;
		}
	};

	template <>
	struct ValueGetter<double> {
		static double get(const Data &data) {
			return data.double_val;
		}
	};

	template <>
	struct ValueGetter<std::string> {
		static const std::string &get(const Data &data) {
			return data.str_val;
		}
	};

	template <>
	struct ValueGetter<std::nullptr_t> {
		static std::nullptr_t get(const Data &) {
			return nullptr;
		}
	};
};

class MinHashSample {
public:
	static std::unique_ptr<MinHashSample> Create(size_t size, AllTypeVariant::Type type);

	// Operators
	virtual std::unique_ptr<MinHashSample> EQ(const AllTypeVariant &v) = 0;
	virtual std::unique_ptr<MinHashSample> LT(const AllTypeVariant &v) = 0;
	virtual std::unique_ptr<MinHashSample> LE(const AllTypeVariant &v) = 0;
	virtual std::unique_ptr<MinHashSample> GT(const AllTypeVariant &v) = 0;
	virtual std::unique_ptr<MinHashSample> GE(const AllTypeVariant &v) = 0;
	virtual std::unique_ptr<MinHashSample> NE(const AllTypeVariant &v) = 0;
	virtual std::unique_ptr<MinHashSample> IN(const std::vector<AllTypeVariant> &vals) = 0;
	virtual std::unique_ptr<MinHashSample> BETWEEN(const AllTypeVariant &v1, const AllTypeVariant &v2) = 0;
	// String operators
	virtual std::unique_ptr<MinHashSample> LIKE(const AllTypeVariant &v) = 0;
	virtual std::unique_ptr<MinHashSample> NOT_LIKE(const AllTypeVariant &v) = 0;
	// Joins
	virtual std::unique_ptr<MinHashSample> JOIN_WITH_RIDS(const MinHashSample *other) = 0;
	virtual std::unique_ptr<MinHashSample> JOIN_WITH_VALUES(const MinHashSample *other) = 0;

	size_t BaseRecordCount() const {
		return record_count;
	}

	size_t SampleSize() const {
		return std::min(sample_size, record_count);
	}

	size_t ActiveSampleCount() const {
		if (!mask) {
			return SampleSize();
		}
		return SampleSize() - mask->InvalidCount();
	}

	double EstimateCardinality() const {
		if (!mask) {
			return record_count;
		}
		auto ssize = SampleSize();
		const size_t valid_sample_count = ssize - mask->InvalidCount();
		return ((double)valid_sample_count / (double)ssize) * (double)record_count;
	}

	AllTypeVariant::Type GetType() const {
		return type;
	}

	virtual std::unique_ptr<MinHashSample> Shrink(size_t new_size) = 0;

	virtual ~MinHashSample() = default;

protected:
	MinHashSample(size_t sample_size_p, AllTypeVariant::Type type_p, size_t record_count_p,
	              std::unique_ptr<ValidityMask> mask_p)
	    : sample_size(sample_size_p), record_count(record_count_p), type(type_p), mask(std::move(mask_p)) {
	}

	size_t sample_size;
	size_t record_count;
	AllTypeVariant::Type type;
	std::unique_ptr<ValidityMask> mask;
};

template <typename T>
class TypedMinHashSample : public MinHashSample {
public:
	TypedMinHashSample(size_t size, AllTypeVariant::Type type, size_t record_count = 0,
	                   std::unique_ptr<ValidityMask> mask = nullptr)
	    : MinHashSample(size, type, record_count, std::move(mask)) {
	}

	// Operators
	std::unique_ptr<MinHashSample> EQ(const AllTypeVariant &v) override {
		return ProcessPredicate([&v](const T &current) { return current == v.GetValue<T>(); });
	}

	std::unique_ptr<MinHashSample> LT(const AllTypeVariant &v) override {
		return ProcessPredicate([&v](const T &current) { return current < v.GetValue<T>(); });
	}

	std::unique_ptr<MinHashSample> LE(const AllTypeVariant &v) override {
		return ProcessPredicate([&v](const T &current) { return current <= v.GetValue<T>(); });
	}

	std::unique_ptr<MinHashSample> GT(const AllTypeVariant &v) override {
		return ProcessPredicate([&v](const T &current) { return current > v.GetValue<T>(); });
	}

	std::unique_ptr<MinHashSample> GE(const AllTypeVariant &v) override {
		return ProcessPredicate([&v](const T &current) { return current >= v.GetValue<T>(); });
	}

	std::unique_ptr<MinHashSample> NE(const AllTypeVariant &v) override {
		return ProcessPredicate([&v](const T &current) { return current >= v.GetValue<T>(); });
	}

	std::unique_ptr<MinHashSample> IN(const std::vector<AllTypeVariant> &vals) override {
		std::set<T> s;
		for (auto &val : vals) {
			s.insert(val.GetValue<T>());
		}

		return ProcessPredicate([&s](const T &current) { return s.find(current) != s.end(); });
	}

	std::unique_ptr<MinHashSample> BETWEEN(const AllTypeVariant &v1, const AllTypeVariant &v2) override {
		return ProcessPredicate(
		    [&v1, &v2](const T &current) { return current >= v1.GetValue<T>() && current <= v2.GetValue<T>(); });
	}

	// String operators
	std::unique_ptr<MinHashSample> LIKE(const AllTypeVariant &) override {
		throw std::runtime_error("Only allowed on strings");
	}

	std::unique_ptr<MinHashSample> NOT_LIKE(const AllTypeVariant &) override {
		throw std::runtime_error("Only allowed on strings");
	}

	// Joins
	std::unique_ptr<MinHashSample> JOIN_WITH_RIDS(const MinHashSample *build_side) override {
		assert(build_side->GetType() == type);
		const auto *typed_build_side = static_cast<const TypedMinHashSample<T>*>(build_side);
		const auto &build_side_data = *typed_build_side->data;
		const auto *other_side_mask = typed_build_side->mask.get();

		std::unordered_set<uint64_t> lookup;
		lookup.reserve(build_side->ActiveSampleCount());
		size_t value_idx = 0;
		for (auto &entry : build_side_data) {
			if (other_side_mask && other_side_mask->IsValid(value_idx)) {
				lookup.insert(entry.first);
			}
			value_idx++;
		}

		auto new_mask = mask ? std::make_unique<ValidityMask>(*mask) : std::make_unique<ValidityMask>(data->size());
		size_t value_end = SampleSize();
		auto it = data->begin();
		value_idx = 0;
		size_t match_count = 0;
		while (value_idx < value_end) {
			auto fk_hash = hash_functions::Hash(it->second);
			bool match = lookup.find(fk_hash) != lookup.end();
			if (match) {
				match_count++;
			} else {
				new_mask->SetInvalid(value_idx);
			}
			it++;
			value_idx++;
		}

		double new_record_count = ((build_side->EstimateCardinality() * (double)record_count) /
		                           (double)(build_side->ActiveSampleCount() * SampleSize())) *
		                          (double)match_count;

		return std::make_unique<TypedMinHashSample<T>>(
		    sample_size, type, std::min(record_count, (size_t)new_record_count), std::move(new_mask));
	}

	std::unique_ptr<MinHashSample> JOIN_WITH_VALUES(const MinHashSample *other) override {
		assert(other->GetType() == type);
		const auto *typed_other_side = static_cast<const TypedMinHashSample<T>*>(other);
		const auto &other_side_data = *typed_other_side->data;
		const auto *other_side_mask = typed_other_side->mask.get();

		std::unordered_map<T, size_t> count_map;
		count_map.reserve(other_side_data.size());
		size_t value_idx = 0;
		for (auto &entry : other_side_data) {
			if (other_side_mask && other_side_mask->IsValid(value_idx)) {
				count_map[entry.second]++;
			}
			value_idx++;
		}

		auto new_mask = mask ? std::make_unique<ValidityMask>(*mask) : std::make_unique<ValidityMask>(data->size());
		value_idx = 0;
		size_t value_end = SampleSize();
		auto it = data->begin();
		size_t match_count = 0;
		while (value_idx < value_end) {
			if (count_map.find(it->second) != count_map.end()) {
				match_count += count_map[it->second];
			} else {
				mask->SetInvalid(value_idx);
			}

			it++;
			value_idx++;
		}

		double new_record_count = ((other->EstimateCardinality() * (double)record_count) /
		                           (double)(other->ActiveSampleCount() * SampleSize())) *
		                          (double)match_count;

		return std::make_unique<TypedMinHashSample<T>>(sample_size, type, (size_t)new_record_count,
		                                               std::move(new_mask));
	}

	std::unique_ptr<MinHashSample> Shrink(size_t new_size) override {
		auto result = std::make_unique<TypedMinHashSample<T>>(new_size, type, record_count, nullptr);
		result->data = data;
		if (mask) {
			result->mask = std::make_unique<ValidityMask>(*mask);
		}
		return result;
	}

private:
	std::unique_ptr<MinHashSample> ProcessPredicate(std::function<bool(T &)> predicate) {
		auto new_mask = mask ? std::make_unique<ValidityMask>(*mask) : std::make_unique<ValidityMask>(data->size());
		size_t value_idx = 0;
		size_t value_end = SampleSize();
		auto it = data->begin();
		while (value_idx < value_end) {
			if (mask && !mask->IsValid(value_idx)) {
				continue;
			}
			bool match = predicate(it->second);
			if (!match) {
				new_mask->SetInvalid(value_idx);
			}
			it++;
			value_idx++;
		}
		return std::make_unique<TypedMinHashSample<T>>(sample_size, type, record_count, std::move(new_mask));
	}

	std::shared_ptr<std::map<uint64_t, T>> data;
};

} // namespace omnisketch
