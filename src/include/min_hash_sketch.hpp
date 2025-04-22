#pragma once

#include <cassert>
#include <set>
#include <stdexcept>
#include <vector>

namespace omnisketch {

class MinHashSketch {
public:
	class SketchIterator {
	public:
		virtual ~SketchIterator() = default;
		virtual uint64_t Next() = 0;
		virtual bool HasNext() = 0;
	};

public:
	virtual ~MinHashSketch() = default;
	virtual void AddRecord(uint64_t hash) = 0;
	virtual size_t Size() const = 0;
	virtual size_t MaxCount() const = 0;
	virtual std::shared_ptr<MinHashSketch> Resize(size_t size) const = 0;
	virtual std::shared_ptr<MinHashSketch> Flatten() const = 0;
	virtual std::shared_ptr<MinHashSketch>
	Intersect(const std::vector<std::shared_ptr<MinHashSketch>> &sketches) const = 0;
	virtual void Combine(const MinHashSketch &other) = 0;
	virtual std::shared_ptr<MinHashSketch> Combine(const std::vector<std::shared_ptr<MinHashSketch>> &others) const = 0;
	virtual std::shared_ptr<MinHashSketch> Copy() const = 0;
	virtual size_t EstimateByteSize() const = 0;
	virtual std::unique_ptr<SketchIterator> Iterator() const = 0;
};

class MinHashSketchSet : public MinHashSketch {
public:
	class SketchIterator : public MinHashSketch::SketchIterator {
	public:
		SketchIterator(std::set<uint64_t>::const_iterator it_p, size_t value_count_p)
		    : it(it_p), values_left(value_count_p) {
		}
		uint64_t Next() override {
			values_left--;
			return *it++;
		}
		bool HasNext() override {
			return values_left != 0;
		}

	private:
		std::set<uint64_t>::const_iterator it;
		size_t values_left;
	};

public:
	explicit MinHashSketchSet(size_t max_count_p) : max_count(max_count_p) {
	}

	void AddRecord(uint64_t hash) override;
	size_t Size() const override;
	size_t MaxCount() const override;
	std::shared_ptr<MinHashSketch> Resize(size_t size) const override;
	std::shared_ptr<MinHashSketch> Flatten() const override;
	std::shared_ptr<MinHashSketch>
	Intersect(const std::vector<std::shared_ptr<MinHashSketch>> &sketches) const override;
	void Combine(const MinHashSketch &other) override;
	std::shared_ptr<MinHashSketch> Combine(const std::vector<std::shared_ptr<MinHashSketch>> &others) const override;
	std::shared_ptr<MinHashSketch> Copy() const override;
	size_t EstimateByteSize() const override;
	std::unique_ptr<MinHashSketch::SketchIterator> Iterator() const override;
	std::set<uint64_t> &Data();
	const std::set<uint64_t> &Data() const;

private:
	std::set<uint64_t> data;
	size_t max_count;
};

class MinHashSketchVector : public MinHashSketch {
public:
	class SketchIterator : public MinHashSketch::SketchIterator {
	public:
		SketchIterator(std::vector<uint64_t>::const_iterator it_p, size_t value_count_p)
		    : it(it_p), values_left(value_count_p) {
		}
		uint64_t Next() override {
			values_left--;
			return *it++;
		}
		bool HasNext() override {
			return values_left != 0;
		}

	private:
		std::vector<uint64_t>::const_iterator it;
		size_t values_left;
	};

public:
	explicit MinHashSketchVector(std::vector<uint64_t> data_p) : data(std::move(data_p)), max_count(data.size()) {
	}

	explicit MinHashSketchVector(size_t max_count_p) : max_count(max_count_p) {
		data.reserve(max_count);
	}

	void AddRecord(uint64_t hash) override;
	size_t Size() const override;
	size_t MaxCount() const override;
	std::shared_ptr<MinHashSketch> Resize(size_t size) const override;
	std::shared_ptr<MinHashSketch> Flatten() const override;
	std::shared_ptr<MinHashSketch>
	Intersect(const std::vector<std::shared_ptr<MinHashSketch>> &sketches) const override;
	void Combine(const MinHashSketch &other) override;
	std::shared_ptr<MinHashSketch> Combine(const std::vector<std::shared_ptr<MinHashSketch>> &others) const override;
	std::shared_ptr<MinHashSketch> Copy() const override;
	size_t EstimateByteSize() const override;
	std::unique_ptr<MinHashSketch::SketchIterator> Iterator() const override;
	std::vector<uint64_t> &Data();
	const std::vector<uint64_t> &Data() const;

private:
	std::vector<uint64_t> data;
	size_t max_count;
};

class MinHashSketchFactory {
public:
	virtual ~MinHashSketchFactory() = default;
	virtual std::shared_ptr<MinHashSketch> Create() const = 0;
	virtual size_t MaxSampleCount() const = 0;
};

class MinHashSketchSetFactory : public MinHashSketchFactory {
public:
	explicit MinHashSketchSetFactory(size_t max_count_p) : max_count(max_count_p) {
	}

	std::shared_ptr<MinHashSketch> Create() const override {
		return std::make_shared<MinHashSketchSet>(max_count);
	}

	size_t MaxSampleCount() const override {
		return max_count;
	}

private:
	size_t max_count;
};

class MinHashSketchVectorFactory : public MinHashSketchFactory {
public:
	explicit MinHashSketchVectorFactory(size_t max_count_p) : max_count(max_count_p) {
	}

	std::shared_ptr<MinHashSketch> Create() const override {
		return std::make_shared<MinHashSketchVector>(max_count);
	}

	size_t MaxSampleCount() const override {
		return max_count;
	}

private:
	size_t max_count;
};

} // namespace omnisketch
