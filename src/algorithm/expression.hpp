#pragma once

#include "omni_sketch_operations.hpp"
#include "join_operations.hpp"
#include "min_hash_sketch.hpp"

#include <memory>

namespace omnisketch {

class ExpressionResult {
public:
	ExpressionResult(MinHashSketch<std::set<uint64_t>> sketch_p, double card, double base_card_p)
	    : sketch(std::move(sketch_p)), unscaled_card(card), sampling_prob(1), base_card(base_card_p) {
	}
	ExpressionResult(MinHashSketch<std::set<uint64_t>> sketch_p, double card, double sampling_probability_p,
	                 double base_card_p)
	    : sketch(std::move(sketch_p)), unscaled_card(card), sampling_prob(sampling_probability_p),
	      base_card(base_card_p) {
	}

	virtual ~ExpressionResult() = default;
	ExpressionResult() = default;
	ExpressionResult(ExpressionResult &) = default;
	double Cardinality() const {
		return unscaled_card / sampling_prob;
	}

	double UnscaledCardinality() const {
		return unscaled_card;
	}

	double BaseCardinality() const {
		return base_card;
	}

	double Selectivity() const {
		return Cardinality() / base_card;
	}

	double SamplingProbability() const {
		return sampling_prob;
	}

	MinHashSketch<std::set<uint64_t>> &MinHashSketch() {
		return sketch;
	}

protected:
	omnisketch::MinHashSketch<std::set<uint64_t>> sketch;
	double unscaled_card {};
	double sampling_prob {};
	double base_card {};
};

template <typename ValueType, typename RecordType>
static std::unique_ptr<ExpressionResult> Equals(const OmniSketch<ValueType, RecordType, std::set<uint64_t>> &sketch,
                                                const ValueType &val) {
	auto card_est = sketch.EstimateCardinality(val);
	return std::make_unique<ExpressionResult>(card_est.min_hash_sketch, card_est.cardinality, sketch.RecordCount());
}

template <typename ValueType, typename RecordType>
static std::unique_ptr<ExpressionResult> Or(const OmniSketch<ValueType, RecordType, std::set<uint64_t>> &sketch,
                                            const std::vector<ValueType> &vals) {
	auto card_est = Algorithm::DisjunctPointQueries(sketch, vals);
	return std::make_unique<ExpressionResult>(card_est.min_hash_sketch, card_est.cardinality, sketch.RecordCount());
}

enum class JoinIntersectType { SelMult, FullRandIntersect };

template <typename HashType>
std::unique_ptr<ExpressionResult> SelMult(const std::vector<std::unique_ptr<ExpressionResult>> &exprs) {
	std::vector<CardEstResult<HashType>> expr_results;
	double selectivity = 1;
	double base_card = exprs.front()->BaseCardinality();

	for (const auto &expr : exprs) {
		selectivity *= expr->Selectivity();
		CardEstResult<uint64_t> card_est(expr->Cardinality());
		card_est.min_hash_sketch = expr->MinHashSketch();
		expr_results.push_back(card_est);
	}

	auto card_est = Algorithm::Intersection(expr_results);
	card_est.cardinality = base_card * selectivity;

	return std::make_unique<ExpressionResult>(card_est.min_hash_sketch, card_est.cardinality, base_card);
}

template <typename HashType>
static std::unique_ptr<ExpressionResult> And(const std::vector<std::unique_ptr<ExpressionResult>> &exprs,
                                             JoinIntersectType type = JoinIntersectType::SelMult) {
	switch (type) {
	case JoinIntersectType::SelMult: {
		return SelMult<HashType>(exprs);
	}
	case JoinIntersectType::FullRandIntersect: {
		return nullptr;
	}
	default:
		return nullptr;
	}
}

template <typename ValueType, typename RecordType>
static std::unique_ptr<ExpressionResult> Join(const OmniSketch<ValueType, RecordType, std::set<uint64_t>> &sketch,
                                              const std::unique_ptr<ExpressionResult> &expr) {
	auto probe_sketch = expr->MinHashSketch();
	double sampling_probability = probe_sketch.Size() / expr->Cardinality();

	std::vector<uint64_t> probe_sample_vec(probe_sketch.hashes.begin(), probe_sketch.hashes.end());
	auto card_est = Algorithm::DisjunctPointQueriesHashed(sketch, probe_sample_vec);

	return std::make_unique<ExpressionResult>(card_est.min_hash_sketch, card_est.cardinality, sampling_probability,
	                                          sketch.RecordCount());
}

} // namespace omnisketch
