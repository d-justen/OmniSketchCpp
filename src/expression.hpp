#pragma once

#include "omni_sketch.hpp"

#include <algorithm>
#include <cassert>
#include <vector>

namespace omnisketch {

enum class ExpressionType { AND, OR };

class Expression {
public:
	explicit Expression(ExpressionType type_p);
	~Expression() = default;
	virtual CardEstResult Execute() const = 0;
	ExpressionType Type() const;

protected:
	const ExpressionType type;
};

template <class T>
class OrExpression : public Expression {
public:
	OrExpression(OmniSketch *sketch_p, std::vector<T> values_p,
	             std::vector<std::shared_ptr<Expression>> child_exprs_p = {}, bool probe_without_hashing_p = false)
	    : Expression(ExpressionType::OR), sketch(sketch_p), values(std::move(values_p)),
	      child_exprs(std::move(child_exprs_p)), probe_without_hashing(probe_without_hashing_p) {
		sketch->InitializeBuffers(values.size());
	}

	virtual ~OrExpression() = default;

	CardEstResult Execute() const override {
		CardEstResult result;
		if (!values.empty()) {
			assert(sketch);
			if (probe_without_hashing) {
				result =
				    sketch->EstimateCardinalityHashed(reinterpret_cast<const uint64_t *>(values.data()), values.size());
			} else {
				result = sketch->EstimateCardinality<T>(values.data(), values.size());
			}
		}

		for (const auto &child_expr : child_exprs) {
			CardEstResult card_est = child_expr->Execute();
			result.cardinality += card_est.cardinality;
			result.min_hash_sketch.Combine(card_est.min_hash_sketch, card_est.max_sample_size);
		}

		return result;
	}

protected:
	OmniSketch *sketch;
	const std::vector<T> values;
	const std::vector<std::shared_ptr<Expression>> child_exprs;
	const bool probe_without_hashing;
};

class AndExpression : public Expression {
public:
	explicit AndExpression(std::vector<std::shared_ptr<Expression>> child_exprs_p);
	virtual ~AndExpression() = default;

	CardEstResult Execute() const override;

protected:
	const std::vector<std::shared_ptr<Expression>> child_exprs;
};

} // namespace omnisketch
