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
	             std::vector<std::shared_ptr<Expression>> child_exprs_p = {})
	    : Expression(ExpressionType::OR), sketch(sketch_p), values(std::move(values_p)),
	      child_exprs(std::move(child_exprs_p)) {
		sketch->InitializeBuffers(values.size());
	}

	virtual ~OrExpression() = default;

	CardEstResult Execute() const override {
		CardEstResult result;
		if (!values.empty()) {
			assert(sketch);
			result = sketch->EstimateCardinality<T>(values.data(), values.size());
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
