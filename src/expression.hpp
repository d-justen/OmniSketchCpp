#pragma once

#include "omni_sketch.hpp"

#include <vector>

namespace omnisketch {

enum class ExpressionType { AND, OR };

class Expression {
public:
	Expression(ExpressionType type);

protected:
	const ExpressionType type;
};

template <class T>
class EqExpr : public Expression {
public:
	EqExpr(OmniSketch *sketch_p, T value_p) : sketch(sketch_p), value(std::move(value_p)) {
	}

	double Execute() {
		return sketch->EstimateCardinality(value);
	}

protected:
	const OmniSketch *sketch;
	const T value;
};

class AndExpr : public Expression {
public:
	AndExpr();

protected:
	std::vector<Expression> child_expressions;
};

} // namespace omnisketch
