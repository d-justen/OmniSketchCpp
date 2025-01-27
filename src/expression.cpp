#include "expression.hpp"

namespace omnisketch {

Expression::Expression(ExpressionType type_p) : type(type_p) {
}

ExpressionType Expression::Type() const {
	return type;
}

AndExpression::AndExpression(std::vector<std::shared_ptr<Expression>> child_exprs_p)
    : Expression(ExpressionType::AND), child_exprs(std::move(child_exprs_p)) {
}

CardEstResult AndExpression::Execute() const {
	std::vector<const CardEstResult> intermediate_results;
	intermediate_results.reserve(child_exprs.size());

	std::vector<const MinHashSketch *> sketch_ptrs;
	sketch_ptrs.reserve(child_exprs.size());

	double n_max = 0;

	for (const auto &child_expr : child_exprs) {
		intermediate_results.push_back(child_expr->Execute());
		const auto &intermediate_result = intermediate_results.back();
		sketch_ptrs.push_back(&intermediate_result.min_hash_sketch);
		n_max = std::max(intermediate_result.cardinality, n_max);
	}

	CardEstResult result;

	// Use smallest max sample size from intermediate results
	const auto &sample_count_element = *std::min_element(
	    intermediate_results.cbegin(), intermediate_results.cend(),
	    [](const CardEstResult &a, const CardEstResult &b) { return a.max_sample_size < b.max_sample_size; });
	result.max_sample_size = std::min(sample_count_element.max_sample_size, (size_t)n_max);

	result.min_hash_sketch = MinHashSketch::Intersect(sketch_ptrs);
	result.cardinality = ((double)n_max / (double)result.max_sample_size) * (double)result.min_hash_sketch.Size();

	return result;
}

} // namespace omnisketch
