#include <gtest/gtest.h>

#include "expression.hpp"
#include "omni_sketch.hpp"

constexpr size_t TUPLE_COUNT = 10;

TEST(OrExpressionTest, SingleSketchBasic) {
	omnisketch::OmniSketch sketch(16, 3, TUPLE_COUNT);
	std::vector<size_t> values(TUPLE_COUNT);
	for (size_t i = 0; i < TUPLE_COUNT; i++) {
		sketch.AddRecord(i, i);
		values[i] = i;
	}

	for (size_t i = 0; i < TUPLE_COUNT; i++) {
		std::vector<size_t> test_values(values.begin(), std::next(values.begin(), i + 1));
		auto expr = std::make_shared<omnisketch::OrExpression<size_t>>(&sketch, test_values);
		auto card_est = expr->Execute();
		EXPECT_GE(card_est.cardinality, i + 1);
		EXPECT_LE(card_est.cardinality, (i + 1) * 1.5);
	}
}

TEST(OrExpressionTest, MultiSketchBasic) {
	omnisketch::OmniSketch sketch_1(16, 3, TUPLE_COUNT);
	omnisketch::OmniSketch sketch_2(16, 3, TUPLE_COUNT);
	std::vector<size_t> values(TUPLE_COUNT);

	for (size_t i = 0; i < TUPLE_COUNT; i++) {
		sketch_1.AddRecord(i, i);
		sketch_2.AddRecord(i, (TUPLE_COUNT - 1) - i);
		values[i] = i;
	}

	for (size_t i = 0; i < TUPLE_COUNT; i++) {
		std::vector<size_t> test_values_1(values.begin(), std::next(values.begin(), i + 1));
		std::vector<size_t> test_values_2(values.rbegin(), std::next(values.rbegin(), i + 1));
		std::vector<std::shared_ptr<omnisketch::Expression>> child_exprs;
		child_exprs.push_back(std::make_shared<omnisketch::OrExpression<size_t>>(&sketch_1, test_values_1));
		child_exprs.push_back(std::make_shared<omnisketch::OrExpression<size_t>>(&sketch_2, test_values_2));
		auto expr = std::make_shared<omnisketch::OrExpression<size_t>>(nullptr, std::vector<size_t>(), child_exprs);
		auto card_est = expr->Execute();
		EXPECT_GE(card_est.cardinality, 2 * (i + 1));
		EXPECT_LE(card_est.cardinality, 3 * (i + 1));
	}
}

TEST(AndExpressionTest, MultiSketchBasic) {
	omnisketch::OmniSketch sketch_1(16, 3, TUPLE_COUNT);
	omnisketch::OmniSketch sketch_2(16, 3, TUPLE_COUNT);
	std::vector<size_t> values(TUPLE_COUNT);

	for (size_t i = 0; i < TUPLE_COUNT; i++) {
		sketch_1.AddRecord(i, i);
		sketch_2.AddRecord(i, i);
		values[i] = i;
	}

	for (size_t i = 0; i < TUPLE_COUNT; i++) {
		std::vector<size_t> test_values(values.begin(), std::next(values.begin(), i + 1));
		std::vector<std::shared_ptr<omnisketch::Expression>> child_exprs;
		child_exprs.push_back(std::make_shared<omnisketch::OrExpression<size_t>>(&sketch_1, test_values));
		child_exprs.push_back(std::make_shared<omnisketch::OrExpression<size_t>>(&sketch_2, test_values));
		auto expr = std::make_shared<omnisketch::AndExpression>(child_exprs);
		auto card_est = expr->Execute();
		EXPECT_GE(card_est.cardinality, i + 1);
		EXPECT_LE(card_est.cardinality, 1.5 * (i + 1));
	}
}