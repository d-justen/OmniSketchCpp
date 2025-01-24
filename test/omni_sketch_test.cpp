#include <gtest/gtest.h>

#include "omni_sketch.hpp"

TEST(OmniSketchTest, BasicEstimation) {
	omnisketch::OmniSketch sketch(4, 2, 2);
	sketch.AddRecord(1, 1);
	sketch.AddRecord(1, 2);
	EXPECT_EQ(sketch.EstimateCardinality(1), 2);
	sketch.AddRecord(2, 3);
	sketch.AddRecord(3, 4);
	EXPECT_EQ(sketch.EstimateCardinality(1), 2);
	EXPECT_EQ(sketch.EstimateCardinality(3), 1);
	EXPECT_EQ(sketch.RecordCount(), 4);
	EXPECT_EQ(sketch.EstimateCardinality(0), 0);
}
