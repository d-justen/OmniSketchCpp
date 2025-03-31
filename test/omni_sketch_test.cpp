#include <gtest/gtest.h>

#include "omni_sketch.hpp"
#include "omni_sketch_cell.hpp"

TEST(OmniSketchTest, BasicEstimation) {
	auto sketch = std::make_shared<omnisketch::PointOmniSketch<int>>(4, 3, 8);
	sketch->AddRecord(1, 1);
	sketch->AddRecord(1, 2);
	EXPECT_EQ(sketch->Probe(1)->RecordCount(), 2);
	sketch->AddRecord(2, 3);
	sketch->AddRecord(3, 4);
	EXPECT_EQ(sketch->Probe(1)->RecordCount(), 2);
	EXPECT_EQ(sketch->Probe(3)->RecordCount(), 1);
	EXPECT_EQ(sketch->RecordCount(), 4);
	EXPECT_EQ(sketch->Probe(0)->RecordCount(), 0);
}

TEST(OmniSketchTest, NoMatches) {
	auto sketch = std::make_shared<omnisketch::PointOmniSketch<int>>(4, 3, 8);
	EXPECT_EQ(sketch->Probe(17)->RecordCount(), 0);
}

TEST(OmniSketchTest, FlattenEstimate) {
	auto sketch = std::make_shared<omnisketch::PointOmniSketch<int>>(4, 3, 8);

	for (size_t i = 0; i < 64; i++) {
		sketch->AddRecord(i, i);
	}

	auto card_est = sketch->Probe(17);
	sketch->Flatten();

	auto card_est_2 = sketch->Probe(17);
	EXPECT_EQ(card_est->RecordCount(), card_est_2->RecordCount());
	EXPECT_EQ(card_est->MaxSampleCount(), card_est_2->MaxSampleCount());
	EXPECT_EQ(card_est->SampleCount(), card_est_2->SampleCount());
}

TEST(OmniSketchTest, BasicEstimationString) {
	auto sketch = std::make_shared<omnisketch::PointOmniSketch<std::string>>(4, 3, 8);
	sketch->AddRecord("String #1", 1);
	sketch->AddRecord("String #1", 2);
	EXPECT_EQ(sketch->Probe("String #1")->RecordCount(), 2);
	sketch->AddRecord("Another String", 3);
	sketch->AddRecord("String #2", 4);
	EXPECT_EQ(sketch->Probe("String #1")->RecordCount(), 2);
	EXPECT_EQ(sketch->Probe("Another String")->RecordCount(), 1);
	EXPECT_EQ(sketch->RecordCount(), 4);
	EXPECT_EQ(sketch->Probe("String #3")->RecordCount(), 0);
}