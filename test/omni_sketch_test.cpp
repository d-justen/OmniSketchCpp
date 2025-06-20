#include <gtest/gtest.h>

#include "min_hash_sketch/min_hash_sketch_set.hpp"
#include "omni_sketch/standard_omni_sketch.hpp"
#include "omni_sketch/foreign_sorted_omni_sketch.hpp"

TEST(OmniSketchTest, BasicEstimation) {
	auto sketch = std::make_shared<omnisketch::TypedPointOmniSketch<int>>(4, 3, 8);
	sketch->AddRecord(1, 1);
	sketch->AddRecord(1, 2);
	EXPECT_EQ(sketch->Probe(1)->RecordCount(), 2);
	sketch->AddRecord(2, 3);
	sketch->AddRecord(3, 4);
	EXPECT_EQ(sketch->Probe(1)->RecordCount(), 2);
	EXPECT_EQ(sketch->Probe(3)->RecordCount(), 1);
	EXPECT_EQ(sketch->RecordCount(), 4);
	EXPECT_EQ(sketch->Probe(4)->RecordCount(), 0);
}

TEST(OmniSketchTest, NoMatches) {
	auto sketch = std::make_shared<omnisketch::TypedPointOmniSketch<int>>(4, 3, 8);
	EXPECT_EQ(sketch->Probe(17)->RecordCount(), 0);
}

TEST(OmniSketchTest, FlattenEstimate) {
	auto sketch = std::make_shared<omnisketch::TypedPointOmniSketch<int>>(4, 3, 8);

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
	auto sketch = std::make_shared<omnisketch::TypedPointOmniSketch<std::string>>(4, 3, 8);
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

TEST(OmniSketchTest, GetAllRids) {
	auto sketch = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(4, 3, 8);
	auto control_sketch = std::make_shared<omnisketch::MinHashSketchSet>(8);
	auto hf = std::make_shared<omnisketch::MurmurHashFunction<size_t>>();
	for (size_t i = 0; i < 1000; i++) {
		sketch->AddRecord(i % 3, i);
		control_sketch->AddRecord(hf->HashRid(i));
	}
	auto unfiltered_rids = sketch->GetRids();
	EXPECT_EQ(unfiltered_rids->RecordCount(), 1000);
	EXPECT_EQ(unfiltered_rids->MaxSampleCount(), control_sketch->MaxCount());
	ASSERT_EQ(unfiltered_rids->SampleCount(), control_sketch->Size());

	auto intersection = control_sketch->Intersect({control_sketch, unfiltered_rids->GetMinHashSketch()});
	EXPECT_EQ(intersection->Size(), control_sketch->Size());
}

TEST(OmniSketchTest, ForeignSortedOmniSketch) {
	auto sketch = std::make_shared<omnisketch::TypedPointOmniSketch<size_t>>(4, 3, 8);
	auto ref_sketch = std::make_shared<omnisketch::ForeignSortedOmniSketch<size_t>>(sketch, 4, 3, 8);
	for (size_t i = 0; i < 1000; i++) {
		sketch->AddRecord(i % 100, i);
	}

	for (size_t i = 0; i < 100; i++) {
		ref_sketch->AddRecord(i % 10, i);
	}

	auto probe_result = ref_sketch->Probe(0);
}

TEST(OmniSketchTest, LikePredicate) {
	auto sketch = std::make_shared<omnisketch::TypedPointOmniSketch<std::string>>(4, 3, 8);
	sketch->AddRecord("cabab", 0);
	sketch->AddRecord("rayex", 1);
	sketch->AddRecord("xabab", 2);
	sketch->AddRecord("fasuy", 3);
	sketch->AddRecord("lakab", 4);
	sketch->AddRecord("cahob", 5);
	sketch->AddRecord("nauaw", 6);
	sketch->AddRecord("nabab", 7);

	auto card_est = sketch->S_Like("%a_a%");
	EXPECT_EQ(card_est->RecordCount(), 5);
	EXPECT_EQ(card_est->SampleCount(), 5);
}
