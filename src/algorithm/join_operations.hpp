#pragma once

#include "omni_sketch_operations.hpp"

namespace omnisketch {

namespace Algorithm {

template <typename HashType>
static CardEstResult<HashType> BernoulliIntersection(const std::vector<CardEstResult<HashType>> sketches) {
	auto card_est = Intersection(sketches);
	double sampling_probability = 1;
	for (auto& sketch : sketches) {
		sampling_probability *= sketch.min_hash_sketch.Size() / sketch.cardinality;
	}
	card_est.cardinality /= sampling_probability;
	return card_est;
}

template <typename ValueType, typename RecordIdType, typename ContainerType>
static CardEstResult<typename ContainerType::value_type> Join(const OmniSketch<ValueType, RecordIdType, ContainerType> &sketch, CardEstResult<typename ContainerType::value_type> probe_sample) {
	// TODO: Skip this conversion and make algorithm work on sets
	std::vector<typename ContainerType::value_type> probe_sample_vec(probe_sample.min_hash_sketch.hashes.begin(), probe_sample.min_hash_sketch.hashes.end());
	auto join_estimate = DisjunctPointQueriesHashed(sketch, probe_sample_vec);
	//if (join_estimate.cardinality == 0) {
	//	return join_estimate;
	//}
	//const double sampling_probability = probe_sample.min_hash_sketch.Size() / probe_sample.cardinality;
	//const double sampling_probability = std::min(probe_sample.min_hash_sketch.max_count / probe_sample.cardinality, 1.0);
	//join_estimate.cardinality /= sampling_probability;
	return join_estimate;
}

template <typename ValueType, typename RecordIdType, typename ContainerType>
static CardEstResult<typename ContainerType::value_type> ApproximateJoin(const OmniSketch<ValueType, RecordIdType, ContainerType> &sketch, CardEstResult<typename ContainerType::value_type> probe_sample) {
	// TODO: Skip this conversion and make algorithm work on sets
	std::vector<typename ContainerType::value_type> probe_sample_vec(probe_sample.min_hash_sketch.hashes.begin(), probe_sample.min_hash_sketch.hashes.end());
	auto join_estimate = SetMembershipHashed(sketch, probe_sample_vec);
	if (join_estimate.cardinality == 0) {
		return join_estimate;
	}
	//const double sampling_probability = probe_sample.min_hash_sketch.Size() / probe_sample.cardinality;
	//join_estimate.cardinality /= sampling_probability;
	return join_estimate;
}

template <typename ValueType, typename ValueType2, typename RecordIdType, typename RecordIdType2, typename ContainerType, typename ContainerType2>
static CardEstResult<typename ContainerType::value_type>
SinglePredicateJoin(const OmniSketch<ValueType, RecordIdType, ContainerType> &fk_sketch, const OmniSketch<ValueType2, RecordIdType2, ContainerType2> &attribute_sketch, const ValueType2 &attribute_value) {
	const auto predicate_estimate = attribute_sketch.EstimateCardinality(attribute_value);
	std::vector<typename ContainerType2::value_type> sample(predicate_estimate.min_hash_sketch.hashes.cbegin(), predicate_estimate.min_hash_sketch.hashes.cend());
	std::vector<size_t> cell_idxs(fk_sketch.Depth());

	return Join(fk_sketch, predicate_estimate);
}

}

} // namespace omnisketch
