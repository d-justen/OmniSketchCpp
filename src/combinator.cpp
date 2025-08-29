#include "combinator.hpp"

#include "omni_sketch/pre_joined_omni_sketch.hpp"
#include "omni_sketch/standard_omni_sketch.hpp"

namespace omnisketch {

size_t GetMaxRecordCount(const std::vector<std::shared_ptr<OmniSketchCell>>& matches) {
    size_t max_record_count = 0;
    for (const auto& match : matches) {
        max_record_count = std::max(max_record_count, match->RecordCount());
    }
    return max_record_count;
}

void AddResultHashesToMap(const std::shared_ptr<OmniSketchCell>& result, const std::shared_ptr<MinHashSketchMap>& map,
                          size_t max_record_count) {
    for (auto it = result->GetMinHashSketch()->Iterator(); !it->IsAtEnd(); it->Next()) {
        map->AddRecord(it->Current(), max_record_count);
    }
}

void CombinedPredicateEstimator::AddPredicate(const std::shared_ptr<OmniSketch>& omni_sketch,
                                              const std::shared_ptr<OmniSketchCell>& probe_sample) {
    base_card = std::max(base_card, omni_sketch->RecordCount());

    if (probe_sample->SampleCount() > MAX_JOIN_PROBE_COUNT) {
        probe_sample->SetMinHashSketch(probe_sample->GetMinHashSketch()->Resize(MAX_JOIN_PROBE_COUNT));
    }

    PredicateResult predicate_result;
    predicate_result.is_set_membership = probe_sample->SampleCount() > 1;
    predicate_result.sampling_probability = probe_sample->SamplingProbability();

    std::vector<std::shared_ptr<OmniSketchCell>> matches(omni_sketch->Depth());

    if (probe_sample->SampleCount() == 1) {
        ProcessSingleSamplePredicate(omni_sketch, probe_sample, matches, predicate_result);
        return;
    }

    ProcessMultiSamplePredicate(omni_sketch, probe_sample, matches, predicate_result);
}

void CombinedPredicateEstimator::ProcessSingleSamplePredicate(const std::shared_ptr<OmniSketch>& omni_sketch,
                                                              const std::shared_ptr<OmniSketchCell>& probe_sample,
                                                              std::vector<std::shared_ptr<OmniSketchCell>>& matches,
                                                              PredicateResult& predicate_result) {
    auto probe_result =
        omni_sketch->ProbeHash(probe_sample->GetMinHashSketch()->Iterator()->Current(), matches, max_sample_count);

    predicate_result.sketch = probe_result->GetMinHashSketch();
    predicate_result.n_max = GetMaxRecordCount(matches);
    predicate_result.selectivity = static_cast<double>(probe_result->RecordCount()) / static_cast<double>(base_card);

    if (predicate_result.selectivity == 0.0) {
        const double normalized_max_count =
            static_cast<double>(predicate_result.n_max) / static_cast<double>(omni_sketch->MinHashSketchSize());
        predicate_result.fallback_selectivity = normalized_max_count / static_cast<double>(base_card);
    }

    intermediate_results.push_back(std::move(predicate_result));
}

void CombinedPredicateEstimator::ProcessMultiSamplePredicate(const std::shared_ptr<OmniSketch>& omni_sketch,
                                                             const std::shared_ptr<OmniSketchCell>& probe_sample,
                                                             std::vector<std::shared_ptr<OmniSketchCell>>& matches,
                                                             PredicateResult& predicate_result) {
    size_t cardinality = 0;
    auto result_map = std::make_shared<MinHashSketchMap>(UINT64_MAX);
    predicate_result.sketch = result_map;
    size_t max_record_count_sum = 0;

    for (auto probe_it = probe_sample->GetMinHashSketch()->Iterator(); !probe_it->IsAtEnd(); probe_it->Next()) {
        auto probe_result = omni_sketch->ProbeHash(probe_it->Current(), matches, max_sample_count);
        const size_t max_record_count = GetMaxRecordCount(matches);
        max_record_count_sum += max_record_count;
        AddResultHashesToMap(probe_result, result_map, max_record_count);
        cardinality += probe_result->RecordCount();
    }

    predicate_result.selectivity = static_cast<double>(cardinality) / static_cast<double>(base_card);

    if (predicate_result.selectivity == 0.0) {
        const double avg_max_record_count =
            static_cast<double>(max_record_count_sum) / static_cast<double>(probe_sample->SampleCount());
        const double normalized_avg = avg_max_record_count / static_cast<double>(omni_sketch->MinHashSketchSize());
        predicate_result.fallback_selectivity =
            (normalized_avg / static_cast<double>(base_card)) * static_cast<double>(probe_sample->SampleCount());
    }

    intermediate_results.push_back(std::move(predicate_result));
}

std::vector<std::shared_ptr<MinHashSketch>> ExtractMapsFromIntermediates(
    const std::vector<PredicateResult>& intermediates, size_t& max_record_count) {
    std::vector<std::shared_ptr<MinHashSketch>> result;
    result.reserve(intermediates.size());

    max_record_count = 0;
    for (const auto& intermediate : intermediates) {
        result.emplace_back(intermediate.sketch);
        if (!intermediate.is_set_membership) {
            max_record_count = std::max(max_record_count, intermediate.n_max);
        }
    }
    return result;
}

double GetPSample(const std::vector<PredicateResult>& intermediates) {
    double p_sample = 1.0;
    for (const auto& intermediate : intermediates) {
        p_sample *= intermediate.sampling_probability;
    }
    return p_sample;
}

std::shared_ptr<OmniSketchCell> CombinedPredicateEstimator::ComputeResult(size_t max_output_size) const {
    auto result = std::make_shared<OmniSketchCell>(max_output_size);
    if (intermediate_results.empty()) {
        result->SetRecordCount(base_card);
        return result;
    }

    if (intermediate_results.size() == 1) {
        auto& intermediate = intermediate_results.front();
        auto sketch = intermediate.is_set_membership ? intermediate.sketch->Flatten() : intermediate.sketch;
        result->SetMinHashSketch(sketch);
        double sel = intermediate.selectivity == 0 ? intermediate.fallback_selectivity : intermediate.selectivity;
        result->SetRecordCount(
            static_cast<size_t>(std::round(((double)base_card * (double)sel) / intermediate.sampling_probability)));
        return result;
    }

    size_t n_max = 0;
    auto intermediate_maps = ExtractMapsFromIntermediates(intermediate_results, n_max);
    auto intersect_result = MinHashSketchMap::IntersectMap(intermediate_maps, n_max, max_sample_count);
    double p_sample = GetPSample(intermediate_results);

    if (intersect_result->Size() > 0) {
        // We add the values of the intersect result (which are each n_max/B) and divide them by all p_samples
        size_t card_sum = 0;
        for (auto it = intersect_result->Iterator(); !it->IsAtEnd(); it->Next()) {
            card_sum += it->CurrentValueOrDefault(0);
        }
        double card_est = (double)card_sum / p_sample;
        result->SetRecordCount(static_cast<size_t>(std::round(card_est)));
        result->SetMinHashSketch(intersect_result->Flatten());
        return result;
    }

    // We have run out of witnesses :( We have to intersect step-by-step
    auto partial_result = intermediate_results.front().sketch;
    double current_card_est = (double)base_card * intermediate_results.front().GetSel();
    assert(current_card_est > 0);
    n_max = intermediate_results.front().is_set_membership ? 0 : intermediate_results.front().n_max;
    double post_intersection_sel = 1.0;
    if (partial_result->Size() == 0) {
        post_intersection_sel *= intermediate_results.front().GetSel();
    }

    for (size_t i = 1; i < intermediate_results.size(); i++) {
        if (partial_result->Size() > 0) {
            n_max = std::max(n_max, intermediate_results[i].is_set_membership ? 0 : intermediate_results[i].n_max);
            std::vector<std::shared_ptr<MinHashSketch>> to_intersect{partial_result, intermediate_results[i].sketch};
            partial_result = MinHashSketchMap::IntersectMap(to_intersect, n_max, max_sample_count);

            if (partial_result->Size() > 0) {
                for (auto it = intersect_result->Iterator(); !it->IsAtEnd(); it->Next()) {
                    current_card_est += (double)it->CurrentValueOrDefault(0);
                }
            } else {
                // We have reached the end of witnesses. From now on, just multiply selectivities
                post_intersection_sel *= intermediate_results[i].GetSel();
            }
        } else {
            post_intersection_sel *= intermediate_results[i].GetSel();
        }
    }

    double final_card_est = (current_card_est * post_intersection_sel) / p_sample;
    result->SetMinHashSketch(partial_result);
    result->SetRecordCount(static_cast<size_t>(std::round(final_card_est)));
    return result;
}

std::shared_ptr<OmniSketchCell> CombinedPredicateEstimator::FilterProbeSet(
    const std::shared_ptr<OmniSketch>& omni_sketch, const std::shared_ptr<OmniSketchCell>& probe_sample) const {
    CombinedPredicateEstimator estimator(omni_sketch->MinHashSketchSize());
    estimator.intermediate_results = intermediate_results;
    estimator.AddPredicate(omni_sketch, probe_sample);
    return estimator.ComputeResult(probe_sample->MaxSampleCount());
}

void CombinedPredicateEstimator::AddUnfilteredRids(const std::shared_ptr<OmniSketch>& omni_sketch) {
    PredicateResult predicate_result;
    predicate_result.selectivity = static_cast<double>(omni_sketch->RecordCount() - omni_sketch->CountNulls()) /
                                   static_cast<double>(omni_sketch->RecordCount());
    predicate_result.is_set_membership = true;
    predicate_result.sampling_probability = 1.0;
    auto all_rids = omni_sketch->GetRids();
    predicate_result.sketch = all_rids->GetMinHashSketch();
    intermediate_results.push_back(std::move(predicate_result));
}

void CombinedPredicateEstimator::AddUnfilteredRids(const std::shared_ptr<OmniSketchCell>& probe_sample,
                                                   size_t base_card_p) {
    base_card = std::max(base_card, probe_sample->RecordCount());
    base_card = std::max(base_card, base_card_p);
    PredicateResult predicate_result{probe_sample->GetMinHashSketch(), 1.0, 1.0, 1.0, true, 0};
    intermediate_results.push_back(std::move(predicate_result));
}

bool CombinedPredicateEstimator::HasPredicates() const {
    return !intermediate_results.empty();
}

// Order intermediate results descending by selectivity to get matches as long as possible
void CombinedPredicateEstimator::Finalize() {
    if (intermediate_results.size() <= 1) {
        return;
    }

    // Use stable_sort to maintain order for equal selectivities and avoid the overhead of map
    std::stable_sort(intermediate_results.begin(), intermediate_results.end(),
                     [](const PredicateResult& a, const PredicateResult& b) {
                         return a.selectivity > b.selectivity;  // Descending order
                     });
}

}  // namespace omnisketch
