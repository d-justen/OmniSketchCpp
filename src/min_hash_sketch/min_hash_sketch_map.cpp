#include "min_hash_sketch/min_hash_sketch_map.hpp"

#include "min_hash_sketch/min_hash_sketch_vector.hpp"

namespace omnisketch {

void MinHashSketchMap::AddRecord(uint64_t hash, uint64_t value) {
    if (data.size() < max_count) {
        data[hash] = value;
    } else {
        auto index_it = std::prev(data.end());
        const uint64_t max_hash = index_it->first;

        if (hash < max_hash) {
            data[hash] = value;
            if (data.size() > max_count) {
                data.erase(index_it);
            }
        }
    }
}

void MinHashSketchMap::AddRecord(uint64_t) {
    throw std::logic_error("MinHashSketchMap needs to be called with a hash pair.");
}

size_t MinHashSketchMap::Size() const {
    return data.size();
}

size_t MinHashSketchMap::MaxCount() const {
    return max_count;
}

std::shared_ptr<MinHashSketch> MinHashSketchMap::Resize(size_t size) const {
    auto result = std::make_shared<MinHashSketchMap>(size);
    for (auto it : data) {
        result->AddRecord(it.first, it.second);
    }
    return result;
}

std::shared_ptr<MinHashSketch> MinHashSketchMap::Flatten() const {
    std::vector<uint64_t> result_vec;
    result_vec.reserve(data.size());
    for (auto it : data) {
        result_vec.push_back(it.first);
    }
    return std::make_shared<MinHashSketchVector>(std::move(result_vec));
}

std::shared_ptr<MinHashSketch> MinHashSketchMap::Intersect(const std::vector<std::shared_ptr<MinHashSketch>>& sketches,
                                                           size_t max_sample_count) {
    return MinHashSketchVector::ComputeIntersection(sketches, nullptr, max_sample_count);
}

void MinHashSketchMap::Combine(const MinHashSketch& other) {
    auto other_it = other.Iterator();
    auto other_map_it = dynamic_cast<MinHashSketchMap::SketchIterator*>(other_it.get());
    assert(other_map_it);
    while (!other_map_it->IsAtEnd()) {
        const auto& kv_pair = other_map_it->CurrentKeyVal();
        if (data.size() == max_count && kv_pair.first > data.rbegin()->first) {
            return;
        }
        AddRecord(kv_pair.first, kv_pair.second);
        other_map_it->Next();
    }
}

std::shared_ptr<MinHashSketch> MinHashSketchMap::Combine(
    const std::vector<std::shared_ptr<MinHashSketch>>& others) const {
    auto result = std::make_shared<MinHashSketchMap>(others.front()->MaxCount());
    for (const auto& sketch : others) {
        result->Combine(*sketch);
    }
    return result;
}

std::shared_ptr<MinHashSketch> MinHashSketchMap::Copy() const {
    auto result = std::make_shared<MinHashSketchMap>(max_count);
    result->data = data;
    return result;
}

size_t MinHashSketchMap::EstimateByteSize() const {
    const size_t max_count_size = sizeof(size_t);
    const size_t set_overhead = 16 + 16;
    const size_t per_item_size = 2 * (32 + sizeof(uint64_t));
    return max_count_size + set_overhead + data.size() * per_item_size;
}

std::unique_ptr<MinHashSketch::SketchIterator> MinHashSketchMap::Iterator() const {
    return std::make_unique<MinHashSketchMap::SketchIterator>(data.cbegin(), data.size());
}

std::unique_ptr<MinHashSketch::SketchIterator> MinHashSketchMap::Iterator(size_t max_sample_count) const {
    return std::make_unique<MinHashSketchMap::SketchIterator>(data.cbegin(), std::min(data.size(), max_sample_count));
}

std::map<uint64_t, uint64_t>& MinHashSketchMap::Data() {
    return data;
}

const std::map<uint64_t, uint64_t>& MinHashSketchMap::Data() const {
    return data;
}

void MinHashSketchMap::EraseRecord(uint64_t hash) {
    data.erase(hash);
}

std::shared_ptr<MinHashSketchMap> MinHashSketchMap::IntersectMap(std::vector<std::shared_ptr<MinHashSketch>>& sketches,
                                                                 size_t n_max, size_t max_sample_size) {
    std::vector<std::unique_ptr<MinHashSketch::SketchIterator>> offsets;
    offsets.reserve(sketches.size());

    for (const auto& sketch : sketches) {
        offsets.push_back(sketch->Iterator());
    }

    auto result = std::make_shared<MinHashSketchMap>(UINT64_MAX);
    auto& result_data = result->Data();

    while (!offsets[0]->IsAtEnd()) {
        uint64_t current_hash = offsets[0]->Current();
        size_t current_n_max = std::max(n_max, (size_t)offsets[0]->CurrentValueOrDefault(0));

        bool found_match = true;
        for (size_t sketch_idx = 1; sketch_idx < offsets.size(); sketch_idx++) {
            uint64_t other_hash = offsets[sketch_idx]->Current();
            while (!offsets[sketch_idx]->IsAtEnd() && other_hash < current_hash) {
                offsets[sketch_idx]->Next();
                other_hash = offsets[sketch_idx]->Current();
            }

            if (offsets[sketch_idx]->IsAtEnd()) {
                // There can be no other matches
                return result;
            }

            if (current_hash == other_hash) {
                // We have a match
                current_n_max = std::max(current_n_max, (size_t)offsets[sketch_idx]->CurrentValueOrDefault(0));
                continue;
            }

            // No match, start from the beginning
            found_match = false;
            while (!offsets[0]->IsAtEnd() && offsets[0]->Current() < other_hash) {
                offsets[0]->Next();
            }
            // Back to the start
            break;
        }
        if (found_match) {
            result_data[current_hash] = static_cast<uint64_t>((double)current_n_max / (double)max_sample_size);
            offsets[0]->Next();
        }
    }

    return result;
}

}  // namespace omnisketch
