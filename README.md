# OmniSketchCpp
This is a C++ OmniSketch library.

**Paper:** [Join Cardinality Estimation with OmniSketches](https://arxiv.org/abs/2508.17931) (2025)

## 🛠️ Compilation

For development:
```shell
mkdir build && cd build
cmake .. -DOMNISKETCH_ENABLE_TESTS=ON -DOMNISKETCH_ENABLE_BENCHMARKS=ON
make
```

In CLion, insert into CMake Arguments field in build settings:
```
-G Ninja -DOMNISKETCH_ENABLE_TESTS=ON -DOMNISKETCH_ENABLE_BENCHMARKS=ON
```

## 🚀 Getting Started

### OmniSketch Creation and Probing

```cpp
#include "registry.hpp"

auto& registry = Registry::Get();
// Register
auto omni_sketch = registry.CreateOmniSketch<size_t>("table", "attribute");
// Fill
for (size_t i = 0; i < 1000; i++) {
    const size_t record_id = i;
    const size_t attribute_value = i / 10;
    omni_sketch->AddRecord(attribute_value, record_id);
}
// Probe
const auto card_est = omni_sketch->Probe(42);
std::cout << "CardEst(table.attribute = 42): " << card_est->RecordCount() << std::endl;
```

### Join Cardinality Estimation

```cpp
#include "combinator.hpp"
#include "execution/query_graph.hpp"
    
// We assume to have fact_table and dim_table in registry

QueryGraph qg;
// Add a predicate on the dimension table
const auto predicate = PredicateConverter::ConvertPoint<size_t>(42);
qg.AddConstantPredicate("dim_table", "attribute", predicate);
// Add join
qg.AddPkFkJoin("fact_table", "foreign_key_column", "dim_table");
// Traverse join graph to estimate cardinality
const double card_est = qg.Estimate();
std::cout << "CardEst(fact_table ⋈ dimension_table, dim_table.attribute = 42): "
          << card_est << std::endl;
```

## 🎓 Citation
```bibtex
@misc{justen2025omnisketch-join,
    title={Join Cardinality Estimation with OmniSketches}, 
    author={David Justen and 
            Matthias Boehm},
    year={2025},
    eprint={2508.17931},
    archivePrefix={arXiv},
    primaryClass={cs.DB},
    url={https://arxiv.org/abs/2508.17931},
}
```
