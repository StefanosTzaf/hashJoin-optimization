# SIGMOD 2025 Contest: Join Pipeline Optimization

This repository contains our highly optimized implementation for the **SIGMOD 2025 Programming Contest**. The project was developed as part of the *Software Development for Information Systems (K23a)* university course of the University of Athens.

While the baseline joining pipeline and dataset were provided, we were tasked with researching, architecting, and implementing from scratch a series of advanced database optimization techniques. Through custom data layout structures, advanced hashing techniques, and lock-free parallelization, we achieved a **93.8% reduction in execution time** (from ~276 seconds down to ~17 seconds) and reduced peak memory consumption by nearly **50%**.

## 🚀 Quick Start

To build and run the optimized pipeline:

### 1. Download the IMDB dataset

> If you are using `Linux x86_64` you can download our prebuilt cache with:
> ```bash
> wget http://share.uoa.gr/protected/all-download/sigmod25/sigmod25_cache_x86.tar.gz
> ```
> If you are using `macOS arm64` you can download our prebuilt cache with:
> ```bash
> wget http://share.uoa.gr/protected/all-download/sigmod25/sigmod25_cache_arm.tar.gz
> ```
> For all other systems you will need to build the cache on your own.

### 2. Unzip the dataset
```bash
tar -xzf sigmod25_cache_x86.tar.gz
```

### 3. Build the project
```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev
cmake --build build -- -j $(nproc) fast
```

### 4. Run the queries
```bash
./build/fast plans.json
```

To run the unit tests:
```bash
./build/unit_tests
./build/unit_tests_unchained
./build/unit_tests_columnStore
```

## 🧠 Core Architecture & Key Optimizations

Our final solution (`main` branch) includes the following major architectural enhancements:

* **Late Materialization:** Tagging data types without the overhead of std::variant`std::variant` and materializing strings only when needed.
* **Paged Column-Store:** Transitioned intermediate results from row-store to a paged column-store structure with $O(\log n)$ access via binary search.
* **Unchained Hash Table:** An in-memory, hardware-accelerated unchained hash table utilizing an adjacency array layout and Bloom filters.
* **Zero-Copy Indexing:** Real-time detection of "dense" INT32 columns. These columns are indexed on the original input table and are used directly in intermediate results, eliminating unnecessary data copying.
* **Lock-Free Parallelization & Work Stealing:** Build and probe phases are parallelized. We implemented dynamic load balancing via an `atomic counter` where threads "steal" batches of pages.
* **Slab Allocator:** A three-level custom allocator handles tuple collection to minimize system calls.

---

## 🔬 Technical Deep Dive

### 1. Late Materialization & Bit Masking
To avoid the overhead of `std::variant`, we introduced a custom `value_t` struct that can represent both an INT32 or a VARCHAR. Integers are materialized directly (storing `tableIdx` and `columnIdx` in the first 32 bits) in the top level join. For type tagging, we utilize bit-masking on the 3MSB bits of the `dataIdx` field:

```cpp
struct value_t {
    uint16_t tableIdx; 
    uint16_t columnIdx; 
    uint16_t pageIdx; 
    uint16_t dataIdx; // 3 MSB used for tagging

    static constexpr uint16_t TYPE_MASK = 0xE000;  // 111...
    static constexpr uint16_t NULL_TAG  = 0x0000;  // 000...
    static constexpr uint16_t INT_TAG   = 0x4000;  // 010...
    static constexpr uint16_t STR_TAG   = 0x8000;  // 100...
    static constexpr uint16_t LONG_STR_TAG = 0xA000; // 101...
    static constexpr uint16_t OFFSET_MASK = 0x1FFF;  // 00011...1
};
```
By performing `dataIdx & TYPE_MASK`, we can instantly identify the type, and `dataIdx & OFFSET_MASK` produces the actual index within a page.

### 2. Column Store & Fast Access
Intermediate results are held in `ColumnT`, which maintains a `pageRowOffset` vector holding the number of rows needed to reach a certain page. To fetch a value at a specific row, `std::upper_bound()` performs a binary search on this vector, achieving $O(\log n)$ access time without iterating through all pages.

### 3. Unchained Hash Table
Instead of a chained hash table, we use an adjacency array layout. The core structures include:
* `Tuples` struct for key-value pairs.
* `DirectoryEntry` (64-bit integer): 48 bits for pointers and 16 bits for the Bloom filter.
* Hashing utilizes the hardware-accelerated **CRC32** via `_mm_crc32_u32` from `<nmmintrin.h>`.

### 4. Zero-Copy Indexing for Dense Columns
We dynamically detect "dense" columns (INT32 with no NULLs) by comparing the first two `uint16_t` metadata fields of each page (number of rows and number of non-null values). If they match, the column is dense.
For dense columns, `ColumnT` simply sets `copied = false` and uses a `colRef` pointer to reference the original input column. Data is accessed directly via `reinterpret_cast`, completely eliminating copy operations:

```cpp
// Direct access to original input page data
key = *reinterpret_cast<const int32_t*>(page->data + 2*sizeof(uint16_t) + row*sizeof(int32_t)); 
```

### 5. Probe Parallelization
During the probe phase, threads write their intermediate results locally to avoid lock contention:
```cpp
std::vector<std::vector<ColumnT>> threadResults(NUMBER_OF_THREADS);
std::vector<std::vector<ColumnTInserter>> threadInserters(NUMBER_OF_THREADS);
```
Each thread inserts matching rows via `threadInserters[threadId][output_col].insert()`. Upon completion, a single thread efficiently merges all local `threadResults` into the final table.

---

## 📌 Version History & Project Evolution

To review the chronological evolution of the project and our intermediate implementations, you can checkout the following Git tags:

* **`v1.0.0`**: Initial implementations of 3 custom Hash Tables (Robin Hood, Hopscotch, Cuckoo). In tis commit there is also a local README file with the description of the hash tables and their implementation details.

* **`v2.1.0`** Late Materialization
* **`v2.2.0`** ColumnStore 
* **`v2.3.0`** Unchained Hash Table
* **`v2.3.1`**: Integration of Late Materialization, Column-Store format, and the Unchained Hash Table and some errors fixed. (newReadme.md file in this commit contains a detailed description of the architecture and the optimizations implemented up to this point)
* **`v3.1.0`**: Introduction of Zero-Copy Indexing optimizations for dense columns.
* **`v3.2.0`** *(Latest)*: Full parallelization of Build/Probe phases, Work Stealing, and Slab Allocator integration.

## 📄 Full Technical Documentation

For an in-depth technical analysis of our algorithms, design choices, scaling benchmarks, and memory consumption charts, you can read our full report:
👉 **[`docs/English/SIGMOD_2025_Join_Pipeline_Report.pdf`](docs/English/SIGMOD_2025_Join_Pipeline_Report.pdf)**

*(Note: Greek and English versions of the assignment instructions are available in the `docs/` directory. Reports in Greek are also available divided in 3 parts, in English they have been merged in one final report mentioned above.)*

## 👥 Team
* **Stefanos Tzaferis** - stzaferis04@gmail.com
* **Eleftheria Galiatsatou** - eleftheria.galiatsatou@gmail.com