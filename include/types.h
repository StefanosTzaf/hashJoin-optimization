// how many bits will be used (from hash function output) for the directory indexing
// for 16 bits we will have 2^16 directory entries , Hardcoded so as to be able to be changed to find the bestvalue
constexpr uint32_t PREFIX_BITS = 16;
constexpr uint32_t PREFIX_COUNT = 1u << PREFIX_BITS;

// parallel build structures
// 5 bits = 32 partitions (1 per thread for 24 threads) -- (for github runner)
constexpr int PARTITION_BITS = 3;
constexpr int NUM_PARTITIONS = 1 << PARTITION_BITS;
constexpr int NUMBER_OF_THREADS = 8;

// used for calculating range of directory entries:
// e.g prefix_bits = 6(64 entries) and partition_bits = 3(8 partitions)
// each partition covers 8 dir entries 
// shift = 6 - 3 = 3
// for p = 2: start_prefix = 2 << 3 = 16, end_prefix = (2 + 1) << 16 = 24
constexpr uint32_t SHIFT = PREFIX_BITS - PARTITION_BITS;