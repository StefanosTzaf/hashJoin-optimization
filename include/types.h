// how many bits will be used (from hash function output) for the directory indexing
// for 16 bits we will have 2^16 directory entries , Hardcoded so as to be able to be changed to find the bestvalue
constexpr uint32_t PREFIX_BITS = 16;
constexpr uint32_t PREFIX_COUNT = 1u << PREFIX_BITS;

// parallel build structures
// 5 bits = 32 partitions (1 per thread for 24 threads) -- (for github runner)
constexpr int PARTITION_BITS = 2;
constexpr int NUM_PARTITIONS = 1 << PARTITION_BITS;
constexpr int NUMBER_OF_THREADS = 4;