// parallel build structures
// 5 bits = 32 partitions (1 per thread for 24 threads) -- (for github runner)
constexpr int PARTITION_BITS = 5;
constexpr int NUM_PARTITIONS = 1 << PARTITION_BITS;
constexpr int NUMBER_OF_THREADS = 24;