// parallel build structures
// 3 bits = 8 partitions (1 per thread for 8 threads)
constexpr int PARTITION_BITS = 3;
constexpr int NUM_PARTITIONS = 1 << PARTITION_BITS;
constexpr int NUMBER_OF_THREADS = 8;