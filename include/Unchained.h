
#include <cstdint>
#include <vector>
#include <array>
#include <nmmintrin.h>
#include <random>
#include "slab.h"



struct Tuple {
    int32_t key;
    size_t row_ids;
    
    Tuple() : key(0), row_ids(0) {}
    Tuple(int32_t k, size_t r) : key(k), row_ids(r) {}
};


struct DirectoryEntry {
    // every entry in the directory contains a bloom filter and a pointer packed in 64 bits
    uint64_t data;
    // as reffered in the paper, save all possible cobination so as to speed up probe
    static std::array<uint16_t, 2048> bloom_lookup;
    

    DirectoryEntry() : data(0){
    }
    
    // get the pointer (high 48 bits)
    inline Tuple* getPointer() const {
        return reinterpret_cast<Tuple*>(data >> 16);
    }

    
    // Returns the Bloom filter (low 16 bits)
    inline uint16_t getBloomFilter() const {
        return (uint16_t)(data & 0xFFFF);
    }
    
    inline void setPointer(Tuple* ptr) {
        uint64_t p = reinterpret_cast<uint64_t>(ptr);
        data = (p << 16) | (data & 0xFFFF);
    }
    
    inline void setBloomFilter(uint16_t bloom) {
        data = (data & 0xFFFFFFFFFFFF0000ULL) | bloom;
    }
    
    // it returns the real bloom tag from the hash padded as paper says
    static inline uint16_t bloomTag(uint32_t hash) {
        return (hash >> 17) & 0x7FF;
    }

    inline void bloomAdd(uint32_t hash) {
        uint16_t bloom = getBloomFilter();
        // gets 11 bits from hash because we have 2048 entries
        // sets the bits in the bloom filter
        bloom |= bloom_lookup[bloomTag(hash)];
        setBloomFilter(bloom);
    }

    static void initBloomLookup();
};




// Unchained Hash Table Class , (all the logic inside, directory entries that contain bloom filters and pointers to tuples)
class UnchainedHashTable {

    private:

        // local partitions per thread (slab allocator)
        ThreadLocalTupleCollector local_partitions[NUMBER_OF_THREADS];
        
        // global allocator
        GlobalAllocator global_allocator;

        // final contiguous storage
        Tuple* final_storage;
        size_t total_tuples;

        // directory entries
        std::vector<DirectoryEntry> directory;

        // how many bits will be used (from hash function output) for the directory indexing
        // for 16 bits we will have 2^16 directory entries , Hardcoded so as to be able to be changed to find the bestvalue
        static constexpr uint32_t PREFIX_BITS = 16;
        static constexpr uint32_t PREFIX_COUNT = 1u << PREFIX_BITS;
        
        inline uint32_t hash_prefix(int32_t key) {
            uint32_t h = _mm_crc32_u32(0, key);
            return h >> (32 - PREFIX_BITS);
        }

        inline uint32_t get_partition_id(int32_t key) {
            uint32_t h = _mm_crc32_u32(0, key);
            return h >> (32 - PARTITION_BITS);
        }

        void count_prefixes();

        void compute_prefix_offsets();

        void scatter_tuples();
        

    public:
        // as reffered in the paper, 4 MB large chunk, 256 KB small chunk
        UnchainedHashTable(size_t large_chunk_bytes = 1 << 22, size_t small_chunk_bytes = 1 << 18);

        // insert tuple for current thread
        void insert(int32_t key, size_t row_id);

        // merges all partitions[0], all partitions[1] etc into a single partition
        void mergePartitions();

        void build();

        std::vector<size_t> search(int32_t key);
};