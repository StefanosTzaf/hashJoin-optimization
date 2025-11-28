#include <cstdint>
#include <vector>
#include <cstring>
#include <random>
#include <array>

// every entry in the directory contains a bloom filter and a pointer
struct DirectoryEntry {
    uint64_t data;
    // as reffered in the paper, save all possible cobination so as to speed up probe
    static std::array<uint16_t, 2048> bloom_lookup;
    

    DirectoryEntry() : data(0){
    }
    
    // get the pointer (high 48 bits)
    inline uint64_t getPointer() const {
        return data >> 16; // cuts the 16 low bits
    }
    
    // Returns the Bloom filter (low 16 bits)
    inline uint16_t getBloomFilter() const {
        return (uint16_t)(data & 0xFFFF);
    }
    
    inline void setPointer(uint64_t ptr) {
        data = (ptr << 16) | (data & 0xFFFF);
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
        // gets 11 bits from hash becaueuase we have 2048 entries
        // sets the bits in the bloom filter
        bloom |= bloom_lookup[bloomTag(hash)];
        setBloomFilter(bloom);
    }


    // we initialize the bloom lookup table
    static void initBloomLookup() {
        std::vector<uint16_t> masks;
        masks.reserve(1820);

        // first 1820 4-bit patterns (C(16,4)) will be the real masks 
        // all combinations have 4 bits set as paper says
        for (int a = 0; a < 16; a++)
            for (int b = a + 1; b < 16; b++)
                for (int c = b + 1; c < 16; c++)
                    for (int d = c + 1; d < 16; d++)
                        masks.push_back((1u<<a)|(1u<<b)|(1u<<c)|(1u<<d));

        // real masks
        for (size_t i = 0; i < masks.size(); i++)
            bloom_lookup[i] = masks[i];

        // pad the rest to 2048 with random BUT with seed so as to be deterministic
        // those entries do not have exactly 4 bits set but it's ok for our purpose
        std::mt19937_64 rng(1234567);
        for (size_t i = masks.size(); i < 2048; i++)
            bloom_lookup[i] = (uint16_t)(rng() & 0xFFFF);
    }

};


struct Tuple {
    int32_t key;
    // row id not in a vector 
    size_t row_ids;
    
    Tuple() : key(0), row_ids(0) {}
    Tuple(int32_t k, size_t r) : key(k), row_ids(r) {}
};



class UnchainedHashTable {
private:
    

    // directory entries
    std::vector<DirectoryEntry> directory;
    // how many entries in the directory
    std::vector<size_t> dir_sizes;
    
    // Tuple storage: sorteda by key (TODO do we need Pagenated?)
    std::vector<Tuple> tuple_buffer;
    
    // how many bits will be used (from hash function output) for the directory indexing
    // for 16 bits we will have 2^16 directory entries , Hardcoded so as to be able to be changed to find the bestvalue
    static constexpr uint32_t PREFIX_BITS = 16;

    uint32_t crc32(uint32_t data) {
        uint32_t crc = 0xFFFFFFFF;
        // for each byte
        for (int i = 0; i < 4; i++) {
            uint8_t byte = (data >> (i*8)) & 0xFF;
            crc ^= byte;
            for (int j = 0; j < 8; j++) {
                if (crc & 1)
                    crc = (crc >> 1) ^ 0xEDB88320;
                else
                    crc = crc >> 1;
            }
        }
        return crc ^ 0xFFFFFFFF;
    }

    
    
    
    public:
        UnchainedHashTable(){
            // compute one time the bloom lookup table
            DirectoryEntry::initBloomLookup();
            directory.resize(1 << PREFIX_BITS);
            dir_sizes.resize(1 << PREFIX_BITS, 0);
            tuple_buffer.reserve(100000);
        }
};
