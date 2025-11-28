#include <cstdint>
#include <vector>
#include <cstring>


// every entry in the directory contains a bloom filter and a pointer
struct DirectoryEntry {
    uint64_t data;
    

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
        bloom |= bloom_lookup[bloomTag(hash)];
        setBloomFilter(bloom);
    }


    // we initialize the bloom lookup table
    static void initBloomLookup() {
        std::vector<uint16_t> masks;
        masks.reserve(1820);

        // first 1820 4-bit patterns (C(16,4)) will be the real masks 
        for (int a = 0; a < 16; a++)
            for (int b = a + 1; b < 16; b++)
                for (int c = b + 1; c < 16; c++)
                    for (int d = c + 1; d < 16; d++)
                        masks.push_back((1u<<a)|(1u<<b)|(1u<<c)|(1u<<d));

        // real masks
        for (size_t i = 0; i < masks.size(); i++)
            bloom_lookup[i] = masks[i];

        // pad the rest to 2048 with random BUT with seed so as to be deterministic
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

/**
 * Unchained Hash Table
 * Υλοποίηση του hashtable χωρίς αλυσίδες βάσει του paper.
 */
class UnchainedHashTable {
private:
    // as reffered in the paper, save all possible cobination so as to speed up probe
    static std::array<uint16_t, 2048> bloom_lookup;

    // directory entries
    std::vector<DirectoryEntry> directory;
    
    // Tuple storage: sorteda by key (TODO do we need Pagenated?)
    std::vector<Tuple> tuple_buffer;
    
    // how many entries in the directory
    size_t directory_size;
    
    // Number of bits for the hash prefix (for indexing into the directory)
    uint32_t prefix_bits;

    
public:

};
