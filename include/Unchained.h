#include <cstdint>
#include <vector>
#include <cstring>
#include <random>
#include <array>


struct Tuple {
    int32_t key;
    // row id not in a vector 
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
        return reinterpret_cast<Tuple*>(data & 0xFFFFFFFFFFFF0000ULL);
    }

    
    // Returns the Bloom filter (low 16 bits)
    inline uint16_t getBloomFilter() const {
        return (uint16_t)(data & 0xFFFF);
    }
    
    inline void setPointer(Tuple* ptr) {
        uint64_t p = reinterpret_cast<uint64_t>(ptr);
        data = (p & 0xFFFFFFFFFFFF0000ULL) | (data & 0xFFFF);
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


    // we initialize the bloom lookup table
    static void initBloomLookup() {

        // so as to be called only once even for different hash tables
        // look up table is the same!
        static bool initialized = false;
        if (initialized){
            return;
        }
        initialized = true;

        std::vector<uint16_t> masks;
        masks.reserve(1820);

        // first 1820 4-bit patterns (C(16,4)) will be the real masks 
        // all combinations have exactly 4 bits set as paper says
        for (int a = 0; a < 16; a++)
            for (int b = a + 1; b < 16; b++)
                for (int c = b + 1; c < 16; c++)
                    for (int d = c + 1; d < 16; d++)
                        masks.push_back((1u<<a)|(1u<<b)|(1u<<c)|(1u<<d));

        for (size_t i = 0; i < masks.size(); i++)
            bloom_lookup[i] = masks[i];

        // pad the rest to 2048 with random BUT with seed so as to be deterministic
        // those entries do not have exactly 4 bits set but it's ok for our purpose
        std::mt19937_64 rng(1234567);
        for (size_t i = masks.size(); i < 2048; i++)
            bloom_lookup[i] = (uint16_t)(rng() & 0xFFFF);
    }

};


// Unchained Hash Table Class , (all the logic inside, directory entries that contain bloom filters and pointers to tuples)
class UnchainedHashTable {

    private:

        // directory entries
        std::vector<DirectoryEntry> directory;

        // Tuple storage: sorted by key (TODO do we need Paginated?)
        std::vector<Tuple> tuple_buffer;
        
        // for insertion phase, in build phase they will be moved to tuple_buffer
        std::vector<Tuple> temp_tuples;

        // how many tuples per prefix
        std::vector<uint32_t> prefix_count;

        // offset for each prefix
        std::vector<uint32_t> prefix_offset;

        // how many bits will be used (from hash function output) for the directory indexing
        // for 16 bits we will have 2^16 directory entries , Hardcoded so as to be able to be changed to find the bestvalue
        static constexpr uint32_t PREFIX_BITS = 16;
        static constexpr uint32_t PREFIX_COUNT = 1u << PREFIX_BITS;

        // simple crc32 hash function 
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

        inline uint32_t hash_prefix(int32_t key) {
            uint32_t h = crc32(key);
            return h >> (32 - PREFIX_BITS);
        }

        void count_prefixes() {
            // initialize counts to zero
            prefix_count.assign(PREFIX_COUNT, 0);

            for (const Tuple& t : temp_tuples) {
                uint32_t prefix = hash_prefix(t.key);
                prefix_count[prefix]++;
            }
        }

        // computes where each prefix starts in the final tuple buffer
        void compute_prefix_offsets() {
            prefix_offset.assign(PREFIX_COUNT, 0);

            uint32_t running_sum = 0;
            for (uint32_t i = 0; i < PREFIX_COUNT; i++) {
                prefix_offset[i] = running_sum;
                running_sum += prefix_count[i];
            }

            // resize final buffer to exact number of tuples
            tuple_buffer.assign(running_sum, Tuple{});
        }
        
        // takes the real tuples and puts them in the final buffer in the right 
        // position (that has been precomputed from previous functions)
        void scatter_tuples() {
            // create write pointers so as not to modify prefix_offset
            std::vector<uint32_t> write_ptr = prefix_offset;

            for (const Tuple& t : temp_tuples) {
                uint32_t prefix = hash_prefix(t.key);
                uint32_t pos = write_ptr[prefix];
                // so as tuples with same prefix go to next position
                tuple_buffer[pos] = t;
                write_ptr[prefix]++;
            }

            // update directory entries
            for (uint32_t i = 0; i < PREFIX_COUNT; i++) {
                // Pointer to end of this bucket (even if count is 0)
                Tuple* bucket_end = &tuple_buffer[prefix_offset[i] + prefix_count[i]];
                
                // Clear low 16 bits before storing pointer
                uint64_t ptr_val = reinterpret_cast<uint64_t>(bucket_end) & 0xFFFFFFFFFFFF0000ULL;
                
                // Build bloom filter (only if bucket is non-empty)
                uint16_t bloom = 0;
                if (prefix_count[i] > 0) {
                    for (uint32_t j = prefix_offset[i]; j < prefix_offset[i] + prefix_count[i]; j++) {
                        bloom |= DirectoryEntry::bloom_lookup[DirectoryEntry::bloomTag(crc32(tuple_buffer[j].key))];
                    }
                }
                
                // Combine pointer and bloom
                directory[i].data = ptr_val | bloom;
            }
        }

    public:
        UnchainedHashTable(){
            // compute one time the bloom lookup table
            DirectoryEntry::initBloomLookup();
            directory.resize(1 << PREFIX_BITS);
            tuple_buffer.reserve(100000);
        }

        void insert(int32_t key, size_t row_id) {
            temp_tuples.emplace_back(key, row_id);
        }
        void build() {
            count_prefixes();
            compute_prefix_offsets();
            scatter_tuples();
            temp_tuples.clear();
        }


        std::vector<size_t> search(int32_t key) {
        }
};
