#include "Unchained.h"


std::array<uint16_t,2048> DirectoryEntry::bloom_lookup;



void DirectoryEntry::initBloomLookup() {

    static bool initialized = false;
    // so as to be called only once even for different hash tables
    // look up table is the same!
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


UnchainedHashTable::UnchainedHashTable(){
    // compute one time the bloom lookup table
    DirectoryEntry::initBloomLookup();
    directory.resize(PREFIX_COUNT);
    tuple_buffer.reserve(100000);
}

void UnchainedHashTable::count_prefixes() {
    // initialize counts to zero
    prefix_count.assign(PREFIX_COUNT, 0);

    for (const Tuple& t : temp_tuples) {
        uint32_t prefix = hash_prefix(t.key);
        prefix_count[prefix]++;
    }
}


// computes where each prefix starts in the final tuple buffer
void UnchainedHashTable::compute_prefix_offsets() {
    prefix_offset.assign(PREFIX_COUNT, 0);

    uint32_t running_sum = 0;
    for (uint32_t i = 0; i < PREFIX_COUNT; i++) {
        prefix_offset[i] = running_sum;
        running_sum += prefix_count[i];
    }

    // resize final buffer to exact number of tuples
    tuple_buffer.resize(running_sum);
}


// takes the real tuples and puts them in the final buffer in the right 
// position (that has been precomputed from previous functions)
void UnchainedHashTable::scatter_tuples() {
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
        Tuple* bucket_end = nullptr;
        if (!tuple_buffer.empty()) {
            // data returns pointer to first element, so we add offset + count to get end
            bucket_end = tuple_buffer.data() + (prefix_offset[i] + prefix_count[i]);
        }

        // Build bloom filter (only if bucket is non-empty)
        uint16_t bloom = 0;
        if (prefix_count[i] > 0) {
            for (uint32_t j = prefix_offset[i]; j < prefix_offset[i] + prefix_count[i]; j++) {
                bloom |= DirectoryEntry::bloom_lookup[DirectoryEntry::bloomTag(_mm_crc32_u32(0, (uint32_t)tuple_buffer[j].key))];
            }
        }

        // Combine pointer and bloom: pointer in upper bits, bloom in low 16 bits
        directory[i].setPointer(bucket_end);
        directory[i].setBloomFilter(bloom);
    }
}


std::vector<size_t> UnchainedHashTable::search(int32_t key) {
    std::vector<size_t> results;

    // 1. check if table is empty
    if (tuple_buffer.empty()) {
        return results;
    }
    
    // 2. Compute hash and get prefix (bucket)
    uint32_t hash = _mm_crc32_u32(0, key);
    uint32_t bucket = hash >> (32 - PREFIX_BITS);
    // 3. Get directory entry
    uint64_t entry = directory[bucket].data;
    
    // 4. Check Bloom filter
    uint16_t bloom_tag = DirectoryEntry::bloomTag(hash);
    uint16_t bloom_mask = DirectoryEntry::bloom_lookup[bloom_tag];
    uint16_t entry_bloom = (uint16_t)(entry & 0xFFFF);
    
    if ((entry_bloom & bloom_mask) != bloom_mask) {
        return results;
    }

    // 5. Get range using directory pointers (paper Figure 4) and NOT
    // the offsets of arrays prefix_count and prefix_offset as before
    Tuple* start = (bucket == 0) ? tuple_buffer.data() : directory[bucket - 1].getPointer();
    Tuple* end = directory[bucket].getPointer();

    if (start == end) {
        return results;
    }
    
    // 6. Linear scan [start, end)
    for (Tuple* cur = start; cur != end; ++cur) {
        if (cur->key == key) {
            results.push_back(cur->row_ids);
        }
    }
    
    return results;
}

void UnchainedHashTable::build() {
    count_prefixes();
    compute_prefix_offsets();
    scatter_tuples();
    temp_tuples.clear();
}

void UnchainedHashTable::insert(int32_t key, size_t row_id) {
    temp_tuples.emplace_back(key, row_id);
}