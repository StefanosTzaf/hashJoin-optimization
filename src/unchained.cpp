#include "Unchained.h"
#include <omp.h>


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


UnchainedHashTable::UnchainedHashTable()
    : global_allocator(1024 * 1024 * 100), collectors(NUMBER_OF_THREADS){
    // compute one time the bloom lookup table
    DirectoryEntry::initBloomLookup();
    directory.resize(PREFIX_COUNT);
    omp_set_num_threads(NUMBER_OF_THREADS);
    local_data.resize(NUMBER_OF_THREADS);
    global_data.resize(NUM_PARTITIONS);

    for (int t = 0; t < NUMBER_OF_THREADS; t++){
        collectors[t].init(global_allocator, 64 * 1024);  // 64KB SmallChunks
    }
    
}


void UnchainedHashTable::insert(int32_t key, size_t row_id) {
    int tid = omp_get_thread_num();
    
    uint32_t p = get_partition_id(key);
    Tuple tuple(key, row_id);
    collectors[tid].consume(p, tuple);

}

void UnchainedHashTable::mergePartitions(){
    
    // each thread processes partition p by stealing from all collectors
    #pragma omp parallel for num_threads(NUMBER_OF_THREADS) shared(global_data, collectors)
    for(uint32_t p = 0; p < NUM_PARTITIONS; p++){
        
        size_t total_size = 0;
        // pre calculate the total size of all partitions[p]
        // of all threads so we can reserve space for the
        // global partition[p] and avoid reallocations
        for(int t = 0; t < NUMBER_OF_THREADS; t++){
            total_size += collectors[t].count(p);
        }

        global_data[p].reserve(total_size);

        // Steal - merge partitions
        for(int t = 0; t < NUMBER_OF_THREADS; t++){
            // segments of collectors[t] partition p
            Segment* segments = reinterpret_cast<Segment*>(
                collectors[t].stealPartitionChunks(p)
            );
            
            // Traverse and COPY tuples from segments to global_data[p] (TODO wihtout copy?)
            while(segments){
                Tuple* begin = reinterpret_cast<Tuple*>(segments->begin);
                Tuple* end = reinterpret_cast<Tuple*>(segments->cur);
                
                global_data[p].insert(global_data[p].end(), begin, end);
                
                segments = segments->next;
            }
        }
    }
}


void UnchainedHashTable::build() {

    mergePartitions();

    // stores the sizes of each partition
    std::vector<size_t> partition_sizes(NUM_PARTITIONS, 0);
    
    // 1. compute global offsets for each partition
    // number of tuples till the start of each partition
    // (like prefix_offset and prefix_count before)
    std::vector<size_t> partition_offsets(NUM_PARTITIONS, 0);
    size_t total_tuples = 0;
    for (uint32_t p = 0; p < NUM_PARTITIONS; ++p) {
        partition_offsets[p] = total_tuples;
        partition_sizes[p] = global_data[p].size();
        total_tuples += global_data[p].size();
    }
    
    // allocate final storage for all tuples
    tuple_buffer.resize(total_tuples); 
    
    // 2. parallel process partitions (Counting + Prefix Sum + Copy)
    #pragma omp parallel for schedule(dynamic, 1) num_threads(NUMBER_OF_THREADS)
    for (uint32_t p = 0; p < NUM_PARTITIONS; ++p) {
        // Range of directory entries covered by this partition
        uint32_t shift = PREFIX_BITS - PARTITION_BITS;
        uint32_t start_prefix = p << shift;
        uint32_t end_prefix = (p + 1) << shift;

        // Step A: Count Tuples for each prefix - bucket and Build Bloom Filters
        for (uint32_t i = start_prefix; i < end_prefix; ++i) {
            directory[i].data = 0; 
        }
        
        // Iterate over all tuples of this partition to compute
        // size of each directory entry
        for (const auto& tuple: global_data[p]){

            // calculate hash value
            uint32_t h = hashFunc(tuple.key);
           
            // take the exact prefix (bucket that this tuple belongs to)
            uint32_t prefix = h >> (32 - PREFIX_BITS);
            
            // Store count of directory entry in high bits 
            // (TEMPORARY we save the count of tuples here) - properly we have the pointer
            uint64_t current_count = directory[prefix].data >> 16;
            current_count++;
            
            // low 16 bits bloom
            uint16_t current_bloom = (uint16_t)(directory[prefix].data & 0xFFFF);
            current_bloom |= DirectoryEntry::bloom_lookup[DirectoryEntry::bloomTag(h)];
            
            directory[prefix].data = (current_count << 16) | current_bloom;
        
        }
        
        // Step B: Prefix Sum to determine Write Pointers
        // (starting offsets for each dir entry)

        // starting index of partition (number of tuples before it)
        size_t current_offset = partition_offsets[p];
        // .data() returns pointer to first element
        Tuple* base_ptr = tuple_buffer.data();
        
        // iterate through all directory entries of the partition
        for (uint32_t i = start_prefix; i < end_prefix; ++i) {
            
            // remember we saved tuples count in highest 48 bits temporarily
            uint64_t count = directory[i].data >> 16;
            
            // Set directory pointer to the START of the bucket
            // We use this as a running write pointer in the next step
            Tuple* bucket_write_ptr = base_ptr + current_offset;
            directory[i].setPointer(bucket_write_ptr);
            current_offset += count; // offset for next directory entry
        }
        
        // Step C: Copy Tuples
        for (const auto& tuple: global_data[p]) {
   
            // find the correct bucket - prefix
            uint32_t h = hashFunc(tuple.key);
            uint32_t prefix = h >> (32 - PREFIX_BITS);
            
            // Get the current write pointer for this bucket
            Tuple* target = directory[prefix].getPointer();
            // Copy the tuple
            *target = tuple;
            
            // Move write pointer forward: at the end of the loop, directory[i] will point
            // to the END of the bucket, which is what Search phase expects
            directory[prefix].setPointer(target + 1);

        
        }
    }
    
    // now the real data is in tuple_buffer and directory pointers point 
    // to the end of each bucket in directory vector
    global_data.clear();
}



std::vector<size_t> UnchainedHashTable::search(int32_t key) {
    std::vector<size_t> results;

    // 1. check if table is empty
    if (tuple_buffer.empty()) {
        return results;
    }
    
    // 2. Compute hash and get prefix (bucket)
    uint32_t hash = hashFunc(key);
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