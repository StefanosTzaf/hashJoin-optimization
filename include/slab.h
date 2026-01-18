#include <iostream>
#include <vector>
#include <array>
#include <cstddef>
#include "types.h"

// Forward declaration για Tuple so as not to have circular dependency
struct Tuple;
struct UnchainedHashTable;

// this chunks will be given from system allocator (saved in linked list)
struct LargeChunk{
    LargeChunk* next = nullptr;
    size_t capacity = 0;
    std::byte* data = nullptr; // the real data of this chunk
};


class GlobalAllocator{
    
    private:
        size_t large_chunk_bytes;
        LargeChunk* head; //begining of linked list
        
    public:

        GlobalAllocator(size_t large_chunk_bytes);
        ~GlobalAllocator();
        LargeChunk* allocateLargeChunk();
        void releaseAll();
};

// this struct is to represent each segment of memory and
// to know how much space is left
struct Segment{
    Segment* next = nullptr;
    std::byte* begin = nullptr;
    std::byte* cur   = nullptr;
    std::byte* end   = nullptr;
    
    Segment(std::byte* d, size_t s) 
        : begin(d), cur(d), end(d + s), next(nullptr){
        }
};

class BumpAlloc{

    private:
        Segment* head = nullptr;
        Segment* tail = nullptr;
        Segment* cur  = nullptr;

    public:
        BumpAlloc() = default;
        ~BumpAlloc();

        // says to allocator that has new memory from upper level
        void addSpace(void* ptr, size_t bytes);

        size_t freeSpace() const;

        // bump allocation with raw bytes
        void* allocateBytes(size_t bytes);

        // reset bump allocator (resets cur pointers but keeps segments for the next querry)
        void reset();

        // get the head segment for partition exchange/merge
        Segment* getSegments(){ 
            return head; 
        }
};

struct SmallChunk{
    SmallChunk* next = nullptr;
    size_t      used = 0;
    size_t      capacity = 0;
    std::byte*  data = nullptr;
};


class ThreadAllocatorL2{

    private:
        // reference to global allocator
        GlobalAllocator& level1;
        size_t small_chunk_bytes;
        // real bump allocator for level2
        BumpAlloc level2;
        LargeChunk* current_large = nullptr;

    public:

        ThreadAllocatorL2(GlobalAllocator& level1, size_t small_chunk_bytes);

        SmallChunk* allocateSmallChunk();

        // reset per query/build
        void reset();
};


class ThreadLocalTupleCollector{

    private:

        // pointer to level2 allocator
        ThreadAllocatorL2* level2_ptr;
        // level3 bump allocators for each partition
        std::array<BumpAlloc, NUM_PARTITIONS> level3;
        // counts of tuples per partition
        std::array<size_t, NUM_PARTITIONS> counts;

    public:

        ThreadLocalTupleCollector() : level2_ptr(nullptr){
        }
        ThreadLocalTupleCollector(GlobalAllocator& level1, size_t small_chunk_bytes) : level2_ptr(nullptr){
            init(level1, small_chunk_bytes);
        }

        // makes level3 allocator ready to use.
        void init(GlobalAllocator& level1, size_t small_chunk_bytes);

        void consume(uint32_t part, const Tuple& tuple);

        SmallChunk* stealPartitionChunks(uint32_t part);

        size_t count(uint32_t part) const{ 
            return counts[part];
        }
        // Accessor for level3 (for migration, will be removed)
        BumpAlloc& getLevel3(uint32_t part){ 
            return level3[part]; 
        }
        ThreadAllocatorL2* getLevel2(){ 
            return level2_ptr; 
        }

        void reset();
        
        ~ThreadLocalTupleCollector();
};