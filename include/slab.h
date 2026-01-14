
#include <iostream>
#include <vector>
#include <array>
#include <cstddef>
#include "definitions.h"
// Forward declaration για Tuple ώστε να μην υπάρχει κυκλική εξάρτηση
struct Tuple;
struct UnchainedHashTable;

// this chunks will be given from system allocator (saved in linked list)
struct LargeChunk{
    LargeChunk* next = nullptr;
    size_t capacity = 0;
    std::byte* data = nullptr; // the real data of this chunk
};


class GlobalAllocator{
public:

    GlobalAllocator(size_t large_chunk_bytes);

    ~GlobalAllocator();

    LargeChunk* allocateLargeChunk();
    void releaseAll();

private:
    size_t large_chunk_bytes;
    LargeChunk* head; //begining of linked list
};


class BumpAlloc{
public:
    BumpAlloc() = default;
    ~BumpAlloc();

    // says to allocator that has new memory from upper level
    void addSpace(void* ptr, size_t bytes);

    size_t freeSpace() const;

    // bump allocation with raw bytes
    void* allocateBytes(size_t bytes);
    void reset() {
        Segment* seg = head;
        while(seg) {
            seg->cur = seg->begin;
            seg = seg->next;
        }
        tail = head;
    }
private:
    // segments of bytes for L2 and L3
    struct Segment {
        Segment* next = nullptr;
        std::byte* begin = nullptr;
        std::byte* cur   = nullptr;
        std::byte* end   = nullptr;
        
        Segment(std::byte* d, size_t s) 
            : begin(d), cur(d), end(d + s), next(nullptr){
            }
        
    };

    Segment* head = nullptr;
    Segment* tail = nullptr;
    Segment* cur  = nullptr;


};

struct SmallChunk {
    SmallChunk* next = nullptr;
    size_t      used = 0;
    size_t      capacity = 0;
    std::byte*  data = nullptr;
};


class ThreadAllocatorL2 {
public:

    ThreadAllocatorL2(GlobalAllocator& level1, size_t small_chunk_bytes);

    SmallChunk* allocateSmallChunk();

    // reset per query/build
    void reset();

private:
    GlobalAllocator& level1;
    size_t small_chunk_bytes;
    BumpAlloc level2;

    LargeChunk* current_large = nullptr;
};


class ThreadLocalTupleCollector {
public:
    ThreadLocalTupleCollector() : level2_ptr(nullptr) {}
    ThreadLocalTupleCollector(GlobalAllocator& level1, size_t small_chunk_bytes) : level2_ptr(nullptr) {
        init(level1, small_chunk_bytes);
    }
    void init(GlobalAllocator& level1, size_t small_chunk_bytes);

    void consume(uint32_t part, const Tuple& tuple);
    SmallChunk* stealPartitionChunks(uint32_t part);
    size_t count(uint32_t part) const { return counts[part]; }
    void reset();
    ~ThreadLocalTupleCollector();

    // Accessor for level3 (for migration, will be removed)
    BumpAlloc& getLevel3(uint32_t part) { return level3[part]; }
    ThreadAllocatorL2* getLevel2() { return level2_ptr; }

private:
    friend class UnchainedHashTable;
    ThreadAllocatorL2* level2_ptr;
    std::array<BumpAlloc, NUM_PARTITIONS> level3;
    std::array<size_t, NUM_PARTITIONS> counts;
    std::array<SmallChunk*, NUM_PARTITIONS> chunks;
};