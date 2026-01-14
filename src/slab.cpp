
#include "Unchained.h"
#include <new>
#include <stdexcept>

//------------------- GlobalAllocator Implementation ------------------//

GlobalAllocator::GlobalAllocator(size_t large_chunk_bytes)
    : large_chunk_bytes(large_chunk_bytes), head(nullptr) {
    }


GlobalAllocator::~GlobalAllocator(){
    releaseAll();
}

// new large chunk
LargeChunk* GlobalAllocator::allocateLargeChunk(){
    LargeChunk* chunk = new LargeChunk;
    chunk->capacity = large_chunk_bytes;

    // allocate bytes
    chunk->data = new std::byte[large_chunk_bytes];
    if (!chunk->data) {
        delete chunk;
        throw std::bad_alloc();
    }

    // in the beggining of linked list
    chunk->next = head;
    head = chunk;

    return chunk;
}

void GlobalAllocator::releaseAll(){
    while (head) {
        delete[] head->data;
        LargeChunk* tmp = head;
        head = head->next;
        delete tmp;
    }
}

//------------------- BumpAlloc Implementation ------------------//

BumpAlloc::~BumpAlloc(){
    Segment* seg = head;
    while (seg) {
        Segment* tmp = seg;
        seg = seg->next;
        // no delete[] L1 will do it
        delete tmp;
    }
}

// new segment
void BumpAlloc::addSpace(void* ptr, size_t bytes){
    Segment* seg = new Segment(reinterpret_cast<std::byte*>(ptr), bytes);
    if (!head){
        head = tail = seg;
    }
    else{
        tail->next = seg;
        tail = seg;
    }
}

// returns the space in current segment
size_t BumpAlloc::freeSpace() const{
    if (!tail){
        return 0;
    }
    return static_cast<size_t>(tail->end - tail->cur);
}

void* BumpAlloc::allocateBytes(size_t bytes){
    // check if we have space
    if (!tail || bytes > static_cast<size_t>(tail->end - tail->cur)) {
        return nullptr;
    }
    void* result = tail->cur;
    tail->cur += bytes;
    return result;
}

//------------------- ThreadAllocatorL2 Implementation ------------------//

ThreadAllocatorL2::ThreadAllocatorL2(GlobalAllocator& level1, size_t small_chunk_bytes)
    : level1(level1), small_chunk_bytes(small_chunk_bytes), current_large(nullptr){
    }

// allocateSmallChunk returns pointer to new SmallChunk
SmallChunk* ThreadAllocatorL2::allocateSmallChunk() {
    // trying to allocate from level2 bump allocator
    void* ptr = level2.allocateBytes(sizeof(SmallChunk));
    if (!ptr) {
        // if there is no space, get a new LargeChunk from level1
        current_large = level1.allocateLargeChunk();
        if (!current_large) {
            throw std::runtime_error("GlobalAllocator returned nullptr!");
        }

        // give the entire buffer of LargeChunk to the bump allocator
        level2.addSpace(current_large->data, current_large->capacity);

        // try again
        ptr = level2.allocateBytes(sizeof(SmallChunk));
        if (!ptr) {
            throw std::bad_alloc();
        }
    }

    // initialize the SmallChunk
    SmallChunk* chunk = new (ptr) SmallChunk;
    chunk->capacity = small_chunk_bytes;
    chunk->used = 0;
    // data will be a pointer to l3 allocation
    chunk->data = nullptr;
    chunk->next = nullptr;

    return chunk;
}

// reset for next query/build
void ThreadAllocatorL2::reset() {
    // reset the bump allocator
    // LargeChunks remain for reuse
    level2.reset();
    if (current_large) {
        // give the last LargeChunk back to level2 for start
        level2.addSpace(current_large->data, current_large->capacity);
    }
}

//------------------- L3 Allocator Implementation ------------------//


ThreadLocalTupleCollector::~ThreadLocalTupleCollector() {
    if (level2_ptr) {
        delete level2_ptr;
        level2_ptr = nullptr;
    }
    for (size_t i = 0; i < NUM_PARTITIONS; ++i) {
        SmallChunk* chunk = chunks[i];
        while (chunk) {
            SmallChunk* next = chunk->next;
            delete[] chunk->data;
            delete chunk;
            chunk = next;
        }
        chunks[i] = nullptr;
    }
}

void ThreadLocalTupleCollector::init(GlobalAllocator& level1, size_t small_chunk_bytes) {
    if (level2_ptr) {
        delete level2_ptr;
    }
    level2_ptr = new ThreadAllocatorL2(level1, small_chunk_bytes);
    counts.fill(0);
    chunks.fill(nullptr);
    for (auto& l3 : level3) l3 = BumpAlloc();
}

// consume function 
void ThreadLocalTupleCollector::consume(uint32_t part, const Tuple& tuple) {
    if (part >= NUM_PARTITIONS) {
        throw std::out_of_range("Partition index out of range");
    }

    // trying to allocate space for Tuple from level3 bump allocator in the partition
    void* ptr = level3[part].allocateBytes(sizeof(Tuple));
    if (!ptr) {
        // no space, need a new SmallChunk from level2
        SmallChunk* chunk = level2_ptr->allocateSmallChunk();
        if (!chunk) {
            throw std::bad_alloc();
        }

        // give the buffer of the chunk to the level3 bump allocator
        chunk->data = new std::byte[chunk->capacity];
        level3[part].addSpace(chunk->data, chunk->capacity);

        // add the chunk to the list
        chunk->next = chunks[part];
        chunks[part] = chunk;

        // try again
        ptr = level3[part].allocateBytes(sizeof(Tuple));
        if (!ptr) {
            throw std::bad_alloc();
        }
    }

    // use placement new to write the tuple
    new (ptr) Tuple(tuple);
    // update used size in the chunk
    if (chunks[part]) {
        chunks[part]->used += sizeof(Tuple);
    }
    counts[part]++;
}


SmallChunk* ThreadLocalTupleCollector::stealPartitionChunks(uint32_t part) {
    if (part >= NUM_PARTITIONS) {
        throw std::out_of_range("Partition index out of range");
    }

    SmallChunk* head = chunks[part];
    chunks[part] = nullptr;
    return head;
}

void ThreadLocalTupleCollector::reset() {
    for (size_t i = 0; i < NUM_PARTITIONS; ++i) {
        level3[i].reset();
        chunks[i] = nullptr;
        counts[i] = 0;
    }
    level2.reset(); // reset L2 allocator
}

