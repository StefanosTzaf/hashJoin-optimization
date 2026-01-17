#include <iostream>
#include <vector>

#define THREAD_CHUNK_SIZE 

//##### LEVEL 1 #####//
// Chunks for each thread
class ThreadChunk{
    private:
        char* data;
        size_t capacity; // total capacity of chunk
        size_t size; // how much is used
        ThreadChunk* next;
    
    public:
        ThreadChunk(): capacity(0), size(0), next(NULL), data(NULL){}

        ThreadChunk(size_t cap): capacity(cap), size(0), next(NULL){
            data = new char[capacity];
        }

        ~ThreadChunk(){
            delete[] data;
        }

    friend class ThreadAllocator;

};

class ThreadAllocator{
    private:
        struct ThreadChunk* first; // points to first chunk of list
        struct ThreadChunk* last; // points to last chunk of list
        int chunksNum; // number of chunks
        size_t chunkCapacity; // size of single chunk
    
    public:
        ThreadAllocator(size_t size): chunksNum(0), chunkCapacity(size), first(NULL), last(NULL){}

        // allocates size bytes of memory in last chunk
        // or creates a new one if full
        void* allocate(size_t size){

            // create first chunk
            if(chunksNum == 0){
                first = new ThreadChunk(chunkCapacity);
                last = first;
                chunksNum++;
                first->size += size;
                first->next = NULL;
                
                return first->data;
            }
            
            // if current chunk has free space
            if((chunkCapacity - last->size) >= size){
                void* memoryPtr = last->data + last->size;
                last->size += size;
                return memoryPtr;
            }

            // current chunk has no free space: create new one
            else{
                ThreadChunk* newChunk = new ThreadChunk(chunkCapacity);
                ThreadChunk* temp = last;
                last = newChunk;
                chunksNum++;
                last->size += size;
                temp->next = last;

                return last->data;
                
            }   
        }

        ~ThreadAllocator(){
            
            if(chunksNum != 0){
                ThreadChunk* curr = first;
                
                while(curr != NULL){
                    ThreadChunk* prev = curr;
                    ThreadChunk* next = curr->next;
                    curr = next;

                    delete prev;

                }
            }
        }
};


