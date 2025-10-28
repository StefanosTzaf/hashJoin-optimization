#include <iostream>
#include <vector>
#include <cstdint>

// Helper function: returns the smallest power of 2 >= n
// is used in Capacity of hash tables
inline size_t nextPowerOf2(size_t n) {
    if (n == 0) return 1;
    // Check if already a power of 2
    if ((n & (n - 1)) == 0) {
        return n;
    }
    // Find the next power of 2
    size_t power = 1;
    while (power < n) {
        power <<= 1;
    }
    return power;
}


template <class keyT, class valueT>
class hashNode{

    keyT key;
    std::vector<valueT> values;
    bool is_occupied;
    uint32_t hopInfo; // to track which slots in hop range are occupied
    // that were initially hashed to this index

    public:

    hashNode(size_t hopRange): is_occupied(false), hopInfo(0) {
        // hopInfo is represented as a bitmask in an integer
        // initialize all bits to 0
    }

      //#### GETTERS ####//

    // getters are marked as inline functions to 
    // reduce function call overhead for small functions

    inline const bool isOccupied() const{
        return is_occupied;
    }

    inline const keyT& getKey() const {
        return key;
    }

    // returns a reference to the values vector meaning no copy is made
    // const before the method: the caller cannot modify the returned vector
    // const after the method: the method does not modify any member variables
    inline const std::vector<valueT>& getValue() const {
        return values;
    }

    inline const uint32_t& getHopInfo() const {
        return hopInfo;
    }

        //#### SETTERS ####//

    // reset hopInfo vector with false values
    void resetHopInfo(size_t hopRange){
        hopInfo = 0;
    }

    // set a specific position in hopInfo to true
    void setHopInfoPosToTrue(const size_t& index){
        // 1u: unsigned integer with value 1
        // left shift by index positions so only bit index is 1
        // bitwise OR with hopInfo to set that bit to 1
        hopInfo |= (1u << index);
    }

    // set a specific position in hopInfo to false
    void setHopInfoPosToFalse(const size_t& index){
        // ~ bitwise not: inverts all bits of (1u << index)
        // bitwise AND with hopInfo to clear that bit to 0
        // hopinfo = 1101, 1u << 2 = 0100, ~ = 1011
        // 1101 & 1011 = 1001
        hopInfo &= ~(1u << index);
    }

    // set values vector by moving from an rvalue reference
    void setValuesVector(const std::vector<valueT>&& vals){
        values = std::move(vals);
    }

    // allow Hopscotch class to access private members
    template <class keyT2, class valueT2>
    friend class Hopscotch;
    
    

};

// HashFunction is a function pointer that takes a key of type keyT
// and a capacity and returns a size_t hash value
template <class keyT>
using HashFunction = size_t (*)(const keyT&, size_t);


template <class keyT, class valueT>
class Hopscotch{

    size_t capacity; // total capacity of the table
    size_t size; // current number of elements in the table
    std::vector<hashNode<keyT, valueT>> table;
    size_t hopRange; // range for hopscotch
    HashFunction<keyT> hashFunc; // hash function provided by user

    public:
    
    // default hash function 1
    static size_t defaultHash(const keyT& key, size_t cap);

    // if the user does not provide a hash function, use the default one
    Hopscotch(size_t cap, size_t hopR = 32, HashFunction<keyT> h = hashFunction): capacity(cap), size(0), hopRange(hopR), hashFunc(h){

        // Ensure minimum capacity to avoid division by zero and empty table
        if (cap == 0){
            capacity = 16;
        }
        else{
            // capacity = nextPowerOf2(capacity);
        }
        // initialize table with empty hashNodes
        table.resize(capacity, hashNode<keyT, valueT>(hopRange));
    }

    // inserts key-value pair into the hash table
    bool hashInsert(const keyT& key, const valueT& value);

    // default hash function 2 using std::hash
    static size_t hashFunction(const keyT& key, size_t cap);
    
    
    void rehash();
    
    inline size_t getSize() const{
        return size;
    }

    // extra functions
    
    // checks if the original hashed position is free
    // if so, inserts the key-value pair there and sets up hop info bitmap and returns true
    bool insertInOriginalPos(const size_t pos, const keyT& key, const valueT& value);
    
    // returns true if all slots in bitmap hop info are occupied
    bool isHopInfoFull(const size_t pos) const;
    
    // finds the next free slot starting from startPos
    // and returns its index or capacity if none found
    size_t findFreeSlot(const size_t startPos) const;
    
    // checks if free slot is within hop range of hashed position
    // if so, inserts the key-value pair there and updates hop info bitmap and returns true
    bool insertWithinHopRange(const size_t freeSlot, const size_t hashedPos, const keyT& key, const valueT& value);
    
    // it checks if the key already exists, if so it appends the value to the existing key's values vector
    bool insertDuplicateKey(size_t pos, const keyT& key, const valueT& value);

    // tries to swap elements to bring free slot within hop range
    bool insertAndSwap(size_t& freeSlot, const size_t pos, const keyT& key, const valueT& value);
    
    
    
    // helps functions for unit tests
    
    void printTable() const;
    
    void printHopInfoBitmap(const size_t pos) const;
    
    // returns the key of the position pos
    size_t getKeyOfPos(const size_t pos) const{
        return table[pos].getKey();
    }

    uint32_t getHopInfoBitmap(const size_t pos) const{
        return table[pos].getHopInfo();
    }

    // returns vector representation of hop info bitmap at pos
    // used for unit tests
    std::vector<bool> getHopInfoToVector(const size_t pos) const {
        auto& hopInfo = table[pos].getHopInfo();
        std::vector<bool> result(hopRange);
        for(size_t i = 0; i < hopRange; ++i){
            result[i] = (hopInfo >> i) & 1;
        }
        return result;
    }
    
    std::vector<valueT> getValuesVectorOfPos(const size_t pos) const{
        return table[pos].getValue();
    }
    
    // returns vector of values for the given key
    const std::vector<valueT> hashSearch(const keyT& key) const;

    // returns the position where the key is found, or capacity if not found
    const size_t hashSearchPos(const keyT& key) const;
};




//##### IMPLEMENTATION #####//

template <typename keyT, typename valueT>
size_t Hopscotch<keyT, valueT>::hashFunction(const keyT& key, size_t cap){
    return cap ? (std::hash<keyT>{}(key) % cap) : 0;

}

template <typename keyT, typename valueT>
void Hopscotch<keyT, valueT>::rehash(){

       
    capacity *= 2; // double the capacity

    size = 0; // reset size, since it will be updated during re-insertions

    // move old table to a temporary variable
    std::vector<hashNode<keyT, valueT>> oldTable = std::move(table);
    
    // create new table with updated capacity
    table = std::vector<hashNode<keyT, valueT>>(capacity, hashNode<keyT, valueT>(hopRange));


    // for each node in old table
    for ( auto node : oldTable) {

        // Skip empty nodes
        if (node.isOccupied()) {

            hashInsert(node.getKey(), node.getValue()[0]); // insert first value

           // if there are duplicates, insert them as well
            size_t newPos = hashSearchPos(node.getKey());
            table[newPos].setValuesVector(std::move(node.getValue()));
        }
    }

}


template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::insertInOriginalPos(const size_t pos, const keyT& key, const valueT& value){
    
    // if the hashed position is free, insert directly
    if(table[pos].isOccupied() == false){

        table[pos].key = key;
        table[pos].values.push_back(value);
        table[pos].is_occupied = true;
        table[pos].setHopInfoPosToTrue(0); // mark position 0 (the slot itself) as occupied
        size++;
        
        return true;
    }
    return false;
}

template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::isHopInfoFull(const size_t pos) const{

    auto& hopInfo = table[pos].getHopInfo();

    // Create mask with all bits set to 1
    uint32_t fullMask = (1u << hopRange) - 1;

    return hopInfo == fullMask;
    
}


template <typename keyT, typename valueT>
size_t Hopscotch<keyT, valueT>::findFreeSlot(const size_t startPos) const{

    size_t i = startPos; // original hashed position

    while(true){

        // check if slot is free
        if(table[i].isOccupied() == false){
            return i;
        }

        i = (i + 1) % capacity; // wrap around

        if(i == startPos){ // completed full loop through table
            return capacity; // indicates no free slot found
        }
    }
}

template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::insertWithinHopRange(const size_t freeSlot, const size_t hashedPos, const keyT& key, const valueT& value){

    size_t distance;

    // if free slot is before hashed position: wrap around case
    // capacity - hashedPos: distance from hashedPos to end of table
    // freeSlot: distance from start of table to freeSlot
    if(freeSlot < hashedPos){
        distance = capacity - hashedPos + freeSlot;
    }
    // free slot is after hashed position and within hop range
    // distance from hashedPos to freeSlot
    else{
        distance = freeSlot - hashedPos;
    }

    // if within hop range, insert directly
    if (distance < hopRange) {
        table[freeSlot].key = key;
        table[freeSlot].values.push_back(value);
        table[freeSlot].is_occupied = true;

        size++;

        // update hop info bitmap at original position
        table[hashedPos].setHopInfoPosToTrue(distance);
        return true;
    }
    return false;  
}


template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::insertDuplicateKey(size_t pos, const keyT& key, const valueT& value){

    // check if the key already exists in positions indicated by hop info bitmap
    // if so append value to vector
    auto& hopInfo = table[pos].getHopInfo();

    for(size_t i = 0; i < hopRange; ++i){

        // check if bit i is 1
        // if hopinfo = 1010 and i = 1 -> 1u << i = 0010
        // hopinfo & 0010 = 0010 -> true
        if(hopInfo & (1u << i)){ // slot is occupied by an element that hashed in pos

            // calculate actual position in table
            size_t checkPos = (pos + i) % capacity;

            // check if key matches, if so append value to vector
            if(table[checkPos].isOccupied() && table[checkPos].getKey() == key){
                table[checkPos].values.push_back(value);
                return true;
            }
        }
    }

    // key not found
    return false;

}


template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::insertAndSwap(size_t& freeSlot, const size_t pos, 
    const keyT& key, const valueT& value) {
        
    // the free slot is outside hop range, now we should bring it closer by swapping elements
   
    // distance of freeSlot from original hashed position pos
    size_t distance;

    // normal case: freeSlot is after pos
    if(freeSlot > pos){
        distance = freeSlot - pos;
    }

    // wrap around case: freeSlot is before pos
    else{
        // capacity - pos: distance from pos to end of table
        // freeSlot: distance from start of table to freeSlot
        distance = capacity - pos + freeSlot;
    }
    
    // while the free slot is outside hop range, swap elements
    while(distance >= hopRange){
       
        bool moved = false; // indicates if an element was moved in this iteration or not
        
        // Calculate start position for search
        // startH is the first position of the H-1 slots before freeSlot
        size_t startH; 

        // normal case: free slot is after the first H-1 slots
        if(freeSlot >= hopRange - 1){
            startH = freeSlot - hopRange + 1;
        }
        // wrap around case: freeSlot is before the first H-1 slots 
        // so the startH is at the end of the table
        else{
            startH = (freeSlot + capacity - hopRange + 1) % capacity;
        }

        size_t slotToMove = capacity; // initialize to invalid index

        // iterate from startH to freeSlot to find an element to move
        for(size_t offset = 0; offset < hopRange - 1; offset++){
           
            size_t i = (startH + offset) % capacity;

            if(i == freeSlot){
                break; // reached freeSlot, stop searching
            }

            // get hop info bitmap of current position
            auto& hopInfo = table[i].getHopInfo();

            // check all bits of the bitmap
            for(size_t bitIndex = 0; bitIndex < hopRange-1; bitIndex++){
               
                // if we reach or exceed freeSlot, stop checking further bits
                // we should only consider bits that point to positions before freeSlot
                if(i + bitIndex >= freeSlot){
                    break;
                }

                // if bit is true, we can move this element
                // result is non-zero if bit at bitIndex is 1
                if(hopInfo & (1u << bitIndex)){ 

                    slotToMove = (i + bitIndex) % capacity; // actual position of the element to move
                    
                    // Calculate the new distance from i to freeSlot
                    size_t newDistance;
                    if (freeSlot >= i) {
                        newDistance = freeSlot - i;
                    } else {
                        newDistance = capacity - i + freeSlot;
                    }
                    
                
                    // swap the empty slot with the slotToMove
                    // Using direct member access since Hopscotch is a friend class
                    table[freeSlot].key = std::move(table[slotToMove].key);        
                    table[freeSlot].values = std::move(table[slotToMove].values);
                    table[freeSlot].is_occupied = true;

                    table[i].setHopInfoPosToTrue(newDistance); // set bit to true of the free slot where we moved the element
                    table[i].setHopInfoPosToFalse(bitIndex); // set bit of old position to false, since it is now empty

                    table[slotToMove].is_occupied = false; // mark old position as free

                    // update variables for next iteration
                    freeSlot = slotToMove; // free slot has now moved closer to original pos
                    
                    // recalculate distance from original pos to new freeSlot
                    if(freeSlot >= pos){ 
                        distance = freeSlot - pos;
                    }
                    // capacity - pos: distance from pos to end of table
                    // freeSlot: distance from start of table to freeSlot
                    else{
                        distance = capacity - pos + freeSlot;
                    }
                    
                    moved = true; // an element was moved in this iteration
                    break; // exit from bitIndex loop
                }      
            }

            if (moved == true){
                break; // exit from i loop and continue with while
            }
        }

        // If no element could be moved, table is too full -> rehash
        if (moved == false){ 
            rehash();
            return hashInsert(key, value);
        }
    }

    return false;
}
    


template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::hashInsert(const keyT& key, const valueT& value){

    size_t pos = hashFunc(key, capacity);

    // check if key already exists, if so append value to existing vector
    if(insertDuplicateKey(pos, key, value) == true){
        return true;
    }

    // if the original hashed position is free, insert there
    if (insertInOriginalPos(pos, key, value) == true) {
        return true;
    }

    // check if hop info is full, if so table needs rehashing
   if(isHopInfoFull(pos) == true){
        rehash();
        return hashInsert(key, value);
    }

    // search for a free slot
    size_t freeSlot = findFreeSlot(pos);

    if(freeSlot == capacity){ // no free slot found
        rehash();
        return hashInsert(key, value);
    }

    // if free slot is within hop range, insert directly
    if(insertWithinHopRange(freeSlot, pos, key, value) == true){
        return true;
    }

    // free slot is outside hop range, try to swap elements to bring it closer
    // returns true only if it rehashed and recursively inserted the element
   if(insertAndSwap(freeSlot, pos, key, value) == true){
        return true;
    }

    // at this point insertAndSwap returned false, since it did not insert the element
    // but brought freeSlot within hop range
    // now the free slot is within hop range, insert the new key-value pair
    if(insertWithinHopRange(freeSlot, pos, key, value) == true){

        return true;
    }

    return false;

}


template <typename keyT, typename valueT>
const std::vector<valueT> Hopscotch<keyT, valueT>::hashSearch(const keyT& key) const{

    size_t pos = hashFunc(key, capacity);

    // check other positions within hop range
    auto& hopInfo = table[pos].getHopInfo();

    for(size_t i = 0; i < hopRange; ++i){

        // result is non-zero if bit at i is 1
        if(hopInfo & (1u << i)){ // slot is occupied by an element that hashed here

            size_t checkPos = (pos + i) % capacity;

            if(table[checkPos].isOccupied() && table[checkPos].getKey() == key){
                return table[checkPos].getValue();
            }
        }
    }

    // key not found, return empty vector
    return std::vector<valueT>();
}

template <typename keyT, typename valueT>
const size_t Hopscotch<keyT, valueT>::hashSearchPos(const keyT& key) const{

    size_t pos = hashFunc(key, capacity);

    // check other positions within hop range
    auto& hopInfo = table[pos].getHopInfo();

    for(size_t i = 0; i < hopRange; ++i){

        // result is non-zero if bit at i is 1
        if(hopInfo & (1u << i)){ // slot is occupied by an element that hashed here

            size_t checkPos = (pos + i) % capacity;

            if(table[checkPos].isOccupied() && table[checkPos].getKey() == key){
                return checkPos;
            }
        }
    }

    // key not found, return capacity
    return capacity;
}

template <typename keyT, typename valueT>
void Hopscotch<keyT, valueT>::printHopInfoBitmap(size_t pos) const{

    std::cout << "[" ;
    for(size_t i = 0; i < hopRange; ++i){
        std::cout << " " << table[pos].getHopInfo()[i];
    }
    std::cout << " ]   ";
}




template <typename keyT, typename valueT>
void Hopscotch<keyT, valueT>::printTable() const{

    std::cout << "\n=== Hash Table State ===\n";
        
    for (size_t i = 0; i < capacity; ++i) {
           
        if (table[i].isOccupied()){

            printHopInfoBitmap(i);

            // get vector of values
            const std::vector<valueT>& values = table[i].getValue();
            
            std::cout << "[" << i << "] key=" << table[i].getKey();
            
            // if the vector is not empty, print its contents
            if(values.size() > 0){
                std::cout << ", Values: ";
                for(const auto& val : values){
                    std::cout << val << ", ";
                }
            };   

            std::cout << "\n";

        }
        else{
            std::cout << "[" << i << "] EMPTY\n";
        }
        
        }
}

template <class keyT, class valueT>
size_t Hopscotch<keyT, valueT>::defaultHash(const keyT& key, size_t cap){
        // Use std::hash first, so as to work with all types
        size_t hash = std::hash<keyT>{}(key);
        
        // FNV-1a hash mixing
        constexpr size_t FNV_prime = 1099511628211ULL;
        constexpr size_t FNV_offset = 14695981039346656037ULL;
        
        hash = (hash ^ FNV_offset) * FNV_prime;
        hash = (hash ^ (hash >> 32)) * FNV_prime;
        
        // Fibonacci hashing for better distribution
        constexpr size_t golden_ratio = 11400714819323198485ULL;
        
        // Calculate leading zeros
        size_t shift = 0;
        size_t temp = cap;
        while (temp > 1) {
            temp >>= 1;
            shift++;
        }
        
        hash = (hash * golden_ratio) >> (64 - shift);
        return hash & (cap - 1); // Faster than modulo if capacity is power of 2
    }