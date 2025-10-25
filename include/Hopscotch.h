#include <iostream>
#include <vector>

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
    std::vector<bool> hopInfo; // to track which slots in hop range are occupied
    // that were initially hashed to this index

    public:

    hashNode(size_t hopRange): is_occupied(false) {
        hopInfo.resize(hopRange, false);
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

    inline const std::vector<bool>& getHopInfo() const {
        return hopInfo;
    }

        //#### SETTERS ####//

    inline void setOccupied(const bool status){
        is_occupied = status;
    }

    void setKey(const keyT& newKey){
        key = newKey;
    }

    // reset hopInfo vector with false values
    void resetHopInfo(size_t hopRange){
        hopInfo.clear();
        hopInfo.resize(hopRange, false);
    }

    // set a specific position in hopInfo to true
    void setHopInfoPosToTrue(const size_t& index){
    
        if(index < hopInfo.size()){
            hopInfo[index] = true;
        }
    }

    void setHopInfoPosToFalse(const size_t& index){
    
        if(index < hopInfo.size()){
            hopInfo[index] = false;
        }
    }

     // set the entire values vector
    void setValuesVector( std::vector<valueT>&& newValues){
        values = std::move(newValues);
    }

     // add a single value to the values vector
    void addValuetoVector(const valueT& newValue){
        values.emplace_back(newValue);
    }

    
    template <class keyT2, class valueT2>
    friend class Hopscotch;
    
    

};

template <class keyT>
using HashFunction = size_t (*)(const keyT&, size_t);

template <class keyT, class valueT>
class Hopscotch{

    size_t capacity; // total capacity of the table
    size_t size; // current number of elements in the table
    std::vector<hashNode<keyT, valueT>> table;
    size_t hopRange; // range for hopscotch
    HashFunction<keyT> hashFunc;

    public:
    
    static size_t defaultHash(const keyT& key, size_t capacity);

    Hopscotch(size_t capacity, size_t hopR, HashFunction<keyT> h = hashFunction): capacity(capacity), size(0), hopRange(hopR), hashFunc(h){
        if (capacity == 0){
            capacity = 16;
        }
        else{
            // capacity = nextPowerOf2(capacity);
        }
        table.resize(capacity, hashNode<keyT, valueT>(hopRange));
        
    }

    bool hashInsert(const keyT& key, const valueT& value);

    // default hash function using std::hash
    static size_t hashFunction(const keyT& key, size_t capacity);
    
    
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

    bool insertAndSwap(size_t& freeSlot, const size_t pos, const keyT& key, const valueT& value);
    
    
    
    // helps functions for unit tests
    
    void printTable() const;
    size_t getKeyOfPos(const size_t pos) const{
        return table[pos].getKey();
    }
    
    void printHopInfoBitmap(const size_t pos) const;

    std::vector<bool> getHopInfoBitmap(const size_t pos) const{
        return table[pos].getHopInfo();
    }
    
    std::vector<valueT> getValuesVectorOfPos(const size_t pos) const{
        return table[pos].getValue();
    }
    
    // returns vector of values for the given key
    const std::vector<valueT> hashSearch(const keyT& key) const;
    
};




//##### IMPLEMENTATION #####//

template <typename keyT, typename valueT>
size_t Hopscotch<keyT, valueT>::hashFunction(const keyT& key, size_t capacity){
    return capacity ? (std::hash<keyT>{}(key) % capacity) : 0;
   
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
    for (const auto& node : oldTable) {

        // Skip empty nodes
        if (node.isOccupied()) {

            // for each value in the values vector of the node, reinsert
            for (const auto& value : node.getValue()) {
                hashInsert(node.getKey(), value);
            }
        }
    }

}


template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::insertInOriginalPos(const size_t pos, const keyT& key, const valueT& value){
    
    // if the hashed position is free, insert directly
    if(table[pos].isOccupied() == false){

        table[pos].setKey(key);
        table[pos].addValuetoVector(value);
        table[pos].setOccupied(true);
        table[pos].resetHopInfo(hopRange); // initialize hop info vector
        table[pos].setHopInfoPosToTrue(0); // mark first slot in hop info as occupied
        size++;
        return true;
    }
    return false;
}

template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::isHopInfoFull(const size_t pos) const{

    auto& hopInfo = table[pos].getHopInfo();
   
    for(size_t i = 0; i < hopRange; ++i){
        
        // there is at least one slot that is free, or
        // occupied by an element that hashed elsewhere
        if(hopInfo[i] == false){
            return false;
        }
    }
    return true;
}


template <typename keyT, typename valueT>
size_t Hopscotch<keyT, valueT>::findFreeSlot(const size_t startPos) const{
    
    size_t i = startPos;

    while(true){

        if(table[i].isOccupied() == false){
            return i;
        }

        i = (i + 1) % capacity;

        if(i == startPos){ // completed full loop through table
            return capacity; // indicates no free slot found
        }
    }
}

template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::insertWithinHopRange(const size_t freeSlot, const size_t hashedPos, const keyT& key, const valueT& value){

    // wrap around
    if (freeSlot < hashedPos) {
        // calculate distance with wrap around
        if ((capacity - hashedPos + freeSlot) < hopRange) {
            table[freeSlot].setKey(key);
            table[freeSlot].addValuetoVector(value);
            table[freeSlot].setOccupied(true);
        
            size++;

            // update hop info bitmap at original position
            table[hashedPos].setHopInfoPosToTrue(capacity - hashedPos + freeSlot);
            return true;
        }
    }

    else if((freeSlot - hashedPos) % capacity < hopRange){

        table[freeSlot].setKey(key);
        table[freeSlot].addValuetoVector(value);
        table[freeSlot].setOccupied(true);
    
        size++;

        // update hop info bitmap at original position
        table[hashedPos].setHopInfoPosToTrue(freeSlot - hashedPos);
        return true;
    }
    
    return false;
}


template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::insertDuplicateKey(size_t pos, const keyT& key, const valueT& value){

    // if the key already exists in original hashed position, append value to vector

    if(table[pos].isOccupied() && table[pos].getKey() == key){
        table[pos].addValuetoVector(value);
        return true;
    }

    // check other positions within hop range
    auto& hopInfo = table[pos].getHopInfo();

    for(size_t i = 1; i < hopRange; ++i){

        if(hopInfo[i] == true){ // slot is occupied by an element that hashed here

            // calculate actual position in table
            size_t checkPos = (pos + i);

            // check if key matches, if so append value to vector
            if(table[checkPos].isOccupied() && table[checkPos].getKey() == key){
                table[checkPos].addValuetoVector(value);
                return true;
            }
        }
    }

    // key not found
    return false;

}

template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::insertAndSwap(size_t& freeSlot, const size_t pos, 
    const keyT& key, const valueT& value){
        
    size_t dist;
    if (freeSlot >= pos) {
        dist = freeSlot - pos;
    } else {
        dist = capacity - pos + freeSlot; // wrap around
    }

    // the free slot is outside hop range, now we should move elements
    while(dist % capacity >= hopRange){

        bool moved = false;
        
        size_t startH; // start position for checking elements to move
        if (freeSlot > pos) {
            startH = freeSlot - hopRange + 1;
        }
        // if free slot is near the beginning of the table, we wrap around
        else{
            startH = (capacity + freeSlot - hopRange + 1) % capacity; // wrap around
        }
        
        size_t slotToMove = capacity; // initialize to invalid index
        
        // iterate from startH to freeSlot to find an element to move
        for(size_t offset = 0; offset < hopRange; offset++){
            
            size_t i = (startH + offset) % capacity; // for wrap around
          
            // if we have checked all positions before freeSlot, hop range is full
            if (i == freeSlot){
                rehash();
                return hashInsert(key, value);
            }

            auto& bitmap = table[i].getHopInfo();
            size_t bitIndex = 0;

            // we should check only bits that correspond to slots before freeSlot
            // bit[hopRange - 1] corresponds to freeSlot
            while(bitIndex < hopRange - 1){
         
                if(bitmap[bitIndex] == true){

                    slotToMove = (i + bitIndex) % capacity; // actual position of the element to move

                    
                    // swap the empty slot with the slotToMove
                    table[freeSlot].setKey(std::move(table[slotToMove].key));
                    table[freeSlot].setValuesVector(std::move(table[slotToMove].values));
                    table[freeSlot].setOccupied(true);
                    
                    // calculate distance from slotToMove to freeSlot with wrap around
                    size_t newBitIndex;
                    if (freeSlot >= slotToMove) {
                        newBitIndex = freeSlot - slotToMove;
                    } else {
                        newBitIndex = capacity - slotToMove + freeSlot;
                    }


                    table[i].setHopInfoPosToTrue(newBitIndex); // set bit to true for the free slot where we moved the element
                    
                    table[slotToMove].setOccupied(false); // mark old position as free
                    table[i].setHopInfoPosToFalse(bitIndex); // update hop info bitmap for old position

                    // update variables for next iteration
                    freeSlot = slotToMove; // free slot has now moved closer to original pos
                    moved = true;

                      // update distance with wrap around
                    if (freeSlot >= pos) {
                        dist = freeSlot - pos;
                    } else {
                        dist = capacity - pos + freeSlot; // wrap around
                    }

                    break;
                }


                bitIndex++; // check next bit
            }

            if (moved == true){
                break;
            }

        }
        // if no element could be moved, rehash
        if(moved == false){
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

    // check if hop info is full, if so table is full and needs rehashing
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

    // free slot is within hop range, insert directly
    if(insertWithinHopRange(freeSlot, pos, key, value) == true){
        return true;
    }

    size_t dist;
    if (freeSlot >= pos) {
        dist = freeSlot - pos;
    } else {
        dist = capacity - pos + freeSlot; // wrap around
    }

    // try to move elements to bring free slot within hop range
    if(insertAndSwap(freeSlot, pos, key, value) == true){
        return true;
    }

    // now the free slot is within hop range, insert the new key-value pair
    if(insertWithinHopRange(freeSlot, pos, key, value) == true){
        return true;
    }
    

    return false;

}


template <typename keyT, typename valueT>
const std::vector<valueT> Hopscotch<keyT, valueT>::hashSearch(const keyT& key) const{

    size_t pos = hashFunc(key, capacity);

    // check if the key at hashed position matches
    if(table[pos].isOccupied() && table[pos].getKey() == key){
        return table[pos].getValue();
    }

    // check other positions within hop range
    auto& hopInfo = table[pos].getHopInfo();

    for(size_t i = 1; i < hopRange; ++i){

        if(hopInfo[i] == true){ // slot is occupied by an element that hashed here

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