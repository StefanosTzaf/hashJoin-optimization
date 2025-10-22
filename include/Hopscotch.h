#include <iostream>
#include <vector>


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

template <class keyT, class valueT>
class Hopscotch{

    size_t capacity; // total capacity of the table
    size_t size; // current number of elements in the table
    std::vector<hashNode<keyT, valueT>> table;
    size_t hopRange; // range for hopscotch

    public:
    
    Hopscotch(size_t capacity, size_t hopR): capacity(capacity), size(0), hopRange(hopR){
        table.resize(capacity, hashNode<keyT, valueT>(hopRange));
    }

    bool hashInsert(const keyT& key, const valueT& value);
    size_t hashFunction(const keyT& key) const;
    
    // returns vector of values for the given key
    const std::vector<valueT> hashSearch(const keyT& key) const;
    
    void rehash();
    void printTable() const; // for debugging

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

    void printHopInfoBitmap(const size_t pos) const;



};




//##### IMPLEMENTATION #####//

template <typename keyT, typename valueT>
size_t Hopscotch<keyT, valueT>::hashFunction(const keyT& key) const{
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

    if((freeSlot - hashedPos) % capacity < hopRange){

        table[freeSlot].setKey(key);
        table[freeSlot].addValuetoVector(value);
        table[freeSlot].setOccupied(true);
        table[freeSlot].resetHopInfo(hopRange);
    
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
bool Hopscotch<keyT, valueT>::hashInsert(const keyT& key, const valueT& value){

    std::cout<< key << " INSIDE----";
   
    size_t pos = hashFunction(key);
    
    // check if key already exists, if so append value to existing vector
    if(insertDuplicateKey(pos, key, value) == true){
        return true;
    }
    std::cout<< key << " no duplicate----";
    // if the original hashed position is free, insert there
    if (insertInOriginalPos(pos, key, value) == true) {
        return true;
    }
    std::cout<< "not in original pos----";

    // check if hop info is full, if so table is full and needs rehashing
   if(isHopInfoFull(pos) == true){
        rehash();
        return hashInsert(key, value);
    }
    std::cout<< "hop info not full----";
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

    // the free slot is outside hop range, now we should move elements
    while((freeSlot - pos) % capacity >= hopRange){
       
        bool moved = false;
        
        size_t startH = freeSlot - hopRange + 1; // the first out of H-1 slots prior to freeSlot

        size_t slotToMove = capacity; // initialize to invalid index

        for(size_t i = startH; i < freeSlot; i++){
            
            auto& bitmap = table[i].getHopInfo();
            size_t bitIndex = 0;
            
            while(true){
               
                // we should check only bits that correspond to slots before freeSlot
                if(bitIndex + i >= freeSlot){
                    break;
                }

                if(bitmap[bitIndex] == true){
                    slotToMove = i + bitIndex;

                    // swap the empty slot with the slotToMove
                    table[freeSlot].setKey(std::move(table[slotToMove].getKey()));
                    table[freeSlot].setValuesVector(std::move(table[slotToMove].values));
                    table[freeSlot].setOccupied(true);
                    
                    table[i].setHopInfoPosToTrue(freeSlot - slotToMove + bitIndex); // set bit to true for the free slot where we moved the element
                    
                    table[slotToMove].setOccupied(false); // mark old position as free
                    table[i].setHopInfoPosToFalse(bitIndex); // update hop info bitmap for old position

                    // update variables for next iteration
                    freeSlot = slotToMove; // free slot has now moved closer to original pos
                    moved = true;
                    break;
                }

                bitIndex++; // check next bit
            }

            if (moved == true){
                break;
            }
        }
    }

    // now the free slot is within hop range, insert the new key-value pair
    if(insertWithinHopRange(freeSlot, pos, key, value) == true){
        return true;
    }


    return false;

}


template <typename keyT, typename valueT>
const std::vector<valueT> Hopscotch<keyT, valueT>::hashSearch(const keyT& key) const{
    
    size_t pos = hashFunction(key);

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