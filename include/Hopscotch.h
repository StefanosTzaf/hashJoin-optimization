#include <iostream>
#include <vector>


template <class keyT, class valueT>
class hashNode{

    keyT key;
    std::vector<valueT> values;
    bool isOccupied;
    std::vector<bool> hopInfo; // to track which slots in hop range are occupied
    // that were initially hashed to this index

    public:

    hashNode(size_t hopRange): isOccupied(false) {}

      //#### GETTERS ####//

    // getters are marked as inline functions to 
    // reduce function call overhead for small functions

    inline bool isOccupied() const{
        return isOccupied;
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

    inline void setOccupied(bool status){
        isOccupied = status;
    }

    void setKey( keyT&& newKey){
        key = std::move(newKey);
    }

    // initialize hopInfo vector with false values
    void initializeHopInfo(size_t hopRange){
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


};

template <class keyT, class valueT>
class Hopscotch{

    size_t capacity; // total capacity of the table
    size_t size; // current number of elements in the table
    std::vector<hashNode<keyT, valueT>> table;
    size_t hopRange; // range for hopscotch

    public:
    
    Hopscotch(size_t capacity, size_t hopR): capacity(capacity), size(0), hopRange(hopR){
        table.resize(capacity);
    }

    bool hashInsert(const keyT& key, const valueT& value);
    size_t hashFunction(const keyT& key) const;
    
    // returns vector of values for the given key
    const std::vector<valueT> hashSearch(const keyT& key) const;
    
    void rehash();
    void printTable(); // for debugging

    // extra functions

    // checks if the original hashed position is free
    // if so, inserts the key-value pair there and sets up hop info bitmap and returns true
    bool insertInOriginalPos(size_t pos, keyT& key, valueT& value);

    // returns true if all slots in bitmap hop info are occupied
    bool isHopInfoFull(size_t pos) const;

    // finds the next free slot starting from startPos
    // and returns its index or capacity if none found
    size_t findFreeSlot(size_t startPos) const;

    // checks if free slot is within hop range of hashed position
    // if so, inserts the key-value pair there and updates hop info bitmap and returns true
    bool insertWithinHopRange(size_t freeSlot, size_t hashedPos, keyT& key, valueT& value);

    const std::vector<valueT> hashSearch(const keyT& key) const;



};




//##### IMPLEMENTATION #####//

template <typename keyT, typename valueT>
size_t Hopscotch<keyT, valueT>::hashFunction(const keyT& key) const{
    return capacity ? (std::hash<keyT>{}(key) % capacity) : 0;
}



template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::insertInOriginalPos(size_t pos, keyT& key, valueT& value){
    
    // if the hashed position is free, insert directly
    if(table[pos].isOccupied() == false){
     
        table[pos].setKey(std::move(key));
        table[pos].addValuetoVector(value);
        table[pos].setOccupied(true);
        table[pos].initializeHopInfo(hopRange); // initialize hop info vector
        table[pos].setHopInfoPosToTrue(0); // mark first slot in hop info as occupied
        size++;
        return true;
    }
    return false;
}

template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::isHopInfoFull(size_t pos) const{

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
size_t Hopscotch<keyT, valueT>::findFreeSlot(size_t startPos) const{
    
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
bool Hopscotch<keyT, valueT>::insertWithinHopRange(size_t freeSlot, size_t hashedPos, keyT& key, valueT& value){

    if((freeSlot - hashedPos) % capacity < hopRange){

        table[freeSlot].setKey(std::move(key));
        table[freeSlot].addValuetoVector(value);
        table[freeSlot].setOccupied(true);
        table[freeSlot].initializeHopInfo(hopRange);
    
        size++;

        // update hop info bitmap at original position
        table[pos].setHopInfoPosToTrue(freeSlot - pos);
        return true;
    }
    
    return false;
}




template <typename keyT, typename valueT>
bool Hopscotch<keyT, valueT>::hashInsert(const keyT& key, const valueT& value){

    size_t pos = hashFunction(key);

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

    // the free slot is outside hop range, now we should move elements
    bool moved = false;
    while((freeSlot - pos) % capacity >= hopRange){

        size_t startH = freeSlot - hopRange + 1; // the first out of H-1 slots prior to freeSlot

        size_t slotToMove = capacity; // initialize to invalid index

        for(size_t i = startH; i < freeSlot; i++){
            
            auto& bitmap = table[i].getHopInfo();
            size_t bitIndex = 0;
            
            while(true){
               
                // we should check only bits that correspond to slots before freeSlot
                if(bitIndex + i >= hopRange){
                    break;
                }

                if(bitmap[bitIndex] == true){
                    slotToMove = i + bitIndex;

                    // swap the empty slot with the slotToMove
                    table[freeSlot].setKey(std::move(table[slotToMove].getKey()));
                    table[freeSlot].setValuesVector(std::move(table[slotToMove].getValue()));
                    table[freeSlot].setOccupied(true);
                    
                    table[i].setHopInfoPosToTrue(freeSlot - slotToMove + 1); // update hop info bitmap for new position
                    
                    table[slotToMove].setOccupied(false); // mark old position as free
                    table[i].setHopInfoPosToFalse(bitIndex); // update hop info bitmap for old position

                    // update variables for next iteration
                    freeSlot = slotToMove;
                    moved = true;
                    break;
                }

                bitIndex++; // check next bit
            }

            if (moved == true){
                freeSlot = slotToMove;
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