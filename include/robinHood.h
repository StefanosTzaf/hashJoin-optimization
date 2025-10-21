#pragma once
#include <vector>
#include <optional>
#include <iostream>
#include <functional>

#define MAX_LOAD_FACTOR 0.75

template <class keyT, class valueT>
class hashNode{

    keyT                 key;
    std::vector<valueT>  values;   // store all values for this key in case of duplicates
    bool                 is_occupied; // indicates if the node is occupied
    size_t               psl; // probe sequence length
    
public:
    //constructor
    hashNode() : is_occupied(false), psl(0) {

    }

    //#### GETTERS ####//

    // getters are marked as inline functions to 
    // reduce function call overhead for small functions

    inline bool isOccupied() const{
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

    inline size_t getPsl() const {
        return psl;
    }

    // ##### SETTERS #### //
    inline void setOccupied(bool status){
        is_occupied = status;
    }

    inline void setPsl(const size_t newPsl){
        psl = newPsl;
    }

    // move instead of copy:
    // std::move casts from lvalue to rvalue, the source object is left in a valid but unspecified state
    // Use of move requires rvalue reference
    // rvalue reference &&: binds to a temporary object allowing move semantics
    // no need for const reference since moving modifies the source
    
    void setKey( keyT&& newKey){
        key = std::move(newKey);
    }
    
    // set the entire values vector
    void setValuesVector( std::vector<valueT>&& newValues){
        values = std::move(newValues);
    }

    // add a single value to the values vector
    void addValuetoVector(const valueT& newValue){
        values.emplace_back(newValue);
    }

    // hash table class needs access to private members
    // for the swap operation during insertion
    template <class keyT2, class valueT2>
    friend class RobinHoodHashTable;

};

template <class keyT, class valueT>
class RobinHoodHashTable {

private:
    size_t capacity; // total capacity of the table
    size_t size; // current number of elements in the table
    std::vector<hashNode<keyT, valueT>> table;

    public:
    
    //constructor
    RobinHoodHashTable(size_t initialCapacity = 16);
    
    bool hashInsert(const keyT& key, const valueT& value);

    void rehash();
    void printTable(); // for debugging


    // returns vector of values for the given key
    const std::vector<valueT> hashSearch(const keyT& key) const;

    //the second const declares that the function does not modify any member variables (read-only function)
    size_t hashFunction(const keyT& key) const;

    inline size_t getSize() const {
        return size;
    }

    float getLoadFactor() const {
        return capacity ? static_cast<float>(size) / static_cast<float>(capacity) : 0.0f;
    }

};



// implementation of hash table with Robin Hood hashing

// using namespace std;

template <typename keyT, typename valueT>
// dynamically allocate table(vector)
RobinHoodHashTable<keyT, valueT>::RobinHoodHashTable(size_t initialCapacity)
    : capacity(initialCapacity == 0 ? 1 : initialCapacity), size(0), table(capacity) {
}

template <typename keyT, typename valueT>
size_t RobinHoodHashTable<keyT, valueT>::hashFunction(const keyT& key) const{
    return capacity ? (std::hash<keyT>{}(key) % capacity) : 0;
}


template <typename keyT, typename valueT>
bool RobinHoodHashTable<keyT, valueT>::hashInsert(const keyT& key, const valueT& value) {

    size_t pos = hashFunction(key);

    // these three variables are the ones we are trying
    // to insert into the hash table each time
    keyT inputKey = key;
    // carry vector of values for the current key we're placing
    std::vector<valueT> inputValues{value};
    
    size_t inputPsl = 0;
    size_t i = pos;
    
    // circular iteration over the table
    while (true) {
        
        // if position is empty, insert in node
        if((table[i].isOccupied()) == false){
            
            table[i].setKey(std::move(inputKey)); 
            table[i].setValuesVector(std::move(inputValues)); // efficient move
            table[i].setOccupied(true);
            table[i].setPsl(inputPsl);

            size++;

            // check load factor and rehash if needed
            if(getLoadFactor() >= MAX_LOAD_FACTOR){
                rehash();
            }

            return true; // setValue
        }
        // if key already exists, append the value in the values vector
        else if(table[i].isOccupied() && table[i].getKey() == inputKey){

            table[i].addValuetoVector(value);
            return true; // value added to existing key

            // no size increment needed since it's the same key
        }

        // if position is occupied from a different key
        else{
            
            // old key is "wealthier" so it needs to be replaced
            // by the new key which is "poorer"
            if(inputPsl > table[i].getPsl()){

                // update input variables so the loop continues
                // searching for a position for the old key

                // exchanges the values of two objects efficiently
                // it uses move internally and not copy
                std::swap(inputKey, table[i].key);
                std::swap(inputValues, table[i].values);
                std::swap(inputPsl, table[i].psl);

            }
        }

        inputPsl++; // one position further from ideal position   
        i = (i + 1) % capacity; // move to next position (circular)
        if (i == pos) { // completed a full loop
            break;
        }
            
    }

    return false; // full table

}

template <typename keyT, typename valueT>
void RobinHoodHashTable<keyT, valueT>::rehash(){
   
    capacity *= 2; // double the capacity

    size = 0; // reset size, since it will be updated during re-insertions

    // move old table to a temporary variable
    std::vector<hashNode<keyT, valueT>> oldTable = std::move(table);
    
    // create new table with updated capacity
    table = std::vector<hashNode<keyT, valueT>>(capacity);

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


// returns the vector of values for the given key
template <typename keyT, typename valueT>
const std::vector<valueT> RobinHoodHashTable<keyT, valueT>::hashSearch(const keyT& key) const {

    size_t pos = hashFunction(key);
    size_t currSpl = 0;
    size_t i = pos;
    // circular probing
    while (true) {
        const auto& node = table[i];
        
        // If position is empty, key definitely not found
        if (!node.isOccupied()) {
            return {};
        }

        // if key found, return its values vector
        if(node.getKey() == key) {
            return node.getValue();
        }

        // if we reach a node with psl less than currSpl, key not found
        if(node.getPsl() < currSpl) {
            return {}; // key not found
        }

        currSpl++; // one position further from ideal position
        i = (i + 1) % capacity; // move to next position (circular)
        if (i == pos)   { // completed a full loop
            break;
        }
    }
    return {}; // key not found
}



template <typename keyT, typename valueT>
void RobinHoodHashTable<keyT, valueT>::printTable(){

    std::cout << "\n=== Hash Table State ===\n";
        
    for (size_t i = 0; i < capacity; ++i) {
           
        if (table[i].isOccupied()){

            // get vector of values
            const std::vector<valueT>& values = table[i].getValue();
            
            std::cout << "[" << i << "] key=" << table[i].getKey();
            
            // if the vector is not empty, print its contents
            if(values.size() > 0){
                std::cout << ", Values: ";
                for(const auto& val : values){
                    std::cout << val << ", ";
                }
            }

            std::cout << "psl=" << table[i].getPsl() << "\n";
                

        }
        else{
            std::cout << "[" << i << "] EMPTY\n";
        }
        
        }
}
