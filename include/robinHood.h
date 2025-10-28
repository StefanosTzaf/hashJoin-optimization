#pragma once
#include <vector>
#include <optional>
#include <iostream>
#include <functional>

#define MAX_LOAD_FACTOR 0.75

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
    
    void setKey(keyT&& newKey){
        key = std::move(newKey);
    }
    
    // set the entire values vector
    void setValuesVector(std::vector<valueT>&& newValues){
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


// generic hash function type, so as to be able to give ours when being constructed
template <class keyT>
using HashFunction = size_t (*)(const keyT&, size_t);



template <class keyT, class valueT>
class RobinHoodHashTable {

private:
    size_t capacity; // total capacity of the table
    size_t size; // current number of elements in the table
    std::vector<hashNode<keyT, valueT>> table;

    HashFunction<keyT> hashFunc;

    // Fast hash function using MurmurHash3 finalizer
    static size_t defaultHash(const keyT& key, size_t cap){
        size_t hash = std::hash<keyT>{}(key);
        
        // MurmurHash3 finalizer
        hash ^= hash >> 33;
        hash *= 0xff51afd7ed558ccdULL;
        hash ^= hash >> 33;
        hash *= 0xc4ceb9fe1a85ec53ULL;
        hash ^= hash >> 33;
        
        return hash & (cap - 1); // Assumes capacity is power of 2
    }

public:
    
    //constructor, if no hash function is provided, use default
    RobinHoodHashTable(size_t initialCapacity, HashFunction<keyT> h = defaultHash);
    
    bool hashInsert(const keyT& key, const valueT& value);

    void rehash();

    void printTable(); // for debugging

    // returns vector of values for the given key
    const std::vector<valueT> hashSearch(const keyT& key) const;

    size_t getSize() const{
        return size;
    }

    size_t getCapacity() const{
        return capacity;
    }

    float getLoadFactor() const {
        return capacity ? static_cast<float>(size) / static_cast<float>(capacity) : 0.0f;
    }

    //------------------------------Test associated functions------------------------------------
    // finally used in rehash as well
    size_t findPosition(const keyT& key) const {
        size_t pos = hashFunc(key, capacity);
        size_t currSpl = 0;
        size_t i = pos;
        while (true) {
            const auto& node = table[i];
            
            // If position is empty, key definitely not found
            if (!node.isOccupied()) {
                return -1;
            }

            // if key found, return its position
            if(node.getKey() == key) {
                return i;
            }

            // if we reach a node with psl less than currSpl, key not found
            if(node.getPsl() < currSpl) {
                return -1;
            }

            currSpl++;
            i = (i + 1) & (capacity - 1);
            if (i == pos){
                break;
            }
        }
        return -1;
    }

    size_t getPslOfKey(const keyT& key) const {
        size_t pos = hashFunc(key, capacity);
        size_t currSpl = 0;
        size_t i = pos;
        while (true) {
            const auto& node = table[i];
            
            // If position is empty, key definitely not found
            if (!node.isOccupied()) {
                return -1;
            }

            // if key found, return its psl
            if(node.getKey() == key) {
                return node.getPsl();
            }

            // if we reach a node with psl less than currSpl, key not found
            if(node.getPsl() < currSpl) {
                return -1;
            }

            currSpl++;
            i = (i + 1) & (capacity - 1);
            if (i == pos){
                break;
            }
        }
        return -1;
    }
};



// implementation of hash table with Robin Hood hashing

// using namespace std;

template <typename keyT, typename valueT>
// dynamically (internally we do not have to do new and delete) allocate table(vector)
RobinHoodHashTable<keyT, valueT>::RobinHoodHashTable(size_t initialCapacity, HashFunction<keyT> h)
    : size(0), hashFunc(h) {
    // Round up to next power of 2
    capacity = 16; // minimum size
    if(initialCapacity == 0) {
        capacity = 16;
    }
    else{
        capacity = nextPowerOf2(initialCapacity);
    }
    size = 0;
    table = std::vector<hashNode<keyT, valueT>>(capacity);
}


template <typename keyT, typename valueT>
bool RobinHoodHashTable<keyT, valueT>::hashInsert(const keyT& key, const valueT& value) {
   
    // check load factor and rehash if needed before insertion
    if(getLoadFactor() >= MAX_LOAD_FACTOR){
        rehash();
    }

    size_t pos = hashFunc(key, capacity);

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


            return true; // setValue
        }
        // if key already exists, append the value in the values vector
        else if(table[i].isOccupied() && table[i].getKey() == inputKey){

            table[i].addValuetoVector(value);
            return true; // value added to existing key

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
        i = (i + 1) & (capacity - 1); // move to next position (circular)
        if (i == pos) { // completed a full loop
            break;
        }
            
    }

    return false; // full table

}

template <typename keyT, typename valueT>
void RobinHoodHashTable<keyT, valueT>::rehash(){
    // Capacity is already power of 2, so just double it
    capacity *= 2;

    size = 0; // reset size, since it will be updated during re-insertions

    // move old table to a temporary variable
    std::vector<hashNode<keyT, valueT>> oldTable = std::move(table);
    
    // create new table with updated capacity
    table = std::vector<hashNode<keyT, valueT>>(capacity);

    // for each node in old table
    for (auto node : oldTable) {

        // Skip empty nodes
        if (node.isOccupied()) {
            hashInsert(node.getKey(), node.getValue()[0]); // insert first value
            size_t pos = findPosition(node.getKey()); // find position in new table
            if (node.getValue().size() > 1) {
                table[pos].setValuesVector(std::move(node.values));
            }
            
        }
    }
}


// returns the vector of values for the given key
template <typename keyT, typename valueT>
const std::vector<valueT> RobinHoodHashTable<keyT, valueT>::hashSearch(const keyT& key) const {

    size_t pos = hashFunc(key, capacity);
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
        i = (i + 1) & (capacity - 1); // move to next position (circular)
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
