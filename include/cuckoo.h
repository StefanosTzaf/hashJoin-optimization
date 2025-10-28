#pragma once
#include <vector>
#include <iostream>
#include <cstdint>
#define MAX_LOAD_FACTOR 0.5

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
    private:
        keyT key;
        std::vector<valueT> values;
        bool is_occupied;


    public:
        hashNode() : is_occupied(false) {}
        inline bool isOccupied() const{
            return is_occupied;
        }
        

        inline const keyT& getKey() const {
            return key;
        }

        inline const std::vector<valueT>& getValues() const {
            return values;
        }

        // Setters
        void setOccupied(bool status) { 
            is_occupied = status; 
        }
        void setKey( keyT&& newKey){
            key = std::move(newKey);
        }

        void setValuesVector( std::vector<valueT>&& newValues){
            values = std::move(newValues);
        }

        // add a single value to the values vector
        void addValuetoVector(const valueT& newValue){
            values.emplace_back(newValue);
        }

        template <class K, class V>
        friend class CuckooHashTable;
 
};

// pointer to hash Function so as to be able to use different hash functions (helpful in testing)
template <class keyT>
using HashFunction = size_t (*)(const keyT&, size_t);

template <class keyT, class valueT>
class CuckooHashTable { 

    private:

        size_t capacity; 
        size_t size;

        std::vector<hashNode<keyT, valueT>> table1;
        std::vector<hashNode<keyT, valueT>> table2;
        
        // two hash functions
        HashFunction<keyT> hashFunc1;
        HashFunction<keyT> hashFunc2;
        
        // Default hash functions defined as static methods
        //As static methods are written one time for all instances of the class (less memory)
        static size_t defaultHash1(const keyT& key, size_t cap) {
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

        static size_t defaultHash2(const keyT& key, size_t cap) {
            // Hash2: SplitMix64 mix of a different seed to decorrelate from Hash1
            uint64_t x = static_cast<uint64_t>(std::hash<keyT>{}(key)) ^ 0x9E3779B97F4A7C15ULL;
            x += 0x9E3779B97F4A7C15ULL;
            x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
            x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
            x =  x ^ (x >> 31);
            return static_cast<size_t>(x) & (cap - 1);
        }

    public:

        //constructor with default hash functions
        CuckooHashTable(size_t initialCapacity, 
                    HashFunction<keyT> h1 = defaultHash1,
                    HashFunction<keyT> h2 = defaultHash2);


        float getLoadFactor() const {
            return capacity ? static_cast<float>(size) / (static_cast<float>(capacity) * 2) : 0.0f;
        }

        size_t getSize() const {
            return size;
        }

        size_t getCapacity() const {
            return capacity;
        }

        bool hashInsert(const keyT& key, const valueT& value);

        const std::vector<valueT> hashSearch(const keyT& key) const;

        void rehash();      


    //------------------------------TESTING ASSOSCIATED METHODS-------------------------------------//
        // returns in which table and position the key is found. useful for testing!!
        std::pair<int, size_t> findPosition(const keyT& key) const {
            size_t pos1 = hashFunc1(key, capacity);
            if (table1[pos1].isOccupied() && table1[pos1].getKey() == key) {
                return {1, pos1};
            }

            size_t pos2 = hashFunc2(key, capacity);
            if (table2[pos2].isOccupied() && table2[pos2].getKey() == key) {
                return {2, pos2};
            }
            
            return {0, 0};
        }

        void displayTable(); // for debugging


};



template <class keyT, class valueT>
CuckooHashTable<keyT, valueT>::CuckooHashTable(size_t initialCapacity,
                                              HashFunction<keyT> h1,
                                              HashFunction<keyT> h2)
    : hashFunc1(h1)
    , hashFunc2(h2) {
        
    if(initialCapacity == 0) {
        initialCapacity = 16;
    }

    capacity = nextPowerOf2(initialCapacity);
    size = 0;
    table1 = std::vector<hashNode<keyT, valueT>>(capacity);
    table2 = std::vector<hashNode<keyT, valueT>>(capacity);
}


    

template <class keyT, class valueT>
bool CuckooHashTable<keyT, valueT>::hashInsert(const keyT& key, const valueT& value) {

    // check load factor and rehash if necessary. Doing it in the beginning to avoid unnecessary computations
    if(getLoadFactor() >= MAX_LOAD_FACTOR){
        rehash();
    }

    // First, check if the key already exists in either table
    size_t pos1 = hashFunc1(key, capacity);
    if (table1[pos1].isOccupied() && table1[pos1].getKey() == key) {
        table1[pos1].addValuetoVector(value);
        return true;
    }

    size_t pos2 = hashFunc2(key, capacity);
    if (table2[pos2].isOccupied() && table2[pos2].getKey() == key) {
        table2[pos2].addValuetoVector(value);
        return true;
    }

    // Key doesn't exist, so we need to insert it
    // if size kicks happen it means that we are in a cycle and need to rehash
    size_t maxKicks = size + 1;
    size_t currentKicks = 0;
    bool tableToSearch = true; // true for table1, false for table2

    keyT inputKey = key;
    std::vector<valueT> inputValues{value};


    while (1) {

        // table1 case
        if (tableToSearch) {
            size_t pos1 = hashFunc1(inputKey, capacity);

            // just insert the key,value if position is empty
            if (!table1[pos1].isOccupied()) {
                table1[pos1].setKey(std::move(inputKey));
                table1[pos1].setValuesVector(std::move(inputValues));
                table1[pos1].setOccupied(true);
                size++;
                return true;
            }
            // if occupied by different key we insert the new one but first keeping the old one to reinsert to the other table
            else {
                // Kick out existing element
                std::swap(inputKey, table1[pos1].key);
                std::swap(inputValues, table1[pos1].values);
                // switch to table2 (we will try to insert there the kicked node)
                tableToSearch = false;
                currentKicks++;
            }
        } 
        else{
            // table2 case
            size_t pos2 = hashFunc2(inputKey, capacity);

            if (!table2[pos2].isOccupied()) {
                table2[pos2].setKey(std::move(inputKey));
                table2[pos2].setValuesVector(std::move(inputValues));
                table2[pos2].setOccupied(true);
                size++;

                return true;
            }
            // if occupied by different key we insert the new one but first keeping the old one to reinsert to the other table
            else {
                // Kick out existing element
                std::swap(inputKey, table2[pos2].key);
                std::swap(inputValues, table2[pos2].values);
                // switch to table1
                tableToSearch = true;
                currentKicks++;
            }
        }

        // it means we are in a cycle, we need a larger table
        if(currentKicks >= maxKicks){
            rehash();
            // After rehash, restart the insertion with the current key/value pair
            maxKicks = size + 1;
            currentKicks = 0;
            tableToSearch = true;
        }
    }
    return true;
}

template <class keyT, class valueT>
void CuckooHashTable<keyT, valueT>::rehash() {
    
    auto old_table1 = std::move(table1);
    auto old_table2 = std::move(table2);

    size = 0;
    capacity *= 2;
    
    // the new tables
    table1 = std::vector<hashNode<keyT, valueT>>(capacity);
    table2 = std::vector<hashNode<keyT, valueT>>(capacity);
    
    for (const auto& node : old_table1) {
        if (node.isOccupied()) {
            for (const auto& val : node.getValues()) {
                hashInsert(node.getKey(), val);
            }
        }
    }
    
    for (const auto& node : old_table2) {
        if (node.isOccupied()) {
            for (const auto& val : node.getValues()) {
                hashInsert(node.getKey(), val);
            }
        }
    }
}


//TODO : does it have to be const the return value?????
template <class keyT, class valueT>
const std::vector<valueT> CuckooHashTable<keyT, valueT>::hashSearch(const keyT& key) const {

    size_t pos1 = hashFunc1(key, capacity);
    const auto& node1 = table1[pos1];
    if(node1.isOccupied() && node1.getKey() == key) {
        return node1.getValues();
    }

    size_t pos2 = hashFunc2(key, capacity);
    const auto& node2 = table2[pos2];
    if(node2.isOccupied() && node2.getKey() == key) {
        return node2.getValues();
    }

    return {};
}



template <class keyT, class valueT>
void CuckooHashTable<keyT, valueT>::displayTable() {
    std::cout << "\n=== Hash Table State ===\n\n";

    std::cout << "Table 1:" << std::endl;
    for (size_t i = 0; i < capacity; ++i) {
        const auto& node = table1[i];
        std::cout <<  "[" << i << "]";
        if (node.isOccupied()) {
            std::cout <<"   Key = " << node.getKey() << ", Values = ";
            for (const auto& val : node.getValues()) {
                std::cout << val << " ";
            }
        }
        std::cout << std::endl;
    }

    std::cout << "\n\nTable 2:" << std::endl;
    for (size_t i = 0; i < capacity; ++i) {
        const auto& node = table2[i];
        std::cout <<  "[" << i << "]";
        if (node.isOccupied()) {
            std::cout << "   Key = " << node.getKey() << ", Values = ";
            for (const auto& val : node.getValues()) {
                std::cout << val << " ";
            }
        }
        std::cout << std::endl;
    }
    std::cout << std::endl;
}
