#pragma once
#include <vector>
#include <iostream>
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
        valueT value;
        bool is_occupied;
        //bool inFirstTable;
        

    public:
        hashNode() : is_occupied(false) {}
        inline bool isOccupied() const{
            return is_occupied;
        }
        

        inline const keyT& getKey() const {
            return key;
        }

        inline const valueT& getValue() const {
            return value;
        }

        // Setters
        void setOccupied(bool status) { 
            is_occupied = status; 
        }
        void setKey(const keyT& newKey) {
            key = newKey;
        }

        void setValue(const valueT& newValue) {
            value = newValue;
       }


 
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

    public:
        //constructor with default hash functions
        CuckooHashTable(size_t initialCapacity, 
                    HashFunction<keyT> h1 = defaultHash1,
                    HashFunction<keyT> h2 = defaultHash2);

    
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


        // Hash function wrappers - encapsulation logic
        size_t hash1(const keyT& key) const {
            return hashFunc1(key, capacity);
        }
        
        size_t hash2(const keyT& key) const {
            return hashFunc2(key, capacity);
        }

        float getLoadFactor() const {
            return capacity ? static_cast<float>(size) / (static_cast<float>(capacity) * 2) : 0.0f;
        }

        size_t getSize() const {
            return size;
        }

        size_t getCapacity() const {
            return capacity;
        }

        bool hashInsert(const keyT& inputKey, const valueT& inputValue);

        valueT hashSearch(const keyT& key) const;

        // Returns mutable pointer to value for key. Returns nullptr if key not found.
        // Is needed in execute.cpp (acts like itr->second for unordered map)
        valueT* getMutable(const keyT& key);

        void rehash();      


    //------------------------------TESTING ASSOSCIATED METHODS-------------------------------------//
        // returns in which table and position the key is found. useful for testing!!
        std::pair<int, size_t> findPosition(const keyT& key) const {
            size_t pos1 = hash1(key);
            if (table1[pos1].isOccupied() && table1[pos1].getKey() == key) {
                return {1, pos1};
            }
            
            size_t pos2 = hash2(key);
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
bool CuckooHashTable<keyT, valueT>::hashInsert(const keyT& inputKey, const valueT& inputValue) {

    // check load factor and rehash if necessary. Doing it in the beginning to avoid unnecessary computations
    if(getLoadFactor() >= MAX_LOAD_FACTOR){
        rehash();
    }

    // if size kicks happen it means that we are in a cycle and need to rehash
    size_t maxKicks = size + 1;
    size_t currentKicks = 0;
    bool tableToSearch = true; // true for table1, false for table2

    keyT key = inputKey;
    valueT value = inputValue;

    while (1) {

        // table1 case
        if (tableToSearch) {
            size_t pos1 = hash1(key);
            auto& node1 = table1[pos1];

            // just insert the key,value if position is empty
            if (!node1.isOccupied()) {
                node1.setKey(key);
                node1.setValue(value);
                node1.setOccupied(true);
                size++;
                return true;
            }
            // if same key exists, just update the value
            else if (node1.getKey() == key) {
                node1.setValue(value);
                return true;
            }
           // if occupied by different key we insert the new one but first keeping the old one to reinsert to the other table
            else {
                // Kick out existing element
                keyT oldKey = node1.getKey();
                valueT oldValue = node1.getValue();

                node1.setKey(key);
                node1.setValue(value);

                key = oldKey;
                value = oldValue;
                // switch to table2 (we will try to insert there the kicked node)
                tableToSearch = false;
                currentKicks++;
            }
        } 
        else{
            // table2 case
            size_t pos2 = hash2(key);
            auto& node2 = table2[pos2];


            if (!node2.isOccupied()) {
                node2.setKey(key);
                node2.setValue(value);
                node2.setOccupied(true);
                size++;
                return true;
            }
            // if same key exists, just update the value
            else if (node2.getKey() == key) {
                node2.setValue(value);
                return true;
            }
            // if occupied by different key we insert the new one but first keeping the old one to reinsert to the other table
            else {
                // Kick out existing element
                keyT oldKey = node2.getKey();
                valueT oldValue = node2.getValue();

                node2.setKey(key);
                node2.setValue(value);

                key = oldKey;
                value = oldValue;

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
            hashInsert(node.getKey(), node.getValue());
        }
    }
    
    for (const auto& node : old_table2) {
        if (node.isOccupied()) {
            hashInsert(node.getKey(), node.getValue());
        }
    }
}


//TODO : does it have to be const the return value?????
template <class keyT, class valueT>
valueT CuckooHashTable<keyT, valueT>::hashSearch(const keyT& key) const {
    size_t pos1 = hash1(key);
    const auto& node1 = table1[pos1];
    if(node1.isOccupied() && node1.getKey() == key) {
        return node1.getValue();
    }
    size_t pos2 = hash2(key);
    const auto& node2 = table2[pos2];
    if(node2.isOccupied() && node2.getKey() == key) {
        return node2.getValue();
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
            std::cout <<"   Key = " << node.getKey() << ", Value = " << node.getValue();
        }
        std::cout << std::endl;
    }
    std::cout << "\n\nTable 2:" << std::endl;
    for (size_t i = 0; i < capacity; ++i) {
        const auto& node = table2[i];
        std::cout <<  "[" << i << "]";
        if (node.isOccupied()) {
            std::cout << "   Key = " << node.getKey() << ", Value = " << node.getValue();
        }
        std::cout << std::endl;
    }
    std::cout << std::endl;
}


template <class keyT, class valueT>
valueT* CuckooHashTable<keyT, valueT>::getMutable(const keyT& key) {
    // Check first table
    size_t pos1 = hash1(key);
    if (table1[pos1].isOccupied() && table1[pos1].getKey() == key) {
        return const_cast<valueT*>(&table1[pos1].getValue());
    }
    
    // Check second table
    size_t pos2 = hash2(key);
    if (table2[pos2].isOccupied() && table2[pos2].getKey() == key) {
        return const_cast<valueT*>(&table2[pos2].getValue());
    }
    
    // Key not found - return nullptr
    return nullptr;
}