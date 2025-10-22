#pragma once
#include <vector>
#define MAX_LOAD_FACTOR 0.5


template <class keyT, class valueT>
class hashNode{
    private:
        keyT key;
        std::vector<valueT> values;
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

        inline const std::vector<valueT>& getValue() const {
            return values;
        }

        // Setters
        void setOccupied(bool status) { 
            is_occupied = status; 
        }
        void setKey(const keyT& newKey) {
            key = newKey;
        }
        void addValue(const valueT& newValue) {
            values.push_back(newValue);
        }
        void setValues(const std::vector<valueT>& newValues) {
            values = newValues;
       }
};

template <class keyT, class valueT>
class CuckooHashTable { 
    private:
        size_t capacity; 

        size_t size;

        std::vector<hashNode<keyT, valueT>> table1;
        std::vector<hashNode<keyT, valueT>> table2;

    public:
        //constructor
        CuckooHashTable(size_t initialCapacity);

        size_t hashFunction1(const keyT& key) const {
            return capacity ? (std::hash<keyT>{}(key) % capacity) : 0;
        }
        
        size_t hashFunction2(const keyT& key) const {
            // Χρησιμοποιούμε διαφορετικό seed για το δεύτερο hash
            return capacity ? ((std::hash<keyT>{}(key) * 16777619) % capacity) : 0;
        }

        float getLoadFactor() const {
            return capacity ? static_cast<float>(size) / static_cast<float>(capacity) : 0.0f;
        }

        bool hashInsert(const keyT& key, const valueT& value);
        std::vector<valueT> hashSearch(const keyT& key) const;
        void rehash();        

};



template <class keyT, class valueT>
CuckooHashTable<keyT, valueT>::CuckooHashTable(size_t initialCapacity = 16) 
    : capacity(initialCapacity == 0 ? 16 : initialCapacity), size(0), table1(initialCapacity), table2(initialCapacity){}


    

template <class keyT, class valueT>
bool CuckooHashTable<keyT, valueT>::hashInsert(const keyT& key, const valueT& value) {

    // check load factor and rehash if necessary. Doing it in the beginning to avoid unnecessary computations
    if(getLoadFactor() >= MAX_LOAD_FACTOR){
        rehash();
    }

    // if size kicks happen it means that we are in a cycle and need to rehash
    size_t maxKicks = size;
    size_t currentKicks = 0;
    bool tableToSearch = true; // true for table1, false for table2

    while (1) {

        // table1 case
        if (tableToSearch) {
            size_t pos1 = hashFunction1(key);
            auto& node1 = table1[pos1];

            // just insert the key,value if position is empty or key already exists
            if (!node1.isOccupied()) {
                node1.setKey(key);
                node1.addValue(value);
                node1.setOccupied(true);
                size++;
                return true;
            } 
            else if (node1.getKey() == key) {
                node1.addValue(value);
                return true;
            } 

            // if occupied by different key we insert the new one but first keeping the old one to reinsert to the other table
            else {
                // Kick out existing element
                keyT oldKey = node1.getKey();
                std::vector<valueT> oldValues = node1.getValue();

                node1.setKey(key);
                node1.setValues({value});

                key = oldKey;
                value = oldValues;
                // switch to table2 (we will try to insert there the kicked node)
                tableToSearch = false;
                currentKicks++;
            }
        } 
        else{
            // table2 case
            size_t pos2 = hashFunction2(key);
            auto& node2 = table2[pos2];


            if (!node2.isOccupied()) {
                node2.setKey(key);
                node2.addValue(value);
                node2.setOccupied(true);
                size++;
                return true;
            } 
            else if (node2.getKey() == key) {
                node2.addValue(value);
                return true;
            } 

            // if occupied by different key we insert the new one but first keeping the old one to reinsert to the other table
            else {
                // Kick out existing element
                keyT oldKey = node2.getKey();
                std::vector<valueT> oldValues = node2.getValue();

                node2.setKey(key);
                node2.setValues({value});

                key = oldKey;
                value = oldValues;

                // switch to table1
                tableToSearch = true;
                currentKicks++;
            }
        }

        // it means we are in a cycle, we need a larger table
        if(currentKicks >= maxKicks){
            rehash();
        }
    }
    
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
            for (const auto& value : node.getValues()) {
                hashInsert(node.getKey(), value);
            }
        }
    }
    
    for (const auto& node : old_table2) {
        if (node.isOccupied()) {
            for (const auto& value : node.getValues()) {
                hashInsert(node.getKey(), value);
            }
        }
    }
}


//TODO : does it have to be const the return value?????
template <class keyT, class valueT>
std::vector<valueT> CuckooHashTable<keyT, valueT>::hashSearch(const keyT& key) const {
    size_t pos1 = hashFunction1(key);
    const auto& node1 = table1[pos1];
    if(node1.isOccupied() && node1.getKey() == key) {
        return node1.getValue();
    }
    size_t pos2 = hashFunction2(key);
    const auto& node2 = table2[pos2];
    if(node2.isOccupied() && node2.getKey() == key) {
        return node2.getValue();
    }
    return {};
}