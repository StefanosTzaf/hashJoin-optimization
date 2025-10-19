#pragma once
#include <vector>
#include <optional>
#include <iostream>


template <class keyT, class valueT>
class hashNode{

    keyT                 key;
    std::vector<valueT>  values;   // store all values for this key in case of duplicates
    bool                 is_occupied; // indicates if the node is occupied
    int                  psl; // probe sequence length
    
public:
    //constructor
    hashNode() : is_occupied(false), psl(0) {

    }

    // GETTERS
    bool isOccupied() const{
        return is_occupied;
    }

    keyT getKey() const {
        return key;
    }

    const std::vector<valueT>& getValue() const {
        return values;
    }

    int getPsl() const {
        return psl;
    }

    // SETTERS
    void setOccupied(bool status){
        is_occupied = status;
    }

    void setPsl(int newPsl){
        psl = newPsl;
    }

    void setKey(const keyT& newKey){
        key = newKey;
    }

    // add a single value to the values vector
    void addValuetoVector(const valueT& newValue){
        values.emplace_back(newValue);
    }

    // set the entire values vector
    void setValuesVector(const std::vector<valueT>& newValues){
        values = std::move(newValues);
    }

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
    bool hashSearch(const keyT& key, valueT& value) const;
    void rehash();
    void printTable(); // for debugging
    
    // if not found, return std::nullopt 
    //std::optional<valueT> hashSearch(const keyT& key) const;
    
    //the second const declares that the function does not modify any member variables (read-only function)
    size_t hashFunction(const keyT& key) const;

    size_t getSize() const {
        return size;
    }

};



// implementation of hash table with Robin Hood hashing

using namespace std;

template <typename keyT, typename valueT>
// dynamically allocate table(vector)
RobinHoodHashTable<keyT, valueT>::RobinHoodHashTable(size_t initialCapacity)
    : capacity(initialCapacity), size(0), table(capacity) {
}

template <typename keyT, typename valueT>
size_t RobinHoodHashTable<keyT, valueT>::hashFunction(const keyT& key) const{
    return key % capacity; // ΠΡΟΧΕΙΡΟ
}


template <typename keyT, typename valueT>
bool RobinHoodHashTable<keyT, valueT>::hashInsert(const keyT& key, const valueT& value) {

    int pos = hashFunction(key);

    // these three variables are the ones we are trying
    // to insert into the hash table each time
    keyT inputKey = key;
    // carry vector of values for the current key we're placing
    std::vector<valueT> inputValues{value};
    int inputPsl = 0;

    // circular iteration over the table
    for(int i = pos; i < capacity; i = (i+1) % capacity){
        
        // if position is empty, insert in node
        if((table[i].isOccupied()) == false){

            hashNode<keyT, valueT> newNode;
            
            newNode.setKey(inputKey);
            newNode.setValuesVector(inputValues); // set entire values vector
            newNode.setOccupied(true);
            newNode.setPsl(inputPsl);

            table[i] = std::move(newNode);
            size++;
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
            hashNode<keyT, valueT>& currNode = table[i];
            
            // old key is "wealthier" so it needs to be replaced
            // by the new key which is "poorer"
            if(inputPsl > currNode.getPsl()){
                
                // store old key's info so we can move it
                keyT oldKey = currNode.getKey();
                std::vector<valueT> oldValues = currNode.getValue();
                int oldPsl = currNode.getPsl();

                // now insert new key into currNode
                currNode.setKey(inputKey);
                currNode.setValuesVector(inputValues);
                currNode.setPsl(inputPsl);

                // update input variables so the loop continues
                // searching for a position for the old key
                inputKey = oldKey;
                inputValues = oldValues;
                inputPsl = oldPsl;


                // update input variables so the loop continues
                // searching for a position for the old key
                inputKey = oldKey;
                inputValues = oldValues;
                inputPsl = oldPsl;     
                
            }
        }

        inputPsl++; // one position further from ideal position   
            
    }

    return false; // full table

}

template <typename keyT, typename valueT>
void RobinHoodHashTable<keyT, valueT>::printTable(){

    cout << "\n=== Hash Table State ===\n";
        
    for (int i = 0; i < capacity; ++i) {
           
        if (table[i].isOccupied()){

            // get vector of values
            std::vector<valueT> values = table[i].getValue();
            
            cout << "[" << i << "] key=" << table[i].getKey();
            
            // if the vector is not empty, print its contents
            if(values.size() > 0){
                cout << ", Values: ";
                for(const auto& val : values){
                    cout << val << ", ";
                }
            }

            cout << "psl=" << table[i].getPsl() << "\n";
                

        }
        else{
            cout << "[" << i << "] EMPTY\n";
        }
        
        }
}
