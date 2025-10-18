// implementation of hash table with Robin Hood hashing

// #include "robinHood.h"

// using namespace std;

// template <typename keyT, typename valueT>
// // dynamically allocate table(vector)
// RobinHoodHashTable<keyT, valueT>::RobinHoodHashTable(size_t initialCapacity)
//     : capacity(initialCapacity), size(0), table(capacity) {
// }

// template <typename keyT, typename valueT>
// size_t RobinHoodHashTable<keyT, valueT>::hashFunction(const keyT& key) const{
//     return key % capacity; // ΠΡΟΧΕΙΡΟ
// }

// template <typename keyT, typename valueT>
// bool RobinHoodHashTable<keyT, valueT>::hashInsert(const keyT& key, const valueT& value) {

//     int pos = hashFunction(key);

//     // these three variables are the ones we are trying
//     // to insert into the hash table each time
//     keyT inputKey = key;
//     valueT inputValue = value
//     int inputPsl = psl;

//     for(int i = pos; i < capacity; i = (i+1) % capacity){
        
//         // if position is empty, insert in node
//         if(table[i].isOccupied == false){

//             hashNode newNode;
//             newNode.key = inputKey;
//             newNode.value = inputValue;
//             newNode.isOccupied = true;
//             newNode.psl = inputPsl;
            
//             table[i] = newNode;
//             size++;

//             return true;
//         }
//         else{
//             hashNode& currNode = table[i];
            
//             // old key is "wealthier" so it needs to be replaced
//             // by the new key which is "poorer"
//             if(psl > currNode.psl){
                
//                 // store old key's info so we can move it
//                 keyT oldKey = currNode.key;
//                 valueT oldValue = currNode.value;
//                 int oldPsl = currNode.psl;

//                 // now insert new key into currNode
//                 currNode.key = inputKey;
//                 currNode.value = inputValue;
//                 currNode.psl = psl;

//                 // update input variables so the loop continues
//                 // searching for a position for the old node
//                 inputKey = oldKey;
//                 inputValue = oldValue;
//                 inputPsl = oldPsl;     
                
//             }
//         }

//         inputPsl++; // one position further from ideal position   
            
//     }

// }