// implementation of hash table with Robin Hood hashing

// #include "robinHood.h"

// template <class keyT, class valueT>
// bool RobinHoodHashTable<keyT, valueT>::hashInsert(const keyT& key, const valueT& value) {
    
//     // if ((double)(size + 1) / (double)capacity > 0.7) {
//     //     rehash();
//     // }

//     size_t idx = hashFunction(key) % capacity;
//     size_t psl = 0;

//     hashNode<keyT, valueT> cur;
//     cur.key = key;
//     // initialize values vector with the incoming value
//     cur.values.clear();
//     cur.values.emplace_back(value);
//     cur.isOccupied = true;
//     cur.psl = psl;

//     while (true) {
//         if (!table[idx].isOccupied) {
//             // check if we need move's function or copy
//             table[idx] = std::move(cur);
//             ++size;
//             return true;
//         }

//         // if key already exists, append the value in the values vector
//         if (table[idx].isOccupied && table[idx].key == cur.key) {
//             table[idx].values.emplace_back(value);
//             // no size increment since it's the same key so return false ??? check it later
//             return false;
//         }


//         if (table[idx].psl < cur.psl) {
//             std::swap(cur, table[idx]);
//             // cur is now displaced
//         }

//         ++cur.psl;
//         idx = (idx + 1) % capacity;
//     }

//     // unreachable
//     return false;
// }