#include "../include/cuckoo.h"
#include <iostream>
#include <cassert>

using namespace std;

// two simple hash functions for testing to
// force collisions and kicks and to be able to know where each key goes
size_t testHash1(const int& key, size_t cap) {
    return key % cap;
}

size_t testHash2(const int& key, size_t cap) {
    return (key / 2) % cap;
}

void checkPosition(const CuckooHashTable<int, string>& table, 
                   const int& key, 
                   const int& expectedTable, 
                   const size_t& expectedPos) {
    auto pos = table.findPosition(key);
    assert(pos.first == expectedTable);
    assert(pos.second == expectedPos);
}

void test1(){ 
    // Χρησιμοποιούμε capacity=16 για να μην κάνει rehash λόγω load factor
    // 5 στοιχεία σε capacity 16 => load factor = 5/(16*2) = 0.15625 < 0.5
    CuckooHashTable<int, string> cuckooTable(16, testHash1, testHash2);
    assert(cuckooTable.getSize() == 0);
    assert(cuckooTable.getCapacity() == 16);

    cuckooTable.hashInsert(1, "Greece");
    // 1 goes to position 1 of table1 (1 % 16 = 1)
    checkPosition(cuckooTable, 1, 1, 1);
    assert(cuckooTable.getSize() == 1);

    // 17 goes to position 1 of table1 (17 % 16 = 1)
    cuckooTable.hashInsert(17, "France");
    checkPosition(cuckooTable, 17, 1, 1);
    // and sends 1 goes to position 0 of table2 ((1/2) % 16 = 0)
    checkPosition(cuckooTable, 1, 2, 0);


    // 32 goes to position 0 of table1 (32 % 16 = 0)
    cuckooTable.hashInsert(32, "Uganda");
    checkPosition(cuckooTable, 32, 1, 0);

    // 48 goes to position 0 of table1 (48 % 16 = 0)
    // 32 goes to position 0 of table2 ((32/2) % 16 = 0)
    cuckooTable.hashInsert(48, "Burundi");
    checkPosition(cuckooTable, 48, 1, 0);
    checkPosition(cuckooTable, 32, 2, 0);

    cuckooTable.hashInsert(16, "Palau");
    //16 goes to position 0 of table1 (16 % 16 = 0)
    //kicks 48 to table2 position 8 ((48/2) % 16 = 8)
    //48 kicks 17 to table1 position 1 (17 % 16 = 1)
    //17 kicks 1 to table2 position 0 ((1/2) % 16 = 0)
    //1 kicks 32 to table1 position 0 (32 % 16 = 0)
    // CIRCLE DETECTED -> rehash

    assert(cuckooTable.getSize() == 5);
    assert(cuckooTable.getCapacity() == 32);
    
    // 17 and 1 and 32 are inserted without collisions after rehash
    checkPosition(cuckooTable, 17, 1, 17);
    checkPosition(cuckooTable, 1, 1, 1);
    checkPosition(cuckooTable, 32, 1, 0);

    //for the other 2 values we have to check the order they entered.
    //based to the order that they were reinserted after rehash we check : 16,48
    checkPosition(cuckooTable, 16, 2, 8); // kicked out from 48 that inserted after
    checkPosition(cuckooTable, 48, 1, 16); // 48 % 32 = 16
    // cuckooTable.displayTable();

    cout<< "Test with Collisions, Kicks and rehashing due to cycles passed successfully!" << endl;
}

void test2(){
    CuckooHashTable<int, string> cuckooTable(4, testHash1, testHash2);
    cuckooTable.hashInsert(1, "Greece");
    cuckooTable.hashInsert(2, "France");
    cuckooTable.hashInsert(3, "Uganda");
    cuckooTable.hashInsert(4, "Burundi");
    cuckooTable.hashInsert(5, "Palau");

    assert(cuckooTable.getSize() == 5);
    //rehash because of load factor. Exaclty before the 5th insertion
    assert(cuckooTable.getCapacity() == 8);

    cuckooTable.hashInsert(6, "Norway");
    cuckooTable.hashInsert(7, "Sweden");
    cuckooTable.hashInsert(8, "Denmark");
    cuckooTable.hashInsert(9, "Finland");

    assert(cuckooTable.getSize() == 9);
    //rehash because of load factor. Exaclty before the 9th insertion
    assert(cuckooTable.getCapacity() == 16);

    //check the entries, must have gone all in the first table
    for(int i=1; i<=9; i++){
        checkPosition(cuckooTable, i, 1, i);
    }
    
    // cuckooTable.displayTable();
    cout << "Test with Rehashing due to Load Factor passed successfully!" << endl;
}


int main(){
    test1();
    test2();
    return 0;
}