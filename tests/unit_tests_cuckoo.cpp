#include "../include/cuckoo.h"
#include <iostream>
#include <catch2/catch_test_macros.hpp>


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
    REQUIRE(pos.first == expectedTable);
    REQUIRE(pos.second == expectedPos);
}


// Testing insertions with collisions, kicks and rehashing due to cycles
TEST_CASE("Test 1", "[cuckoo]") {

    CuckooHashTable<int, string> cuckooTable(16, testHash1, testHash2);
    REQUIRE(cuckooTable.getSize() == 0);
    REQUIRE(cuckooTable.getCapacity() == 16);

    cuckooTable.hashInsert(1, "Greece");
    // 1 goes to position 1 of table1 (1 % 16 = 1)
    checkPosition(cuckooTable, 1, 1, 1);
    REQUIRE(cuckooTable.getSize() == 1);

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
    //32 kicks 16 to table2 position 8 ((16/2) % 16 = 8) 5th kick and since size is still 4(5th not added yet)
    // CIRCLE DETECTED -> rehash
    REQUIRE(cuckooTable.getSize() == 5);
    REQUIRE(cuckooTable.getCapacity() == 32);
    
    // 17 and 1 and 32 are inserted without collisions after rehash
    checkPosition(cuckooTable, 17, 1, 17);
    checkPosition(cuckooTable, 1, 1, 1);
    checkPosition(cuckooTable, 32, 1, 0);
    
    //for the other 2 values we have to check the order they entered.
    //based to the order that they were reinserted after rehash we check : 48,16
    
    //kicked by 16 in rehasing
    checkPosition(cuckooTable, 48, 2, 24); // 48 / 2 = 24
    checkPosition(cuckooTable, 16, 1, 16);
    
    cout<< "Test with Collisions, Kicks and rehashing due to cycles passed successfully!" << endl;
}

// Testing rehashing due to load factor
TEST_CASE("Test 2", "[cuckoo]") {
    CuckooHashTable<int, string> cuckooTable(4, testHash1, testHash2);
    cuckooTable.hashInsert(1, "Greece");
    cuckooTable.hashInsert(2, "France");
    cuckooTable.hashInsert(3, "Uganda");
    cuckooTable.hashInsert(4, "Burundi");
    cuckooTable.hashInsert(5, "Palau");

    REQUIRE(cuckooTable.getSize() == 5);
    //rehash because of load factor. Exaclty before the 5th insertion
    REQUIRE(cuckooTable.getCapacity() == 8);

    cuckooTable.hashInsert(6, "Norway");
    cuckooTable.hashInsert(7, "Sweden");
    cuckooTable.hashInsert(8, "Denmark");
    cuckooTable.hashInsert(9, "Finland");

    REQUIRE(cuckooTable.getSize() == 9);
    //rehash because of load factor. Exaclty before the 9th insertion
    REQUIRE(cuckooTable.getCapacity() == 16);

    //check the entries, must have gone all in the first table
    for(int i=1; i<=9; i++){
        checkPosition(cuckooTable, i, 1, i);
    }
    
    // cuckooTable.displayTable();
    cout << "Test with Rehashing due to Load Factor passed successfully!" << endl;
}

// Testing with Default Hash Functions
TEST_CASE("Test 3", "[cuckoo]") {
    CuckooHashTable<int, string> cuckooTable(4);
    REQUIRE(cuckooTable.getCapacity() == 4);
    REQUIRE(cuckooTable.getSize() == 0);
    cuckooTable.hashInsert(1, "Greece");
    cuckooTable.hashInsert(2, "France");
    cuckooTable.hashInsert(3, "Uganda");
    cuckooTable.hashInsert(4, "Burundi");
    cuckooTable.hashInsert(5, "Palau");
    cuckooTable.hashInsert(6, "Norway");
    cuckooTable.hashInsert(7, "Sweden");
    cuckooTable.hashInsert(8, "Denmark");
    cuckooTable.hashInsert(9, "Finland");

    REQUIRE(cuckooTable.getSize() == 9);
    // cannot be less than 16 because of rehashing due to load factor
    REQUIRE(cuckooTable.getCapacity() >= 16);

    cout << "Test with Default Hash Functions passed successfully!" << endl;
}

// Testing with string keys, search function and duplicate values
TEST_CASE("Test 4", "[cuckoo]") {
    vector<int> myVec;
    CuckooHashTable<string, int> cuckooTable(4);
    cuckooTable.hashInsert("apple", 1);
    cuckooTable.hashInsert("banana", 2);
    cuckooTable.hashInsert("orange", 3);
    cuckooTable.hashInsert("grape", 4);
    cuckooTable.hashInsert("melon", 5);
    
    REQUIRE(cuckooTable.getSize() == 5);
    REQUIRE(cuckooTable.getCapacity() >= 8);
    //search tests
    myVec = cuckooTable.hashSearch("apple");
    REQUIRE(myVec[0] == 1);

    myVec = cuckooTable.hashSearch("banana");
    REQUIRE(myVec[0] == 2);
    myVec = cuckooTable.hashSearch("orange");
    REQUIRE(myVec[0] == 3);
    myVec = cuckooTable.hashSearch("grape");
    REQUIRE(myVec[0] == 4);
    myVec = cuckooTable.hashSearch("melon");
    REQUIRE(myVec[0] == 5);
    myVec = cuckooTable.hashSearch("kiwi");
    REQUIRE(myVec.size() == 0); // not found

    cuckooTable.hashInsert("apple", 32);
    cuckooTable.hashInsert("apple", 99);
    myVec = cuckooTable.hashSearch("apple");
    REQUIRE(myVec.size() == 3);
    REQUIRE(myVec[0] == 1);
    REQUIRE(myVec[1] == 32);
    REQUIRE(myVec[2] == 99);

    //size of hash table must remain same after duplicate inserts
    // REQUIRE(cuckooTable.getSize() == 5);

    cout << "Test with String Keys and Search Function passed successfully!" << endl;
    
}
