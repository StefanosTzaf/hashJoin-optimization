#include <iostream>
#include <string>
#include "../include/robinHood.h"
#include <vector>
#include <cassert>

using namespace std;

void checkPosition(const RobinHoodHashTable<int, string>& table, 
                   const int& key, 
                   const size_t& expectedPos) {
    auto pos = table.findPosition(key);
    assert(pos == expectedPos);
}

// two simple hash functions for testing to
// force collisions and kicks and to be able to know where each key goes
size_t testHash1(const int& key, size_t cap) {
    return key % cap;
}

void test_RobinHood_insertion(){
 
    RobinHoodHashTable<int, string> table(7, testHash1);
    assert(table.getSize() == 0);
    assert(table.getCapacity() == 8); // next power of 2

    // Insertion
    assert(table.hashInsert(10, "A"));
    assert(table.getSize() == 1);
    // 10 goes to position 2
    checkPosition(table, 10, 2);
    
    assert(table.hashInsert(18, "B"));
    // 18 collides with 10, goes to position 3 --they have the same psl
    checkPosition(table, 18, 3);
    
    // collides with 10 and 18, goes to position 4
    assert(table.hashInsert(26, "C"));
    checkPosition(table, 26, 4);

    assert(table.hashInsert(1, "D"));
    // 1 goes to position 1
    checkPosition(table, 1, 1);

    assert(table.hashInsert(9, "E"));
    // 9 collides with 1 they have the same psl so continues
    // then colliddes with 10 that has psl 0 and 9 has already 1 so it kicks 10
    checkPosition(table, 9, 2);
    // 10 gets kicked, now has psl 1, collides with 18 that has psl 1 so continues
    // collides with 26 that has psl 2 so continues
    // finds empty position at 5
    checkPosition(table, 10, 5);

    assert(table.getPslOfKey(10) == 3);
    assert(table.getPslOfKey(9) == 1);
    assert(table.getPslOfKey(1) == 0);
    assert(table.getPslOfKey(18) == 1);
    assert(table.getPslOfKey(26) == 2);


    assert(table.getSize() == 5);
    assert(table.getCapacity() == 8);


    assert(table.hashInsert(6, "G"));
    //no collisions
    checkPosition(table, 6, 6);
    assert(table.getPslOfKey(6) == 0);
    assert(table.getSize() == 6);
    assert(table.getCapacity() == 8);

    assert(table.hashInsert(7, "maou"));

    // rehash must have happened
    assert(table.getSize() == 7);
    assert(table.getCapacity() == 16);

    // must be inserted in the vecotor -- size does not change
    assert(table.hashInsert(1, "DD"));
    assert(table.hashInsert(7, "MM"));

    assert(table.getSize() == 7);

    // Search tests (after rehash to be sure that everything is still ok)
    vector<string> values1 = table.hashSearch(10);
    assert(values1.size() == 1);
    assert(values1[0] == "A");

    vector<string> values2 = table.hashSearch(1);
    assert(values2.size() == 2);
    assert(values2[0] == "D");
    assert(values2[1] == "DD");

    vector<string> values4 = table.hashSearch(7);
    assert(values4.size() == 2);
    assert(values4[0] == "maou");
    assert(values4[1] == "MM");


    vector<string> values3 = table.hashSearch(100);
    assert(values3.size() == 0); // key not found
    
    cout << "\nAll unit tests passed!\n";

}


int main(){
    test_RobinHood_insertion();
    return 0;
}



