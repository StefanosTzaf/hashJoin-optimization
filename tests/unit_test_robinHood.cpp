#include <iostream>
#include <string>
#include "../include/robinHood.h"
#include <vector>
#include <cassert>

using namespace std;

void test_RobinHood(){
 
    RobinHoodHashTable<int, string> table(7);

    // Insertion
    bool inserted1 = table.hashInsert(10, "A");
    bool inserted2 = table.hashInsert(17, "B"); // collides with 10
    bool inserted3 = table.hashInsert(24, "C"); // collides with 10 and 17
    bool inserted4 = table.hashInsert(3, "D");
    bool inserted5 = table.hashInsert(11, "E"); 
    bool inserted6 = table.hashInsert(10, "F"); // same key as first insertion

    assert(inserted1);
    assert(inserted2);
    assert(inserted3);
    assert(inserted4);
    assert(inserted5);
    assert(inserted6);

    // size should be 5 since one key was duplicate
    assert(table.getSize() == 5);

    table.printTable();

    cout << "\nAll unit tests passed!\n";

}


int main(){
    test_RobinHood();
    return 0;
}



