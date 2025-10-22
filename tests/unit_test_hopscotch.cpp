#include <iostream>
#include <string>
#include "../include/Hopscotch.h"
#include <vector>
#include <cassert>

using namespace std;

void test_Hopscotch_insertion(){

    Hopscotch<int, string> table(10, 3);

    // Insertion
    assert(table.hashInsert(0, "zero") == true);
    assert(table.hashInsert(1, "one") == true);
    assert(table.hashInsert(11, "eleven") == true); // collides with key 1, should be placed in pos 2
    assert(table.hashInsert(2, "two") == true); // collides with key 11, should be placed in pos 3
    assert(table.hashInsert(3, "three") == true); // collides with key 2, should be placed in pos 4
    assert(table.hashInsert(5, "five") == true);
    assert(table.hashInsert(6, "six") == true);
    assert(table.hashInsert(16, "sixteen") == true); // collides with key 6, should be placed in pos 7
    assert(table.hashInsert(26, "twenty-six") == true); // collides with key 16, should be placed in pos 8

    // this insertion causes rehashing since there is no space within hop range
    // assert(table.hashInsert(36, "thirty-six") == true); // collides with key 26, should be placed in pos 9
    
    assert(table.hashInsert(1, "one-again") == true); // inserting duplicate key

    // table.printTable();

    assert(table.hashInsert(12, "twenty-one") == true); 

  


    table.printTable();


    // Search tests
    assert(table.hashSearch(0) == vector<string>{"zero"});
    assert((table.hashSearch(1) == vector<string>{"one", "one-again"}));
    assert(table.hashSearch(11) == vector<string>{"eleven"});
    assert(table.hashSearch(2) == vector<string>{"two"});
    // assert(table.hashSearch(3) == vector<string>{"three"});
    // assert(table.hashSearch(5) == vector<string>{"five"});
    assert(table.hashSearch(6) == vector<string>{"six"});
    assert(table.hashSearch(16) == vector<string>{"sixteen"});
    assert(table.hashSearch(26) == vector<string>{"twenty-six"});
    // assert(table.hashSearch(36) == vector<string>{"thirty-six"});
    // assert(table.hashSearch(21) == vector<string>{"twenty-one"});
    
  


    cout << "\nAll unit tests passed!\n";

}


int main(){
    test_Hopscotch_insertion();
    return 0;
}