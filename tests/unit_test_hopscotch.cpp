#include <iostream>
#include <string>
#include "../include/Hopscotch.h"
#include <vector>
#include <cassert>

using namespace std;

size_t testHash1(const int& key, size_t cap) {
    return key % cap;
}

void test_simple_insertion(){

    std:: cout << "\nTesting simple insertions, collisions and duplicate key handling ...\n";

    // table with capacity 10 and hop range 3
    Hopscotch<int, string> table(10, 3, testHash1);
    
    // Insertion
    assert(table.hashInsert(0, "zero") == true);
    assert(table.getKeyOfPos(0) == 0); // simple insertion at original position
    assert(table.hashSearch(0) == vector<string>{"zero"});

    assert(table.hashInsert(1, "one") == true);
    assert(table.getKeyOfPos(1) == 1); // simple insertion at original position

    assert(table.hashInsert(11, "eleven") == true); // collides with key 1, should be placed in pos 2
    assert(table.getKeyOfPos(2) == 11);

    assert(table.hashInsert(2, "two") == true); // collides with key 11, should be placed in pos 3
    assert(table.getKeyOfPos(3) == 2);

    // bitmap of pos 1 should be [1 1 0] since keys 1,11 hashed there but 2 hashes to pos 2
    assert(table.getHopInfoBitmap(1) == vector<bool>({true, true, false}));

    assert(table.hashInsert(3, "three") == true); // collides with key 2, should be placed in next pos 4
    assert(table.getKeyOfPos(4) == 3);

    // bitmap of pos 2 should be [0 1 0 ] since only key 2 hashed there and was put in pos 3
    assert(table.getHopInfoBitmap(2) == vector<bool>({false, true, false}));
    
    assert(table.hashInsert(5, "five") == true); // simple insertion at original position
    assert(table.getKeyOfPos(5) == 5);
    
    assert(table.hashInsert(6, "six") == true); // simple insertion at original position
    assert(table.getKeyOfPos(6) == 6);
    
    assert(table.hashInsert(16, "sixteen") == true); // collides with key 6, should be placed in pos 7
    assert(table.getKeyOfPos(7) == 16);

    // bitmap of pos 6 should be [1 1 0] since 6, 16 hashed there
    assert(table.getHopInfoBitmap(6) == vector<bool>({true, true, false})); 
    
    assert(table.hashInsert(26, "twenty-six") == true); // original pos:6, next pos 7 is occupied by 16 so it is placed in pos 8
    assert(table.getKeyOfPos(8) == 26);

    // bitmap of pos 6 should be now [1 1 1] since 6,16,26 hashed there
    assert(table.getHopInfoBitmap(6) == vector<bool>({true, true, true}));

    assert(table.hashInsert(1, "one-again") == true); // inserting duplicate key
    assert(table.getSize() == 9); // size does not increase for duplicate key
    assert(table.getValuesVectorOfPos(1) == vector<string>({"one", "one-again"}));
    
    // collides with key 2, should be placed in next available slot pos 9
    // however, all hop range slots 7,8,9 are occupied, so rehashing should occur
    assert(table.hashInsert(12, "twelve") == true); 


    // table.printTable();
         
}

void test_rehashing(){

    std:: cout << "\nTesting rehashing when no free slots are available in hop range:\n";

    // table with capacity 10 and hop range 3
    Hopscotch<int, string> table(10, 3, testHash1);
    
    // Inserting same keys as previous test to fill the table
    assert(table.hashInsert(0, "zero") == true);
    assert(table.hashInsert(1, "one") == true);
    assert(table.hashInsert(11, "eleven") == true);
    assert(table.hashInsert(2, "two") == true); 
    assert(table.hashInsert(3, "three") == true); 
    assert(table.hashInsert(5, "five") == true); 
    assert(table.hashInsert(6, "six") == true); 
    assert(table.hashInsert(16, "sixteen") == true);  
    assert(table.hashInsert(26, "twenty-six") == true); 
    assert(table.hashInsert(1, "one-again") == true); // inserting duplicate key


    // this insertion causes rehashing since there is no space within hop range
    // 36 originally hashes to pos 6, but all hop range slots are occupied byt 6,16,26
    assert(table.hashInsert(36, "thirty-six") == true); // collides with key 26, should be placed in pos 9

    // check positions after rehashing
    assert(table.getKeyOfPos(0) == 0);
    assert(table.getKeyOfPos(1) == 1);
    assert(table.getKeyOfPos(11) == 11);
    assert(table.getKeyOfPos(2) == 2);
    assert(table.getKeyOfPos(3) == 3);
    assert(table.getKeyOfPos(5) == 5);
    assert(table.getKeyOfPos(6) == 6);
    assert(table.getKeyOfPos(16) == 16);
    
    // key 26 collides is originally hashed to pos 6, so it should be put in next available slot after 6 which is pos 7
    assert(table.getKeyOfPos(7) == 26);

    // bitmap of pos 6 should be [1 1 0] since 6, 26 hashed there
    assert(table.getHopInfoBitmap(6) == vector<bool>({true, true, false}));

    // key 36 hashes to pos 16, next available slot after 16 is pos 17
    assert(table.getKeyOfPos(17) == 36); 

    // bitmap of pos 16 should be [1 1 0] since 16, 36 hashed there
    assert(table.getHopInfoBitmap(16) == vector<bool>({true, true, false}));

    // table.printTable();
   
}

void test_no_free_slots(){

    std:: cout << "\nTesting hashing when no free slots are available in table ...\n";
    
    // table with capacity 10 and hop range 3
    Hopscotch<int, string> table(10, 3, testHash1);
    
    // Inserting same keys as previous test to fill the table
    assert(table.hashInsert(0, "zero") == true);
    assert(table.hashInsert(1, "one") == true);
    assert(table.hashInsert(11, "eleven") == true);
    assert(table.hashInsert(2, "two") == true); 
    assert(table.hashInsert(3, "three") == true); 
    assert(table.hashInsert(5, "five") == true); 
    assert(table.hashInsert(6, "six") == true); 
    assert(table.hashInsert(16, "sixteen") == true);  
    assert(table.hashInsert(26, "twenty-six") == true); 
    assert(table.hashInsert(1, "one-again") == true); // inserting duplicate key

    // inserting element to last free slot
    assert(table.hashInsert(9, "nine") == true);
    assert(table.getKeyOfPos(9) == 9);

    // this insertion causes rehashing since there is no free slot in the table
    assert(table.hashInsert(19, "nineteen") == true); 
    assert(table.getKeyOfPos(19) == 19); // should be placed in pos 19 after rehashing
    

    // table.printTable();

}

void test_swapping_elements(){

    std:: cout << "\nTesting swapping elements to bring free slot within hop range ...\n";
    
    // table with capacity 10 and hop range 3
    Hopscotch<int, string> table(10, 3, testHash1);
    
    // Insertion
    assert(table.hashInsert(0, "zero") == true);
    assert(table.hashInsert(1, "one") == true);
    assert(table.hashInsert(2, "two") == true); 
    assert(table.hashInsert(3, "three") == true); 
    assert(table.hashInsert(4, "four") == true); 
    assert(table.hashInsert(5, "five") == true); 
    assert(table.hashInsert(6, "six") == true); 

    // inserting 11 which hashes to pos 1, but all hop range slots are occupied
    // first free slot is pos 7, which is outside hop range of pos 1
    // so elements should be swapped to bring free slot within hop range
    assert(table.hashInsert(12, "twelve") == true);

    // initially all pos have a bitmap of [1 0 0]
    // the free pos 7 is swapped with pos 5 (the first in hop range that can move)
    // now pos 5 is free and has bitmap [0 0 1]
    // then pos 5 should be swapped with pos 3
    // now pos 3 is free and has bitmap [0 0 1]
    // since pos 3 is within hop range of pos 2, 12 should be placed there
    assert(table.getKeyOfPos(3) == 12);

    // pos 2 should have bitmap [1 1 0] since keys 2 and 12 hashed there
    assert(table.getHopInfoBitmap(2) == vector<bool>({true, true, false}));

    // pos 5 should have a bitmap of [0 0 1] since it is now free and was moved from pos 7
    assert(table.getHopInfoBitmap(5) == vector<bool>({false, false, true}));

    // pos 3 should have bitmap [0 0 1] since it is now free and was moved from pos 5
    assert(table.getHopInfoBitmap(3) == vector<bool>({false, false, true}));

    // trying to insert 22 which hashes to pos 2, so it should be put in pos 4
    // since 2,3 are occupied
    assert(table.hashInsert(22, "twenty-two") == true);
    
    // free slot: 8, should be swapped with pos 6
    // bitmap of pos 6 should now be [0 0 1]
    assert(table.getHopInfoBitmap(6) == vector<bool>({false, false, true}));

    // free slot is now: 6, should be swapped with pos 4
    // bitmap of pos 4 should now be [0 0 1]
    assert(table.getHopInfoBitmap(4) == vector<bool>({false, false, true}));

    // free slot is now: 4, which is within hop range of pos 2
    // so 22 should be placed there
    assert(table.getKeyOfPos(4) == 22);

    // bitmap of pos 2 should now be [1 1 1] since 2,12,22 hashed there
    assert(table.getHopInfoBitmap(2) == vector<bool>({true, true, true}));



    // table.printTable();
 
}


void test_swapping_elements_2(){
    
    std:: cout << "\nTesting swapping elements with free slot being in the middle ...\n";
    
    // table with capacity 10 and hop range 4
    Hopscotch<int, string> table(10, 3, testHash1);
    
    // Insertion
    assert(table.hashInsert(0, "zero") == true);
    assert(table.hashInsert(1, "one") == true);
    assert(table.hashInsert(2, "two") == true); 
    assert(table.hashInsert(12, "twelve") == true);  // collides with key 2, should be placed in pos 3
    assert(table.hashInsert(4, "four") == true); 
    assert(table.hashInsert(6, "six") == true); 
    assert(table.hashInsert(16, "sixteen") == true);
    assert(table.hashInsert(8, "eight") == true);
    assert(table.hashInsert(9, "nine") == true);

    assert(table.hashInsert(10, "ten") == true); // original pos:0, should cause swapping to bring free slot within hop range

    // first free slot is pos 5, which is outside hop range of pos 0
    // pos 5 should be swapped with pos 4 and not pos 3 since it has no elements in its hop range that can be moved
    // now pos 4 is free and has bitmap [0 1 0]
    assert(table.getHopInfoBitmap(4) == vector<bool>({false, true, false}));

    // then pos 4 should be swapped with pos 2
    // now pos 2 is free and has bitmap [0 1 1]
    assert(table.getHopInfoBitmap(2) == vector<bool>({false, true, true}));

    // pos 2 is within hop range of pos 0, so 10 should be placed there
    assert(table.getKeyOfPos(2) == 10);
    
    // table.printTable();
}

void test_swapping_with_rehashing(){
      std:: cout << "\nTesting rehashing when no slot can be swapped ...\n";

    // table with capacity 10 and hop range 3
    Hopscotch<int, string> table(10, 3, testHash1);
    
    // Insertion
    assert(table.hashInsert(0, "zero") == true);
    assert(table.hashInsert(1, "one") == true);
    assert(table.hashInsert(11, "eleven") == true); // collides with key 1, should be placed in pos 2
    assert(table.hashInsert(2, "two") == true); // collides with key 11, should be placed in pos 3
    assert(table.hashInsert(3, "three") == true); // collides with key 2, should be placed in next pos 4    
    assert(table.hashInsert(5, "five") == true); // simple insertion at original position    
    assert(table.hashInsert(6, "six") == true); // simple insertion at original position    
    assert(table.hashInsert(16, "sixteen") == true); // collides with key 6, should be placed in pos 7    
    assert(table.hashInsert(26, "twenty-six") == true); // original pos:6, next pos 7 is occupied by 16 so it is placed in pos 8
    
    // collides with key 2, should be placed in next available slot pos 9
    // however, all hop range slots 7,8,9 are occupied, so rehashing should occur
    assert(table.hashInsert(12, "twelve") == true); 


    // table.printTable();
        
}

int main(){
    test_simple_insertion();
    test_rehashing();
    test_no_free_slots();
    test_swapping_elements();
    test_swapping_elements_2();
    test_swapping_with_rehashing();

    cout << "\nAll unit tests passed!\n";
    return 0;
}