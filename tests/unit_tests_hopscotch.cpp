#include <iostream>
#include <string>
#include "../include/Hopscotch.h"
#include <vector>
#include <catch2/catch_test_macros.hpp>

using namespace std;

size_t testHash1(const int& key, size_t cap) {
    return key % cap;
}

TEST_CASE("Simple insertions", "[hopscotch]") {

    std:: cout << "\nTesting simple insertions, collisions and duplicate key handling ...";

    // table with capacity 10 and hop range 3
    Hopscotch<int, string> table(10, 3, testHash1);
    
    // Insertion
    REQUIRE(table.hashInsert(0, "zero") == true);
    REQUIRE(table.getKeyOfPos(0) == 0); // simple insertion at original position
    REQUIRE(table.hashSearch(0) == vector<string>{"zero"});

    REQUIRE(table.hashInsert(1, "one") == true);
    REQUIRE(table.getKeyOfPos(1) == 1); // simple insertion at original position

    REQUIRE(table.hashInsert(11, "eleven") == true); // collides with key 1, should be placed in pos 2
    REQUIRE(table.getKeyOfPos(2) == 11);

    REQUIRE(table.hashInsert(2, "two") == true); // collides with key 11, should be placed in pos 3
    REQUIRE(table.getKeyOfPos(3) == 2);

    // bitmap of pos 1 should be [1 1 0] since keys 1,11 hashed there but 2 hashes to pos 2
    REQUIRE(table.getHopInfoToVector(1) == vector<bool>({true, true, false}));

    REQUIRE(table.hashInsert(3, "three") == true); // collides with key 2, should be placed in next pos 4
    REQUIRE(table.getKeyOfPos(4) == 3);

    // bitmap of pos 2 should be [0 1 0 ] since only key 2 hashed there and was put in pos 3
    REQUIRE(table.getHopInfoToVector(2) == vector<bool>({false, true, false}));
    
    REQUIRE(table.hashInsert(5, "five") == true); // simple insertion at original position
    REQUIRE(table.getKeyOfPos(5) == 5);
    
    REQUIRE(table.hashInsert(6, "six") == true); // simple insertion at original position
    REQUIRE(table.getKeyOfPos(6) == 6);
    
    REQUIRE(table.hashInsert(16, "sixteen") == true); // collides with key 6, should be placed in pos 7
    REQUIRE(table.getKeyOfPos(7) == 16);

    // bitmap of pos 6 should be [1 1 0] since 6, 16 hashed there
    REQUIRE(table.getHopInfoToVector(6) == vector<bool>({true, true, false})); 
    
    REQUIRE(table.hashInsert(26, "twenty-six") == true); // original pos:6, next pos 7 is occupied by 16 so it is placed in pos 8
    REQUIRE(table.getKeyOfPos(8) == 26);

    // bitmap of pos 6 should be now [1 1 1] since 6,16,26 hashed there
    REQUIRE(table.getHopInfoToVector(6) == vector<bool>({true, true, true}));

    REQUIRE(table.hashInsert(1, "one-again") == true); // inserting duplicate key
    REQUIRE(table.getSize() == 9); // size does not increase for duplicate key
    REQUIRE(table.getValuesVectorOfPos(1) == vector<string>({"one", "one-again"}));
    
    // collides with key 2, should be placed in next available slot pos 9
    // however, all hop range slots 7,8,9 are occupied, so rehashing should occur
    REQUIRE(table.hashInsert(12, "twelve") == true); 


    // table.printTable();
         
}

TEST_CASE("Rehashing", "[hopscotch]") {

    std:: cout << "\nTesting rehashing when no free slots are available in hop range...";

    // table with capacity 10 and hop range 3
    Hopscotch<int, string> table(10, 3, testHash1);
    
    // Inserting same keys as previous test to fill the table
    REQUIRE(table.hashInsert(0, "zero") == true);
    REQUIRE(table.hashInsert(1, "one") == true);
    REQUIRE(table.hashInsert(11, "eleven") == true);
    REQUIRE(table.hashInsert(2, "two") == true); 
    REQUIRE(table.hashInsert(3, "three") == true); 
    REQUIRE(table.hashInsert(5, "five") == true); 
    REQUIRE(table.hashInsert(6, "six") == true); 
    REQUIRE(table.hashInsert(16, "sixteen") == true);  
    REQUIRE(table.hashInsert(26, "twenty-six") == true); 
    REQUIRE(table.hashInsert(1, "one-again") == true); // inserting duplicate key


    // this insertion causes rehashing since there is no space within hop range
    // 36 originally hashes to pos 6, but all hop range slots are occupied byt 6,16,26
    REQUIRE(table.hashInsert(36, "thirty-six") == true); // collides with key 26, should be placed in pos 9

    // check positions after rehashing
    REQUIRE(table.getKeyOfPos(0) == 0);
    REQUIRE(table.getKeyOfPos(1) == 1);
    REQUIRE(table.getKeyOfPos(11) == 11);
    REQUIRE(table.getKeyOfPos(2) == 2);
    REQUIRE(table.getKeyOfPos(3) == 3);
    REQUIRE(table.getKeyOfPos(5) == 5);
    REQUIRE(table.getKeyOfPos(6) == 6);
    REQUIRE(table.getKeyOfPos(16) == 16);
    
    // key 26 collides is originally hashed to pos 6, so it should be put in next available slot after 6 which is pos 7
    REQUIRE(table.getKeyOfPos(7) == 26);

    // bitmap of pos 6 should be [1 1 0] since 6, 26 hashed there
    REQUIRE(table.getHopInfoToVector(6) == vector<bool>({true, true, false}));

    // key 36 hashes to pos 16, next available slot after 16 is pos 17
    REQUIRE(table.getKeyOfPos(17) == 36); 

    // bitmap of pos 16 should be [1 1 0] since 16, 36 hashed there
    REQUIRE(table.getHopInfoToVector(16) == vector<bool>({true, true, false}));

    // table.printTable();
   
}

TEST_CASE("No free slots", "[hopscotch]") {

    std:: cout << "\nTesting hashing when no free slots are available in table ...";
    
    // table with capacity 10 and hop range 3
    Hopscotch<int, string> table(10, 3, testHash1);
    
    // Inserting same keys as previous test to fill the table
    REQUIRE(table.hashInsert(0, "zero") == true);
    REQUIRE(table.hashInsert(1, "one") == true);
    REQUIRE(table.hashInsert(11, "eleven") == true);
    REQUIRE(table.hashInsert(2, "two") == true); 
    REQUIRE(table.hashInsert(3, "three") == true); 
    REQUIRE(table.hashInsert(5, "five") == true); 
    REQUIRE(table.hashInsert(6, "six") == true); 
    REQUIRE(table.hashInsert(16, "sixteen") == true);  
    REQUIRE(table.hashInsert(26, "twenty-six") == true); 
    REQUIRE(table.hashInsert(1, "one-again") == true); // inserting duplicate key

    // inserting element to last free slot
    REQUIRE(table.hashInsert(9, "nine") == true);
    REQUIRE(table.getKeyOfPos(9) == 9);

    // this insertion causes rehashing since there is no free slot in the table
    REQUIRE(table.hashInsert(19, "nineteen") == true); 
    REQUIRE(table.getKeyOfPos(19) == 19); // should be placed in pos 19 after rehashing
    

    // table.printTable();

}

TEST_CASE("Swapping elements", "[hopscotch]") {

    std:: cout << "\nTesting swapping elements to bring free slot within hop range ...";
    
    // table with capacity 10 and hop range 3
    Hopscotch<int, string> table(10, 3, testHash1);
    
    // Insertion
    REQUIRE(table.hashInsert(0, "zero") == true);
    REQUIRE(table.hashInsert(1, "one") == true);
    REQUIRE(table.hashInsert(2, "two") == true); 
    REQUIRE(table.hashInsert(3, "three") == true); 
    REQUIRE(table.hashInsert(4, "four") == true); 
    REQUIRE(table.hashInsert(5, "five") == true); 
    REQUIRE(table.hashInsert(6, "six") == true); 

    // inserting 11 which hashes to pos 1, but all hop range slots are occupied
    // first free slot is pos 7, which is outside hop range of pos 1
    // so elements should be swapped to bring free slot within hop range
    REQUIRE(table.hashInsert(12, "twelve") == true);

    // initially all pos have a bitmap of [1 0 0]
    // the free pos 7 is swapped with pos 5 (the first in hop range that can move)
    // now pos 5 is free and has bitmap [0 0 1]
    // then pos 5 should be swapped with pos 3
    // now pos 3 is free and has bitmap [0 0 1]
    // since pos 3 is within hop range of pos 2, 12 should be placed there
    REQUIRE(table.getKeyOfPos(3) == 12);

    // pos 2 should have bitmap [1 1 0] since keys 2 and 12 hashed there
    REQUIRE(table.getHopInfoToVector(2) == vector<bool>({true, true, false}));

    // pos 5 should have a bitmap of [0 0 1] since it is now free and was moved from pos 7
    REQUIRE(table.getHopInfoToVector(5) == vector<bool>({false, false, true}));

    // pos 3 should have bitmap [0 0 1] since it is now free and was moved from pos 5
    REQUIRE(table.getHopInfoToVector(3) == vector<bool>({false, false, true}));

    // trying to insert 22 which hashes to pos 2, so it should be put in pos 4
    // since 2,3 are occupied
    REQUIRE(table.hashInsert(22, "twenty-two") == true);
    
    // free slot: 8, should be swapped with pos 6
    // bitmap of pos 6 should now be [0 0 1]
    REQUIRE(table.getHopInfoToVector(6) == vector<bool>({false, false, true}));

    // free slot is now: 6, should be swapped with pos 4
    // bitmap of pos 4 should now be [0 0 1]
    REQUIRE(table.getHopInfoToVector(4) == vector<bool>({false, false, true}));

    // free slot is now: 4, which is within hop range of pos 2
    // so 22 should be placed there
    REQUIRE(table.getKeyOfPos(4) == 22);

    // bitmap of pos 2 should now be [1 1 1] since 2,12,22 hashed there
    REQUIRE(table.getHopInfoToVector(2) == vector<bool>({true, true, true}));



    // table.printTable();
 
}


TEST_CASE("Swapping with free slot in middle", "[hopscotch]") {
    
    std:: cout << "\nTesting swapping elements with free slot being in the middle ...";
    
    // table with capacity 10 and hop range 4
    Hopscotch<int, string> table(10, 3, testHash1);
    
    // Insertion
    REQUIRE(table.hashInsert(0, "zero") == true);
    REQUIRE(table.hashInsert(1, "one") == true);
    REQUIRE(table.hashInsert(2, "two") == true); 
    REQUIRE(table.hashInsert(12, "twelve") == true);  // collides with key 2, should be placed in pos 3
    REQUIRE(table.hashInsert(4, "four") == true); 
    REQUIRE(table.hashInsert(6, "six") == true); 
    REQUIRE(table.hashInsert(16, "sixteen") == true);
    REQUIRE(table.hashInsert(8, "eight") == true);
    REQUIRE(table.hashInsert(9, "nine") == true);

    REQUIRE(table.hashInsert(10, "ten") == true); // original pos:0, should cause swapping to bring free slot within hop range

    // first free slot is pos 5, which is outside hop range of pos 0
    // pos 5 should be swapped with pos 4 and not pos 3 since it has no elements in its hop range that can be moved
    // now pos 4 is free and has bitmap [0 1 0]
    REQUIRE(table.getHopInfoToVector(4) == vector<bool>({false, true, false}));

    // then pos 4 should be swapped with pos 2
    // now pos 2 is free and has bitmap [0 1 1]
    REQUIRE(table.getHopInfoToVector(2) == vector<bool>({false, true, true}));

    // pos 2 is within hop range of pos 0, so 10 should be placed there
    REQUIRE(table.getKeyOfPos(2) == 10);
    
    // table.printTable();
}

TEST_CASE("Rehashing when no slot can be swapped", "[hopscotch]") {
      std:: cout << "\nTesting rehashing when no slot can be swapped ...";

    // table with capacity 10 and hop range 3
    Hopscotch<int, string> table(10, 3, testHash1);
    
    // Insertion
    REQUIRE(table.hashInsert(0, "zero") == true);
    REQUIRE(table.hashInsert(1, "one") == true);
    REQUIRE(table.hashInsert(11, "eleven") == true); // collides with key 1, should be placed in pos 2
    REQUIRE(table.hashInsert(2, "two") == true); // collides with key 11, should be placed in pos 3
    REQUIRE(table.hashInsert(3, "three") == true); // collides with key 2, should be placed in next pos 4    
    REQUIRE(table.hashInsert(5, "five") == true); // simple insertion at original position    
    REQUIRE(table.hashInsert(6, "six") == true); // simple insertion at original position    
    REQUIRE(table.hashInsert(16, "sixteen") == true); // collides with key 6, should be placed in pos 7    
    REQUIRE(table.hashInsert(26, "twenty-six") == true); // original pos:6, next pos 7 is occupied by 16 so it is placed in pos 8
    
    // collides with key 2, should be placed in next available slot pos 9
    // however, all hop range slots 7,8,9 are occupied, so rehashing should occur
    REQUIRE(table.hashInsert(12, "twelve") == true); 


    // table.printTable();
        
}

TEST_CASE("Simple wrap around insertion within hop range", "[hopscotch]") {
    std:: cout << "\nTesting simple wrap around insertion within hop range...";

    // table with capacity 10 and hop range 3
    Hopscotch<int, string> table(10, 3, testHash1);

    // Insertion
    REQUIRE(table.hashInsert(0, "zero") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(2, "two") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(12, "twelve") == true); // collides with key 2, should be placed in pos 3
    REQUIRE(table.hashInsert(4, "four") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(5, "five") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(6, "six") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(7, "seven") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(8 , "eight") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(9 , "nine") == true); // simple insertion at original position

    REQUIRE(table.hashInsert(19 , "nineteen") == true); // original pos:9, should cause wrap around swapping

    // first free slot is pos 1, which is inside hop range of pos 9 due to wrap around
    // so 19 should be placed there
    REQUIRE(table.getKeyOfPos(1) == 19);
    // bitmap of pos 9 should be [1 0 1] since keys 9 and 19 hashed there
    REQUIRE(table.getHopInfoToVector(9) == vector<bool>({true, false, true}));

    // table.printTable();
}

TEST_CASE("Wrap around insertion with swapping", "[hopscotch]") {
    std:: cout << "\nTesting wrap around insertion with swapping...\n";

    // table with capacity 10 and hop range 3
    Hopscotch<int, string> table(10, 3, testHash1);

    // Insertion
    REQUIRE(table.hashInsert(0, "zero") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(1, "one") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(2, "two") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(3, "three") == true); // simple insertion at original position

    REQUIRE(table.hashInsert(5, "five") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(6, "six") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(7, "seven") == true); // simple insertion at original position
    REQUIRE(table.hashInsert(9, "nine") == true); // simple insertion at original position
    
    REQUIRE(table.hashInsert(19, "nineteen") == true); // original pos:9, should cause wrap around swapping
    
    // first free slot is pos 4, which is outside hop range of pos 9
    // pos 4 should be swapped with pos 2 which will have a bitmap of [0 0 1]
    REQUIRE(table.getHopInfoToVector(2) == vector<bool>({false, false, true}));
    
    // now pos 2 is free and should be swapped with pos 0 which will have a bitmap of [0 0 1]
    REQUIRE(table.getHopInfoToVector(0) == vector<bool>({false, false, true}));
    
    // now pos 0 is free and is within hop range of pos 9 due to wrap around
    // so 19 should be placed there
    REQUIRE(table.getKeyOfPos(0) == 19);
    // bitmap of pos 9 should be [1 1 0] since keys 9 and 19 hashed there
    REQUIRE(table.getHopInfoToVector(9) == vector<bool>({true, true, false}));
    
    // table.printTable();
    
}

