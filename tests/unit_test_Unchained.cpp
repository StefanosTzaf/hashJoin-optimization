#include "Unchained.h"
#include <catch2/catch_test_macros.hpp>

TEST_CASE("Test1", "creation"){
    UnchainedHashTable hashTable;
    REQUIRE(true);

    hashTable.insert(1, 100);
    hashTable.insert(2, 200);
    hashTable.insert(3, 300);
    hashTable.build();

    auto results = hashTable.search(2);
    REQUIRE(results.size() == 1);
    REQUIRE(results[0] == 200);
}