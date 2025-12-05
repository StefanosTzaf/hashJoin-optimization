#include "catch2/catch_test_macros.hpp"
#include "Unchained.h"
#include <iostream>


uint32_t crc32(uint32_t data) {
    uint32_t crc = 0xFFFFFFFF;
    // for each byte
    for (int i = 0; i < 4; i++) {
        uint8_t byte = (data >> (i*8)) & 0xFF;
        crc ^= byte;
        for (int j = 0; j < 8; j++) {
            if (crc & 1)
                crc = (crc >> 1) ^ 0xEDB88320;
            else
                crc = crc >> 1;
        }
    }
    return crc ^ 0xFFFFFFFF;
}


TEST_CASE("Bloom Filter Initialization", "[bloom]") {
    DirectoryEntry::initBloomLookup();
    
    // All first 1820 entries must have exactly 4 bits set
    for(size_t i = 0; i < 1820; i++) {
        uint16_t mask = DirectoryEntry::bloom_lookup[i];
        int bits_set = __builtin_popcount(mask);
        REQUIRE(bits_set == 4);
    }
}

TEST_CASE("Bloom Filter Correctness", "[bloom]") {
    DirectoryEntry::initBloomLookup();
    
    DirectoryEntry entry;
    entry.data = 0;
    
    std::vector<int32_t> keys = {1001, 5234, 98765, 123456, 777888};
    
    // Insert keys into bloom filter
    for (int32_t key : keys) {
        uint32_t hash = crc32(key);
        uint16_t tag = (hash >> 17) & 0x7FF;
        uint16_t mask = DirectoryEntry::bloom_lookup[tag];
        entry.data |= mask;
    }
    
    uint16_t bloom = entry.getBloomFilter();
    
    // All inserted keys should be found (true positives)
    for (int32_t key : keys) {
        uint32_t hash = crc32(key);
        uint16_t tag = (hash >> 17) & 0x7FF;
        uint16_t mask = DirectoryEntry::bloom_lookup[tag];
        bool match = (bloom & mask) == mask;
        REQUIRE(match == true);
    }
    
    // Test false positive
    int false_positives = 0;
    for (int32_t key = 10000; key < 11000; key++) {
        uint32_t hash = crc32(key);
        uint16_t tag = (hash >> 17) & 0x7FF;
        uint16_t mask = DirectoryEntry::bloom_lookup[tag];
        
        if ((bloom & mask) == mask) {
            false_positives++;
        }
    }
    
    std::cout << "False positives % in range 10000-10999: " << (false_positives / 1000.0) << std::endl;
}

TEST_CASE("Insert and Build", "[build]") {
    UnchainedHashTable hash_table;
    
    // Insert 1000 keys
    for (int i = 0; i < 1000; i++) {
        hash_table.insert(i, i);
    }
    hash_table.build();
    
    // Check that all keys are found
    for(int i = 0; i < 1000; i++) {
        std::vector<size_t> results = hash_table.search(i);
        REQUIRE(results.size() == 1);
        REQUIRE(results[0] == (size_t)i);
    }

    for(int j = 1111; j < 1211; j++) {
        std::vector<size_t> results = hash_table.search(j);
        REQUIRE(results.size() == 0);
    }
}

TEST_CASE("Duplicate Keys", "[duplicates]") {
    UnchainedHashTable hash_table;
    
    // Insert same key multiple times
    hash_table.insert(42, 100);
    hash_table.insert(42, 200);
    hash_table.insert(42, 300);
    hash_table.insert(99, 999);
    
    hash_table.build();
    
    // Key 42 should have 3 results
    std::vector<size_t> results = hash_table.search(42);
    REQUIRE(results.size() == 3);
    
    // Check all row_ids are present
    std::vector<size_t> expected = {100, 200, 300};
    for (size_t exp : expected) {
        bool found = false;
        for (size_t res : results) {
            if (res == exp) {
                found = true;
                break;
            }
        }
        REQUIRE(found);
    }
    
    // Key 99 should have 1 result
    std::vector<size_t> results_99 = hash_table.search(99);
    REQUIRE(results_99.size() == 1);
    REQUIRE(results_99[0] == 999);
}

TEST_CASE("Empty Buckets", "[search]") {
    UnchainedHashTable hash_table;
    
    // Insert only a few keys
    hash_table.insert(10, 100);
    hash_table.insert(20, 200);
    
    hash_table.build();
    
    // Search for non-existent keys (most buckets will be empty)
    for (int i = 19000; i < 19100; i++) {
        std::vector<size_t> results = hash_table.search(i);
        REQUIRE(results.size() == 0);
    }
}
