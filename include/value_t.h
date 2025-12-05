#pragma once
#include <cstdint>


// value_t can hold either an int32_t or a stringRef or Null
// since pages are 8192 bytes we can use 3 Most Significant Bits of offset to indicate type!!!
// if we have an integer we can store it in tableIdx + columnIdx since they are not needed in that case
struct value_t {
    uint16_t tableIdx; // which table the string belongs to
    uint16_t columnIdx; // column of the table where the string is located
    uint16_t pageIdx; // page index within the column
    uint16_t dataIdx; // index of the string within the page

    // this is used so as to take the 3 MSB bits of offset for type tagging
    static constexpr uint16_t TYPE_MASK = 0xE000;  // 111.....
    static constexpr uint16_t NULL_TAG  = 0x0000;  // 000.....
    static constexpr uint16_t INT_TAG   = 0x4000;  // 010.....
    static constexpr uint16_t STR_TAG   = 0x8000;  // 100.....
    static constexpr uint16_t LONG_STR_TAG = 0xA000; // 101.....
    static constexpr uint16_t OFFSET_MASK = 0x1FFF; // 0001 1111 1111 1111

    // constructor
    value_t() : tableIdx(NULL_TAG), columnIdx(NULL_TAG), pageIdx(NULL_TAG), dataIdx(NULL_TAG) {}

    // create a value_t representing an integer
    static value_t from_int(int32_t val) {
        value_t v;
        // uint32_t so as to avoid sign extension during bit shifts
        uint32_t uval = static_cast<uint32_t>(val);
        v.tableIdx = static_cast<uint16_t>(uval >> 16);  // high 16 bits
        v.columnIdx = static_cast<uint16_t>(uval & 0xFFFF);  // low 16 bits
        v.pageIdx = 0;
        v.dataIdx = INT_TAG;  // type tag only
        return v;
    }

    // Create value_t from string reference
    static value_t from_string_ref(uint16_t table, uint16_t col, 
                                    uint16_t page, uint16_t off) {
        value_t v;
        v.tableIdx = table;
        v.columnIdx = col;
        v.pageIdx = page;
        
        if ((off & TYPE_MASK) == LONG_STR_TAG) {
            v.dataIdx = LONG_STR_TAG | (off & OFFSET_MASK);  // especial case for long strings
        }
        else {
            v.dataIdx = STR_TAG | (off & OFFSET_MASK);  // type + 13 bits data index
        }
        
        return v;
    }

    // Type checks
    bool is_null() const { return (dataIdx & TYPE_MASK) == NULL_TAG; }
    bool is_int() const { return (dataIdx & TYPE_MASK) == INT_TAG; }
    bool is_string() const { 
        auto tag = dataIdx & TYPE_MASK;
        return tag == STR_TAG || tag == LONG_STR_TAG;
    }

    // Getters
    int32_t get_int() const {
        uint32_t high = static_cast<uint32_t>(tableIdx) << 16;
        uint32_t low = columnIdx;
        return static_cast<int32_t>(high | low);
    }

};
