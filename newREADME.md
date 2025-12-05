Eleftheria Galiatsatou 1115202200025
Stefanos Tzaferis 1115202200183


**Late Materialization**

Row-store tables are now of type vector<vector<value_t>> which can
hold either INT32 or VARCHAR and stores metadata for the ColumnarTable
where the final Data value is located.
Integers are stored in the first 32 bits (tableIdx + columnIdx), since
we materialize them directly. TableIdx stores the high 16 bits and 
columnIdx the low 16 bits of the number.


```C++
struct value_t {
    uint16_t tableIdx; // which table the string belongs to
    uint16_t columnIdx; // column of the table where the string is located
    uint16_t pageIdx; // page index within the column
    uint16_t dataIdx; // index of the string within the page

    // this is used so as to take the 2 MSB bits of dataIdx for type tagging
    static constexpr uint16_t TYPE_MASK = 0xE000;  // 111.....
    static constexpr uint16_t NULL_TAG  = 0x0000;  // 000.....
    static constexpr uint16_t INT_TAG   = 0x4000;  // 010.....
    static constexpr uint16_t STR_TAG   = 0x8000;  // 100.....
    static constexpr uint16_t LONG_STR_TAG = 0xA000; // 101.....
    static constexpr uint16_t OFFSET_MASK = 0x1FFF; // 0001 1111 1111 1111

};
```
-   The three MSB of dataIdx indicate the type of the value_t:
    1. NULL_TAG: 3 MSB are 000 and indicate a null value_t
    2. INT_TAG: 3 MSB are 010 and indicate an INT32
    3. STR_TAG: 3 MSB are 100 and indicate a regular string
    4. LONG_STR_TAG: 3 MSB are 101 and indicate a long string  

-   Masks:
    1. TYPE_MASK: used for isolating only the 3 MSB of dataIdx.
        dataIdx & TYPE_MASK give the type of the value_t.

    2. OFFSET_MASK: used for isolating all bits except the 3 MSB of 
        dataIdx. dataIdx & OFFSET_MASK gives the actual dataIdx
        we need to locate a string within a page.


**Column store**
```C++
class ColumnT{
    DataType type; // type of column
    size_t size; // number of elements (+null)
    std::vector<Page*> pages;
    std::vector<size_t> pageRowOffset; // a vector holding the number of offset rows till each page
};

class ColumnTInserter{
    private:
        ColumnT& column;
        int lastPageIdx = 0;
};
```

The implementation of the vector<ColumnT> data structure is following the same logic
as that of the existing ColumnarTable. The difference is the format of the page which was 
simplified and the implementation of the ColumnTInserter. ColumnT holds also a vector<size_t>
which is used to store the offset of rows for each page. For instance, pageRowOffset[0] = 0,
pageRowOffset[1] = number of rows of page0 etc. This way, we do not have to iterate through every 
page of the column to get its number of rows, meaning we have fast access to any value of the column.

```C++
value_t* getValueAt(size_t rowIdx) const
```

The above function is used to get the value_t at row rowIdx of a table.
With the help of std::upper_bound() function, we find the first element of the
pageRowOffset vector that is greater than rowIdx and we calculate the index for
its previous element. 
For instance, if we have 4 pages of: 4, 4, 4, 3 rows and we are looking for the
value_t located at row 9 we have:
    -   pageRowOffset = (0, 4, 8, 12)
    -   upper_bound(vector.begin, vector.end, 9) = 12
    -   calculation of previous page index
    -   calculation of row where the value is located within the page (9 - 8 = 1)

upper_bound performs a binary search, so this operation is O(nlogn)

### Page format
Each page is the same for both INT32 and VARCHAR values, since it holds value_t.
The first 2 bytes (uint16_t) is the number of rows for each page(including nulls).
The rest is the data, which is consequtive value_t.

**PART 3**