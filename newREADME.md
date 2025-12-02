**PART 1**



**PART 2**
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