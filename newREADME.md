Eleftheria Galiatsatou 1115202200025
Stefanos Tzaferis 1115202200183

Parts implemented by each member:
    - Late Materialization: both
    - Column Store: Eleftheria Galiatsatou
    - Hash table: Stefanos Tzaferis

```bash
/local-disk/sigmod25/make_links.sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev
cmake --build build -- -j $(nproc) fast

./build/fast plans.json
```

The final version of each part of the project has been tagged with git tags as follows:
-   first part - Late Materialization: v2.1.0 (branch LateMaterialization)
-   second part - Column Store: v2.2.0 (branch ColumnStore)
-   third part - Hash Table: v2.3.0 (branch main)

To run the tests:

```bash
./build/unit_tests
./build/unit_tests_unchained
./build/unit_tests_columnStore
```

[2ND-PART]


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


**Hash Table**

### Hash Table Structure

So as to implement the unchained hash table, we have created one struct Tuples 
that holds the key-value pairs, and NOT the kev-vector<value> as in the chained hash table.
Each pair key(int32_t) - value(size_t) is stored in a vector<Tuples> even if the key is the
same, the hash table will handle it internally.
Then we have created DirectoryEntry struct that has just one 64bit integer. As reffered
in the paper, the first 48 bits are used tp save pointers to the buckets (pointer to the first Tuple)
and the second 16 bits are used for the bloom filter. Also we have a static array of 2048 entries
that is being computed only once and the most of its entries (1820) have exactly 4 bits set to 1.
The hash table itself is implemented in the class UnchainedHashTable that has a vector of
DirectoryEntry so as to have fast access to each bucket. It also has a vector<Tuples> that
saves the real key-value pairs sorted by the hash prefix that we have set before (16 bits).
There is also a temporary vector that saves the data before the build phase. And finally,
there are 2 vectors for prefix counts and offsets for each prefix.


### Hash Function
As hash function we have used the hardware accelarated CRC32 _mm_crc32_u32 from <nmmintrin.h>
library.

### Build phase

During the build phase, we first compute the prefixes and the ofsets that will be needed. Then we scatter the tuples into the
buckets according to their prefixes and we update the bloom filter. 

### Search phase
In the search phase, for each key we find its bucket (DirectoryEntry) and
then we check the bloom filter. If the bloom filter indicates that the maybe is present, we have to search linearly the bucket
to find all the duplicates. If the bloom filter indicates that the key is definitely not present, we skip the bucket. Finally
we return a vector<size_t> with all the values found for the given key.


[3RD-PART]

**Index optimization**

ColumnT has now two extra fields: 
```C++
class ColumnT{
    DataType type; // type of column
    size_t size; // number of elements (+null)
    std::vector<Page*> pages;
    std::vector<size_t> pageRowOffset; // a vector holding the number of offset rows till each page

    bool copied; // if copied = true, column is not dense
    const Column* colRef; // if copied = false, colRef points to the original Column

};
```

- copied is a boolean variable that indicates whether or not 
a column is dense, meaning it is of Datatype INT32 and has no nulls.

- colRef represents the column if copied == false. In this case the dense
column is not copied and basically the ColumnT created points to the
original Column of the ColumnarTable. In this way, every information 
or data needed for the column is accessed through the Column struct
and its own Page format.

The discrimination between dense columns and those including null values
is implemented in [my_copy_scan()] function. When iterating through the
columns of the ColumnarTable to be processed, we first need to check
whether the current column is INT32 and has NULLS. This is feasible,
by iterating through the pages and comparing the first two uint16_t
numbers of each page. If they are equal, the page does not have any
NULL values at all. If this is true for all pages it means the column is dense. 
In that case, the corresponding ColumnT representing that dense column 
will be marked with copied = false and a reference to the original column 
will be created. For every other page which is not dense, the function 
proceeds with the copying of the column to a ColumnT as before.

During the join algorithm, both the build and probing phase need 
to distinguish when an ExecuteResult table has dense columns or not.
This is implemented, by checking the value of the copied boolean. In
this way, when the current column is a dense one (thus copied = false)
the value can be obtained directly by accessing the original Page format.

```C++
 key = *reinterpret_cast<const int32_t*>(page->data + 2*sizeof(uint16_t) + row*sizeof(int32_t)); 
 ```

On the other hand, columns that contain NULL values obviously hold value_t
which can be accessed by:

```C++
const value_t& val = *reinterpret_cast<const value_t*>(page->data + sizeof(uint16_t) + row*sizeof(value_t));
key = val.get_int();
                   
```

During the probe phase, to obtain the value of a certain column through 
its row index (the row whose columns we want in the final result of join) 
[getValueAtRow()] is used.
This function returns a void* so it can represent both a value_t or an 
int32 for dense columns.  

**Build Parallelization**                    


**Probe Parallelization**
The parallelization of the probe phase consists of two main structures:

```C++
std::vector<std::vector<ColumnT>> threadResults(NUMBER_OF_THREADS);
std::vector<std::vector<ColumnTInserter>> threadInserter(NUMBER_OF_THREADS);
```

The first is basically a vector with NUMBER_OF_THREADS "tables". These
tables (which are vector<ColumnT> as before) hold the local results of each
thread. Basically, during the probe phase each thread is responsible for
different pages of the column that holds the key in the probe-side table.  
While, processing those pages for each key, it finds the matching build-side
rows and stores the values of the desired columns(from output_attrs) into
the appropriate column of the calling thread. This is done by all threads
with this instruction:

```C++
threadInserters[threadId][output_col].insert()
```
The second structure holds the inserter for each column of each thread.

After processing all pages, each thread has now gathered its own 
intermediate results which need to be merged into a single vector<ColumnT>
table. The merging is being done only by one single thread, which iterates
through all local columns of each thread and directly inserts their pages
into the final results table. The logic for the top-level hash join is the 
same, with the only difference being that the results table is then
converted into a ColumnarTable format.

The ColumnStore format that we have implemented, has now a new method 
which stores directly to the vector of pages of the column the input page.
```C++ 
void insertPage(Page* page); 
```


