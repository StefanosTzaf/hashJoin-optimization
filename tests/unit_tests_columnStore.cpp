#include <iostream>
#include <string>
#include "ColumnStore.h"
#include "LateMaterialization.h"
#include "value_t.h"
#include <vector>
#include <catch2/catch_test_macros.hpp>


using namespace std;

TEST_CASE("Simple INT32 column", "[ColumnStore]"){

    ColumnT col(DataType::INT32);
    ColumnTInserter inserter(col);

    REQUIRE(col.getNumPages() == 0);
    REQUIRE(inserter.getLastPageIdx() == 0);
    REQUIRE(col.getType() == DataType::INT32);
    REQUIRE(col.getSize() == 0);

    value_t val1;
    value_t val2;
    value_t val3;
    value_t val4;

    val1 = value_t::from_int(1);
    val2 = value_t::from_int(2);
    val3 = value_t::from_int(3);
    val4 = value_t::from_int(4);

    inserter.insert(val1);
    inserter.insert(val2);
    inserter.insert(val3);
    inserter.insert(val4);

    REQUIRE(col.getNumPages() == 1);
    REQUIRE(col.getSize() == 4);
    REQUIRE(col.getValueAt(0)->get_int() == 1);
    REQUIRE(col.getValueAt(1)->get_int() == 2);
    REQUIRE(col.getValueAt(2)->get_int() == 3);
    REQUIRE(col.getValueAt(3)->get_int() == 4);

    size_t dataStart = inserter.dataBegin(col.getPage(0));
    REQUIRE(dataStart == sizeof(uint16_t) + 4*sizeof(value_t));
    


}

TEST_CASE("INT32 column with multiple pages", "[ColumnStore]"){
  
    ColumnT col(DataType::INT32);
    ColumnTInserter inserter(col);

    size_t maxValues = (PAGE_SIZE - sizeof(uint16_t))/sizeof(value_t);

    for(size_t i = 0; i < maxValues; i++){
        
        value_t val = value_t::from_int(static_cast<int32_t>(i));
        inserter.insert(val);
    }

    REQUIRE(inserter.isFull(col.getPage(0)) == true);
    REQUIRE(col.getNumPages() == 1);
    REQUIRE(col.getSize() == maxValues);

    // insert one more value to trigger new page creation
    value_t val = value_t::from_int(maxValues);
    inserter.insert(val);

    REQUIRE(col.getNumPages() == 2);
    REQUIRE(col.getSize() == maxValues + 1);
    REQUIRE(inserter.getLastPageIdx() == 1);

    Page* firstPage = col.getPage(0);
    Page* secondPage = col.getPage(1);

    int numValuesFirstPage = *reinterpret_cast<uint16_t*>(firstPage->data);
    REQUIRE(numValuesFirstPage == maxValues);
    REQUIRE(col.getValueAt(maxValues-2)->get_int() == (maxValues-2));

    int numValuesSecondPage = *reinterpret_cast<uint16_t*>(secondPage->data);
    REQUIRE(numValuesSecondPage == 1);
    REQUIRE(col.getValueAt(maxValues - 1)->get_int() == (maxValues-1));

}


TEST_CASE("Simple string column", "[ColumnStore]"){

    ColumnT col(DataType::VARCHAR);
    ColumnTInserter inserter(col);

    REQUIRE(col.getNumPages() == 0);
    REQUIRE(inserter.getLastPageIdx() == 0);
    REQUIRE(col.getType() == DataType::VARCHAR);
    REQUIRE(col.getSize() == 0);

    value_t val1;
    value_t val2;
    value_t val3;
    value_t val4;

    val1 = value_t::from_string_ref(0, 0, 0, 0);
    val2 = value_t::from_string_ref(0, 0, 0, 1);
    val3 = value_t::from_string_ref(0, 0, 0, 2);
    val4 = value_t::from_string_ref(0, 0, 0, 3);

    inserter.insert(val1);
    inserter.insert(val2);
    inserter.insert(val3);
    inserter.insert(val4);

    REQUIRE(col.getNumPages() == 1);
    REQUIRE(col.getSize() == 4);


}


TEST_CASE("STRING column with multiple pages", "[ColumnStore]"){
  
    ColumnT col(DataType::VARCHAR);
    ColumnTInserter inserter(col);

    size_t maxValues = (PAGE_SIZE - sizeof(uint16_t))/sizeof(value_t);

    for(size_t i = 0; i < maxValues; i++){
        
        value_t val = value_t::from_string_ref(0, 0, 0, i);
        inserter.insert(val);
    }

    REQUIRE(inserter.isFull(col.getPage(0)) == true);
    REQUIRE(col.getNumPages() == 1);
    REQUIRE(col.getSize() == maxValues);

    // insert one more value to trigger new page creation
    value_t val = value_t::from_string_ref(0, 0, 0, 0);
    inserter.insert(val);

    REQUIRE(col.getNumPages() == 2);
    REQUIRE(col.getSize() == maxValues + 1);
    REQUIRE(inserter.getLastPageIdx() == 1);

    Page* firstPage = col.getPage(0);
    Page* secondPage = col.getPage(1);

    int numValuesFirstPage = *reinterpret_cast<uint16_t*>(firstPage->data);
    REQUIRE(numValuesFirstPage == maxValues);

    int numValuesSecondPage = *reinterpret_cast<uint16_t*>(secondPage->data);
    REQUIRE(numValuesSecondPage == 1);


}


TEST_CASE("Simple null insertion", "[ColumnStore]"){

    ColumnT col(DataType::INT32);
    ColumnTInserter inserter(col);

    REQUIRE(col.getNumPages() == 0);
    REQUIRE(inserter.getLastPageIdx() == 0);
    REQUIRE(col.getType() == DataType::INT32);
    REQUIRE(col.getSize() == 0);

    value_t val1;
    value_t val2;
    value_t val3;
    value_t val4;

    val1 = value_t::from_int(0);
    val2 = value_t::from_int(1);
    // val3 is null
    val4 = value_t::from_int(3);

    inserter.insert(val1);
    inserter.insert(val2);
    inserter.insert(val3);
    inserter.insert(val4);

    REQUIRE(col.getNumPages() == 1);
    REQUIRE(col.getSize() == 4);
    
    Page* page = col.getPage(0);
    int numValues = *reinterpret_cast<uint16_t*>(page->data);
    
    value_t valCheck = *reinterpret_cast<value_t*>(page->data + sizeof(uint16_t) + 2*sizeof(value_t));;
    REQUIRE(valCheck.is_null() == true);


}

TEST_CASE("Creation of ColumnT from Column", "[ColumnStore]"){

    Column column(DataType::INT32);
    ColumnInserter<int32_t> inserter(column);
    
    REQUIRE(column.pages.size() == 0);
    
    inserter.insert(10);
    inserter.insert(20);
    inserter.insert(30);
    inserter.finalize();
    REQUIRE(column.pages.size() == 1);

    ColumnT col_t(std::move(column));
    

    REQUIRE(col_t.getType() == DataType::INT32);
    REQUIRE(col_t.getNumPages() == 1);
    REQUIRE(col_t.getSize() == 3);
}

TEST_CASE("Creation of columnT from Column with multiple pages", "[Columnstore]"){

    Column column(DataType::INT32);
    ColumnInserter<int32_t> inserter(column);

    size_t maxValues = 2000;

    for(size_t i = 0; i < maxValues; i++){
        
        inserter.insert(i);
    }

    inserter.finalize();

    ColumnT col_t(std::move(column));

    REQUIRE(col_t.getType() == DataType::INT32);
    REQUIRE(col_t.getNumPages() > 1);
    REQUIRE(col_t.getSize() == maxValues);



}