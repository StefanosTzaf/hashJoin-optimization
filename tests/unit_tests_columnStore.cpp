#include <iostream>
#include <string>
#include "ColumnStore.h"
#include "LateMaterialization.h"
#include <vector>
#include <catch2/catch_test_macros.hpp>


using namespace std;

TEST_CASE("Single INT32 column", "[ColumnStore]"){

    ColumnT col(DataType::INT32);
    ColumnTInserter inserter(col);

    REQUIRE(col.getNumPages() == 0);
    REQUIRE(inserter.getLastPage() == 0);
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


}