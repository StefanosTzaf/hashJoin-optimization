#pragma once
#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <inner_column.h>
#include "ColumnStore.h"
#include "value_t.h"


// similar to typedef: write ExecuteResult instead of std::vector<ColumnT>
// this is column-based
using ExecuteResult = std::vector<ColumnT>;


// Forward declarations
struct RootJoinAlgorithm;


ColumnarTable execute_root_hash_join(
    const Plan&                                      plan,
    const JoinNode&                                  join,
    ExecuteResult&                                   left,
    ExecuteResult&                                   right, 
    const std::vector<std::tuple<size_t, DataType>>& output_attrs);
   
    

ExecuteResult my_copy_scan(const ColumnarTable& table,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs, uint16_t table_id);

    
Data valuet_to_Data(const value_t& val, const ColumnarTable& table);


std::vector<std::vector<Data>> convert_to_Data(const ExecuteResult& results, const Plan& plan);
