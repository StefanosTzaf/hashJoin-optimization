#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <inner_column.h>

struct stringRef{
    uint16_t tableIdx; // which table the string belongs to
    uint16_t columnIdx; // column of the table where the string is located
    uint16_t pageIdx; // page index within the column
    uint16_t offset; // offset of string within page
};


// value_t can hold either an int32_t or a stringRef
union value_t {
    int32_t intValue;
    stringRef strRef;
};



namespace Contest {

// similar to typedef: write ExecuteResult instead of std::vector<std::vector<Data>>
// this is row-based
// Data is std::variant<int32_t, int64_t, double, std::string, std::monostate>
// and holds one value of any of these types
using ExecuteResult = std::vector<std::vector<Data>>;

// recursively computes the result of the plan node with index node_idx
// depending on its type: ScanNode or JoinNode
ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

struct JoinAlgorithm {
    bool                                             build_left; // if true: build with left table, probe with right
    ExecuteResult&                                   left; // rows of left table
    ExecuteResult&                                   right; // rows of right table
    ExecuteResult&                                   results; // rows of join result
    size_t                                           left_col, right_col; // indexes of join keys for each table
    const std::vector<std::tuple<size_t, DataType>>& output_attrs; // (column index, type)


    template <class T>
    auto run() {
        namespace views = ranges::views;

        // STEP 1: the hash table for joining: type of join key, vector of row indexes that contain the key: 
        // one key might correspond to multiple rows
        std::unordered_map<T, std::vector<size_t>> hash_table;

         // STEP 2 BUILD PHASE: WE TAKE THE ROWS OF LEFT TABLE AND 
        // CALCULATE THE HASH VALUE OF EACH KEY AND STORE IT IN THE HASH TABLE
        // if we build on the left table
        if (build_left) {

            // iterates over all rows of left table with row index idx
            // record: the actual row
            for (auto&& [idx, record]: left | views::enumerate) {

                         
                // use of visit because record[left_col] is a Data variant
                // it might contain an int, double, string etc
                std::visit(
                    [&hash_table, idx = idx](const auto& key) {

                        // holds the type of key
                        using Tk = std::decay_t<decltype(key)>;

                         // if the key's type matches the join type, insert it into hash table
                        if constexpr (std::is_same_v<Tk, T>) {

                            // try to find the key in the hash table
                            if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                                // append idx to the appropriate vector of the hash table
                                hash_table.emplace(key, std::vector<size_t>(1, idx));

                            // if not found, create new entry in hash table with idx
                            } else {
                                itr->second.push_back(idx);
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    record[left_col]); // extracts the join key
            }

            // STEP 3 PROBE PHASE: WE TAKE THE ROWS OF THE RIGHT TABLE, CALCULATE
            // THE HASH VALUE OF EACH KEY AND CHECK IF IT ALREADY EXISTS
            // IF SO, WE STORE IT IN THE FINAL TABLE, SINCE IT IS A RESULT OF JOIN

            // for each row of the right table
            for (auto& right_record: right) {
                std::visit(
                    [&](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {

                            // for every matching key, find all matching build-side rows
                            // combine columns from both tables into a new_record and 
                            // add it to the final results

                            // search for the key in the hash table
                            if (auto itr = hash_table.find(key); itr != hash_table.end()) {

                                // for each matching left row index from
                                // itr->second: the vector with row indices
                                for (auto left_idx: itr->second) {
                                    auto&             left_record = left[left_idx]; //get the whole left row
                                    std::vector<Data> new_record;
                                    new_record.reserve(output_attrs.size());

                                    // STEP 4 COMBINE RECORDS

                                    //left_record  = [1, "Alice", 25]        // 3 columns (indices 0, 1, 2)
                                    //right_record = [100, "Order1"]         // 2 columns (indices 0, 1)

                                    // Virtual combined record: [1, "Alice", 25, 100, "Order1"]
                                    // Indices:                   0    1      2   3      4


                                    // for each column index 
                                    for (auto [col_idx, _]: output_attrs) {

                                        // if index < number of columns (wanted column is on left table)
                                        if (col_idx < left_record.size()) {
                                            new_record.emplace_back(left_record[col_idx]);
                                        } 
                                        // wanted column is on right table
                                        else {
                                            new_record.emplace_back(
                                                right_record[col_idx - left_record.size()]);
                                        }
                                    }

                                    // add the combined row to final result
                                    results.emplace_back(std::move(new_record));
                                }
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    right_record[right_col]);
            }

        // STEP 5: BUILD ON RIGHT TABLE
        // PROBE WITH LEFT TABLE, COMBINE MATCHES
        } else {
            for (auto&& [idx, record]: right | views::enumerate) {
                std::visit(
                    [&hash_table, idx = idx](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                                hash_table.emplace(key, std::vector<size_t>(1, idx));
                            } else {
                                itr->second.push_back(idx);
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    record[right_col]);
            }
            for (auto& left_record: left) {
                std::visit(
                    [&](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            if (auto itr = hash_table.find(key); itr != hash_table.end()) {
                                for (auto right_idx: itr->second) {
                                    auto&             right_record = right[right_idx];
                                    std::vector<Data> new_record;
                                    new_record.reserve(output_attrs.size());
                                    for (auto [col_idx, _]: output_attrs) {
                                        if (col_idx < left_record.size()) {
                                            new_record.emplace_back(left_record[col_idx]);
                                        } else {
                                            new_record.emplace_back(
                                                right_record[col_idx - left_record.size()]);
                                        }
                                    }
                                    results.emplace_back(std::move(new_record));
                                }
                            }
                        } else if constexpr (not std::is_same_v<Tk, std::monostate>) {
                            throw std::runtime_error("wrong type of field");
                        }
                    },
                    left_record[left_col]);
            }
        }
    }
};

ExecuteResult execute_hash_join(const Plan&          plan,
    const JoinNode&                                  join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {

    auto                           left_idx    = join.left;
    auto                           right_idx   = join.right;
    auto&                          left_node   = plan.nodes[left_idx];
    auto&                          right_node  = plan.nodes[right_idx];
    auto&                          left_types  = left_node.output_attrs;
    auto&                          right_types = right_node.output_attrs;
    auto                           left        = execute_impl(plan, left_idx); // recursiving computing of table
    auto                           right       = execute_impl(plan, right_idx);
    
    std::vector<std::vector<Data>> results; // for the joined rows

    JoinAlgorithm join_algorithm{.build_left = join.build_left,
        .left                                = left,
        .right                               = right,
        .results                             = results,
        .left_col                            = join.left_attr,
        .right_col                           = join.right_attr,
        .output_attrs                        = output_attrs};
    
    // join key is always int32_t, so no need to check its type
    join_algorithm.run<int32_t>();
      

    // this is filled by the run<T> method
    return results;
}

bool get_bitmap(const uint8_t* bitmap, uint16_t idx) {
    auto byte_idx = idx / 8;
    auto bit      = idx % 8;
    return bitmap[byte_idx] & (1u << bit);
}



// converts columnar table to row-based format
std::vector<std::vector<value_t>> my_copy_scan(const ColumnarTable& table,
     const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    
    namespace views = ranges::views;
    
    // result is a vector of rows, each element of the outer vector is a row
    std::vector<std::vector<value_t>> results(table.num_rows,
        std::vector<value_t>(output_attrs.size())); // initialize with null values
    
    // stores data type of each column
    std::vector<DataType>          types(table.columns.size());
    
    // process columns from begin to end(indexes of output_attrs)
    auto task = [&](size_t begin, size_t end) {

        
        // iterate through all columns to be processed
        for (size_t column_idx = begin; column_idx < end; ++column_idx) {
            
            size_t in_col_idx = std::get<0>(output_attrs[column_idx]);
            auto& column = table.columns[in_col_idx];
            types[in_col_idx] = column.type;
            size_t row_idx = 0;
            
            // iterate through all pages of the column
            for (auto* page:
                column.pages | views::transform([](auto* page) { return page->data; })) {
                    
                    // check data type of column: either INT32 or VARCHAR
                    switch (column.type) {
    
                    case DataType::INT32: {
                        auto  num_rows   = *reinterpret_cast<uint16_t*>(page); // first 2 bytes: number of rows
                        auto* data_begin = reinterpret_cast<int32_t*>(page + 4); // data of int32 starts at offset 4
                        
                        // bitmap is at the end of the page
                        // indicates whether the corresponding row is null or not
                        auto* bitmap =
                            reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
                        
                        uint16_t data_idx = 0; 
        
                        for (uint16_t i = 0; i < num_rows; ++i) {
                            
                            // if the i-th row is not null
                            if (get_bitmap(bitmap, i)) {
                                // data_idx++ increments it by sizeof(int32_t) bytes
                                auto value = data_begin[data_idx++]; // get the actual int32 value
        
                                if (row_idx >= table.num_rows) { // check if row index is valid
                                    throw std::runtime_error("row_idx");
                                }
                                
                                results[row_idx][column_idx].intValue = value;
        
                                ++row_idx; // move to next row

                            } else { // row is null so continue
                                ++row_idx;
                            }
                        }
                        break;
                    }
               
                    case DataType::VARCHAR: {
                        auto num_rows = *reinterpret_cast<uint16_t*>(page); // first 2 bytes: number of rows
                        
                        // // LONG string page (first page of string)
                        // if (num_rows == 0xffff) {
                        //     auto        num_chars  = *reinterpret_cast<uint16_t*>(page + 2); // next 2 bytes: number of characters
                        //     auto*       data_begin = reinterpret_cast<char*>(page + 4); // actual data at page + 4
                          
                        //     std::string value{data_begin, data_begin + num_chars}; // copy num_chars bytes pointed by data_begin
        
                        //     if (row_idx >= table.numA_rows) {
                        //         throw std::runtime_error("row_idx");
                        //     }
        
                        //     results[row_idx++][column_idx].emplace<std::string>(std::move(value));
        
                        // } 
                        // // LONG string (following page)
                        // else if (num_rows == 0xfffe) {
                        //     auto  num_chars  = *reinterpret_cast<uint16_t*>(page + 2);
                        //     auto* data_begin = reinterpret_cast<char*>(page + 4);
        
                        //     std::visit(
                        //         [data_begin, num_chars](auto& value) {
                        //             using T = std::decay_t<decltype(value)>;
                        //             if constexpr (std::is_same_v<T, std::string>) {
                        //                 value.insert(value.end(), data_begin, data_begin + num_chars);
                        //             } else {
                        //                 throw std::runtime_error(
                        //                     "long string page 0xfffe must follows a string");
                        //             }
                        //         },
                        //         results[row_idx - 1][column_idx]);
                        // } 
                        
                        // REGULAR string page
                        else {
                            auto  num_non_null = *reinterpret_cast<uint16_t*>(page + 2); // num of non null strings
                            
                            // array of offsets of strings, each offset is 2 bytes
                            auto* offset_begin = reinterpret_cast<uint16_t*>(page + 4); 

                            // actual string data starts after offsets
                            auto* data_begin   = reinterpret_cast<char*>(page + 4 + num_non_null * 2);
                            auto* string_begin = data_begin;
                            
                            auto* bitmap =
                                reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);
                            
                            
                            uint16_t data_idx = 0;
                
                            for (uint16_t i = 0; i < num_rows; ++i) {
                                
                                // if the i-th row is not null
                                if (get_bitmap(bitmap, i)) {
                                   
                                    auto        offset = offset_begin[data_idx++]; // get offset of string

                                    // construct new string by copying bytes in range
                                    std::string value{string_begin, data_begin + offset};

                                    // update string_begin to point to end of current string
                                    string_begin = data_begin + offset;
                
                                    if (row_idx >= table.num_rows) {
                                        throw std::runtime_error("row_idx");
                                    }
                                   
                                    results[row_idx++][column_idx].emplace<std::string>(std::move(value));
                                } 
                                // row is null, continue with next row
                                else {
                                    ++row_idx;
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }
    };

    // run the task in parallel using the filter thread pool
    // each task processes a subset of columns
    filter_tp.run(task, output_attrs.size());

    return results;
}

ExecuteResult execute_scan(const Plan& plan, const ScanNode& scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    
    auto table_id = scan.base_table_id;
    auto& input = plan.inputs[table_id];
    return my_copy_scan(input, output_attrs);
}

ExecuteResult execute_impl(const Plan& plan, size_t node_idx) {
    auto& node = plan.nodes[node_idx];
    
    return std::visit( // visit: to determine node type : scan or join node
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, JoinNode>) {
                return execute_hash_join(plan, value, node.output_attrs);
            } else {
                return execute_scan(plan, value, node.output_attrs);
            }
        },
        node.data);
}

ColumnarTable execute(const Plan& plan, [[maybe_unused]] void* context) {
    namespace views = ranges::views;
    auto ret        = execute_impl(plan, plan.root); // row-based table
    
    // output_attrs: vector of tuples of (column index, data type)
    auto ret_types  = plan.nodes[plan.root].output_attrs
                   
                    // extracts only the data types
                   | views::transform([](const auto& v) { return std::get<1>(v); })
                   
                   // converts the result to a vector<DataType> containing
                   // just the types of the output vectors
                   | ranges::to<std::vector<DataType>>();
    
                   Table table{std::move(ret), std::move(ret_types)};

    return table.to_columnar();
}

void* build_context() {
    return nullptr;
}

void destroy_context([[maybe_unused]] void* context) {}

} // namespace Contest