#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <inner_column.h>

// value_t can hold either an int32_t or a stringRef or Null
// since pages are 8192 bytes we can use 3 Most Significant Bits of offset to indicate type!!!
// if we have an integer we can store it in tableIdx + columnIdx since they are not needed in that case
struct value_t {
    uint16_t tableIdx; // which table the string belongs to
    uint16_t columnIdx; // column of the table where the string is located
    uint16_t pageIdx; // page index within the column
    uint16_t dataIdx; // index of the string within the page

    // this is used so as to take the 2 MSB bits of offset for type tagging
    static constexpr uint16_t TYPE_MASK = 0xC000;  // 110.....
    static constexpr uint16_t NULL_TAG  = 0x0000;  // 000.....
    static constexpr uint16_t INT_TAG   = 0x4000;  // 010.....
    static constexpr uint16_t STR_TAG   = 0x8000;  // 100.....
    static constexpr uint16_t OFFSET_MASK = 0x1FFF; // 0001 1111 1111 1111

    // constructor
    value_t() : tableIdx(NULL_TAG), columnIdx(0), pageIdx(0), dataIdx(NULL_TAG) {}

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
        v.dataIdx = STR_TAG | (off & OFFSET_MASK);  // type + 13 bits data index
        return v;
    }

    // Type checks
    bool is_null() const { return (dataIdx & TYPE_MASK) == NULL_TAG; }
    bool is_int() const { return (dataIdx & TYPE_MASK) == INT_TAG; }
    bool is_string() const { return (dataIdx & TYPE_MASK) == STR_TAG; }

    // Getters
    int32_t get_int() const {
        uint32_t high = static_cast<uint32_t>(tableIdx) << 16;
        uint32_t low = columnIdx;
        return static_cast<int32_t>(high | low);
    }

};



namespace Contest {

// similar to typedef: write ExecuteResult instead of std::vector<std::vector<Data>>
// this is row-based
// Data is std::variant<int32_t, int64_t, double, std::string, std::monostate>
// and holds one value of any of these types
using ExecuteResult = std::vector<std::vector<value_t>>;

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
        std::unordered_map<int32_t, std::vector<size_t>> hash_table;

         // STEP 2 BUILD PHASE: WE TAKE THE ROWS OF LEFT TABLE AND 
        // CALCULATE THE HASH VALUE OF EACH KEY AND STORE IT IN THE HASH TABLE
        // if we build on the left table
        if (build_left) {

            // iterates over all rows of left table with row index idx
            // record: the actual row
            for (auto&& [idx, record]: left | views::enumerate) {                        

                int32_t key = record[left_col].get_int();

                // try to find the key in the hash table
                if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                    // append idx to the appropriate vector of the hash table
                    hash_table.emplace(key, std::vector<size_t>(1, idx));

                // if not found, create new entry in hash table with idx
                } else {
                    itr->second.push_back(idx);
                }         
            }

            // STEP 3 PROBE PHASE: WE TAKE THE ROWS OF THE RIGHT TABLE, CALCULATE
            // THE HASH VALUE OF EACH KEY AND CHECK IF IT ALREADY EXISTS
            // IF SO, WE STORE IT IN THE FINAL TABLE, SINCE IT IS A RESULT OF JOIN

            // for each row of the right table
            for (auto& right_record: right) {

                int32_t key = right_record[right_col].get_int();
                
                // for every matching key, find all matching build-side rows
                // combine columns from both tables into a new_record and 
                // add it to the final results

                // search for the key in the hash table
                if (auto itr = hash_table.find(key); itr != hash_table.end()) {

                    // for each matching left row index from
                    // itr->second: the vector with row indices
                    for (auto left_idx: itr->second) {
                        auto&             left_record = left[left_idx]; //get the whole left row
                        std::vector<value_t> new_record;
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
            }

        // STEP 5: BUILD ON RIGHT TABLE
        // PROBE WITH LEFT TABLE, COMBINE MATCHES
        } else {
            for (auto&& [idx, record]: right | views::enumerate) {

                int32_t key = record[right_col].get_int();
              
                if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                    hash_table.emplace(key, std::vector<size_t>(1, idx));
                } 
                else {
                    itr->second.push_back(idx);
                }
            }
            for (auto& left_record: left) {
                
                int32_t key = left_record[left_col].get_int();

                if (auto itr = hash_table.find(key); itr != hash_table.end()) {
                   
                    for (auto right_idx: itr->second) {
                   
                        auto&             right_record = right[right_idx];
                        std::vector<value_t> new_record;
                        new_record.reserve(output_attrs.size());
                   
                        for (auto [col_idx, _]: output_attrs) {
                   
                            if (col_idx < left_record.size()) {
                                new_record.emplace_back(left_record[col_idx]);
                            } 
                            else {
                                new_record.emplace_back(right_record[col_idx - left_record.size()]);
                            }
                        }
                        results.emplace_back(std::move(new_record));
                    }
                }
                
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
    
    std::vector<std::vector<value_t>> results; // for the joined rows

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
     const std::vector<std::tuple<size_t, DataType>>& output_attrs, uint16_t table_id) {
    
    namespace views = ranges::views;
    
    // result is a vector of rows, each element of the outer vector is a row
    // initialize all values to null (with NULLTAG in default constructor of value_t)
    std::vector<std::vector<value_t>> results(table.num_rows,
        std::vector<value_t>(output_attrs.size()));
    
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
            uint16_t page_idx = 0;
            
            // iterate through all pages of the column
            for (auto* page:
                column.pages | views::transform([](auto* page) { return page->data; })) {
                    ++page_idx;
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
                                auto value = data_begin[data_idx]; // get the actual int32 value
        
                                if (row_idx >= table.num_rows) { // check if row index is valid
                                    throw std::runtime_error("row_idx");
                                }
                                
                                results[row_idx][column_idx] = value_t::from_int(value); // store the int value in results
        
                                ++row_idx; // move to next row
                                ++data_idx; // move to next data index

                            } else { // row is null so continue
                                ++row_idx;
                            }
                        }
                        break;
                    }
               
                    case DataType::VARCHAR: {
                        auto num_rows = *reinterpret_cast<uint16_t*>(page); // first 2 bytes: number of rows
                        
                        // LONG string page (first page of string)
                        if (num_rows == 0xffff) {
                            auto        num_chars  = *reinterpret_cast<uint16_t*>(page + 2); // next 2 bytes: number of characters
                            auto*       data_begin = reinterpret_cast<char*>(page + 4); // actual data at page + 4
                          
                            // std::string value{data_begin, data_begin + num_chars}; // copy num_chars bytes pointed by data_begin
        
                            if (row_idx >= table.num_rows) {
                                throw std::runtime_error("row_idx");
                            }
                            

                            // results[row_idx++][column_idx].emplace<std::string>(std::move(value));
        
                        } 
                        // LONG string (following page)
                        else if (num_rows == 0xfffe) {
                            // auto  num_chars  = *reinterpret_cast<uint16_t*>(page + 2);
                            // auto* data_begin = reinterpret_cast<char*>(page + 4);
        
                            // std::visit(
                            //     [data_begin, num_chars](auto& value) {
                            //         using T = std::decay_t<decltype(value)>;
                            //         if constexpr (std::is_same_v<T, std::string>) {
                            //             value.insert(value.end(), data_begin, data_begin + num_chars);
                            //         } else {
                            //             throw std::runtime_error(
                            //                 "long string page 0xfffe must follows a string");
                            //         }
                            //     },
                            //     results[row_idx - 1][column_idx]);
                        } 
                        
                        // REGULAR string page
                        else {
                                              
                            auto* bitmap =
                                reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);          
                            
                            uint16_t data_idx = 0;
                
                            for (uint16_t i = 0; i < num_rows; ++i) {
                                
                                // if the i-th row is not null
                                if (get_bitmap(bitmap, i)) {
                                                  
                                    if (row_idx >= table.num_rows) {
                                        throw std::runtime_error("row_idx");
                                    }
                                   
                                    results[row_idx][column_idx] = value_t::from_string_ref(
                                        table_id, static_cast<uint16_t>(in_col_idx), page_idx - 1, data_idx);

                                    data_idx++; // move to next string offset
                                    row_idx++;  // move to next row
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
    return my_copy_scan(input, output_attrs, static_cast<uint16_t>(table_id));
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

// converts a value_t to Data type
Data valuet_to_Data(const value_t& v, const ColumnarTable& table) {
   
    if (v.is_null()) {
        return std::monostate{};
    }

    if (v.is_int()) {
        return v.get_int();
    }

    if (v.is_string()) {
        // Materialize the string from the page
        uint16_t table_idx = v.tableIdx;
        uint16_t col_idx = v.columnIdx;
        uint16_t page_idx = v.pageIdx;

        // Access the page and extract the string
        auto& column = table.columns[col_idx];
        auto* page = column.pages[page_idx]->data;
        
        // the first 3 bits are set to 0 to get the data index
        auto idx = v.dataIdx & value_t::OFFSET_MASK;
        auto  num_non_null = *reinterpret_cast<uint16_t*>(page + 2);
        auto* offset_begin = reinterpret_cast<uint16_t*>(page + 4);
   
        // this is where the string ends
        uint16_t offset = offset_begin[idx];       
        uint16_t prevOffset = 0;
        
        if (idx != 0) {
        
            // this is where the previous string ends
            // and where the current string starts
            prevOffset = offset_begin[idx-1];
        }
      
        
        // this is where the actual strings start
        auto* data_begin = reinterpret_cast<char*>(page + 4 + num_non_null * 2);

        // return the string by copying bytes from data_begin up to offset
        return std::string(data_begin + prevOffset, data_begin + offset);
        
    }
    throw std::runtime_error("Unknown value_t type");
}

// Convert ExecuteResult (value_t) to Table format (Data)
std::vector<std::vector<Data>> convert_to_Data(const ExecuteResult& results, const Plan& plan) {
    
    std::vector<std::vector<Data>> data_results; // final result in Data format
    data_results.reserve(results.size());

    
    for (const auto& row : results) {
        
        std::vector<Data> data_row;
        data_row.reserve(row.size());
        
        for (const auto& val : row) {
         
            if (val.is_null()) {
                data_row.emplace_back(std::monostate{});
            } 
            else if (val.is_int()) {
                data_row.emplace_back(val.get_int());
            } 
            else {
                // String materialization from value_t
                uint16_t table_idx = val.tableIdx;
                auto& table = plan.inputs[table_idx];
                Data data_value = valuet_to_Data(val, table); // convert value_t to Data
                data_row.emplace_back(std::move(data_value));

            }
        }
        
        data_results.emplace_back(std::move(data_row));
    }
    
    return data_results;
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

                   // convert ExecuteResult (value_t) to Table format (Data)
                   auto data_table = convert_to_Data(ret, plan);
    
                   Table table{std::move(data_table), std::move(ret_types)};

    return table.to_columnar();
}

void* build_context() {
    return nullptr;
}

void destroy_context([[maybe_unused]] void* context) {}

} // namespace Contest