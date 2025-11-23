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



namespace Contest {

// similar to typedef: write ExecuteResult instead of std::vector<std::vector<Data>>
// this is row-based
using ExecuteResult = std::vector<std::vector<value_t>>;

// recursively computes the result of the plan node with index node_idx
// depending on its type: ScanNode or JoinNode
ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

Data valuet_to_Data(const value_t& val, const ColumnarTable& table);

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

         // STEP 2 BUILD PHASE: take rows of left table, calculate hash value
         // and store in hash table
        if (build_left) {

            // iterates over all rows of left table with row index idx
            // record: the actual row
            for (auto&& [idx, record]: left | views::enumerate) {                        

                int32_t key = record[left_col].get_int();

                if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                    // append idx to the appropriate vector of the hash table
                    hash_table.emplace(key, std::vector<size_t>(1, idx));

                // if not found, create new entry in hash table with idx
                } else {
                    itr->second.push_back(idx);
                }         
            }

            // STEP 3 PROBE PHASE: take rows of right table, calculate hash value
            // and if it exists store it in temp_results (vector of columns)

            // for each row of the right table
            for (auto& right_record: right) {

                int32_t key = right_record[right_col].get_int();
                
                // for every matching key, find all matching build-side rows
                // combine columns from both tables into a new_record and 
                // add it to the temp_results

                if (auto itr = hash_table.find(key); itr != hash_table.end()) {

                    // for each matching left row index from
                    // itr->second: the vector with row indices
                    for (auto left_idx: itr->second) {
                        auto&             left_record = left[left_idx]; 
                        std::vector<value_t> new_record;
                        new_record.reserve(output_attrs.size());

                        // STEP 4 COMBINE RECORDS

                        //left_record  = [1, "Alice", 25]        // 3 columns (indices 0, 1, 2)
                        //right_record = [100, "Order1"]         // 2 columns (indices 0, 1)

                        // Virtual combined record: [1, "Alice", 25, 100, "Order1"]
                        // Indices:                   0    1      2   3      4

                        // for each column index we want in the final result
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

struct RootJoinAlgorithm{
    bool                                             build_left; // if true: build with left table, probe with right
    ExecuteResult&                                   left; // rows of left table
    ExecuteResult&                                   right; // rows of right table
    ColumnarTable&                                   results; // rows of join result
    size_t                                           left_col, right_col; // indexes of join keys for each table
    const std::vector<std::tuple<size_t, DataType>>& output_attrs; // (column index, type)
    const Plan&                                          plan;

    template <class T>
    auto run(){
        namespace views = ranges::views;

        std::unordered_map<int32_t, std::vector<size_t>> hash_table;

        auto numOfColumns = output_attrs.size(); // number of columns in final result
        std::vector<std::vector<value_t>> temp_results; // temporary storage for each column of final result
        temp_results.resize(numOfColumns);
        
        // BUILD PHASE
        if (build_left) {

            for (auto&& [idx, record]: left | views::enumerate) {                        

                int32_t key = record[left_col].get_int();
      
                if (auto itr = hash_table.find(key); itr == hash_table.end()) {
           
                    hash_table.emplace(key, std::vector<size_t>(1, idx));

                // if not found, create new entry in hash table with idx
                } else {
                    itr->second.push_back(idx);
                }         
            }

            // PROBE PHASE
            for (auto& right_record: right) {

                int32_t key = right_record[right_col].get_int();
                
                // for every matching key, find all matching build-side rows
                // combine columns from both tables and store each column in temp_results
                if (auto itr = hash_table.find(key); itr != hash_table.end()) {                   

                    for (auto left_idx: itr->second) {
                        auto&             left_record = left[left_idx]; 

                        // combine records
                        // we should now create columns instead of rows

                        size_t output_col = 0; // index for column of temp_results
                        // for each (column index, type) of desired output
                        for (auto [col_idx, data_type]: output_attrs) {

                            // wanted column is on left table
                            if (col_idx < left_record.size()) {
                                // store the col_idx-th value of the row
                                temp_results[output_col].emplace_back(left_record[col_idx]);
                            } 
                            // wanted column is on right table
                            else {
                                temp_results[output_col].emplace_back(
                                    right_record[col_idx - left_record.size()]);
                            }
                            output_col++;
                        }
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
                   
                        size_t output_col = 0;
                        for (auto [col_idx, _]: output_attrs) {
                   
                            if (col_idx < left_record.size()) {
                                temp_results[output_col].emplace_back(left_record[col_idx]);
                            } 
                            else {
                                temp_results[output_col].emplace_back(right_record[col_idx - left_record.size()]);
                            }
                            output_col++;
                        }
                    }
                }
                
            }
        }

        // now that we have all columns in temp_results
        // we need to fill the ColumnarTable with the materialized columns
        size_t output_col = 0;
        for (auto [col_idx, data_type]: output_attrs) {
            
            results.columns.emplace_back(data_type); // add new column to table
            auto& column = results.columns.back();

            // check data type of column

            if(data_type == DataType::INT32){
                ColumnInserter<int32_t> inserter(column);

                // iterate through all values of the column
                for(auto& val: temp_results[output_col]){
                    if(val.is_int()){
                        inserter.insert(val.get_int()); // simple insertion
                    }
                    else{
                        inserter.insert_null();
                    }
                }
                inserter.finalize();
            }
            else if(data_type == DataType::VARCHAR){
                ColumnInserter<std::string> inserter(column);

                for(auto& val: temp_results[output_col]){
                    if(val.is_null()){
                        inserter.insert_null();
                    }
                    else{
                        // materialization of string
                        const ColumnarTable& table = plan.inputs[val.tableIdx];
                        Data data_str = valuet_to_Data(val, table);
                        std::string str = std::get<std::string>(data_str);
                        inserter.insert(str);

                    }
                }
                inserter.finalize();
            }
            output_col++;
        }
        
        // Set the number of rows in the result
        if (temp_results.empty() == false) {
            results.num_rows = temp_results[0].size();
        } 
        else {
            results.num_rows = 0;
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

ColumnarTable execute_root_hash_join(
    const Plan&                                      plan,
    const JoinNode&                                  join,
    ExecuteResult&                                   left,
    ExecuteResult&                                   right, 
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
  
    ColumnarTable results; // result of final join 

    RootJoinAlgorithm root_join_algorithm{
        .build_left                          = join.build_left,
        .left                                = left,
        .right                               = right,
        .results                             = results,
        .left_col                            = join.left_attr,
        .right_col                           = join.right_attr,
        .output_attrs                        = output_attrs,
        .plan                                = plan};

    
    // join key is always int32_t, so no need to check its type
    root_join_algorithm.run<int32_t>();
      

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
                            
                            if (row_idx >= table.num_rows) {
                                throw std::runtime_error("row_idx");
                            }
                            
                            results[row_idx][column_idx] = value_t::from_string_ref(
                                        table_id, static_cast<uint16_t>(in_col_idx), page_idx - 1, value_t::LONG_STR_TAG);
                            row_idx++;  // move to next row
                        } 
                        // LONG string (following page)
                        else if (num_rows == 0xfffe) {
                            // second page will be handled in materialization since we know that it
                            // is the continuation of a long string
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
            } 
            else {
                return execute_scan(plan, value, node.output_attrs);
            }
        },
        node.data);
}

// converts a value_t to Data type
Data valuet_to_Data(const value_t& v, const ColumnarTable& table) {
   

    if (v.is_string()) {
        // Materialize the string from the page
        uint16_t table_idx = v.tableIdx;
        uint16_t col_idx = v.columnIdx;
        uint16_t page_idx = v.pageIdx;
        auto& column = table.columns[col_idx];
        auto* page = column.pages[page_idx]->data;

        if((v.dataIdx & value_t::TYPE_MASK) == value_t::LONG_STR_TAG){
            size_t num_chars = *reinterpret_cast<uint16_t*>(page + 2);           
            auto* data_begin = reinterpret_cast<char*>(page + 4);
            std::string result(data_begin, data_begin + num_chars);


            auto* next_page = column.pages[++page_idx]->data;
            size_t type_of_next = *reinterpret_cast<uint16_t*>(next_page);
            while(type_of_next == 0xfffe){ // While we have more LONG string pages

                // Now handle next page
                size_t next_num_chars = *reinterpret_cast<uint16_t*>(next_page + 2);
                auto* next_data_begin = reinterpret_cast<char*>(next_page + 4);
                result.append(next_data_begin, next_data_begin + next_num_chars);

                next_page = column.pages[++page_idx]->data;
                type_of_next = *reinterpret_cast<uint16_t*>(next_page);

            }
            
            return result;
        }
        else{

            // Access the page and extract the string
            
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
        
        // val is of type value_t
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
    auto& root_node = plan.nodes[plan.root];

    return std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;

            if constexpr (std::is_same_v<T, JoinNode>) {
                // Get left and right children as ExecuteResult
                auto left = execute_impl(plan, value.left);
                auto right = execute_impl(plan, value.right);
                
                // Join directly to columnar
                return execute_root_hash_join(plan, value, left, right, root_node.output_attrs);
            } 
            else {
                // Root is just a scan 
                auto ret = execute_impl(plan, plan.root);
                auto ret_types = plan.nodes[plan.root].output_attrs
                               | views::transform([](const auto& v) { return std::get<1>(v); })
                               | ranges::to<std::vector<DataType>>();
                               
                auto data_table = convert_to_Data(ret, plan);
                
                Table table{std::move(data_table), std::move(ret_types)};
                
                return table.to_columnar();
            }
        },
        root_node.data);
}

void* build_context() {
    return nullptr;
}

void destroy_context([[maybe_unused]] void* context) {}

} // namespace Contest