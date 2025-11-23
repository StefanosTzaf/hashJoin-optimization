#include <LateMaterialization.h>


static bool get_bitmap(const uint8_t* bitmap, uint16_t idx) {
    auto byte_idx = idx / 8;
    auto bit      = idx % 8;
    return bitmap[byte_idx] & (1u << bit);
}


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
