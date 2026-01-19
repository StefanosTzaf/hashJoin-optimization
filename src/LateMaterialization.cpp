#include <LateMaterialization.h>
#include <iostream>
#include "ColumnStore.h"
#include <Unchained.h>
#include <omp.h>


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

    auto run(){
        namespace views = ranges::views;
        
        // STEP 1: the hash table for joining
        UnchainedHashTable hash_table;

        // create a Column (not ColumnT) for each output column based on type
        for (auto [col_idx, data_type]: output_attrs) {
                
            results.columns.emplace_back(data_type); // add new Column to columnarTable
        }

        // vector with different table results for each thread
        std::vector<std::vector<ColumnT>> threadResults(NUMBER_OF_THREADS);

        // vector with inserters for each table of each thread
        std::vector<std::vector<ColumnTInserter>> threadInserters(NUMBER_OF_THREADS);


        for(size_t i = 0; i < NUMBER_OF_THREADS; i++){

            threadResults[i].reserve(output_attrs.size());
            threadInserters[i].reserve(output_attrs.size());
            
            for(size_t j = 0; j < output_attrs.size(); j++){
                
                DataType type = std::get<1>(output_attrs[j]);

                //initialize columns
                threadResults[i].emplace_back(type);

                //initialize inserterts
                threadInserters[i].emplace_back(threadResults[i][j]);
            }
        }

        // temporary ColumnT storage for joined columns
        std::vector<ColumnT> tempResults; 
        tempResults.reserve(output_attrs.size());
        for(size_t i = 0; i < output_attrs.size(); i++){
            DataType type = std::get<1>(output_attrs[i]);
            tempResults.emplace_back(type);
        }
        
        // create an inserter for each columnT of tempResults
        std::vector<ColumnTInserter> inserters; 
        inserters.reserve(output_attrs.size());
        for(int i = 0; i < output_attrs.size(); i++){
            DataType type = std::get<1>(output_attrs[i]);
            inserters.emplace_back(tempResults[i]);
        }
        
        // BUILD PHASE
        if (build_left) {

            if(left.empty() || (left[left_col].getSize() == 0)){
                return;
            }
            
            ColumnT& keyColumn = left[left_col]; // extract the column with the key

            const std::vector<Page*>& pages = keyColumn.getPages();
            size_t sizePages = pages.size();
            
            const std::vector<size_t>& pageRowOffsets = keyColumn.getPageRowOffsets();

            #pragma omp parallel for num_threads(NUMBER_OF_THREADS) 
           
            // iterates over all pages of column with join key
            for (size_t pageIdx = 0; pageIdx < sizePages; pageIdx++) { 

                // take local index of row for current page
                size_t idx = pageRowOffsets[pageIdx]; 
                const Page* page = keyColumn.getPage(pageIdx);
                // first 2 bytes is numRows in both page formats
                uint16_t numRows = *reinterpret_cast<const uint16_t*>(page->data);

                for(size_t row = 0; row < numRows; row++) {

                    int32_t key;

                    if(keyColumn.isCopied() == true){

                        const value_t& val = *reinterpret_cast<const value_t*>(page->data + sizeof(uint16_t) + row*sizeof(value_t));
                   
                        if(val.is_null()){ // ingore null values
                            idx++;
                            continue;
                        }
                       
                        key = val.get_int();
                    }

                    // take int32 value directly
                    else{
                        key = *reinterpret_cast<const int32_t*>(page->data + 2*sizeof(uint16_t) + row*sizeof(int32_t)); 
                    }
                                       
                
                    hash_table.insert(key,idx);      
    
                    idx++;
                }
            }

            hash_table.build();

            // PROBE PHASE

            ColumnT& probeKeyColumn = right[right_col]; // column with join key
          
            const std::vector<Page*>& pagesProbe = probeKeyColumn.getPages();
            const std::vector<size_t>& pageProbeRowOffsets = probeKeyColumn.getPageRowOffsets();
            size_t probeSize = pagesProbe.size();
            
            #pragma omp parallel for num_threads(NUMBER_OF_THREADS) 
            
            // iterate through all pages of column with join key
            for (size_t pageIdx = 0; pageIdx < probeSize; pageIdx++) {
           
                // each thread should have its own vector
                std::vector<size_t> matching_indices;
                
                int threadId = omp_get_thread_num();
                
                const Page* page = probeKeyColumn.getPage(pageIdx);
                size_t right_idx = pageProbeRowOffsets[pageIdx];
                const uint16_t numRows = *reinterpret_cast<const uint16_t*>(page->data);

                for(size_t row = 0; row < numRows; row++) {
                  
                    int32_t key;

                    if(probeKeyColumn.isCopied() == true){

                        const value_t& val = *reinterpret_cast<const value_t*>(page->data + sizeof(uint16_t) + row * sizeof(value_t));
                        if(val.is_null()){
                            right_idx++;
                            continue;
                        }
                       
                        key = val.get_int();
                    }

                    else{
                        key = *reinterpret_cast<const int32_t*>(page->data + 2*sizeof(uint16_t) + row*sizeof(int32_t));
                    }

                    // for every matching key, find all matching build-side rows
                    matching_indices = hash_table.search(key);

                    // for each matching left row index 
                    // keep only the desired columns
                    for (auto left_idx: matching_indices) {               
                        
                        size_t output_col = 0;

                        // for each column index we want in the final result
                        for (auto [col_idx, _]: output_attrs) {

                            void* val = NULL;
                            bool copied = false;

                            // if index < number of columns (wanted column is on left table)
                            if (col_idx < left.size()) {

                                val = left[col_idx].getValueAtRow(left_idx);     
                                if(left[col_idx].isCopied() == true){
                                    copied = true;
                                }
                            } 
                            // wanted column is on right table
                            else {
                                val = right[col_idx - left.size()].getValueAtRow(right_idx);
                                if(right[col_idx - left.size()].isCopied() == true){
                                    copied = true;
                                }
                            }

                            if(val == NULL){
                                continue;
                            }
           
                            if(copied == true){

                                threadInserters[threadId][output_col].insert(*(value_t*)(val));
                            }
                            // convert the int32_t to a value_t storing it
                            else{
                                threadInserters[threadId][output_col].insert(value_t::from_int(*(int32_t*)(val)));

                            }
                            
                            output_col++;
                        }
                    }
                    
                    right_idx++;
                }
                
            }

        // STEP 5: BUILD ON RIGHT TABLE
        // PROBE WITH LEFT TABLE, COMBINE MATCHES
        } 
        else {

            if(right.empty() || (right[right_col].getSize() == 0)){
                return;
            }


            ColumnT& keyColumn = right[right_col];
            
            const std::vector<Page*>& pages = keyColumn.getPages();
            size_t sizePages = pages.size();
            
            const std::vector<size_t>& pageRowOffsets = keyColumn.getPageRowOffsets();
            
            #pragma omp parallel for num_threads(NUMBER_OF_THREADS) 

            for(size_t pageIdx = 0; pageIdx < sizePages; pageIdx++){

                size_t idx = pageRowOffsets[pageIdx]; // row index
                const Page* page = keyColumn.getPage(pageIdx);
                uint16_t numRows = *reinterpret_cast<const uint16_t*>(page->data);

                int32_t key;
                for(size_t row = 0; row < numRows; row++){

                    if(keyColumn.isCopied() == true){

                        const value_t& val = *reinterpret_cast<const value_t*>(page->data + sizeof(uint16_t) + row*sizeof(value_t));
                        if(val.is_null()){
                            idx++;
                            continue;
                        }
                       
                        key = val.get_int();
                    }

                    else{
                        key = *reinterpret_cast<const int32_t*>(page->data + 2*sizeof(uint16_t) + row*sizeof(int32_t));
                    }
                    
                    hash_table.insert(key, idx);    
    
                    idx++;
                }
            }
         
            hash_table.build();

            ColumnT& probeKeyColumn = left[left_col];
           
            const std::vector<Page*>& pagesProbe = probeKeyColumn.getPages();
            const std::vector<size_t>& pageProbeRowOffsets = probeKeyColumn.getPageRowOffsets();
            size_t probeSize = pagesProbe.size();
         
            #pragma omp parallel for num_threads(NUMBER_OF_THREADS) 

            // iterate through all pages of column with join key
            for (size_t pageIdx = 0; pageIdx < probeSize; pageIdx++) {
               
                // each thread should have its own vector
                std::vector<size_t> matching_indices;
               
                int threadId = omp_get_thread_num();
                
                const Page* page = probeKeyColumn.getPage(pageIdx);
                size_t left_idx = pageProbeRowOffsets[pageIdx];
                const uint16_t numRows = *reinterpret_cast<const uint16_t*>(page->data);

                for(size_t row = 0; row < numRows; row++){
                    
                    int32_t key;

                    if(probeKeyColumn.isCopied() == true){

                        const value_t& val = *reinterpret_cast<const value_t*>(page->data + sizeof(uint16_t) + row * sizeof(value_t));
                        if(val.is_null()){
                            left_idx++;
                            continue;
                        }
                        
                        key = val.get_int();               
                    }

                    else{
                        key = *reinterpret_cast<const int32_t*>(page->data + 2*sizeof(uint16_t) + row*sizeof(int32_t));
                    }

                    matching_indices = hash_table.search(key);
                        
                    for (auto right_idx: matching_indices) {

                        size_t output_col = 0;
                                    
                        for (auto [col_idx, _]: output_attrs) {

                            void* val = NULL;
                            bool copied = false;
                    
                            if (col_idx < left.size()) {
                                val = left[col_idx].getValueAtRow(left_idx);
                                
                                if(left[col_idx].isCopied() == true){
                                    copied = true;
                                }
                            } 
                            else {
                                val = right[col_idx - left.size()].getValueAtRow(right_idx);

                                if(right[col_idx - left.size()].isCopied() == true){
                                    copied = true;
                                }
                            }

                            if(val == NULL){
                                continue;
                            }

                            if(copied == true){
                                threadInserters[threadId][output_col].insert(*(value_t*)(val));
                            }
                            else{
                                threadInserters[threadId][output_col].insert(value_t::from_int(*(int32_t*)(val)));
                            }
                            
                            output_col++;
                        }

                    }
                
                    left_idx++;
                }
                
            }
        }

        // now we have collected all local results and we need to merge them into one
        // this will be done only by a single thread
        for(size_t tid = 0; tid < NUMBER_OF_THREADS; tid++){

            for(size_t col = 0; col < output_attrs.size(); col++){

                ColumnT& localCol = threadResults[tid][col]; 
                ColumnT& finalCol = tempResults[col];

                for(Page* page: localCol.getPages()){

                    inserters[col].insertPage(page);
                }
            }
        }

        // now that we have all columns in tempResults (vector<ColumnT>)
        // we need to fill the ColumnarTable results with the materialized columns
        size_t output_col = 0;

        for (auto [col_idx, data_type]: output_attrs) {

            Column& column = results.columns[output_col];

            // check data type of column and create the appropriate inserter
            if(data_type == DataType::INT32){
                ColumnInserter<int32_t> inserter(column);

                // iterate through all values of the column in tempResults
                for(Page* page: tempResults[output_col].getPages()){

                    uint16_t numRows = *reinterpret_cast<uint16_t*>(page->data);

                    for(size_t i = 0; i < numRows; i++){
                        const value_t& val = *reinterpret_cast<value_t*>(page->data + sizeof(uint16_t) + i*sizeof(value_t));

                        if(val.is_null()){
                            inserter.insert_null();

                        }
                        else{
                            inserter.insert(val.get_int());
                        }
                    }
                }

                // finalize inserter of current column
                inserter.finalize();
            }

            else if(data_type == DataType::VARCHAR){

                ColumnInserter<std::string> inserter(column);

                for(Page* page: tempResults[output_col].getPages()){
                    
                    uint16_t numRows = *reinterpret_cast<uint16_t*>(page->data);

                    for(size_t i = 0; i < numRows; i++){
                        const value_t& val = *reinterpret_cast<value_t*>(page->data + sizeof(uint16_t) + i*sizeof(value_t));

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
                }
                inserter.finalize();
            }
            output_col++;
        }

        int total_rows = tempResults[0].getSize(); 

        // Set the number of rows in the result
        results.num_rows = total_rows;           
    
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
    root_join_algorithm.run();
      

    // this is filled by the run method
    return results;
}


// converts columnar table to vector<ColumnT>:
// creates a column reference for dense columns
// and copies those with null values or varchar
ExecuteResult my_copy_scan(const ColumnarTable& table,
     const std::vector<std::tuple<size_t, DataType>>& output_attrs, uint16_t table_id) {
    
    namespace views = ranges::views;
    
    // a vector with ColumnT as elements that hold value_t
    // output_attrs.size because we dont need all of the columns
    std::vector<ColumnT> results;
    results.reserve((output_attrs.size()));

    // initially create copied columns and each time a dense column is 
    // met move Column to existing ColumnT
    for (auto [col_idx, data_type] : output_attrs) {
        results.emplace_back(data_type);
    }
    
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

        
            // check if INT32 column has nulls:
            // if it does not have any nulls (dense column)
            // for each page num_rows = num_values
            bool hasNulls = false; // indicates wether the column is dense or not

            if(column.type == DataType::INT32){

                for (auto* page:
                    column.pages | views::transform([](auto* page) { return page->data; })) {
                               
                    auto  num_rows   = *reinterpret_cast<uint16_t*>(page); // first 2 bytes: number of rows
                    auto num_values = *reinterpret_cast<uint16_t*>(page + 2); // number of non-null values
                    
                    if(num_rows == num_values){
                        // page is dense
                    }
                    else{
                        hasNulls = true;
                    }
                    
                    // exit the for loop since we already know
                    // the column is not dense
                    if(hasNulls == true){
                        break;
                    }
                }

                // if the column is dense, don't copy it
                if(hasNulls == false){

                    // move to existing ColumnT 
                    results[column_idx] = ColumnT(&column);
                    continue;
                }

            }

    
            ColumnT& column_t = results[column_idx]; 
            
            // create an inserter for each columnT
            ColumnTInserter inserter(column_t); 
                
            // iterate through all pages of the column
            for (auto* page:
                column.pages | views::transform([](auto* page) { return page->data; })) {
                    
                ++page_idx;
                    
                // check data type of column: either INT32 or VARCHAR
                switch (column.type) {

                    case DataType::INT32: {
                        
                        auto  num_rows   = *reinterpret_cast<uint16_t*>(page); // first 2 bytes: number of rows
                        auto num_values = *reinterpret_cast<uint16_t*>(page); // number of non-null values
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
                                
                                value_t val = value_t::from_int(value);
                                inserter.insert(val); // insert value into ColumnT
        
                                ++row_idx; // move to next row
                                ++data_idx; // move to next data index

                            } 
                            else { // add null value
                                value_t val;
                                inserter.insert(val);
                                ++row_idx;
                                // should not increment data_idx
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

                            value_t val = value_t::from_string_ref(table_id, (uint16_t)(in_col_idx), 
                                                        page_idx - 1, value_t::LONG_STR_TAG);

                            inserter.insert(val);

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

                                    value_t val = value_t::from_string_ref(table_id, 
                                        (uint16_t)(in_col_idx), page_idx - 1, data_idx);

                                    inserter.insert(val);

                                    data_idx++; // move to next string offset
                                    row_idx++;  // move to next row
                                } 
                                // insert null value
                                else {
                                    value_t val;
                                    inserter.insert(val);
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
        const Column& column = table.columns[col_idx];
        Page* page = column.pages[page_idx];

        if((v.dataIdx & value_t::TYPE_MASK) == value_t::LONG_STR_TAG){
            
            size_t num_chars = *reinterpret_cast<uint16_t*>(page->data + 2);           
            auto* data_begin = reinterpret_cast<char*>(page->data + 4);

            // create string with num_chars length
            std::string result(data_begin, data_begin + num_chars);

            // take next page of long string
            Page* next_page = column.pages[++page_idx];
            size_t type_of_next = *reinterpret_cast<uint16_t*>(next_page->data);
           
            while(type_of_next == 0xfffe){ // While we have more LONG string pages

                // Now handle next page
                size_t next_num_chars = *reinterpret_cast<uint16_t*>(next_page->data + 2);
                auto* next_data_begin = reinterpret_cast<char*>(next_page->data + 4);
                
                // append long string to existing string
                result.append(next_data_begin, next_data_begin + next_num_chars);

                next_page = column.pages[++page_idx];
                type_of_next = *reinterpret_cast<uint16_t*>(next_page->data);

            }
            
            return result;
        }
        else{
            // Access the page and extract the string
            
            // the first 3 bits are set to 0 to get the data index
            auto idx = v.dataIdx & value_t::OFFSET_MASK;
            uint16_t  num_non_null = *reinterpret_cast<uint16_t*>(page->data + 2);
            uint16_t* offset_begin = reinterpret_cast<uint16_t*>(page->data + 4);
    
            // this is where the string ends
            uint16_t offset = offset_begin[idx];       
            uint16_t prevOffset = 0;
            
            if (idx != 0) {
            
                // this is where the previous string ends
                // and where the current string starts
                prevOffset = offset_begin[idx-1];
            }

            // this is where the actual strings start
            auto* data_begin = reinterpret_cast<char*>(page->data + 4 + num_non_null * 2);
    
            // return the string by copying bytes from data_begin up to offset
            return std::string(data_begin + prevOffset, data_begin + offset);
        }    
        
    }
    throw std::runtime_error("Unknown value_t type");
}

//Convert ExecuteResult (value_t) to Table format (Data)
std::vector<std::vector<Data>> convert_to_Data(const ExecuteResult& results, const Plan& plan) {
    
    std::vector<std::vector<Data>> data_results; // final result in Data format
    data_results.reserve(results.size());

    
    for (const ColumnT& col: results) {
        
        std::vector<Data> data_row;
        data_row.reserve(results.size());
        
        for (Page* page: col.getPages()) {
            
            uint16_t numRows = *reinterpret_cast<uint16_t*>(page->data);

            for(size_t i = 0; i < numRows; i++){

                value_t val = *reinterpret_cast<value_t*>(page->data + sizeof(numRows) + sizeof(value_t) * i);
              
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

        }
        
        data_results.emplace_back(std::move(data_row));
    }
    
    return data_results;
}
