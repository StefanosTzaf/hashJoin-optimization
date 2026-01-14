#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <inner_column.h>
#include "LateMaterialization.h"
#include "ColumnStore.h"
#include <Unchained.h>
#include <omp.h>
#include <atomic>

namespace Contest {

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


    auto run() {
        namespace views = ranges::views;

        // STEP 1: the hash table for joining
        UnchainedHashTable hash_table;

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

        // create a ColumnT for each output column based on type
        for(size_t i = 0; i < output_attrs.size(); i++){
            DataType type = std::get<1>(output_attrs[i]);
            results.emplace_back(type);
        }

        // create an inserter for each output column
        std::vector<ColumnTInserter> inserters;
        inserters.reserve(output_attrs.size());
        for(int i = 0; i < output_attrs.size(); i++){
            inserters.emplace_back(results[i]);
        }

         // STEP 2 BUILD PHASE: take rows of left table, calculate hash value
         // and store in hash table
        if (build_left) {

            if(left.empty() || (left[left_col].getSize() == 0)){
                return;
            }

            ColumnT& keyColumn = left[left_col]; // extract the column with the key
            
            const std::vector<Page*>& pages = keyColumn.getPages();
            size_t sizePages = pages.size();
            
            const std::vector<size_t>& pageRowOffsets = keyColumn.getPageRowOffsets();
            
            std::atomic<size_t> probe_counter{0};
  
            #pragma omp parallel num_threads(NUMBER_OF_THREADS)
            {
            while(true){

                // fetch 4 pages to minimize atomic operation overhead
                size_t base = probe_counter.fetch_add(4, std::memory_order_relaxed);
                
                if (base >= sizePages){
                    break;
                }

                for(size_t offset = 0; offset < 4; offset++){
                    // take local index of row for current page
                    size_t pageIdx = base + offset ;
                    if(pageIdx >= sizePages){
                        break;
                    }
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
                            
                        hash_table.insert(key, idx);  
        
                        idx++;
                    }


                }
            }
            }
            hash_table.build();

            // STEP 3 PROBE PHASE: take keys of right table, calculate hash value
            // and if it exists store it in temp_results (vector of columns)

            ColumnT& probeKeyColumn = right[right_col]; // column with join key

            const std::vector<Page*>& pagesProbe = probeKeyColumn.getPages();
            const std::vector<size_t>& pageProbeRowOffsets = probeKeyColumn.getPageRowOffsets();
            size_t probeSize = pagesProbe.size();

            std::atomic<size_t> probe_counter2{0};

            #pragma omp parallel num_threads(NUMBER_OF_THREADS)
            {
            int threadId = omp_get_thread_num();
            // each thread should have its own vector
            std::vector<size_t> matching_indices;
            while(true){
                size_t base = probe_counter2.fetch_add(4, std::memory_order_relaxed);
                if(base >= probeSize){
                    break;
                }
                for(size_t offset = 0; offset < 4; offset++){
                    size_t pageIdx = base + offset ;
                    if(pageIdx >= probeSize){
                        break;
                    } 
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
                            
                            // index of column of result
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
                                    // inserters[output_col].insert(*(value_t*)(val));
                                }
                                // convert the int32_t to a value_t storing it
                                else{
                                    threadInserters[threadId][output_col].insert(value_t::from_int(*(int32_t*)(val)));
                                    // inserters[output_col].insert(value_t::from_int(*(int32_t*)(val)));
                                }
                                
                                output_col++;
                            }
                        }

                        right_idx++;
                    }
                }   
            }
            }
        // STEP 5: BUILD ON RIGHT TABLE
        // PROBE WITH LEFT TABLE, COMBINE MATCHES
        } else {

            if(right.empty() || (right[right_col].getSize() == 0)){
                return;
            }

            ColumnT& keyColumn = right[right_col];
            
            const std::vector<Page*>& pages = keyColumn.getPages();
            size_t sizePages = pages.size();
            
            const std::vector<size_t>& pageRowOffsets = keyColumn.getPageRowOffsets();
            
            std::atomic<size_t> probe_counter{0};

            #pragma omp parallel num_threads(NUMBER_OF_THREADS)
            {
            
            while(true){
                size_t base = probe_counter.fetch_add(4, std::memory_order_relaxed);
                if(base >= sizePages){
                    break;
                }
                for(size_t offset = 0; offset < 4; offset++){
                    size_t pageIdx = base + offset;
                    if(pageIdx >= sizePages){
                        break;
                    }
                    size_t idx = pageRowOffsets[pageIdx]; // row index
                    const Page* page = keyColumn.getPage(pageIdx);
                    const uint16_t numRows = *reinterpret_cast<const uint16_t*>(page->data);

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
            }
            }
            hash_table.build();

            ColumnT& probeKeyColumn = left[left_col];

            const std::vector<Page*>& pagesProbe = probeKeyColumn.getPages();
            const std::vector<size_t>& pageProbeRowOffsets = probeKeyColumn.getPageRowOffsets();
            size_t probeSize = pagesProbe.size();
         
            std::atomic<size_t> probe_counter2{0};
            #pragma omp parallel num_threads(NUMBER_OF_THREADS)
            {
            // each thread should have its own vector
            std::vector<size_t> matching_indices;

            int threadId = omp_get_thread_num();
            while(true){
                size_t base = probe_counter2.fetch_add(4, std::memory_order_relaxed);
                if(base >= probeSize){
                    break;
                }
                for(size_t offset = 0; offset < 4; offset++){
                    size_t pageIdx = base + offset ;
                    if(pageIdx >= probeSize){
                        break;
                    }
        
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
                                    // inserters[output_col].insert(*(value_t*)(val));
                                }
                                else{
                                    threadInserters[threadId][output_col].insert(value_t::from_int(*(int32_t*)(val)));
                                    // inserters[output_col].insert(value_t::from_int(*(int32_t*)(val)));
                                }
                                output_col++;
                            }

                        }
                        
                        left_idx++;
                    }
                    
                }
            }
            }
        }

        // now we have collected all local results and we need to merge them into one
        // this will be done only by a single thread
        for(size_t tid = 0; tid < NUMBER_OF_THREADS; tid++){

            for(size_t col = 0; col < output_attrs.size(); col++){

                ColumnT& localCol = threadResults[tid][col]; 
                ColumnT& finalCol = results[col];

                for(Page* page: localCol.getPages()){

                    inserters[col].insertPage(page);
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
    ExecuteResult                  left        = execute_impl(plan, left_idx); // recursiving computing of table
    ExecuteResult                  right       = execute_impl(plan, right_idx);
    
  
    ExecuteResult results; // for the joined rows   

    JoinAlgorithm join_algorithm{.build_left = join.build_left,
        .left                                = left,
        .right                               = right,
        .results                             = results,
        .left_col                            = join.left_attr,
        .right_col                           = join.right_attr,
        .output_attrs                        = output_attrs};

    
    // join key is always int32_t, so no need to check its type
    join_algorithm.run();
      

    // this is filled by the run<T> method
    return results;
}


ExecuteResult execute_scan(const Plan& plan, const ScanNode& scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    
    auto table_id = scan.base_table_id;
    auto& input = plan.inputs[table_id];
    return my_copy_scan(input, output_attrs, (uint16_t)(table_id));
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