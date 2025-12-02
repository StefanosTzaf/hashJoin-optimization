#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <inner_column.h>
#include "LateMaterialization.h"
#include "ColumnStore.h"

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

        // STEP 1: the hash table for joining: type of join key, vector of row indexes that contain the key: 
        // one key might correspond to multiple rows
        std::unordered_map<int32_t, std::vector<size_t>> hash_table;

        for(size_t i = 0; i < output_attrs.size(); i++){
            DataType type = std::get<1>(output_attrs[i]);
            results.emplace_back(std::move(ColumnT(type)));
        }

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
            size_t  idx       = 0; // row index

            // iterates over all pages of column with join key
            for (const Page* page: keyColumn.getPages()) {    

                uint16_t numRows = *reinterpret_cast<const uint16_t*>(page->data);

                for(size_t row = 0; row < numRows; row++) {
                    
                    const value_t& val = *reinterpret_cast<const value_t*>(page->data + sizeof(uint16_t) + row * sizeof(value_t));
                    if(val.is_null()){
                        idx++;
                        continue;
                    }
                    
                    int32_t key = val.get_int();
                
                    if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                        // append idx to the appropriate vector of the hash table
                        hash_table.emplace(key, std::vector<size_t>(1, idx));
    
                    // if not found, create new entry in hash table with idx
                    } else {
                        itr->second.push_back(idx);
                    }         
    
                    idx++;
                }


            }

            // STEP 3 PROBE PHASE: take keys of right table, calculate hash value
            // and if it exists store it in temp_results (vector of columns)

            ColumnT& probeKeyColumn = right[right_col]; // column with join key
            size_t right_idx = 0; //row index for right table

            // iterate through all pages of column with join key
            for (const Page* page: probeKeyColumn.getPages()) {
                
                const uint16_t numRows = *reinterpret_cast<const uint16_t*>(page->data);

                for(size_t row = 0; row < numRows; row++) {
                
                    const value_t& val = *reinterpret_cast<const value_t*>(page->data + sizeof(uint16_t) + row * sizeof(value_t));
                    if(val.is_null()){
                        right_idx++;
                        continue;
                    }
                   
                    int32_t key = val.get_int();
                
                    // for every matching key, find all matching build-side rows
                    if (auto itr = hash_table.find(key); itr != hash_table.end()) {

                        // for each matching left row index from
                        // itr->second: the vector with row indices
                        for (auto left_idx: itr->second) {               
                           
                            size_t output_col = 0;

                            // for each column index we want in the final result
                            for (auto [col_idx, _]: output_attrs) {

                                value_t* val = NULL;

                                // if index < number of columns (wanted column is on left table)
                                if (col_idx < left.size()) {
                                    val = left[col_idx].getValueAt(left_idx);
                                } 
                                // wanted column is on right table
                                else {
                                    val = right[col_idx - left.size()].getValueAt(right_idx);
                                }

                                if(val == NULL){
                                    continue;
                                }

                                inserters[output_col].insert(*val);
                                
                                output_col++;
                            }
                        }
                    }
                    right_idx++;
                }
                
            }

        // STEP 5: BUILD ON RIGHT TABLE
        // PROBE WITH LEFT TABLE, COMBINE MATCHES
        } else {

            if(right.empty() || (right[right_col].getSize() == 0)){
                return;
            }

            ColumnT& keyColumn = right[right_col];
            size_t idx = 0; // row index

            for (const Page* page: keyColumn.getPages()) {

                const uint16_t numRows = *reinterpret_cast<const uint16_t*>(page->data);

                for(size_t row = 0; row < numRows; row++){
                    
                    const value_t& val = *reinterpret_cast<const value_t*>(page->data + sizeof(uint16_t) + row * sizeof(value_t));
                    if(val.is_null()){
                        idx++;
                        continue;
                    }
                    
                    int32_t key = val.get_int();
                
                    if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                        // append idx to the appropriate vector of the hash table
                        hash_table.emplace(key, std::vector<size_t>(1, idx));
    
                    // if not found, create new entry in hash table with idx
                    } else {
                        itr->second.push_back(idx);
                    }         
    
                    idx++;
                }
            }

            ColumnT& probeKeyColumn = left[left_col];
            size_t left_idx = 0; //row idx for left table

            for (const Page* page: probeKeyColumn.getPages()) {

                const uint16_t numRows = *reinterpret_cast<const uint16_t*>(page->data);

                for(size_t row = 0; row < numRows; row++){
                    
                    const value_t& val = *reinterpret_cast<const value_t*>(page->data + sizeof(uint16_t) + row * sizeof(value_t));
                    if(val.is_null()){
                        left_idx++;
                        continue;
                    }
                    
                    int32_t key = val.get_int();               
    

                    if (auto itr = hash_table.find(key); itr != hash_table.end()) {
                        
                        for (auto right_idx: itr->second) {

                            size_t output_col = 0;
                                        
                            for (auto [col_idx, _]: output_attrs) {

                                value_t* val = NULL;
                        
                                if (col_idx < left.size()) {
                                    val = left[col_idx].getValueAt(left_idx);
                              } 
                                else {
                                    val = right[col_idx - left.size()].getValueAt(right_idx);
                                }

                                inserters[output_col].insert(*val);
                                output_col++;
                            }

                        }
                    }
                    left_idx++;
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