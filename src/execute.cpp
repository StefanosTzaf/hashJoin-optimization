#include <hardware.h>
#include <plan.h>
#include <table.h>
#include "robinHood.h"
#include <algorithm>

namespace Contest {

// similar to typedef: write ExecuteResult instead of std::vector<std::vector<Data>>
// this is row-based
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
    // std::unordered_map<T, std::vector<size_t>> hash_table;
 
         // STEP 2 BUILD PHASE: WE TAKE THE ROWS OF LEFT TABLE AND 
        // CALCULATE THE HASH VALUE OF EACH KEY AND STORE IT IN THE HASH TABLE
        // if we build on the left table
        if (build_left) {
            RobinHoodHashTable<T, size_t> hash_table(left.size() * 2); // set initial capacity


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

                            // // try to find the key in the hash table
                            // if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                            //     // append idx to the appropriate vector of the hash table
                            //     hash_table.emplace(key, std::vector<size_t>(1, idx));

                            // // if not found, create new entry in hash table with idx
                            // } else {
                            //     itr->second.push_back(idx);
                            // }

                            // insert the row index; RobinHood stores a vector internally
                            hash_table.hashInsert(key, idx);
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
                            if (auto itr = hash_table.hashSearch(key); !itr.empty()) {

                                // for each matching left row index from
                                // itr->second: the vector with row indices
                                   for (auto left_idx: itr) {
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

            RobinHoodHashTable<T, size_t> hash_table(right.size() * 2); // set initial capacity

            for (auto&& [idx, record]: right | views::enumerate) {
                std::visit(
                    [&hash_table, idx = idx](const auto& key) {
                        using Tk = std::decay_t<decltype(key)>;
                        if constexpr (std::is_same_v<Tk, T>) {
                            // if (auto itr = hash_table.find(key); itr == hash_table.end()) {
                            //     hash_table.emplace(key, std::vector<size_t>(1, idx));
                            // } else {
                            //     itr->second.push_back(idx);
                            // }
                            // insert the row index; RobinHood stores a vector internally
                            hash_table.hashInsert(key, idx);
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
                            if (auto itr = hash_table.hashSearch(key); !itr.empty()) {
                                for (auto right_idx: itr) {
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
    
        if (join.build_left) {
            // check data type of join key of left table
            switch (std::get<1>(left_types[join.left_attr])) {
            case DataType::INT32:   join_algorithm.run<int32_t>(); break;
            case DataType::INT64:   join_algorithm.run<int64_t>(); break;
            case DataType::FP64:    join_algorithm.run<double>(); break;
            case DataType::VARCHAR: join_algorithm.run<std::string>(); break;
        }
    } else {
        switch (std::get<1>(right_types[join.right_attr])) {
        case DataType::INT32:   join_algorithm.run<int32_t>(); break;
        case DataType::INT64:   join_algorithm.run<int64_t>(); break;
        case DataType::FP64:    join_algorithm.run<double>(); break;
        case DataType::VARCHAR: join_algorithm.run<std::string>(); break;
        }
    }

    // this is filled by the run<T> method
    return results;
}

ExecuteResult execute_scan(const Plan& plan, const ScanNode& scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    
    auto table_id = scan.base_table_id;
    auto& input = plan.inputs[table_id];
    return Table::copy_scan(input, output_attrs);
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