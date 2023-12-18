//
// Created by zhou822 on 5/23/23.
//
#pragma once
//#ifndef BWGRAPH_V2_TRANSACTION_TABLES_HPP
//#define BWGRAPH_V2_TRANSACTION_TABLES_HPP

#include <cstdint>
#include <atomic>
#include <vector>
#include <unordered_set>
#include <map>
#include "graph_global.hpp"
#include "exceptions.hpp"
#include "../Libraries/parallel_hashmap/phmap.h"
#include "utils.hpp"

namespace bwgraph {
#define TXN_TABLE_TEST false
    class BwGraph;
    class GarbageBlockQueue;
    struct touched_block_entry{
        touched_block_entry(vertex_t vertex_id, label_t label, uint64_t input_version):block_id(generate_block_id(vertex_id,label)), block_version_num(input_version){
        }
        touched_block_entry(vertex_t input_id, uint64_t input_version):block_id(input_id),block_version_num(input_version){}
        uint64_t block_id;
        uint64_t block_version_num;//a safety mark, to check if scan is needed
    };

#if USING_ARRAY_TABLE
    struct alignas(64) txn_table_entry {
        txn_table_entry(){
            txn_id.store(0,std::memory_order_release);
            op_count.store(0,std::memory_order_release);
            status.store(IN_PROGRESS,std::memory_order_release);
        }

        explicit txn_table_entry(uint64_t new_txn_id) {
            txn_id.store(new_txn_id,std::memory_order_release);
            op_count.store(0,std::memory_order_release);
            status.store(IN_PROGRESS,std::memory_order_release);
        }

        txn_table_entry(const txn_table_entry &other) {
            txn_id.store(other.txn_id.load(std::memory_order_acquire),std::memory_order_release);
            status.store(other.status.load(std::memory_order_acquire),std::memory_order_release);
            op_count.store(other.op_count.load(std::memory_order_acquire),std::memory_order_release);
        }

        txn_table_entry &operator=(const txn_table_entry &other) {
            txn_id.store(other.txn_id.load(std::memory_order_acquire),std::memory_order_release);
            status.store(other.status.load(std::memory_order_acquire),std::memory_order_release);
            op_count.store(other.op_count.load(std::memory_order_acquire),std::memory_order_release);
            return *this;
        }
        //we do not eagerly delete entries, just check if the next entry has op_count == 0.
        inline bool reduce_op_count(int64_t num) {
            auto result = op_count.fetch_sub(num,std::memory_order_acq_rel);
            if(result ==0 && num ==1){
                throw std::runtime_error("lazy update error");
            }
            if(result == num){
            //if (op_count.fetch_sub(num,std::memory_order_acq_rel) == num) {
                return true;
            }
#if TXN_TABLE_TEST
            if (op_count.load(std::memory_order_acquire) < 0) {
                std::cout<<num<<std::endl;
                throw TransactionTableOpCountException();
            }
#endif
            return false;
        }

        inline void commit(uint64_t ts) {
            status.store(ts,std::memory_order_release);
        }

        inline void abort() {
            status.store(ABORT,std::memory_order_release);
        }

        std::atomic_uint64_t txn_id{};
        std::atomic_uint64_t status{};
        std::atomic_int64_t op_count{};
        std::vector<touched_block_entry> touched_blocks;
        char padding[16]{};
       //std::map<touched_block_entry,LockOffsetCache> touched_blocks;
       // char padding[16]
    };
    static_assert(sizeof(txn_table_entry) == 64);
#else
    struct txn_table_entry {
        txn_table_entry():txn_id(0),op_count(0) {
            status.store(IN_PROGRESS);
        };

        txn_table_entry(uint64_t new_txn_id) : txn_id(new_txn_id),op_count(0) {
            status.store(IN_PROGRESS);
        }

        txn_table_entry(const txn_table_entry &other) {
            txn_id.store(other.txn_id.load());
            status.store(other.status.load());
            op_count.store(other.op_count.load());
        }

        txn_table_entry &operator=(const txn_table_entry &other) {
            txn_id.store(other.txn_id.load());
            status.store(other.status.load());
            op_count.store(other.op_count.load());
            return *this;
        }
        //we do not eagerly delete entries, just check if the next entry has op_count == 0.
        inline bool reduce_op_count(int64_t num) {
            if (op_count.fetch_sub(num) == num) {
                return true;
            }
#if TXN_TABLE_TEST
            if (op_count.load() < 0) {
                throw TransactionTableOpCountException();
            }
#endif
            return false;
        }

        inline void commit(uint64_t ts) {
            status.store(ts);
        }

        inline void abort() {
            status.store(ABORT);
        }

        std::atomic_uint64_t txn_id;
        std::atomic_uint64_t status;
        std::atomic_int64_t op_count;
#endif

    using entry_ptr = txn_table_entry *;
    using Map = phmap::parallel_flat_hash_map<
            uint64_t,
            entry_ptr ,
            phmap::priv::hash_default_hash<uint64_t>,
            phmap::priv::hash_default_eq<uint64_t>,
            std::allocator<std::pair<const uint64_t, entry_ptr>>,
            12,
            std::mutex>;
    class ConcurrentTransactionTable{
    public:
        ConcurrentTransactionTable(){}
        inline bool get_status(uint64_t txn_id,uint64_t& status_result){
            return local_table.if_contains(txn_id,[&status_result](typename Map::value_type& pair){status_result=pair.second->status.load();});
        }
        void reduce_op_count(uint64_t txn_id,int64_t op_count){
            entry_ptr ptr=nullptr;
#if TXN_TABLE_TEST
            if(!local_table.if_contains(txn_id,[&ptr](typename Map::value_type& pair){ ptr = pair.second;})){
                std::cerr<<"panic"<<std::endl;
            }
            if(ptr== nullptr||ptr->op_count<=0){
                throw TransactionTableOpCountException();
            }
#else
            local_table.if_contains(txn_id,[&ptr](typename Map::value_type& pair){ptr = pair.second;});
#endif
            if(ptr->reduce_op_count(op_count)){
                local_table.erase(txn_id);
                delete ptr;//always safe, if op_count reaches 0 no other thread will access it
            }
        }
        inline entry_ptr put_entry(uint64_t txn_id){
            entry_ptr ptr = new txn_table_entry(txn_id);
#if TXN_TABLE_TEST
            if(local_table.contains(txn_id)){
                throw new std::runtime_error("duplicate transaction entry being inserted");
            }
#endif
            local_table.emplace(txn_id,ptr);
            return ptr;
        }
        inline void commit_txn(entry_ptr ptr, uint64_t op_count, uint64_t commit_ts){
#if TXN_TABLE_TEST
            if(!local_table.contains(ptr->txn_id)){
                throw TransactionTableMissingEntryException();
            }
#endif
            //todo:: do not commit txn in the table with no wrties?
            if(!op_count){
                local_table.erase(ptr->txn_id);
                delete ptr;
                return;
            }
            ptr->op_count = op_count;
            ptr->status.store(commit_ts);
        }
        inline void abort_txn(entry_ptr ptr, uint64_t op_count){
            if(!op_count){
                local_table.erase(ptr->txn_id);
                delete ptr;
                return;
            }
            ptr->op_count.store(op_count);
            ptr->status.store(ABORT);
        }
        inline uint64_t generate_txn_id(uint8_t thread_id){
            return generate_txnID(thread_id,offset++);
        }
    private:
        uint64_t offset;
        Map local_table;
    };
    class ConcurrentTransactionTables {
    public:
        ConcurrentTransactionTables()
        {
            tables.reserve(worker_thread_num);
            for(uint32_t i=0;i<worker_thread_num;i++){
                tables.push_back(ConcurrentTransactionTable());
            }
        }
        inline bool get_status(uint64_t txn_id,uint64_t& status_result){
            uint8_t thread_id = get_threadID(txn_id);
            return tables[thread_id].get_status(txn_id,status_result);
        }
        inline void reduce_op_count(uint64_t txn_id,int64_t op_count){
            uint8_t thread_id = get_threadID(txn_id);
            tables[thread_id].reduce_op_count(txn_id, op_count);
        }
        inline entry_ptr put_entry(uint64_t txn_id){
            uint8_t thread_id = get_threadID(txn_id);
            return tables[thread_id].put_entry(txn_id);
        }
        //only read-write transactions are stored in the table, if a read-write transaction commits, it must has op_count>0
        //todo: check if this statement is true, can there be a read-write transaction that commits without any writes.
        inline void commit_txn(entry_ptr ptr, uint64_t op_count, uint64_t commit_ts){
#if TXN_TABLE_TEST
            if(op_count<=0){
                throw TransactionTableOpCountException();
            }
#endif
            ptr->op_count.store(op_count);
            ptr->status.store(commit_ts);
        }
        inline void commit_txn_with_no_writes(entry_ptr ptr){
            uint8_t thread_id = get_threadID(ptr->txn_id);
            tables[thread_id].commit_txn(ptr,0,0);
        }
        inline void abort_txn(entry_ptr ptr, uint64_t op_count){
            uint8_t thread_id = get_threadID(ptr->txn_id);
            tables[thread_id].abort_txn(ptr,op_count);
        }

    private:
        std::vector<ConcurrentTransactionTable>tables;
    };

    //now we program the array based transaction tables
    class ArrayTransactionTables;

    using Array = std::vector<txn_table_entry>; //std::array<txn_table_entry,per_thread_table_size>;
    class ArrayTransactionTable{
    public:
        ArrayTransactionTable():offset(0),bwGraph(nullptr),txn_tables(nullptr){local_table.resize(per_thread_table_size);}
        ArrayTransactionTable(BwGraph* source_graph, ArrayTransactionTables* all_tables):offset(0),bwGraph(source_graph),txn_tables(all_tables){local_table.resize(per_thread_table_size);}
        inline bool get_status(uint64_t txn_id, uint64_t& status_result){
#if TXN_TABLE_TEST
            uint64_t index = get_local_txn_id(txn_id)%per_thread_table_size;
            uint64_t to_compare_index = txn_id % per_thread_table_size;
            if(index!=to_compare_index){
                throw std::runtime_error("error, should not use this approach");
            }
#else
            uint64_t index =txn_id % per_thread_table_size;
#endif

            status_result= local_table[index].status.load(std::memory_order_acquire);
            return local_table[index].txn_id.load(std::memory_order_acquire) == txn_id;
        }
        //only invoked at the end to check lazy update progress
        inline bool is_empty(){
            for(uint32_t i=0; i<per_thread_table_size;i++){
                if(local_table[i].op_count.load(std::memory_order_acquire)){
                    std::cout<<"txn id is "<<local_table[i].txn_id<<" status is "<<local_table[i].status<<" op_count is "<<local_table[i].op_count<<" touched block count is "<<local_table[i].touched_blocks.size()<<std::endl;
                    return false;
                }
            }
            return true;
        }
        //reduce op_count no longer deletes entry, it only reduces op_count to 0 at most. And an entry with no op_count becomes a candidate for new entry.
        inline void reduce_op_count(uint64_t txn_id,int64_t op_count){
#if TXN_TABLE_TEST
            //std::cout<<txn_id<<" "<<op_count<<std::endl;
            uint64_t index = get_local_txn_id(txn_id)%per_thread_table_size;
            uint64_t to_compare_index = txn_id % per_thread_table_size;
            if(index!=to_compare_index){
                throw std::runtime_error("error, should not use this approach");
            }
#else
            uint64_t index =txn_id % per_thread_table_size;
#endif
#if TXN_TABLE_TEST
            if(local_table[index].txn_id.load(std::memory_order_acquire)!=txn_id){
                throw LazyUpdateException();
            }
            if(local_table[index].status.load(std::memory_order_acquire)==IN_PROGRESS){
                throw std::runtime_error("error, try to reduce operation count of an in progress transaction");
            }
            if(local_table[index].op_count.load(std::memory_order_acquire)<op_count){
                throw std::runtime_error("error, reduce op count is greater than remaining op count");
            }
#endif
            local_table[index].reduce_op_count(op_count);
        }
        inline entry_ptr put_entry(uint64_t txn_id){
#if TXN_TABLE_TEST
            uint64_t index = get_local_txn_id(txn_id)%per_thread_table_size;
            uint64_t to_compare_index = txn_id % per_thread_table_size;
            if(index!=to_compare_index){
                throw std::runtime_error("error, should not use this approach");
            }
#else
            uint64_t index =txn_id % per_thread_table_size;
#endif
#if TXN_TABLE_TEST
            //assert(!local_table[index].txn_id.load());
            if(local_table[index].op_count.load(std::memory_order_acquire)){
                throw TransactionTableOpCountException();
            }
#endif
            local_table[index].txn_id.store(txn_id,std::memory_order_release);
            local_table[index].status.store(IN_PROGRESS,std::memory_order_release);
            local_table[index].touched_blocks.clear();
            return &local_table[index];
        }
        inline void commit_txn(entry_ptr ptr, uint64_t op_count, uint64_t commit_ts){
            ptr->op_count.store(op_count,std::memory_order_release);
            ptr->status.store(commit_ts,std::memory_order_release);
        }
        inline void abort_txn(entry_ptr ptr, uint64_t op_count){
            ptr->op_count.store(op_count,std::memory_order_release);
            ptr->status.store(ABORT,std::memory_order_release);
        }
        inline void set_bwgraph(BwGraph* source_graph){
            bwGraph = source_graph;
        }
        inline void set_txn_tables(ArrayTransactionTables* all_tables){
            txn_tables = all_tables;
        }
        void resize(uint64_t entry_num){
            local_table.resize(entry_num);
        }
        void eager_clean(uint64_t index);
        void eager_clean(uint64_t index, std::unordered_set<uint64_t>& cleaned_blocks);
        void range_eager_clean(uint64_t index);
        inline void set_thread_id(uint8_t id){thread_id=id;}

        inline uint64_t generate_txn_id(){
            uint64_t index = offset%per_thread_table_size;
#if TXN_TABLE_TEST
            if(offset>per_thread_table_size&&local_table[index].status.load(std::memory_order_acquire)==IN_PROGRESS){
                throw std::runtime_error("error, the txn did not get a final state");
            }
#endif
            if(local_table[index].op_count.load(std::memory_order_acquire)){
                eager_clean(index);
            }
            uint64_t new_txn_id = bwgraph::generate_txnID(thread_id,offset);
            offset++;
            return new_txn_id;
        }

        inline uint64_t calculate_range_clean_index(uint64_t index){
            auto division = index/clean_threshold;//division can be 0, 1, 2, 3
            if(division){
                return (division-1)*clean_threshold;
            }else{
                return 3*clean_threshold;
            }
        }
        inline uint64_t periodic_clean_generate_txn_id(){
            uint64_t index = offset%per_thread_table_size;
            if(!(index%clean_threshold)){
                range_eager_clean(calculate_range_clean_index(index));
            }
#if TXN_TABLE_TEST
            if(offset>per_thread_table_size&&local_table[index].status.load(std::memory_order_acquire)==IN_PROGRESS){
                throw std::runtime_error("error, the txn did not get a final state");
            }
#endif
          /*  if(local_table[index].op_count.load()){
                eager_clean(index);
            }*/
            uint64_t new_txn_id = bwgraph::generate_txnID(thread_id,offset);
            offset++;
            return new_txn_id;
        }
        //a necessary function for worker thread setup
        inline void set_garbage_queue(GarbageBlockQueue* input_queue){thread_local_garbage_queue = input_queue;}
        //put_entry, abort and commit txn don't need to be accessed by other threads
    private:
        void lazy_update_block(uintptr_t block_ptr);
       //todo::for debug, add more values
       int32_t lazy_update_block(uintptr_t block_ptr, uint64_t txn_id);
        uint8_t thread_id;
        uint64_t offset;
        Array local_table;
        BwGraph* bwGraph;
        ArrayTransactionTables* txn_tables;
        GarbageBlockQueue* thread_local_garbage_queue;
    };
    class ArrayTransactionTables{
    public:
        ArrayTransactionTables(BwGraph* source_graph){
            for(uint8_t i=0; i<worker_thread_num;i++){
                tables.emplace_back();
                tables[i].set_thread_id(i);
                tables[i].set_bwgraph(source_graph);
                tables[i].set_txn_tables(this);
                //compatible with vector based table
            }
        }
        inline bool get_status(uint64_t txn_id,uint64_t& status_result){
          /*  if(!(txn_id&TS_ID_MASK)){
                std::cout<<txn_id<<std::endl;
            }*/
            uint8_t thread_id = bwgraph::get_threadID(txn_id);
            return tables[thread_id].get_status(txn_id,status_result);
        }
        inline void reduce_op_count(uint64_t txn_id,int64_t op_count){
            if(!(txn_id&TS_ID_MASK))[[unlikely]]{
                throw LazyUpdateException();
            }
            uint8_t thread_id = bwgraph::get_threadID(txn_id);
            tables[thread_id].reduce_op_count(txn_id, op_count);
        }
        inline void commit_txn(entry_ptr ptr, int64_t op_count, uint64_t commit_ts){
            ptr->op_count.store(op_count,std::memory_order_release);
            ptr->status.store(commit_ts,std::memory_order_release);
        }
        inline void abort_txn(entry_ptr ptr, int64_t op_count){
            ptr->op_count.store(op_count,std::memory_order_release);
            ptr->status.store(ABORT,std::memory_order_release);
        }
        inline ArrayTransactionTable& get_table(uint8_t thread_id){
            return tables[thread_id];
        }
        void resize(uint64_t thread_num, BwGraph* source_graph){
            tables.clear();
            for(uint8_t i=0; i<thread_num;i++){
                tables.emplace_back();
                tables[i].set_thread_id(i);
                tables[i].set_bwgraph(source_graph);
                tables[i].set_txn_tables(this);
                //compatible with vector based table
            }
        }
    private:
        std::vector<ArrayTransactionTable> tables;
        //std::array<ArrayTransactionTable,worker_thread_num> tables;
    };

# if USING_ARRAY_TABLE
    using TxnTables = ArrayTransactionTables;
#else
    using TxnTables = ConcurrentTransactionTables;
#endif

}//namespace bwgraph

//#endif //BWGRAPH_V2_TRANSACTION_TABLES_HPP
