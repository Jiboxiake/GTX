//
// Created by zhou822 on 5/29/23.
//

//#ifndef BWGRAPH_V2_EDGE_ITERATOR_HPP
//#define BWGRAPH_V2_EDGE_ITERATOR_HPP
#pragma once

#include "block.hpp"
#include "bwgraph.hpp"
//#include "block_access_ts_table.hpp"
//#include "block_manager.hpp"
#include "utils.hpp"
#include "exceptions.hpp"

namespace bwgraph {
    //We abandoned this design
    //only at rare occasions txn needs to read 2 blocks
    /*
     * 3 scenarios:
     * 1. read the current block, most transactions should do that: read_ts>= block.creation_ts
     * 2. read the previous block, read_ts < block.creation_ts
     * 3. read both current and previous block, read_ts < block.creation_ts and has_deltas
     */
    class EdgeDeltaIterator {
    public:
        EdgeDeltaIterator() {}//empty iterator
        //when iterator ends, need to exit the block protection
        void close() {
            if (block_access_ts_table)
                block_access_ts_table->release_block_access(get_threadID(txn_id));
        }

        //give the current block, determine what to read
        EdgeDeltaIterator(EdgeDeltaBlockHeader *input_block, timestamp_t input_ts, uint64_t input_id, bool has_deltas,
                          uint32_t input_offset, BwGraph &source_graph, lazy_update_map *lazy_update_record_ptr,
                          BlockAccessTimestampTable *access_table) : current_delta_block(input_block),
                                                                     txn_read_ts(input_ts), txn_id(input_id),
                                                                     current_delta_offset(input_offset),
                                                                     txn_tables(&source_graph.get_txn_tables()),
                                                                     block_manager(&source_graph.get_block_manager()),
                                                                     txn_lazy_update_records(lazy_update_record_ptr),
                                                                     block_access_ts_table(access_table) {
            if (txn_read_ts >= current_delta_block->get_creation_time() || has_deltas) {
                read_current_block = true;
                current_delta = current_delta_block->get_edge_delta(current_delta_offset);
            }
            if (txn_read_ts < current_delta_block->get_creation_time()) {
                read_previous_block = true;
                //if no need to read the current block, we just always start from previous block
                if (!read_current_block) {
                    while (txn_read_ts < current_delta_block->get_creation_time()) {
                        if (current_delta_block->get_previous_ptr()) {
                            current_delta_block = block_manager->convert<EdgeDeltaBlockHeader>(
                                    current_delta_block->get_previous_ptr());
                        } else {
                            break;
                            //throw EdgeIteratorNoBlockToReadException();
                        }
                    }
                    auto previous_block_offset = current_delta_block->get_current_offset();
                    current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                    current_delta = current_delta_block->get_edge_delta(current_delta_offset);
                }
            }
            //todo::if it is null, then there just does not exist any visible blocks at all.
            /* if(current_delta== nullptr){
                 throw EdgeIteratorNoBlockToReadException();
             }*/
        }

        BaseEdgeDelta *next_delta() {
            //keep scanning the current block with lazy update, when the current block is exhausted, set "read_current_block" to false and move on
            if (read_current_block) {
                //scan the current block, return pointers as appropriate, then maybe switch to the previous block
                while (current_delta_offset > 0) {
                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    if (!original_ts)[[unlikely]] {
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        current_delta++;
                        continue;
                    }
#if EDGE_DELTA_TEST
                    if(!original_ts){
                        throw LazyUpdateException();
                    }
#endif
                    if (is_txn_id(original_ts)) {
                        uint64_t status = 0;
                        if (txn_tables->get_status(original_ts, status)) {
                            if (status == IN_PROGRESS) {
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                current_delta++;
                                continue;
                            } else {
                                if (status != ABORT) {
#if CHECKED_PUT_EDGE
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,
                                                                                             current_delta->previous_version_offset,
                                                                                             status);
#else
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
#endif
                                    if (current_delta->lazy_update(original_ts, status)) {
#if LAZY_LOCKING
                                        if(current_delta->is_last_delta.load()){
                                            current_delta_block-> release_protection(current_delta->toID);
                                        }
#endif
                                        //record lazy update
                                        record_lazy_update_record(txn_lazy_update_records, original_ts);
                                    }
                                }
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                    throw LazyUpdateException();
                                }
#endif
                            }
                        }
                    }
                    if (current_delta->creation_ts.load(std::memory_order_acquire) == txn_id &&
                        current_delta->invalidate_ts.load(std::memory_order_acquire) != txn_id &&
                        current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        return current_delta++;
                    }
                    //cannot be delete delta
                    if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA &&
                        (current_delta->creation_ts.load(std::memory_order_acquire) <= txn_read_ts) &&
                        (current_delta->invalidate_ts.load(std::memory_order_acquire) == 0 ||
                         current_delta->invalidate_ts.load(std::memory_order_acquire) > txn_read_ts) &&
                        current_delta->invalidate_ts.load(std::memory_order_acquire) != txn_id) {
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        return current_delta++;
                    }
                    current_delta_offset -= ENTRY_DELTA_SIZE;
                    current_delta++;
                }
                read_current_block = false;
                while (txn_read_ts < current_delta_block->get_creation_time()) {
                    if (current_delta_block->get_previous_ptr()) {
                        current_delta_block = block_manager->convert<EdgeDeltaBlockHeader>(
                                current_delta_block->get_previous_ptr());
                    } else {
                        current_delta = nullptr;
                        return current_delta;
                    }
                }
                auto previous_block_offset = current_delta_block->get_current_offset();
                current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                current_delta = current_delta_block->get_edge_delta(current_delta_offset);
            }
            if ((!read_current_block) && read_previous_block) {
                while (current_delta_offset > 0) {
                    //abort deltas will always have larger creation ts than any read ts
                    if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA &&
                        current_delta->creation_ts.load(std::memory_order_acquire) <= txn_read_ts &&
                        (current_delta->invalidate_ts.load(std::memory_order_acquire) == 0 ||
                         current_delta->invalidate_ts.load(std::memory_order_acquire) > txn_read_ts)) {
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        return current_delta++;
                    }
                    current_delta_offset -= ENTRY_DELTA_SIZE;
                    current_delta++;
                }
            }
            return nullptr;
        }

        inline char *get_data(uint32_t offset) {
            return current_delta_block->get_edge_data(offset);
        }
    private:
        EdgeDeltaBlockHeader *current_delta_block;
        timestamp_t txn_read_ts;
        uint64_t txn_id;
        //bool txn_has_deltas;//whether this txn has deltas in the current delta block
        uint32_t current_delta_offset;
        bool read_current_block = false;
        bool read_previous_block = false;
        BaseEdgeDelta *current_delta = nullptr;
        //BaseEdgeDelta* current_visible_delta = nullptr;
        TxnTables *txn_tables;
        BlockManager *block_manager;
        lazy_update_map *txn_lazy_update_records;
        BlockAccessTimestampTable *block_access_ts_table = nullptr;//necessary at destructor, need to release the protection
    };

    //todo: ideally read only and read write transactions should use different iterators. Read write transactions need more checks
    /*
     * it simplifies the read/write transaction visibility issue, a transaction can only read at most 1 block
     */
    class SimpleEdgeDeltaIterator {
    public:
        SimpleEdgeDeltaIterator() {}//empty iterator
        SimpleEdgeDeltaIterator(TxnTables *input_txn_tables, BlockManager *input_block_manager,
                                lazy_update_map *input_lazy_map, BlockAccessTimestampTable *input_block_table,
                                timestamp_t input_ts, uint64_t input_id) : txn_read_ts(input_ts), txn_id(input_id),
                                                                           txn_tables(input_txn_tables),
                                                                           block_manager(input_block_manager),
                                                                           txn_lazy_update_records(input_lazy_map),
                                                                           block_access_ts_table(input_block_table) {}

        void fill_new_iterator(EdgeDeltaBlockHeader *input_block, uint32_t input_offset) {
            current_delta_block = input_block;
            current_delta_offset = input_offset;
            if (txn_read_ts >= current_delta_block->get_creation_time()) {
                read_current_block = true;
                latest_version_start_offset = current_delta_block->latest_version_delta_num()*LIGHT_DELTA_SIZE;
                normal_end_offset = current_delta_block->is_padded()*LIGHT_DELTA_SIZE+ latest_version_start_offset;
                if(current_delta_offset>normal_end_offset)[[likely]]{
                    current_delta = current_delta_block->get_edge_delta(current_delta_offset);
                }
                if(latest_version_start_offset)[[likely]]{
                    current_light_delta = current_delta_block->get_light_edge_delta(latest_version_start_offset);
                }
            } else /*(txn_read_ts<current_delta_block->get_creation_time())*/{
                block_access_ts_table->release_block_access(get_threadID(txn_id));
                bool found = false;
                while (current_delta_block->get_previous_ptr()) {
                    current_delta_block = block_manager->convert<EdgeDeltaBlockHeader>(
                            current_delta_block->get_previous_ptr());
                    if (txn_read_ts >= current_delta_block->get_creation_time()) {
                        found = true;
                        break;
                    }
                }
                if (found) {
#if EDGE_DELTA_TEST
                    if(txn_read_ts<current_delta_block->get_creation_time()){
                        throw TransactionReadException();
                    }
#endif
                    auto previous_block_offset = current_delta_block->get_current_offset();
                    current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                    latest_version_start_offset = current_delta_block->latest_version_delta_num()*LIGHT_DELTA_SIZE;
                    normal_end_offset = current_delta_block->is_padded()*LIGHT_DELTA_SIZE+ latest_version_start_offset;
                    if(current_delta_offset>normal_end_offset)[[likely]]{
                        current_delta = current_delta_block->get_edge_delta(current_delta_offset);
                    }
                    if(latest_version_start_offset)[[likely]]{
                        current_light_delta = current_delta_block->get_light_edge_delta(latest_version_start_offset);
                    }

                } else {
                    current_delta = nullptr;
                    current_delta_offset = 0;
                }
            }
        }

        //when iterator ends, need to exit the block protection
        void close() {
            if (block_access_ts_table)
                block_access_ts_table->release_block_access(get_threadID(txn_id));
        }

        //give the current block, determine what to read
        SimpleEdgeDeltaIterator(EdgeDeltaBlockHeader *input_block, timestamp_t input_ts, uint64_t input_id,
                                uint32_t input_offset, BwGraph &source_graph, lazy_update_map *lazy_update_record_ptr,
                                BlockAccessTimestampTable *access_table) : current_delta_block(input_block),
                                                                           txn_read_ts(input_ts), txn_id(input_id),
                                                                           current_delta_offset(input_offset),
                                                                           txn_tables(&source_graph.get_txn_tables()),
                                                                           block_manager(
                                                                                   &source_graph.get_block_manager()),
                                                                           txn_lazy_update_records(
                                                                                   lazy_update_record_ptr),
                                                                           block_access_ts_table(access_table) {
            if (txn_read_ts >= current_delta_block->get_creation_time()) {
                read_current_block = true;
                latest_version_start_offset = current_delta_block->latest_version_delta_num()*LIGHT_DELTA_SIZE;
                normal_end_offset = current_delta_block->is_padded()*LIGHT_DELTA_SIZE+ latest_version_start_offset;
                if(current_delta_offset>normal_end_offset)[[likely]]{
                    current_delta = current_delta_block->get_edge_delta(current_delta_offset);
                }
                if(latest_version_start_offset)[[likely]]{
                    current_light_delta = current_delta_block->get_light_edge_delta(latest_version_start_offset);
                }
            } else /*(txn_read_ts<current_delta_block->get_creation_time())*/{
                block_access_ts_table->release_block_access(get_threadID(txn_id));
                bool found = false;
                while (current_delta_block->get_previous_ptr()) {
                    current_delta_block = block_manager->convert<EdgeDeltaBlockHeader>(
                            current_delta_block->get_previous_ptr());
                    if (txn_read_ts >= current_delta_block->get_creation_time()) {
                        found = true;
                        break;
                    }
                }
                if (found) {
#if EDGE_DELTA_TEST
                    if(txn_read_ts<current_delta_block->get_creation_time()){
                        throw TransactionReadException();
                    }
#endif
                    auto previous_block_offset = current_delta_block->get_current_offset();
                    current_delta_offset = static_cast<uint32_t>(previous_block_offset & SIZE2MASK);
                    latest_version_start_offset = current_delta_block->latest_version_delta_num()*LIGHT_DELTA_SIZE;
                    normal_end_offset = current_delta_block->is_padded()*LIGHT_DELTA_SIZE+ latest_version_start_offset;
                    if(current_delta_offset>normal_end_offset)[[likely]]{
                        current_delta = current_delta_block->get_edge_delta(current_delta_offset);
                    }
                    if(latest_version_start_offset)[[likely]]{
                        current_light_delta = current_delta_block->get_light_edge_delta(latest_version_start_offset);
                    }
                } else {
                    current_delta = nullptr;
                    current_delta_offset = 0;
                }
            }
            //todo::if it is null, then there just does not exist any visible blocks at all.
            /* if(current_delta== nullptr){
                 throw EdgeIteratorNoBlockToReadException();
             }*/
        }

#if USING_SEPARATED_PREFETCH

        BaseEdgeDelta *next_delta() {
            //keep scanning the current block with lazy update, when the current block is exhausted, set "read_current_block" to false and move on
            if (read_current_block) {
                while (current_delta_offset > 0) {
                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    if (!original_ts)[[unlikely]] {
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        current_delta++;
                        continue;
                    }
#if EDGE_DELTA_TEST
                    if(!original_ts){
                        throw LazyUpdateException();
                    }
#endif
                    //only prefetch is current delta is not in progress
                    if (is_txn_id(original_ts)/*&&original_ts!=txn_id*/) {
                        uint64_t status = 0;
                        if (txn_tables->get_status(original_ts, status))[[likely]] {
                            if (status == IN_PROGRESS)[[likely]] {
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                current_delta++;
                                continue;
                            } else {
                                if (status != ABORT)[[likely]] {
#if CHECKED_PUT_EDGE
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,
                                                                                             current_delta->previous_version_offset,
                                                                                             status);
#else
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
#endif
                                    if (current_delta->lazy_update(original_ts, status)) {
#if LAZY_LOCKING
                                        if(current_delta->is_last_delta.load(std::memory_order_acquire)){
                                            current_delta_block-> release_protection(current_delta->toID);
                                        }
#endif
                                        //record lazy update
                                        record_lazy_update_record(txn_lazy_update_records, original_ts);
                                    }
                                }
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                    throw LazyUpdateException();
                                }
#endif
                                original_ts = status;
                            }
                        } else {
                            original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                        }
                        if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                            //uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                            uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                    std::memory_order_acquire);
                            //for debug
                            /*  if(!current_delta->toID){
                                  std::cout<<"error, current block is cleared"<<std::endl;
                                  current_delta->print_stats();
                                  throw std::runtime_error("error bad");
                              }*/
                            //cannot be the delta deleted by the current transaction
                            //visible committed delta
                            if (/*current_creation_ts*/original_ts <= txn_read_ts && (current_invalidation_ts == 0 ||
                                                                                      current_invalidation_ts >
                                                                                      txn_read_ts)) {
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                return current_delta++;
                            }
                            //visible delta by myself
                            //we currently omit this condition as the experiments only use read-only transactions to scan adjacency list
                            /*else if(current_creation_ts==txn_id)[[unlikely]]{
                                current_delta_offset-=  ENTRY_DELTA_SIZE;
                                return current_delta++;
                            }*/
                            // }//txn_id
                        }
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        current_delta++;
                    } else {
                        if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                            //uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                            uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                    std::memory_order_acquire);
                            //for debug
                            /*  if(!current_delta->toID){
                                  std::cout<<"error, current block is cleared"<<std::endl;
                                  current_delta->print_stats();
                                  throw std::runtime_error("error bad");
                              }*/
                            //cannot be the delta deleted by the current transaction
                            // if(current_invalidation_ts!=txn_id)[[likely]]{//txn_id
                            //visible committed delta
                            if (original_ts <= txn_read_ts && (current_invalidation_ts == 0 ||
                                                                                      current_invalidation_ts >
                                                                                      txn_read_ts)) {
                                current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                                //if(current_delta_offset>=prefetch_offset)
                                _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                                return current_delta++;
                            }
                            //we currently omit this condition as the experiments only use read-only transactions to scan adjacency list
                            //visible delta by myself
                            /*else if(current_creation_ts==txn_id)[[unlikely]]{
                                current_delta_offset-=  ENTRY_DELTA_SIZE;
                                _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
                                return current_delta++;
                            }*/

                        }
                        current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                        //if(current_delta_offset>=prefetch_offset)
                        _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                        current_delta++;
                    }


                }
            } else {
                while (current_delta_offset > 0) {
                    if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                        uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                        uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_relaxed);
                        //for debug
                        /* if(!current_delta->toID){
                             std::cout<<"error, previous block is cleared"<<std::endl;
                             current_delta->print_stats();
                             throw std::runtime_error("error bad");
                         }*/
                        if (current_creation_ts <= txn_read_ts &&
                            (current_invalidation_ts == 0 || current_invalidation_ts > txn_read_ts)) {
                            current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                            //if(current_delta_offset>=prefetch_offset)
                            _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                            return current_delta++;
                        }
                    }
                    current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                    //if(current_delta_offset>=prefetch_offset)
                    _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                    current_delta++;
                }
            }
            return nullptr;
        }
        inline bool valid(){
            return (return_result_delta||return_result_light_delta);
        }
        inline vertex_t dst_id(){
            if(return_result_delta){
                return return_result_delta->toID;
            }else if(return_result_light_delta){
                return return_result_light_delta->toID;
            }
#if EDGE_DELTA_TEST
            else{
                throw std::runtime_error("iterator should not reach here");
            }
#endif
        }
        inline std::string_view get_edge_data(){
            if(return_result_delta){
                if(return_result_delta->data_length<=16){
                    return {return_result_delta->data, return_result_delta->data_length};
                }else{
                    return {current_delta_block->get_edge_data(return_result_delta->data_offset),return_result_delta->data_length};
                }
            }else if(return_result_light_delta){
                return {current_delta_block->get_edge_data(return_result_light_delta->data_offset),return_result_light_delta->data_length};
            }
#if EDGE_DELTA_TEST
            else{
                throw std::runtime_error("iterator should not reach here");
            }
#endif
        }
        inline double get_weight(){
            if(current_delta){
                if(current_delta->data_length<=16){
                    return std::atof(current_delta->data);//{current_delta->data, current_delta->data_length};
                }else{
                    return std::atof(current_delta_block->get_edge_data(current_delta->data_offset));//{current_delta_block->get_edge_data(current_delta->data_offset),current_delta->data_length};
                }
            }else if(current_light_delta){
                return std::atof(current_delta_block->get_edge_data(current_light_delta->data_offset));//{current_delta_block->get_edge_data(current_light_delta->data_offset),current_light_delta->data_length};
            }
#if EDGE_DELTA_TEST
            else{
                throw std::runtime_error("iterator should not reach here");
            }
#endif
        }
        void next(){
            //keep scanning the current block with lazy update, when the current block is exhausted, set "read_current_block" to false and move on
            if (read_current_block) {
                //read normal blocks
                while (current_delta_offset > normal_end_offset) {
                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    if (!original_ts)[[unlikely]] {
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        current_delta++;
                        continue;
                    }
#if EDGE_DELTA_TEST
                    if(!original_ts){
                        throw LazyUpdateException();
                    }
#endif
                    //only prefetch is current delta is not in progress
                    if (is_txn_id(original_ts)/*&&original_ts!=txn_id*/) {
                        uint64_t status = 0;
                        if (txn_tables->get_status(original_ts, status))[[likely]] {
                            if (status == IN_PROGRESS)[[likely]] {
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                current_delta++;
                                continue;
                            } else {
                                if (status != ABORT)[[likely]] {
#if CHECKED_PUT_EDGE
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,
                                                                                             current_delta->previous_version_offset,
                                                                                             status);
#else
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
#endif
                                    if (current_delta->lazy_update(original_ts, status)) {
#if LAZY_LOCKING
                                        if(current_delta->is_last_delta.load(std::memory_order_acquire)){
                                            current_delta_block-> release_protection(current_delta->toID);
                                        }
#endif
                                        //record lazy update
                                        record_lazy_update_record(txn_lazy_update_records, original_ts);
                                    }
                                }
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                    throw LazyUpdateException();
                                }
#endif
                                original_ts = status;
                            }
                        } else {
                            original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                        }
                        if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                            //uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                            uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                    std::memory_order_acquire);
                            //for debug
                            /*  if(!current_delta->toID){
                                  std::cout<<"error, current block is cleared"<<std::endl;
                                  current_delta->print_stats();
                                  throw std::runtime_error("error bad");
                              }*/
                            //cannot be the delta deleted by the current transaction
                            //visible committed delta
                            if (/*current_creation_ts*/original_ts <= txn_read_ts && (current_invalidation_ts == 0 ||
                                                                                      current_invalidation_ts >
                                                                                      txn_read_ts)) {
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                return_result_delta = current_delta++;
                                return;
                            }
                            //visible delta by myself
                            //we currently omit this condition as the experiments only use read-only transactions to scan adjacency list
                            /*else if(current_creation_ts==txn_id)[[unlikely]]{
                                current_delta_offset-=  ENTRY_DELTA_SIZE;
                                return current_delta++;
                            }*/
                            // }//txn_id
                        }
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        current_delta++;
                    } else {
                        if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                            //uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                            uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                    std::memory_order_acquire);
                            //for debug
                            /*  if(!current_delta->toID){
                                  std::cout<<"error, current block is cleared"<<std::endl;
                                  current_delta->print_stats();
                                  throw std::runtime_error("error bad");
                              }*/
                            //cannot be the delta deleted by the current transaction
                            // if(current_invalidation_ts!=txn_id)[[likely]]{//txn_id
                            //visible committed delta
                            if (original_ts <= txn_read_ts && (current_invalidation_ts == 0 ||
                                                               current_invalidation_ts >
                                                               txn_read_ts)) {
                                current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                                //if(current_delta_offset>=prefetch_offset)
                                _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                                return_result_delta = current_delta++;
                                return;
                            }
                            //we currently omit this condition as the experiments only use read-only transactions to scan adjacency list
                            //visible delta by myself
                            /*else if(current_creation_ts==txn_id)[[unlikely]]{
                                current_delta_offset-=  ENTRY_DELTA_SIZE;
                                _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
                                return current_delta++;
                            }*/

                        }
                        current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                        //if(current_delta_offset>=prefetch_offset)
                        _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                        current_delta++;
                    }
                }
                current_delta = nullptr;
                return_result_delta = nullptr;
                if(current_delta_offset==normal_end_offset&&current_delta_offset!=latest_version_start_offset){
                    current_delta_offset-=LIGHT_DELTA_SIZE;
                }
                while(current_delta_offset>0){
                    if(current_light_delta->creation_ts<=txn_read_ts&&(current_light_delta->invalidate_ts.load(std::memory_order_acquire)>txn_read_ts||current_light_delta->invalidate_ts.load(std::memory_order_acquire)==0)){
                        //found delta
                        current_delta_offset-=LIGHT_DELTA_SIZE;
                        return_result_light_delta = current_light_delta++;
                        return;
                    }
                    current_light_delta++;
                    current_delta_offset-=LIGHT_DELTA_SIZE;
                }
                return_result_light_delta = nullptr;

            } else {//read previous block
                while (current_delta_offset > normal_end_offset) {
                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                        if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                            //uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                            uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                    std::memory_order_acquire);
                            //for debug
                            /*  if(!current_delta->toID){
                                  std::cout<<"error, current block is cleared"<<std::endl;
                                  current_delta->print_stats();
                                  throw std::runtime_error("error bad");
                              }*/
                            //cannot be the delta deleted by the current transaction
                            // if(current_invalidation_ts!=txn_id)[[likely]]{//txn_id
                            //visible committed delta
                            if (original_ts <= txn_read_ts && (current_invalidation_ts == 0 ||
                                                               current_invalidation_ts >
                                                               txn_read_ts)) {
                                current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                                //if(current_delta_offset>=prefetch_offset)
                                _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                                return_result_delta = current_delta++;
                                return;
                            }
                            //we currently omit this condition as the experiments only use read-only transactions to scan adjacency list
                            //visible delta by myself
                            /*else if(current_creation_ts==txn_id)[[unlikely]]{
                                current_delta_offset-=  ENTRY_DELTA_SIZE;
                                _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
                                return current_delta++;
                            }*/

                        }
                        current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                        //if(current_delta_offset>=prefetch_offset)
                        _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                        current_delta++;
                }
                current_delta = nullptr;
                return_result_delta = nullptr;
                if(current_delta_offset==normal_end_offset&&current_delta_offset!=latest_version_start_offset){
                    current_delta_offset-=LIGHT_DELTA_SIZE;
                }
                while(current_delta_offset>0){
                    if(current_light_delta->creation_ts<=txn_read_ts&&(current_light_delta->invalidate_ts.load(std::memory_order_acquire)>txn_read_ts||current_light_delta->invalidate_ts.load(std::memory_order_acquire)==0)){
                        //found delta
                        current_delta_offset-=LIGHT_DELTA_SIZE;
                        return_result_light_delta = current_light_delta++;
                        return;
                    }
                    current_light_delta++;
                    current_delta_offset-=LIGHT_DELTA_SIZE;
                }
                return_result_light_delta = nullptr;
            }
        }
        //An optimization for pagerank, but we did not use it eventually
        BaseEdgeDelta *next_delta_second_round() {
            //keep scanning the current block with lazy update, when the current block is exhausted, set "read_current_block" to false and move on
            if (read_current_block) {
                //use __builtin_expect
                //if(__builtin_expect(read_current_block,true)){
                //scan the current block, return pointers as appropriate, then maybe switch to the previous block
                while (current_delta_offset > 0) {
                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    if (!original_ts)[[unlikely]] {
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        current_delta++;
                        continue;
                    }
#if EDGE_DELTA_TEST
                    if(!original_ts){
                        throw LazyUpdateException();
                    }
#endif
                    //only prefetch is current delta is not in progress

                    if (original_ts <= txn_read_ts &&current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                        //uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                        uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_acquire);
                        //for debug
                        /*  if(!current_delta->toID){
                              std::cout<<"error, current block is cleared"<<std::endl;
                              current_delta->print_stats();
                              throw std::runtime_error("error bad");
                          }*/
                        //cannot be the delta deleted by the current transaction
                        //visible committed delta
                        if (/*current_creation_ts original_ts <= txn_read_ts &&*/ (current_invalidation_ts == 0 ||
                                                                                  current_invalidation_ts >
                                                                                  txn_read_ts)) {
                            current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                            //if(current_delta_offset>=prefetch_offset)
                            _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                            return current_delta++;
                        }
                        //visible delta by myself
                        //we currently omit this condition as the experiments only use read-only transactions to scan adjacency list
                        /*else if(current_creation_ts==txn_id)[[unlikely]]{
                            current_delta_offset-=  ENTRY_DELTA_SIZE;
                            _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
                            return current_delta++;
                        }*/
                        // }//txn_id
                    }
                    current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                    //if(current_delta_offset>=prefetch_offset)
                    _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                    current_delta++;


                }
            } else {
                while (current_delta_offset > 0) {
                    //abort deltas will always have larger creation ts than any read ts
                    if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                        uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                        uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_relaxed);
                        //for debug
                        /* if(!current_delta->toID){
                             std::cout<<"error, previous block is cleared"<<std::endl;
                             current_delta->print_stats();
                             throw std::runtime_error("error bad");
                         }*/
                        if (current_creation_ts <= txn_read_ts &&
                            (current_invalidation_ts == 0 || current_invalidation_ts > txn_read_ts)) {
                            current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                            //if(current_delta_offset>=prefetch_offset)
                            _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                            return current_delta++;
                        }
                    }
                    current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                    //if(current_delta_offset>=prefetch_offset)
                    _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                    current_delta++;
                }
            }
            return nullptr;
        }

#else //USING_SEPARATED_PREFETCH
        BaseEdgeDelta *next_delta() {
            //keep scanning the current block with lazy update, when the current block is exhausted, set "read_current_block" to false and move on
            if(read_current_block){
            //use __builtin_expect
            //if(__builtin_expect(read_current_block,true)){
                //scan the current block, return pointers as appropriate, then maybe switch to the previous block
                while(current_delta_offset>0){
                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    if(!original_ts)[[unlikely]]{
                        current_delta_offset-=ENTRY_DELTA_SIZE;
#if USING_PREFETCH
                      //  _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
#endif
                        current_delta++;
                        continue;
                    }
#if EDGE_DELTA_TEST
                    if(!original_ts){
                        throw LazyUpdateException();
                    }
#endif
                    //only prefetch is current delta is not in progress
                    if(is_txn_id(original_ts)/*&&original_ts!=txn_id*/){
                        uint64_t status=0;
                        if(txn_tables->get_status(original_ts,status))[[likely]]{
                            if(status == IN_PROGRESS)[[likely]]{
                                current_delta_offset-=ENTRY_DELTA_SIZE;
#if USING_PREFETCH
                             //   _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
#endif
                                current_delta++;
                                continue;
                            }else{
                                if(status!=ABORT)[[likely]]{
#if CHECKED_PUT_EDGE
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_version_offset,status);
#else
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
#endif
                                    if(current_delta->lazy_update(original_ts,status)){
#if LAZY_LOCKING
                                        if(current_delta->is_last_delta.load(std::memory_order_acquire)){
                                            current_delta_block-> release_protection(current_delta->toID);
                                        }
#endif
                                        //record lazy update
                                        record_lazy_update_record(txn_lazy_update_records,original_ts);
                                    }
                                }
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                    throw LazyUpdateException();
                                }
#endif
                                original_ts= status;//not in progress
                            }
                        }
                    }
                    if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                        //uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                        uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_acquire);
                        //for debug
                        /*  if(!current_delta->toID){
                              std::cout<<"error, current block is cleared"<<std::endl;
                              current_delta->print_stats();
                              throw std::runtime_error("error bad");
                          }*/
                        //cannot be the delta deleted by the current transaction
                        // if(current_invalidation_ts!=txn_id)[[likely]]{//txn_id
                        //visible committed delta
                        if(/*current_creation_ts*/original_ts<=txn_read_ts&&(current_invalidation_ts==0||current_invalidation_ts>txn_read_ts)){
                            current_delta_offset-=  ENTRY_DELTA_SIZE;
#if USING_PREFETCH
                            _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
#endif
                            return current_delta++;
                        }
                        //visible delta by myself
                        /*else if(current_creation_ts==txn_id)[[unlikely]]{
                            current_delta_offset-=  ENTRY_DELTA_SIZE;
                            _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
                            return current_delta++;
                        }*/
                        // }//txn_id
                    }
                    _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
                    current_delta_offset-=ENTRY_DELTA_SIZE;
                    current_delta++;

                }
            }else{
                while(current_delta_offset>0){
                    //abort deltas will always have larger creation ts than any read ts
               /*     if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA&&current_delta->creation_ts.load()<=txn_read_ts&&(current_delta->invalidate_ts==0||current_delta->invalidate_ts>txn_read_ts)){
                        current_delta_offset-=  ENTRY_DELTA_SIZE;
                        return current_delta++;
                    }*/
                    if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                        uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_relaxed);
                        uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(std::memory_order_relaxed);
                        //for debug
                       /* if(!current_delta->toID){
                            std::cout<<"error, previous block is cleared"<<std::endl;
                            current_delta->print_stats();
                            throw std::runtime_error("error bad");
                        }*/
                        if(current_creation_ts<=txn_read_ts&&(current_invalidation_ts==0||current_invalidation_ts>txn_read_ts)){
                            current_delta_offset-=  ENTRY_DELTA_SIZE;
#if USING_PREFETCH
                            //another manual prefetch
                            //if(current_delta_offset>8*ENTRY_DELTA_SIZE){
                                _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
                            //}
#endif
                            return current_delta++;
                        }
                    }
#if USING_PREFETCH
                    //another manual prefetch
                    //if(current_delta_offset>8*ENTRY_DELTA_SIZE){
                        _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
                    //}
#endif
                    current_delta_offset-=ENTRY_DELTA_SIZE;
                    current_delta++;
                }
            }
           /* if((!read_current_block)&&read_previous_block){
                while(current_delta_offset>0){
                    //abort deltas will always have larger creation ts than any read ts
                    if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA&&current_delta->creation_ts.load()<=txn_read_ts&&(current_delta->invalidate_ts==0||current_delta->invalidate_ts>txn_read_ts)){
                        current_delta_offset-=  ENTRY_DELTA_SIZE;
                        return current_delta++;
                    }
                    current_delta_offset-=ENTRY_DELTA_SIZE;
                    current_delta++;
                }
            }*/
            return nullptr;
        }
#endif //USING_SEPARATED_PREFETCH

        uint64_t vertex_degree() {
            uint64_t edge_count = 0;
            //keep scanning the current block with lazy update, when the current block is exhausted, set "read_current_block" to false and move on
            if (read_current_block) {
                //read normal blocks
                while (current_delta_offset > normal_end_offset) {
                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    if (!original_ts)[[unlikely]] {
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        current_delta++;
                        continue;
                    }
#if EDGE_DELTA_TEST
                    if(!original_ts){
                        throw LazyUpdateException();
                    }
#endif
                    //only prefetch is current delta is not in progress
                    if (is_txn_id(original_ts)/*&&original_ts!=txn_id*/) {
                        uint64_t status = 0;
                        if (txn_tables->get_status(original_ts, status))[[likely]] {
                            if (status == IN_PROGRESS)[[likely]] {
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                current_delta++;
                                continue;
                            } else {
                                if (status != ABORT)[[likely]] {
#if CHECKED_PUT_EDGE
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,
                                                                                             current_delta->previous_version_offset,
                                                                                             status);
#else
                                    current_delta_block->update_previous_delta_invalidate_ts(current_delta->toID,current_delta->previous_offset,status);
#endif
                                    if (current_delta->lazy_update(original_ts, status)) {
#if LAZY_LOCKING
                                        if(current_delta->is_last_delta.load(std::memory_order_acquire)){
                                            current_delta_block-> release_protection(current_delta->toID);
                                        }
#endif
                                        //record lazy update
                                        record_lazy_update_record(txn_lazy_update_records, original_ts);
                                    }
                                }
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts.load(std::memory_order_acquire)!=status){
                                    throw LazyUpdateException();
                                }
#endif
                                original_ts = status;
                            }
                        } else {
                            original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                        }
                        if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                            //uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                            uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                    std::memory_order_acquire);
                            //for debug
                            /*  if(!current_delta->toID){
                                  std::cout<<"error, current block is cleared"<<std::endl;
                                  current_delta->print_stats();
                                  throw std::runtime_error("error bad");
                              }*/
                            //cannot be the delta deleted by the current transaction
                            //visible committed delta
                            if (/*current_creation_ts*/original_ts <= txn_read_ts && (current_invalidation_ts == 0 ||
                                                                                      current_invalidation_ts >
                                                                                      txn_read_ts)) {
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                return_result_delta = current_delta++;
                                edge_count++;
                                continue;
                                //return;
                            }
                            //visible delta by myself
                            //we currently omit this condition as the experiments only use read-only transactions to scan adjacency list
                            /*else if(current_creation_ts==txn_id)[[unlikely]]{
                                current_delta_offset-=  ENTRY_DELTA_SIZE;
                                return current_delta++;
                            }*/
                            // }//txn_id
                        }
                        current_delta_offset -= ENTRY_DELTA_SIZE;
                        current_delta++;
                    } else {
                        if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                            //uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                            uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                    std::memory_order_acquire);
                            //for debug
                            /*  if(!current_delta->toID){
                                  std::cout<<"error, current block is cleared"<<std::endl;
                                  current_delta->print_stats();
                                  throw std::runtime_error("error bad");
                              }*/
                            //cannot be the delta deleted by the current transaction
                            // if(current_invalidation_ts!=txn_id)[[likely]]{//txn_id
                            //visible committed delta
                            if (original_ts <= txn_read_ts && (current_invalidation_ts == 0 ||
                                                               current_invalidation_ts >
                                                               txn_read_ts)) {
                                current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                                //if(current_delta_offset>=prefetch_offset)
                                _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                                return_result_delta = current_delta++;
                                edge_count++;
                                continue;
                                //return;
                            }
                            //we currently omit this condition as the experiments only use read-only transactions to scan adjacency list
                            //visible delta by myself
                            /*else if(current_creation_ts==txn_id)[[unlikely]]{
                                current_delta_offset-=  ENTRY_DELTA_SIZE;
                                _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
                                return current_delta++;
                            }*/

                        }
                        current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                        //if(current_delta_offset>=prefetch_offset)
                        _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                        current_delta++;
                    }
                }
                //current_delta = nullptr;
                //return_result_delta = nullptr;
                if(current_delta_offset==normal_end_offset&&current_delta_offset!=latest_version_start_offset){
                    current_delta_offset-=LIGHT_DELTA_SIZE;
                }
                while(current_delta_offset>0){
                    if(current_light_delta->creation_ts<=txn_read_ts&&(current_light_delta->invalidate_ts.load(std::memory_order_acquire)>txn_read_ts||current_light_delta->invalidate_ts.load(std::memory_order_acquire)==0)){
                        //found delta
                        current_delta_offset-=LIGHT_DELTA_SIZE;
                        return_result_light_delta = current_light_delta++;
                        edge_count++;
                        continue;
                        //return;
                    }
                    current_light_delta++;
                    current_delta_offset-=LIGHT_DELTA_SIZE;
                }
                return_result_light_delta = nullptr;

            } else {//read previous block
                while (current_delta_offset > normal_end_offset) {
                    uint64_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                        //uint64_t current_creation_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                        uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                std::memory_order_acquire);
                        //for debug
                        /*  if(!current_delta->toID){
                              std::cout<<"error, current block is cleared"<<std::endl;
                              current_delta->print_stats();
                              throw std::runtime_error("error bad");
                          }*/
                        //cannot be the delta deleted by the current transaction
                        // if(current_invalidation_ts!=txn_id)[[likely]]{//txn_id
                        //visible committed delta
                        if (original_ts <= txn_read_ts && (current_invalidation_ts == 0 ||
                                                           current_invalidation_ts >
                                                           txn_read_ts)) {
                            current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                            //if(current_delta_offset>=prefetch_offset)
                            _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                            return_result_delta = current_delta++;
                            edge_count++;
                            continue;
                            //return;
                        }
                        //we currently omit this condition as the experiments only use read-only transactions to scan adjacency list
                        //visible delta by myself
                        /*else if(current_creation_ts==txn_id)[[unlikely]]{
                            current_delta_offset-=  ENTRY_DELTA_SIZE;
                            _mm_prefetch((const void*)(current_delta+8),_MM_HINT_T2);
                            return current_delta++;
                        }*/

                    }
                    current_delta_offset -= ENTRY_DELTA_SIZE;
#if USING_READER_PREFETCH
                    //if(current_delta_offset>=prefetch_offset)
                    _mm_prefetch((const void *) (current_delta + PREFETCH_STEP), _MM_HINT_T2);
#endif
                    current_delta++;
                }
                current_delta = nullptr;
                return_result_delta = nullptr;
                if(current_delta_offset==normal_end_offset&&current_delta_offset!=latest_version_start_offset){
                    current_delta_offset-=LIGHT_DELTA_SIZE;
                }
                while(current_delta_offset>0){
                    if(current_light_delta->creation_ts<=txn_read_ts&&(current_light_delta->invalidate_ts.load(std::memory_order_acquire)>txn_read_ts||current_light_delta->invalidate_ts.load(std::memory_order_acquire)==0)){
                        //found delta
                        current_delta_offset-=LIGHT_DELTA_SIZE;
                        return_result_light_delta = current_light_delta++;
                        edge_count++;
                        continue;
                        //return;
                    }
                    current_light_delta++;
                    current_delta_offset-=LIGHT_DELTA_SIZE;
                }
                return_result_light_delta = nullptr;
            }
            close();
            return edge_count;
        }

        inline char *get_data(uint32_t offset) {
            return current_delta_block->get_edge_data(offset);
        }

        inline double get_double_weight(uint32_t offset) {
            return *reinterpret_cast<double *>(current_delta_block->get_edge_data(offset));
        }



    private:
        EdgeDeltaBlockHeader *current_delta_block;
        timestamp_t txn_read_ts;
        uint64_t txn_id;
        //bool txn_has_deltas;//whether this txn has deltas in the current delta block
        uint32_t current_delta_offset = 0;
        bool read_current_block = false;
        BaseEdgeDelta *current_delta = nullptr;
        LightEdgeDelta *current_light_delta = nullptr;
        //BaseEdgeDelta* current_visible_delta = nullptr;
        TxnTables *txn_tables;
        BlockManager *block_manager;
        lazy_update_map *txn_lazy_update_records;
        BlockAccessTimestampTable *block_access_ts_table = nullptr;//necessary at destructor, need to release the protection
        uint32_t normal_end_offset;
        uint32_t latest_version_start_offset;
        BaseEdgeDelta* return_result_delta;
        LightEdgeDelta* return_result_light_delta;
    };

    //the iterator for the static graph
    class StaticEdgeDeltaIterator {
    public:
        StaticEdgeDeltaIterator() {}

        StaticEdgeDeltaIterator(EdgeDeltaBlockHeader *input_block, uint32_t input_offset) : current_delta_block(
                input_block) {
            edge_deltas = current_delta_block->get_edge_delta(input_offset);
            size = input_offset / ENTRY_DELTA_SIZE;
        }

        void fill_information(EdgeDeltaBlockHeader *input_block, uint32_t input_offset) {
            current_delta_block = input_block;
            edge_deltas = current_delta_block->get_edge_delta(input_offset);
            size = input_offset / ENTRY_DELTA_SIZE;
            current_index = 0;
        }

        BaseEdgeDelta *next_delta() {
            if (current_index < size)[[likely]] {
                return &edge_deltas[current_index++];
            } else {
                return nullptr;
            }
        }

        inline char *get_data(uint32_t offset) {
            return current_delta_block->get_edge_data(offset);
        }

        inline uint32_t get_degree() { return size; }

    private:
        EdgeDeltaBlockHeader *current_delta_block;
        //bool txn_has_deltas;//whether this txn has deltas in the current delta block
        BaseEdgeDelta *edge_deltas;
        uint32_t current_index = 0;
        uint32_t size = 0;
    };
    //not used
    class StaticArrayEdgeDeltaIterator {
    public:
        StaticArrayEdgeDeltaIterator() {}

        StaticArrayEdgeDeltaIterator(EdgeDeltaBlockHeader *input_block, uint32_t input_offset) : current_delta_block(
                input_block) {
            edge_deltas = current_delta_block->get_edge_delta(input_offset);
            size = input_offset / ENTRY_DELTA_SIZE;
#if USING_PREFETCH
            /*    if(size<=10)[[likely]]{
                     for(uint32_t i=0; i<size; i++){
                         _mm_prefetch((const void*)(edge_deltas+i),_MM_HINT_T2);
                     }
                }*/
#endif
        }

        BaseEdgeDelta *next_delta() {
            if (current_index < size)[[likely]] {
                return &edge_deltas[current_index++];
            } else {
                return nullptr;
            }
        }

        inline char *get_data(uint32_t offset) {
            return current_delta_block->get_edge_data(offset);
        }

        inline uint32_t get_degree() { return size; }

    private:
        EdgeDeltaBlockHeader *current_delta_block;
        //bool txn_has_deltas;//whether this txn has deltas in the current delta block
        BaseEdgeDelta *edge_deltas;
        uint32_t current_index = 0;
        uint32_t size = 0;
    };
    
}
//#endif //BWGRAPH_V2_EDGE_ITERATOR_HPP
