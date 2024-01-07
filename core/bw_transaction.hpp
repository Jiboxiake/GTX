//
// Created by zhou822 on 5/28/23.
//
#pragma once
//#ifndef BWGRAPH_V2_BW_TRANSACTION_HPP
//#define BWGRAPH_V2_BW_TRANSACTION_HPP

//#include <unordered_set>
//#include <map>
#include "bw_index.hpp"
#include "transaction_tables.hpp"
#include "edge_iterator.hpp"
#include "previous_version_garbage_queue.hpp"
#include "types.hpp"
#include "edge_delta_block_state_protection.hpp"
#include <set>
namespace bwgraph{
#define CONSOLIDATION_TEST false
#define TXN_TEST false
    struct LockOffsetCache{
        LockOffsetCache(uint64_t input_version, int32_t input_size):block_version_num(input_version),delta_chain_num(input_size){}
        ~LockOffsetCache() = default;
        inline bool is_outdated(uint64_t current_version){
            return current_version != block_version_num;
        }
        inline bool is_same_version(timestamp_t current_version){
            return current_version==block_version_num;
        }

#if PESSIMISTIC_DELTA_BLOCK
        /*
         * Reclaim the locks according to the new block delta chain structure. Return false upon conflict
         * But should not be used, it is not efficient enough
         */
        bool reclaim_delta_chain_lock(EdgeDeltaBlockHeader* current_block, BwLabelEntry* current_label_entry, uint64_t txn_id, uint64_t txn_read_ts, uint64_t current_block_offset){
            delta_chain_num = current_block->get_delta_chain_num();//todo: check if we can update delta chain num directly in place
            std::set<delta_chain_id_t> to_reclaim_locks;//use set because we want a deterministic order of reclaiming locks
            already_updated_delta_chain_head_offsets.clear();
            block_version_num = current_label_entry->block_version_number.load(std::memory_order_acquire);
            for(auto it = already_modified_edges.begin();it!=already_modified_edges.end();it++){
                delta_chain_id_t delta_chain_id = calculate_owner_delta_chain_id(*it,delta_chain_num);
                to_reclaim_locks.emplace(delta_chain_id);
                already_updated_delta_chain_head_offsets.try_emplace(delta_chain_id,0);//initialize entries
            }
            //our new solution: reclaim offsets first
            reconstruct_offsets(current_block,txn_id,current_block_offset);
            std::set<delta_chain_id_t> already_reclaimed_locks;
            bool to_abort = false;
            uint32_t latest_version_start_offset = current_block->latest_version_delta_num()*LIGHT_DELTA_SIZE;//how far from the right border is the first light entry
            for(auto it = to_reclaim_locks.begin();it!=to_reclaim_locks.end();it++){
                bool lock_result = current_block->try_set_protection_on_delta_chain(*it);
                if(lock_result){
                    already_reclaimed_locks.emplace(*it);
                    auto& delta_chain_index_entry = current_label_entry->delta_chain_index->at(*it);
                    uint32_t current_delta_chain_head_offset = delta_chain_index_entry.get_raw_offset();
                    if(current_delta_chain_head_offset){
                        //hybrid storage: need to distinguish normal vs. light deltas
                        if(current_delta_chain_head_offset>latest_version_start_offset){
                            BaseEdgeDelta* current_delta_chain_head = current_block->get_edge_delta(current_delta_chain_head_offset);
#if EDGE_DELTA_TEST
                            if(current_delta_chain_head->creation_ts.load(std::memory_order_acquire)==0){
                                throw DeltaChainCorruptionException();
                            }
#endif//endif EDGE_DELTA_TEST
                            uint64_t current_delta_chain_head_ts = current_delta_chain_head->creation_ts.load(std::memory_order_acquire);
                            //because lock and offset are bind together
#if EDGE_DELTA_TEST
                            if(is_txn_id(current_delta_chain_head_ts)||current_delta_chain_head_ts==ABORT){
                                throw LazyUpdateException();//how can we have the lock while the delta chain head is in-progress?
                            }
#endif//endif EDGE_DELTA_TEST
                            if(current_delta_chain_head_ts>txn_read_ts){
                                to_abort=true;
                                break;
                            }
                        }else{
                            LightEdgeDelta* current_delta_chain_head = current_block->get_light_edge_delta(current_delta_chain_head_offset);
#if EDGE_DELTA_TEST
                            if(current_delta_chain_head->creation_ts==0|| is_txn_id(current_delta_chain_head->creation_ts)||current_delta_chain_head->creation_ts==ABORT){
                                throw DeltaChainCorruptionException();
                            }
#endif//endif EDGE_DELTA_TEST
                            if(current_delta_chain_head->creation_ts>txn_read_ts){
                                to_abort=true;
                                break;
                            }
                        }
                    }
                }else{
                    to_abort=true;
                    break;
                }
            }
            if(to_abort){
                for(auto it = already_reclaimed_locks.begin();it!=already_reclaimed_locks.end();it++){
                   // current_block->release_protection(it);
                   current_block->release_protection_delta_chain(*it);
                }
                return false;
            }
            //reconstruct offsets
            return true;
        }

        /*
         * Reclaim the locks according to the new block delta chain structure. Return false upon conflict
         * For Simple Protocol
         */
        bool reclaim_delta_chain_lock(EdgeDeltaBlockHeader* current_block, BwLabelEntry* current_label_entry, uint64_t txn_id, uint64_t txn_read_ts, uint64_t current_block_offset, lazy_update_map* lazy_update_records){
            delta_chain_num = current_block->get_delta_chain_num();//todo: check if we can update delta chain num directly in place
            block_version_num = current_label_entry->block_version_number.load(std::memory_order_acquire);
            already_updated_delta_chain_head_offsets.clear();
            //if the transaction did not update anything really
            if(already_modified_edges.empty()){
                return true;
            }
            //std::cout<<"reclaim delta chain lock"<<std::endl;
            std::set<delta_chain_id_t> to_reclaim_locks;//use set because we want a deterministic order of reclaiming locks
            for(auto it = already_modified_edges.begin();it!=already_modified_edges.end();it++){
                delta_chain_id_t delta_chain_id = calculate_owner_delta_chain_id(*it,delta_chain_num);
                to_reclaim_locks.emplace(delta_chain_id);
                already_updated_delta_chain_head_offsets.try_emplace(delta_chain_id,0);//initialize entries
            }
            //our new solution: reclaim offsets first
            reconstruct_offsets(current_block,txn_id,current_block_offset);
            std::set<delta_chain_id_t> already_reclaimed_locks;
            bool to_abort = false;
            for(auto it = to_reclaim_locks.begin();it!=to_reclaim_locks.end();){
                auto lock_result = current_block->simple_set_protection_on_delta_chain(*it,lazy_update_records,txn_read_ts);
                if(lock_result==Delta_Chain_Lock_Response::SUCCESS){
                    //even timestamp is matching
                    already_reclaimed_locks.emplace(*it);
                    it++;
                }else if(lock_result==Delta_Chain_Lock_Response::CONFLICT){
                    to_abort = true;
                    break;
                }else{//lock_result == UNCLEAR
                    //do nothing, retry
                }
            }
            if(to_abort){
                for(auto it = already_reclaimed_locks.begin();it!=already_reclaimed_locks.end();it++){
                    // current_block->release_protection(it);
                    current_block->release_protection_delta_chain(*it);
                }
                return false;
            }
            //reconstruct offsets
            return true;
        }
        //reconstruct all private transaction delta chain heads: assume we already computed which delta chains need to be reclaimed, delta chain num is also updated
        //Note that we should still be able to reconstruct all offsets
        void reconstruct_offsets(EdgeDeltaBlockHeader* current_block, uint64_t txn_id, uint64_t current_block_offset){
            size_t delta_chains_to_reclaim_num = already_updated_delta_chain_head_offsets.size();//the total number of delta chains we want to reclaim
            std::unordered_set<delta_chain_id_t>settled_delta_chains(delta_chains_to_reclaim_num);
            uint32_t current_delta_offset = EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_block_offset);
            auto current_delta = current_block->get_edge_delta(current_delta_offset);
#if EDGE_DELTA_TEST
            uint32_t latest_version_start_offset = current_block->latest_version_delta_num()*LIGHT_DELTA_SIZE;
#endif
            while(settled_delta_chains.size()<delta_chains_to_reclaim_num){
#if EDGE_DELTA_TEST
                /*if(current_delta_offset==0)[[unlikely]]{
                    throw DeltaChainReclaimException();
                }*/
                if(current_delta_offset<=latest_version_start_offset)[[unlikely]]{//should never reach this far in search
                    throw DeltaChainReclaimException();
                }
#endif
                timestamp_t original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                //invalid deltas, skip
                /*if(!original_ts){
                    current_delta++;
                    current_delta_offset-=ENTRY_DELTA_SIZE;
                    continue;
                }*/
                //found my deltas, my txn_id cannot be 0
                if(original_ts==txn_id){
                    delta_chain_id_t delta_chain_id = calculate_owner_delta_chain_id(current_delta->toID,delta_chain_num);
                    auto emplace_result = settled_delta_chains.emplace(delta_chain_id);
                    if(emplace_result.second){
#if EDGE_DELTA_TEST
                        if( already_updated_delta_chain_head_offsets[delta_chain_id]!=0){
                            throw std::runtime_error("error, updating wrong entry");
                        }
                        auto to_check_delta = current_block->get_edge_delta(current_delta_offset);
                        if(to_check_delta!=current_delta){
                            throw std::runtime_error("pointer arithmetic is wrong");
                        }
#endif //endif EDGE_DELTA_TEST
                        already_updated_delta_chain_head_offsets[delta_chain_id]=current_delta_offset;
                    }
                }
                current_delta++;
                current_delta_offset-=ENTRY_DELTA_SIZE;
            }
        }

#else //else if optimistic mode

#endif //if pessimistic mode
//Delta chain cache functions: under pessimistic mode these can only be invoked after the locks are acquired. So they are lock caches as well.
//Each non-zero delta chain cache entry corresponds to a lock under this mode. The exception is when the lock is granted but the block overflows. But in this
//we will not cache the update to the edge.
        /*
        * if the delta chain offset is cached, return the cached offset
        * Otherwise create a cache entry to cache the delta chain offset and return 0
        */
        inline uint32_t ensure_delta_chain_cache(vertex_t vid){
            delta_chain_id_t delta_chain_id = calculate_owner_delta_chain_id(vid,delta_chain_num);
            auto emplace_result = already_updated_delta_chain_head_offsets.try_emplace(delta_chain_id,0);
            return emplace_result.first->second;//either 0 or cached offset
        }
        /*
         * check whether this edge has already been modified by the current transaction
         * if so return the cached offset, otherwise return 0
         */
        inline uint32_t is_edge_already_locked(vertex_t vid){
            if(already_modified_edges.find(vid)!=already_modified_edges.end()){
                delta_chain_id_t delta_chain_id = calculate_owner_delta_chain_id(vid,delta_chain_num);
                return already_updated_delta_chain_head_offsets[delta_chain_id];
            }
            return 0;
        }
        /*
         * after a write to the delta chain, cache the most recent txn local head
         */
        inline void cache_vid_offset_exist(vertex_t vid, uint32_t new_offset){
            already_modified_edges.emplace(vid);
            already_updated_delta_chain_head_offsets[calculate_owner_delta_chain_id(vid,delta_chain_num)]=new_offset;
        }
        /*
         * after a write to the delta chain, cache the most recent txn local head
         */
        inline void cache_vid_offset_new(vertex_t vid, uint32_t new_offset){
            already_modified_edges.emplace(vid);
            auto result = already_updated_delta_chain_head_offsets.try_emplace(calculate_owner_delta_chain_id(vid,delta_chain_num),new_offset);
            if(!result.second){
                result.first->second= new_offset;
            }
            //already_updated_delta_chain_head_offsets[calculate_owner_delta_chain_id(vid,delta_chain_num)]=new_offset;
        }
        void release_protections(EdgeDeltaBlockHeader* current_block){
            for(auto it = already_updated_delta_chain_head_offsets.begin();it!=already_updated_delta_chain_head_offsets.end();it++){
                if(!it->second){
                    continue;
                }
                current_block->release_protection_delta_chain(it->first);
            }
        }
        /*
         * function invoked only after protection is grabbed and offset is not overflowing
         * locks are released before this function is called
         * when possible, use the cached offset to abort. If timestamps are different, use scan to abort.
         */
        int64_t eager_abort(EdgeDeltaBlockHeader* current_block, BwLabelEntry* current_label_entry, uint64_t txn_id,uint64_t current_block_offset){
            int64_t total_abort_count=0;
            //use offset cache to eager abort
            //I have locks must release
            uint32_t latest_version_start_offset = LIGHT_DELTA_SIZE*current_block->latest_version_delta_num();
            if(current_label_entry->block_version_number.load(std::memory_order_acquire)==block_version_num){
                //double check if we will ever get a zero?
                for(auto it = already_updated_delta_chain_head_offsets.begin();it!=already_updated_delta_chain_head_offsets.end();it++){
#if EDGE_DELTA_TEST
                    if(!it->second){
                       //throw ConsolidationException();
                       continue;//todo: check which one to pick
                    }
#endif
                    uint32_t current_delta_offset = it->second;
                    //can we guarantee that this is always BaseEdgeDelta
                    auto current_delta = current_block->get_edge_delta(current_delta_offset);

                    while(current_delta_offset>0){
                        if(current_delta_offset<=latest_version_start_offset){
                            break;
                        }
#if EDGE_DELTA_TEST
                        if(current_delta->creation_ts.load(std::memory_order_acquire)==0){
                            throw DeltaChainCorruptionException();
                        }
#endif
                        if(current_delta->creation_ts.load(std::memory_order_acquire)==txn_id){//my delta
#if EDGE_DELTA_TEST
                            current_delta->eager_abort(txn_id);
#else
                            current_delta->eager_abort();
#endif
                            total_abort_count++;
                        }else{//reach the committed delta
#if EDGE_DELTA_TEST
                            if(current_delta->creation_ts.load(std::memory_order_acquire)==ABORT|| is_txn_id(current_delta->creation_ts.load(std::memory_order_acquire))){
                                throw LazyUpdateAbortException();//our delta chain should only contain our deltas linked to committed delta chain or no delta
                            }
#endif
                            break;
                        }
                        current_delta_offset = current_delta->previous_offset;
                        current_delta = current_block->get_edge_delta(current_delta_offset);
                    }
                }
            }else{//use scan to abort; I have no locks
                uint32_t current_delta_offset = EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_block_offset);
                auto current_delta = current_block->get_edge_delta(current_delta_offset);
                //scan all normal deltas
                while(current_delta_offset>latest_version_start_offset){
                   /* if(current_delta_offset<=latest_version_start_offset){
                        break;
                    }*/
                    auto original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                    /*if(!original_ts){
                        current_delta++;
                        current_delta_offset-=ENTRY_DELTA_SIZE;
                        continue;
                    }*/
                    //my txn id cannot be 0
                    if(original_ts==txn_id){
#if EDGE_DELTA_TEST
                        current_delta->eager_abort(txn_id);
#else
                        current_delta->eager_abort();
#endif
                        total_abort_count++;
                    }
                    current_delta_offset-=ENTRY_DELTA_SIZE;
                    current_delta++;
                }
            }
            return total_abort_count;
        }

        uint64_t block_version_num;
        int32_t delta_chain_num;
        std::unordered_set<vertex_t>already_modified_edges;
        std::map<delta_chain_id_t ,uint32_t>already_updated_delta_chain_head_offsets;
    };
    //it is used to cache the validation the current transaction did to each delta chain of each block.
    struct validation_to_revise_entry{
        validation_to_revise_entry(int32_t input_delta_id, uint32_t input_offset):/*block_id(input_id),*/delta_chain_id(input_delta_id),original_offset(input_offset){}
        // int64_t block_id;
        int32_t delta_chain_id;
        uint32_t original_offset;
    };
    class ROTransaction{
    public:
        ROTransaction(BwGraph& source_graph,timestamp_t input_ts,TxnTables& input_txn_tables, BlockManager& input_block_manager, GarbageBlockQueue& input_garbage_queue, BlockAccessTimestampTable& input_block_ts_table, uint8_t input_thread_id):graph(source_graph),read_timestamp(input_ts),txn_tables(input_txn_tables),
        block_manager(input_block_manager),per_thread_garbage_queue(input_garbage_queue),block_access_ts_table(input_block_ts_table), thread_id(input_thread_id){}
        ROTransaction(ROTransaction&& other):graph(other.graph),read_timestamp(other.read_timestamp),txn_tables(other.txn_tables),
                                             block_manager(other.block_manager),per_thread_garbage_queue(other.per_thread_garbage_queue),block_access_ts_table(other.block_access_ts_table), thread_id(other.thread_id){}
        std::pair<Txn_Operation_Response,std::string_view> get_edge(vertex_t src, vertex_t dst, label_t label);
        ROTransaction(const ROTransaction &) = delete;
        ROTransaction& operator =(const ROTransaction &)=delete;
        ~ROTransaction();
        std::pair<Txn_Operation_Response,EdgeDeltaIterator> get_edges(vertex_t src, label_t label);
        std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator> simple_get_edges(vertex_t src, label_t label);
        //specialized function for weight
        Txn_Operation_Response get_edge_weight(vertex_t src, label_t label, vertex_t dst, double*& weight);
        std::string_view get_vertex(vertex_t src);
        inline void commit(){
            batch_lazy_updates();
        }
    private:
        //this function is only invoked when we know the entry must exist (access from txn own label cache)
        inline BwLabelEntry* get_label_entry(uint64_t block_id){
            return accessed_edge_label_entry_cache.at(block_id);
        }
        //todo: need to optimize this function, no cache
        inline BwLabelEntry* reader_access_label(vertex_t vid, label_t label){
            auto block_id = generate_block_id(vid,label);
            auto emplace_result = accessed_edge_label_entry_cache.try_emplace(block_id, nullptr);
            if(emplace_result.second){
                auto& vertex_index_entry = graph.get_vertex_index_entry(vid);
                if(!vertex_index_entry.valid.load(std::memory_order_acquire)){
                    accessed_edge_label_entry_cache.erase(emplace_result.first);
                    return nullptr;
                }
                auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
                auto found = edge_label_block->reader_lookup_label(label, emplace_result.first->second);
                if(!found){
                    accessed_edge_label_entry_cache.erase(emplace_result.first);
                    return nullptr;
                }
            }
            return emplace_result.first->second;
        }
        inline void batch_lazy_updates(){
          //  std::cout<<"batch size is "<<lazy_update_records.size()<<std::endl;
            for(auto it = lazy_update_records.begin();it!=lazy_update_records.end();it++){
                if(it->second>0){
                    txn_tables.reduce_op_count(it->first,it->second);
                }
            }
            lazy_update_records.clear();
        }
        std::string_view scan_previous_block_find_edge(EdgeDeltaBlockHeader* previous_block, vertex_t vid);
        void scan_previous_block_find_weight(EdgeDeltaBlockHeader* previous_block, vertex_t vid,double*& weight);
        BwGraph& graph;
        const timestamp_t read_timestamp;
        TxnTables& txn_tables;
        lazy_update_map lazy_update_records;
        BlockManager& block_manager;
        GarbageBlockQueue& per_thread_garbage_queue;
        BlockAccessTimestampTable& block_access_ts_table;
        uint8_t thread_id;
        std::unordered_map<uint64_t, BwLabelEntry*> accessed_edge_label_entry_cache;
    };
    /*
     * read-only transaction that can be executed concurrently by multiple worker threads
     */
    class SharedROTransaction{
    public:
        SharedROTransaction(BwGraph& source_graph,timestamp_t input_ts,TxnTables& input_txn_tables, BlockManager& input_block_manager,  BlockAccessTimestampTable& input_block_ts_table):graph(source_graph),read_timestamp(input_ts), txn_tables(input_txn_tables),
        block_manager(input_block_manager), block_access_ts_table(input_block_ts_table),per_thread_op_count(0){}
        SharedROTransaction(SharedROTransaction&& other):graph(other.graph),read_timestamp(other.read_timestamp),txn_tables(other.txn_tables),
         thread_specific_lazy_update_records(other.thread_specific_lazy_update_records),block_manager(other.block_manager), block_access_ts_table(other.block_access_ts_table),per_thread_op_count(0){}
        ~SharedROTransaction();
        SharedROTransaction(const SharedROTransaction& other)=delete;
        SharedROTransaction& operator =(const SharedROTransaction &)=delete;
        //assume the operation is on a static loaded graph
        std::string_view static_get_edge(vertex_t src, vertex_t dst, label_t label);
        StaticEdgeDeltaIterator static_get_edges(vertex_t src, label_t label);
        void static_get_edges(vertex_t src, label_t label, std::unique_ptr<StaticEdgeDeltaIterator>& edge_iterator);
        std::string_view static_get_vertex(vertex_t src);
        //assume a dynamic graph
        std::pair<Txn_Operation_Response,std::string_view> get_edge(vertex_t src, vertex_t dst, label_t label);
        std::pair<Txn_Operation_Response,EdgeDeltaIterator> get_edges(vertex_t src, label_t label);
        std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator> simple_get_edges(vertex_t src, label_t label);
        std::pair<Txn_Operation_Response,std::string_view> get_edge(vertex_t src, vertex_t dst, label_t label,uint8_t thread_id);
        std::pair<Txn_Operation_Response,EdgeDeltaIterator> get_edges(vertex_t src, label_t label,uint8_t thread_id);
        std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator> simple_get_edges(vertex_t src, label_t label,uint8_t thread_id);
        Txn_Operation_Response simple_get_edges(vertex_t src, label_t label, uint8_t thread_id,std::unique_ptr<SimpleEdgeDeltaIterator>& edge_iterator);
        //std::pair<Txn_Operation_Response,EarlyStopEdgeDeltaIterator> early_stop_get_edges(vertex_t src, label_t label,uint8_t thread_id);
        std::string_view get_vertex(vertex_t src);
        std::string_view get_vertex(vertex_t src, uint8_t thread_id);
        uint64_t neighborhood_size(uint64_t vid, label_t label, uint8_t thread_id);
        EdgeDeltaBlockHeader* get_block_header(uint64_t vid, label_t label, uint8_t thread_id, uint32_t* current_delta_offset);
        inline void unregister_thread_block_access(uint8_t thread_id){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
        }
        //uint64_t vertex_degree(vertex_t src, label_t label, std::unique_ptr<SimpleEdgeDeltaIterator>& edge_iterator);
        inline void commit(){
           /* batch_lazy_updates();
            auto& local_garbage_queue = graph.get_per_thread_garbage_queue();
            //if(local_garbage_queue.get_queue().size()>=garbage_collection_threshold){
                auto safe_ts = block_access_ts_table.calculate_safe_ts();
                local_garbage_queue.free_block(safe_ts);
            //}*/

        }
        inline void static_commit(){
            //do nothing
        }
        inline void on_operation_finish(){
            per_thread_op_count.local()++;
            if(per_thread_op_count.local()==shared_txn_op_threshold){
                //batch_lazy_updates();
                auto& local_garbage_queue = graph.get_per_thread_garbage_queue();
                if(local_garbage_queue.get_queue().size())[[unlikely]]{
                    auto safe_ts = block_access_ts_table.calculate_safe_ts();
                    local_garbage_queue.free_block(safe_ts);
                }
                per_thread_op_count.local()=0;
            }
        }
        inline void on_operation_finish(uint8_t thread_id){
            per_thread_op_count.local()++;
            if(per_thread_op_count.local()==shared_txn_op_threshold)[[unlikely]]{
                batch_lazy_updates();
                auto& local_garbage_queue = graph.get_per_thread_garbage_queue(thread_id);
                if(local_garbage_queue.get_queue().size())[[unlikely]]{
                    auto safe_ts = block_access_ts_table.calculate_safe_ts();
                    local_garbage_queue.free_block(safe_ts);
                }
                per_thread_op_count.local()=0;
            }
        }
        void thread_on_openmp_section_finish(uint8_t thread_id){
            batch_lazy_updates();
            auto& local_garbage_queue = graph.get_per_thread_garbage_queue(thread_id);
            if(local_garbage_queue.get_queue().size())[[unlikely]]{
                auto safe_ts = block_access_ts_table.calculate_safe_ts();
                local_garbage_queue.free_block(safe_ts);
            }
        }
        SimpleEdgeDeltaIterator generate_edge_iterator(uint8_t thread_id);
        StaticEdgeDeltaIterator generate_static_edge_iterator();
        inline timestamp_t get_read_ts(){return read_timestamp;}
        inline BwGraph* get_graph(){return &graph;}
    private:
        inline BwLabelEntry* reader_access_label(vertex_t vid, label_t label){
            auto& vertex_index_entry = graph.get_vertex_index_entry(vid);
            if(!vertex_index_entry.valid.load(std::memory_order_acquire)){
                return nullptr;
            }
            auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
            BwLabelEntry* return_label = nullptr;
            edge_label_block->reader_lookup_label(label, return_label);
            return return_label;
        }
        inline void batch_lazy_updates(){
           // std::cout<<"batch size is "<<lazy_update_records.size()<<std::endl;
           auto& lazy_update_records = thread_specific_lazy_update_records.local();
            for(auto it = lazy_update_records.begin();it!=lazy_update_records.end();it++){
                if(it->second>0){
                    txn_tables.reduce_op_count(it->first,it->second);
                }
            }
            lazy_update_records.clear();
        }
        std::string_view scan_previous_block_find_edge(EdgeDeltaBlockHeader* previous_block, vertex_t vid);
        BwGraph& graph;
        const timestamp_t read_timestamp;
        TxnTables& txn_tables;
        tbb::enumerable_thread_specific<lazy_update_map>  thread_specific_lazy_update_records;
        BlockManager& block_manager;
        //GarbageBlockQueue& per_thread_garbage_queue;
        BlockAccessTimestampTable& block_access_ts_table;
        tbb::enumerable_thread_specific<size_t> per_thread_op_count;
        //todo:: add a local operation count variaable: if op count reaches a threshold, do garbage collection
        //std::unordered_map<uint64_t, BwLabelEntry*> accessed_edge_label_entry_cache;
        //tbb::enumerable_thread_specific<std::unordered_map<uint64_t, BwLabelEntry*>> thread_local_accessed_edge_label_entry_cache;
    };
    class RWTransaction{
    public:
        //implement constructor
        RWTransaction(BwGraph& source_graph,uint64_t input_txn_id, timestamp_t input_ts, entry_ptr input_txn_ptr, TxnTables& input_txn_tables, CommitManager& input_commit_manager,  BlockManager& input_block_manager,GarbageBlockQueue& input_garbage_queue, BlockAccessTimestampTable& input_bts_table,std::queue<vertex_t>& input_thread_local_recycled_vertices):graph(source_graph),  local_txn_id(input_txn_id),read_timestamp(input_ts),
        self_entry(input_txn_ptr),txn_tables(input_txn_tables),commit_manager(input_commit_manager), block_manager(input_block_manager),per_thread_garbage_queue(input_garbage_queue), block_access_ts_table(input_bts_table),thread_local_recycled_vertices(input_thread_local_recycled_vertices){
            thread_id = get_threadID(local_txn_id);
            current_delta_offset=0;
            current_data_offset=0;
#if TRACK_EXECUTION_TIME
            txn_start_time = std::chrono::high_resolution_clock::now();
#endif
        }
        RWTransaction( RWTransaction&& other):graph(other.graph),  local_txn_id(other.local_txn_id),read_timestamp(other.read_timestamp),
                                              self_entry(other.self_entry),txn_tables(other.txn_tables),commit_manager(other.commit_manager), block_manager(other.block_manager),per_thread_garbage_queue(other.per_thread_garbage_queue), block_access_ts_table(other.block_access_ts_table),thread_local_recycled_vertices(other.thread_local_recycled_vertices){
            thread_id = other.thread_id;
            current_delta_offset=0;
            current_data_offset=0;
#if TRACK_EXECUTION_TIME
            txn_start_time = std::chrono::high_resolution_clock::now();
#endif
        }
        RWTransaction(const RWTransaction &)=delete;
        RWTransaction& operator = (const RWTransaction &)=delete;
        ~RWTransaction();
        //transaction graph write operations
        //for edge creation and update
        Txn_Operation_Response put_edge(vertex_t src, vertex_t dst, label_t label, std::string_view edge_data);
        Txn_Operation_Response delete_edge(vertex_t src, vertex_t dst, label_t label);
        vertex_t create_vertex();
        Txn_Operation_Response update_vertex(vertex_t src, std::string_view vertex_data);
        //checked version
        Txn_Operation_Response checked_put_edge(vertex_t src, vertex_t dst, label_t label, std::string_view edge_data);
        Txn_Operation_Response checked_delete_edge(vertex_t src, vertex_t dst, label_t label);
        Txn_Operation_Response checked_single_edge_insertion(vertex_t src, vertex_t dst, label_t label, std::string_view edge_data);
        Txn_Operation_Response checked_single_edge_deletion(vertex_t src, vertex_t dst, label_t label);
        //Txn_Operation_Response delete_vertex(vertex_t src);
        //Txn_Operation_Response delete(vertex_t src, vertex_t dst, label_t label);
        //transaction graph read operations
        std::pair<Txn_Operation_Response,std::string_view> get_edge(vertex_t src, vertex_t dst, label_t label);
        std::pair<Txn_Operation_Response,EdgeDeltaIterator> get_edges(vertex_t src, label_t label);
        std::pair<Txn_Operation_Response,SimpleEdgeDeltaIterator> simple_get_edges(vertex_t src, label_t label);
        //std::pair<Txn_Operation_Response,std::string_view> checked_get_edge(vertex_t src, vertex_t dst, label_t label);
        //std::pair<Txn_Operation_Response,EdgeDeltaIterator> checked_get_edges(vertex_t src, label_t label);
        std::string_view get_vertex(vertex_t src);
        //transaction status operation
        void abort();
        bool commit();
        bool eager_commit();
        inline BwGraph& get_graph(){return graph;}
    private:
        void eager_clean_edge_block(uint64_t block_id, LockOffsetCache& validated_offsets);
        void eager_clean_vertex_chain(vertex_t vid);
        //handle the scenario that block becomes overflow
        void consolidation(BwLabelEntry* current_label_entry, EdgeDeltaBlockHeader* current_block, uint64_t block_id);
        void checked_consolidation(BwLabelEntry* current_label_entry, EdgeDeltaBlockHeader* current_block, uint64_t block_id);
        //validation delta chain writes before commit
        bool validation();
        bool simple_validation();
        //eagerly abort my deltas. If a transaction validated for a block, then the block enters Installation phase, this txn will not eager abort for that block.
        void eager_abort();
        //for pessimistic mode: release all locks in the current block
        //abort all my deltas using scans
        //todo:: only concern is with delete vertex, what if the vertex entry is deleted? then we should return error
        //old version of caching entries
       /* inline BwLabelEntry* writer_access_label(vertex_t vid, label_t label){
            auto block_id = generate_block_id(vid,label);
            auto emplace_result = accessed_edge_label_entry_cache.try_emplace(block_id, nullptr);
            if(emplace_result.second){
                auto& vertex_index_entry = graph.get_vertex_index_entry(vid);
                //cannot insert to invalid vertex entry
                if(!vertex_index_entry.valid.load()){
                    accessed_edge_label_entry_cache.erase(emplace_result.first);
                    return nullptr;
                }
                auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
                //either access an existing entry or creating a new entry
                emplace_result.first->second = edge_label_block->writer_lookup_label(label,&txn_tables,read_timestamp);
            }
            return emplace_result.first->second;
        }*/
       inline BwLabelEntry* writer_access_label(vertex_t vid, label_t label){
           auto& vertex_index_entry = graph.get_vertex_index_entry(vid);
           //cannot insert to invalid vertex entry
           if(!vertex_index_entry.valid.load(std::memory_order_acquire)){
               return nullptr;
           }
           auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
           //either access an existing entry or creating a new entry
           return edge_label_block->writer_lookup_label(label,&txn_tables,read_timestamp);
       }
        /*inline BwLabelEntry* reader_access_label(vertex_t vid, label_t label){
            auto block_id = generate_block_id(vid,label);
            auto emplace_result = accessed_edge_label_entry_cache.try_emplace(block_id, nullptr);
            if(emplace_result.second){
                auto& vertex_index_entry = graph.get_vertex_index_entry(vid);
                if(!vertex_index_entry.valid.load()){
                    accessed_edge_label_entry_cache.erase(emplace_result.first);
                    return nullptr;
                }
                auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
                auto found = edge_label_block->reader_lookup_label(label, emplace_result.first->second);
                if(!found){
                    accessed_edge_label_entry_cache.erase(emplace_result.first);
                    return nullptr;
                }
            }
            return emplace_result.first->second;
        }*/
        inline BwLabelEntry* reader_access_label(vertex_t vid, label_t label){
           // [[maybe_unused]]auto block_id = generate_block_id(vid,label);
                auto& vertex_index_entry = graph.get_vertex_index_entry(vid);
                if(!vertex_index_entry.valid.load(std::memory_order_acquire)){
                    return nullptr;
                }
                auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
                BwLabelEntry* result= nullptr;
                auto found = edge_label_block->reader_lookup_label(label, result);
                if(found){
                    return result;
                }else{
                    return nullptr;
                }
        }
        //this function is only invoked when we know the entry must exist (access from txn own label cache)
        inline BwLabelEntry* get_label_entry(uint64_t block_id){
            //return accessed_edge_label_entry_cache.at(block_id);
            auto vid_label = decompose_block_id(block_id);
            return writer_access_label(vid_label.first,vid_label.second);
        }
        //allocate space in the current block for delta
        EdgeDeltaInstallResult allocate_delta(EdgeDeltaBlockHeader* current_block, int32_t data_size){
            //todo:; for debug
        /*    if(data_size>128){
                throw std::runtime_error("for debug, too large delta");
            }*/
            uint32_t block_size = current_block->get_size();
            uint64_t original_block_offset;
            if(data_size<=16){
                original_block_offset = current_block->allocate_space_for_new_delta(0);
            }else{
                original_block_offset = current_block->allocate_space_for_new_delta(data_size);
            }
#if EDGE_DELTA_TEST
            uint64_t current_new_block_offset = current_block->get_current_offset();
            if(original_block_offset>=current_new_block_offset){
                throw std::runtime_error("for debug, offset overflow");
            }
#endif
            uint32_t original_data_offset = static_cast<uint32_t>(original_block_offset>>32);
            uint32_t original_delta_offset = static_cast<uint32_t>(original_block_offset&SIZE2MASK);
            uint32_t new_data_offset = original_data_offset;
            if(data_size>16){
                new_data_offset+=data_size;
            }
            uint32_t new_delta_offset = original_delta_offset+ENTRY_DELTA_SIZE;
            current_data_offset = original_data_offset;//grow from left to right;
            current_delta_offset = new_delta_offset; //grow from right to left
#if EDGE_DELTA_TEST
         /*   if(current_delta_offset>10000000){
                throw std::runtime_error("for debug, too large delta 2");
            }*/
#endif
            if((new_delta_offset+new_data_offset)>block_size)[[unlikely]]{
                if(original_delta_offset+original_data_offset<=block_size){
                    return EdgeDeltaInstallResult::CAUSE_OVERFLOW;
                }else{
                    return EdgeDeltaInstallResult::ALREADY_OVERFLOW;
                }
            }
            return EdgeDeltaInstallResult::SUCCESS;
        }
        //scan the previous block for an edge delta
        std::string_view scan_previous_block_find_edge(EdgeDeltaBlockHeader* previous_block, vertex_t vid);
        inline void batch_lazy_updates(){
            for(auto it = lazy_update_records.begin();it!=lazy_update_records.end();it++){
                if(it->second>0){
                    txn_tables.reduce_op_count(it->first,it->second);
                }
            }
            lazy_update_records.clear();
        }

        //consolidation heuristics
        /*
         * use approximate lifespan of the current block to estimate how long the block served us, may need to adjust numbers
         */
        inline uint64_t calculate_nw_block_size_from_lifespan(uint64_t delta_storage_size,uint64_t lifespan, uint64_t min_lifespan_threshold){
            lifespan = (lifespan > min_lifespan_threshold)?lifespan:min_lifespan_threshold;
           // uint64_t ratio = std::max(static_cast<double>(500/lifespan), 2);//old version size calculation
            uint64_t ratio = std::max((500/lifespan), static_cast<uint64_t>(2));
            //todo: optimize for pwoer law graph
            /*if(delta_storage_size>8192)
                delta_storage_size*=2;*/
            //delta_storage_size *= std::max(static_cast<uint64_t>(1),delta_storage_size/2048);
            return delta_storage_size*ratio + sizeof(EdgeDeltaBlockHeader);
        }
        /*
         * if the current size is close to the next order, we allocate an additional order
         * otherwise we return size_to_order directly
         */
        inline order_t calculate_new_fit_order(uint64_t block_storage_size){
            order_t new_order = size_to_order(block_storage_size);
            uint64_t new_size = 1ul<<new_order;
            //if the next order is less than 25% larger than the current size, double the new size
            if(new_size-block_storage_size<block_storage_size/4){
                return new_order+1;
            }else{
                //otherwise return the next size
                return new_order;
            }
        }
        //don't forget to subtract op_count
        inline ReclaimDeltaChainResult reclaim_delta_chain_offsets(LockOffsetCache& txn_offset_cache, EdgeDeltaBlockHeader* current_block, BwLabelEntry* current_label_entry){
            uint64_t current_combined_offset = current_block->get_current_offset();
            if(current_block->is_overflow_offset(current_combined_offset)){
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                return ReclaimDeltaChainResult::RETRY;
            }
#if LAZY_LOCKING
            auto reclaim_delta_chains_result = txn_offset_cache.reclaim_delta_chain_lock(current_block,current_label_entry,local_txn_id,read_timestamp,current_combined_offset);
#else
            auto reclaim_delta_chains_result = txn_offset_cache.reclaim_delta_chain_lock(current_block,current_label_entry,local_txn_id,read_timestamp,current_combined_offset,&lazy_update_records);
#endif //LAZY_LOCKING
            if(!reclaim_delta_chains_result){
                //abort my deltas and track the number
                auto abort_delta_count = txn_offset_cache.eager_abort(current_block,current_label_entry,local_txn_id,current_combined_offset);
                BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
                //record the number of reduced op_count
                op_count-= abort_delta_count;
                return ReclaimDeltaChainResult::FAIL;
            }
            return ReclaimDeltaChainResult::SUCCESS;
        }
        void print_edge_delta_block_metadata(EdgeDeltaBlockHeader* current_block){
            //std::cout<<"owner id is "<<current_block->get_owner_id()<<" creation ts is "<<current_block->get_creation_time()<<" order is "<<static_cast<int32_t>(current_block->get_order())<<" previous ptr is "<<current_block->get_previous_ptr()<<" delta chain num is "<<current_block->get_delta_chain_num()<<std::endl;
        }
        inline void cache_updated_block_id_and_version(uint64_t block_id, uint64_t version_number){
            auto emplace_result = graph.to_check_blocks.local().try_emplace(block_id,version_number);
            if(!emplace_result.second){
                emplace_result.first->second = version_number;
            }
        }
        void eager_garbage_collection(){
            if(per_thread_garbage_queue.has_entries()){
                auto safe_ts = block_access_ts_table.calculate_safe_ts();
                per_thread_garbage_queue.free_block(safe_ts);
            }
        }
        //txn local fields
        BwGraph& graph;
        const uint64_t local_txn_id;
        const timestamp_t read_timestamp;
        entry_ptr self_entry;
        TxnTables& txn_tables;
        CommitManager& commit_manager;
        lazy_update_map lazy_update_records;
        std::map<uint64_t, LockOffsetCache>per_block_cached_delta_chain_offsets;//store cache the blocks accessed
        int64_t op_count=0;
        BlockManager& block_manager;
        GarbageBlockQueue& per_thread_garbage_queue;
        BlockAccessTimestampTable& block_access_ts_table;
        uint8_t thread_id;
        uint32_t current_delta_offset;
        uint32_t current_data_offset;
        //std::unordered_map<uint64_t, BwLabelEntry*> accessed_edge_label_entry_cache;
        std::unordered_set<vertex_t> updated_vertices;//cache the vertex deltas that the transaction has touched
        std::queue<vertex_t>& thread_local_recycled_vertices;
        std::unordered_set<vertex_t> created_vertices;
#if TRACK_EXECUTION_TIME
        std::chrono::time_point<std::chrono::high_resolution_clock> txn_start_time;
#endif
    };
}
//#endif //BWGRAPH_V2_BW_TRANSACTION_HPP
