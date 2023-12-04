//
// Created by zhou822 on 5/28/23.
//
#pragma once
//#ifndef BWGRAPH_V2_BWGRAPH_HPP
//#define BWGRAPH_V2_BWGRAPH_HPP
#include "bw_index.hpp"
#include "block_manager.hpp"
#include "transaction_tables.hpp"
#include "exceptions.hpp"
#include "types.hpp"
#include "block_access_ts_table.hpp"
#include "commit_manager.hpp"
#include "worker_thread_manager.hpp"
#include "previous_version_garbage_queue.hpp"
namespace bwgraph{
    class ROTransaction;
    class RWTransaction;
    class SharedROTransaction;


    class BwGraph {
    public:
#if USING_ARRAY_TABLE
        BwGraph(std::string block_path = "",size_t _max_block_size = 1ul << 32,
            std::string wal_path = ""): block_manager(block_path,_max_block_size), vertex_index(block_manager),txn_tables(this),garbage_queues(worker_thread_num, GarbageBlockQueue(&block_manager))
#if TRACK_EXECUTION_TIME
            , local_thread_vertex_write_time(0),local_thread_edge_write_time(0),local_thread_commit_time(0),local_thread_abort_time(0),local_rwtxn_creation_time(0)
            ,local_get_thread_id_time(0),local_generate_txn_id_time(0),local_install_txn_entry_time(0),local_garbage_collection_time(0),local_eager_clean_real_work_time(0)
            ,local_edge_clean_real_work_time(0),local_vertex_clean_real_work_time(0),txn_execution_time(0)/*,commit_manager(txn_tables)*/
#endif //TRACK_EXECUTION_TIME
            ,to_check_blocks(std::unordered_map<uint64_t,uint64_t>()),thread_local_update_count(0)
            {
                block_access_ts_table.set_total_worker_thread_num(worker_thread_num);
            for(uint32_t i=0; i<worker_thread_num;i++){
                txn_tables.get_table(static_cast<uint8_t>(i)).set_garbage_queue(&garbage_queues[i]);
                recycled_vids.emplace_back(std::queue<vertex_t>());
#if TRACK_EXECUTION_TIME
            /*    global_vertex_read_time_array[i]=0;
                global_vertex_write_time_array[i]=0;
                global_edge_read_time_array[i]=0;
                global_edge_write_time_array[i]=0;*/
#endif //TRACK_EXECUTION_TIME
            }
            std::cout<<"using bwgraph hybrid store"<<std::endl;
    }
#else //USING_ARRAY_TABLE
        BwGraph(std::string block_path = "",size_t _max_block_size = 1ul << 32,
            std::string wal_path = ""): block_manager(block_path,_max_block_size), vertex_index(block_manager){
    }
#endif //USING_ARRAY_TABLE
        ~BwGraph(){
#if PRINTING_FINAL_GARBAGE_STATUS
            print_garbage_status();
#endif
            auto max_vid = vertex_index.get_current_allocated_vid();
            for(vertex_t vid = 1; vid<=max_vid; vid++){
                auto& vertex_index_entry = vertex_index.get_vertex_index_entry(vid);
                auto label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
                label_block->deallocate_all_delta_chains_indices();
            }
#if TRACK_EXECUTION_TIME
            uint64_t participating_thread_count = 0;
            uint64_t total_v_write_time=0;
            uint64_t total_e_write_time =0;
            uint64_t total_thread_commit_time =0;
            uint64_t total_thread_abort_time =0;
            uint64_t total_rw_txn_creation_time =0;
            uint64_t total_get_thread_id_time =0;
            uint64_t total_generate_txn_id_time=0;
            uint64_t total_install_txn_entry_time =0;
            uint64_t total_garbage_collection_time =0;
            uint64_t total_eager_clean_real_work_time = 0;
            uint64_t total_edge_clean_time =0;
            uint64_t total_vertex_clean_time =0;
            uint64_t total_txn_execution_time =0;
            for(auto v_w_t : local_thread_vertex_write_time){
                total_v_write_time+= v_w_t;
            }
            for(auto e_w_t : local_thread_edge_write_time){
                total_e_write_time+=e_w_t;
                participating_thread_count++;
            }
            for(auto thread_c_t : local_thread_commit_time){
                total_thread_commit_time+=thread_c_t;
            }
            for(auto thread_a_t : local_thread_abort_time){
                total_thread_abort_time+= thread_a_t;
            }
            for(auto thread_txn_creation_t :local_rwtxn_creation_time){
                total_rw_txn_creation_time+=thread_txn_creation_t;
            }
            for(auto get_thread_id_t : local_get_thread_id_time){
                total_get_thread_id_time+=get_thread_id_t;
            }
            for(auto generate_txn_id_t : local_generate_txn_id_time){
                total_generate_txn_id_time+=generate_txn_id_t;
            }
            for(auto install_txn_entry_time : local_install_txn_entry_time){
                total_install_txn_entry_time+=install_txn_entry_time;
            }
            for(auto garbage_collection_time : local_garbage_collection_time){
                total_garbage_collection_time+=garbage_collection_time;
            }
            for(auto eager_clean_work_time : local_eager_clean_real_work_time){
                total_eager_clean_real_work_time+=eager_clean_work_time;
            }
            for(auto edge_clean_t : local_edge_clean_real_work_time){
                total_edge_clean_time+=edge_clean_t;
            }
            for(auto vertex_clean_t : local_vertex_clean_real_work_time){
                total_vertex_clean_time+=vertex_clean_t;
            }
            for(auto txn_execution_t : txn_execution_time){
                total_txn_execution_time+= txn_execution_t;
            }
            if(participating_thread_count){
                std::cout<<"total worker thread count is "<<participating_thread_count<<std::endl;
                std::cout<<"average vertex write time per thread is "<<total_v_write_time/participating_thread_count<<std::endl;
                std::cout<<"average edge write time per thread is "<<total_e_write_time/participating_thread_count<<std::endl;
                std::cout<<"average txn commit time per thread is "<<total_thread_commit_time/participating_thread_count<<std::endl;
                std::cout<<"average txn abort time per thread is "<<total_thread_abort_time/participating_thread_count<<std::endl;
                std::cout<<"average txn creation time per thread is "<<total_rw_txn_creation_time/participating_thread_count<<std::endl;
                std::cout<<"average get thread id time per thread is "<<total_get_thread_id_time/participating_thread_count<<std::endl;
                std::cout<<"generate txn id time per thread is "<<total_generate_txn_id_time/participating_thread_count<<std::endl;
                std::cout<<"average txn entry install time per thread is "<<total_install_txn_entry_time/participating_thread_count<<std::endl;
                std::cout<<"average garbage collection time per thread is "<<total_garbage_collection_time/participating_thread_count<<std::endl;
                std::cout<<"average eager clean real work time per thread is "<<total_eager_clean_real_work_time/participating_thread_count<<std::endl;
                std::cout<<"average edge clean real work time per thread is "<<total_edge_clean_time/participating_thread_count<<std::endl;
                std::cout<<"average vertex clean real work time per thread is "<<total_vertex_clean_time/participating_thread_count<<std::endl;
                std::cout<<"average txn execution time is "<<total_txn_execution_time/participating_thread_count<<std::endl;
            }
#endif
        }
        ROTransaction begin_read_only_transaction();
        RWTransaction begin_read_write_transaction();
        SharedROTransaction begin_shared_ro_transaction();
        inline vertex_t get_max_allocated_vid(){
            return vertex_index.get_current_allocated_vid();
        }
        inline VertexIndexEntry& get_vertex_index_entry(vertex_t vid){
            return vertex_index.get_vertex_index_entry(vid);
        }
        inline BlockManager& get_block_manager(){return block_manager;}
        inline BlockAccessTimestampTable& get_block_access_ts_table(){return block_access_ts_table;}
        inline CommitManager& get_commit_manager(){return commit_manager;}
        inline TxnTables & get_txn_tables(){return txn_tables;}
        inline VertexIndex& get_vertex_index(){return vertex_index;}
        inline GarbageBlockQueue& get_per_thread_garbage_queue(uint8_t thread_id){return garbage_queues.at(thread_id);}
        inline GarbageBlockQueue& get_per_thread_garbage_queue(){
            return garbage_queues.at(get_worker_thread_id());
        }
        inline uint8_t get_worker_thread_id(){return thread_manager.get_worker_thread_id();}
        inline uint8_t get_openmp_worker_thread_id(){return thread_manager.get_openmp_worker_thread_id();}
        void execute_manual_delta_block_checking(vertex_t vid);
        void force_consolidation_clean();
        void configure_distinct_readers_and_writers(uint64_t reader_count, uint64_t writer_count){
#if USING_SEPARATED_TABLES
            block_access_ts_table.set_total_worker_thread_num(writer_count,reader_count);
#else
            block_access_ts_table.set_total_worker_thread_num(reader_count+writer_count);
#endif
            total_writer_num = writer_count;
            thread_manager.reset_worker_thread_id();
            thread_manager.reset_openmp_thread_id();
            txn_tables.resize(total_writer_num,this);
            garbage_queues.clear();
            garbage_queues.reserve(reader_count+writer_count);
            for(uint64_t i=0; i<reader_count+writer_count;i++){
                garbage_queues.emplace_back(&block_manager);
            }
            //will be done while the commit server is down
            commit_manager.resize_commit_array(writer_count);
            set_writer_thread_num(writer_count);
        }
        inline void increment_thread_local_update_count(){thread_local_update_count.local()++;}
        inline void print_garbage_status(){
            std::cout<<"total has "<<thread_manager.get_real_worker_thread_size()<<" worker threads"<<std::endl;
            block_access_ts_table.print_ts_status();
            for(uint64_t i=0; i<thread_manager.get_real_worker_thread_size();i++){
                std::cout<<"worker thread "<<i<<": ";
                garbage_queues[i].print_status();
            }
        }
        void print_ts_table(){
            block_access_ts_table.print_ts_status();
        }
        inline void thread_exit(){
            block_access_ts_table.thread_exit(get_worker_thread_id());
            auto local_thread_id = get_worker_thread_id();
            while(!garbage_queues[local_thread_id].get_queue().empty()){
                auto safe_ts = block_access_ts_table.calculate_safe_ts();
                garbage_queues.at(local_thread_id).free_block(safe_ts);
            }
        }
        inline void garbage_clean(){
            auto local_thread_id = get_worker_thread_id();
            auto safe_ts = block_access_ts_table.calculate_safe_ts();
            garbage_queues.at(local_thread_id).free_block(safe_ts);
        }
        /*
         * in insert only or graphalytics, they are the total writer or total reader num
         * in mixed workload, they equal reader + writer
         */
        inline void set_worker_thread_num(uint64_t new_num){
            if(new_num>worker_thread_num){
                throw std::runtime_error("error, the number of worker thread is larger than the max threshold");
            }
            block_access_ts_table.set_total_worker_thread_num(new_num);
            thread_manager.reset_worker_thread_id();
            total_writer_num=0;
        }
        /*
         * invoked at system loadup time. Specify which threads are for updates or simply specify how many writers do we have.
         * Libin added on July 25th: this function is executed while the commit server is idle
         */
        inline void set_writer_thread_num(uint64_t writer_num){
            uint64_t total_worker_num = block_access_ts_table.get_total_thread_num();
            for(uint64_t i=0; i<total_worker_num; i++){
                if(i<writer_num){
                    block_access_ts_table.store_current_ts(static_cast<uint8_t>(i),0);
                }else{
                    block_access_ts_table.store_current_ts(static_cast<uint8_t>(i),std::numeric_limits<uint64_t>::max());
                }
            }
            total_writer_num = writer_num;
            commit_manager.resize_commit_array(writer_num);//also reset writer number
        }

        /*
         * specialized for gfe mixed workload, set all remaining workers to a large enough safe ts as a guard
         */
        inline void on_finish_loading(){
            uint64_t total_worker_num = block_access_ts_table.get_total_thread_num();
            auto safe_ts = block_access_ts_table.calculate_safe_ts();
            for(uint64_t i=total_writer_num; i<total_worker_num; i++){
                block_access_ts_table.store_current_ts(static_cast<uint8_t>(i),safe_ts);
            }
        }
        /*
         * used whenever an openmp shared txn starts
         */
        inline void on_openmp_transaction_start(timestamp_t read_ts){
            uint64_t total_worker_num = block_access_ts_table.get_total_thread_num();
            for(uint64_t i=total_writer_num; i<total_worker_num; i++){
                block_access_ts_table.store_current_ts(static_cast<uint8_t>(i),read_ts);
            }
        }
        inline void on_openmp_parallel_session_finish(){
            thread_manager.reset_openmp_thread_id();
        }
        void eager_consolidation_on_edge_delta_block(vertex_t vid, label_t label);
        inline WorkerThreadManager& get_thread_manager(){return thread_manager;}
        void on_openmp_workloads_finish(){
            uint64_t total_worker_num = block_access_ts_table.get_total_thread_num();
            for(uint64_t i=total_writer_num; i<total_worker_num; i++){
                block_access_ts_table.store_current_ts(static_cast<uint8_t>(i),std::numeric_limits<uint64_t>::max());
            }
        }
       /* inline void reset_worker_thread_num(uint64_t new_num){
            if(new_num>worker_thread_num){
                throw std::runtime_error("error, the number of worker thread is larger than the max threshold");
            }
            block_access_ts_table.set_total_worker_thread_num(new_num);
        }*/
#if TRACK_EXECUTION_TIME
        tbb::enumerable_thread_specific<size_t> local_thread_vertex_read_time;
        //std::array<std::atomic_uint64_t , worker_thread_num> global_vertex_read_time_array;
        tbb::enumerable_thread_specific<size_t> local_thread_vertex_write_time;
        //std::array<std::atomic_uint64_t , worker_thread_num> global_vertex_write_time_array;
        tbb::enumerable_thread_specific<size_t> local_thread_edge_read_time;
        //std::array<std::atomic_uint64_t , worker_thread_num> global_edge_read_time_array;
        tbb::enumerable_thread_specific<size_t> local_thread_edge_write_time;
        //std::array<std::atomic_uint64_t , worker_thread_num> global_edge_write_time_array;
        tbb::enumerable_thread_specific<size_t> local_thread_commit_time;
        tbb::enumerable_thread_specific<size_t> local_thread_abort_time;
        tbb::enumerable_thread_specific<size_t> local_rwtxn_creation_time;
        tbb::enumerable_thread_specific<size_t> local_get_thread_id_time;
        tbb::enumerable_thread_specific<size_t> local_generate_txn_id_time;
        tbb::enumerable_thread_specific<size_t> local_install_txn_entry_time;
        tbb::enumerable_thread_specific<size_t> local_garbage_collection_time;
        tbb::enumerable_thread_specific<size_t> local_eager_clean_real_work_time;
        tbb::enumerable_thread_specific<size_t> local_edge_clean_real_work_time;
        tbb::enumerable_thread_specific<size_t> local_vertex_clean_real_work_time;
        tbb::enumerable_thread_specific<size_t> txn_execution_time;
#endif
#if TRACK_COMMIT_ABORT
        inline void register_commit(){commit_count.fetch_add(1,std::memory_order_acq_rel);}
        inline void register_abort(){abort_count.fetch_add(1,std::memory_order_acq_rel);}
        inline void register_loop(){retry_count.fetch_add(1,std::memory_order_acq_rel);}
        void print_and_clear_txn_stats(){
            std::cout<<"total committed txns number is "<<commit_count.load(std::memory_order_acquire)<<std::endl;
            std::cout<<"total abort txns number is "<<abort_count.load(std::memory_order_acquire)<<std::endl;
            std::cout<<"total retry count is "<<retry_count.load(std::memory_order_acquire)<<std::endl;
            commit_count.store(0,std::memory_order_release);
            abort_count.store(0,std::memory_order_release);
            retry_count.store(0,std::memory_order_release);
        }
#endif
    private:
        //todo::add eager clean related functions
        void eager_consolidation_clean();
        BlockManager block_manager;
        VertexIndex vertex_index;
        TxnTables txn_tables;
        //CommitManager commit_manager;
        CommitManager commit_manager;
        BlockAccessTimestampTable block_access_ts_table;
        WorkerThreadManager thread_manager;
        std::vector<GarbageBlockQueue> garbage_queues;
        //std::array<std::queue<vertex_t>,worker_thread_num> recycled_vids;
        std::vector<std::queue<vertex_t>>recycled_vids;
        tbb::enumerable_thread_specific<size_t> executed_txn_count;
        //delete deltas and update deltas are both considered new versions: they are the ones that's worth checking on
        //tbb::enumerable_thread_specific<std::unordered_set<uint64_t>> to_check_blocks;
        tbb::enumerable_thread_specific<std::unordered_map<uint64_t, uint64_t>> to_check_blocks;
        tbb::enumerable_thread_specific<size_t> thread_local_update_count;
        uint64_t total_writer_num =0;
#if TRACK_COMMIT_ABORT
        std::atomic_uint64_t commit_count = 0;
        std::atomic_uint64_t abort_count = 0;
        std::atomic_uint64_t retry_count = 0;
#endif
        //uint64_t total_reader_num = 0;
        friend class ROTransaction;
        friend class RWTransaction;
    };
}


//#endif //BWGRAPH_V2_BWGRAPH_HPP
