//
// Created by zhou822 on 1/4/24.
//

#pragma once
#include "bw_index.hpp"
#include "bw_transaction.hpp"
#include "transaction_tables.hpp"
#include "edge_iterator.hpp"
#include "previous_version_garbage_queue.hpp"
#include "types.hpp"
#include "edge_delta_block_state_protection.hpp"
#include <set>
#include "Libraries/gapbs.hpp"
namespace bwgraph{
    /*
     * Implementation as taken from Graphalytics
     */
    class PageRank{
    public:
        PageRank(BwGraph* input_graph, uint64_t input_num):graph(input_graph),num_vertices(input_num){}
        void compute_pagerank(uint64_t num_iterations, double damping_factor){
            SharedROTransaction txn = graph->begin_shared_ro_transaction();
            auto read_ts = txn.get_read_ts();
            auto txn_tables = &graph->get_txn_tables();
            const double init_score = 1.0/num_vertices;
            const double base_score = (1.0-damping_factor)/num_vertices;
            auto max_vid = graph->get_max_allocated_vid();
            //std::vector<double> scores(max_vid);
            scores.resize(max_vid);
            std::vector<uint64_t> degrees(max_vid);
#pragma omp parallel
            {
                uint8_t thread_id = graph->get_openmp_worker_thread_id();
#pragma omp for
                for(uint64_t v = 1; v<=max_vid; v++){
                    scores[v-1] = init_score;
                    degrees[v-1]= txn.neighborhood_size(v,1,thread_id);
                }
                txn.thread_on_openmp_section_finish(thread_id);
            }
            graph->on_openmp_parallel_session_finish();
            gapbs::pvector<double> outgoing_contrib(max_vid, 0.0);
            for(uint64_t iteration = 0; iteration < num_iterations; iteration++){
                double dangling_sum = 0.0;
#pragma omp parallel for reduction(+:dangling_sum)
                for(uint64_t v = 1; v <= max_vid; v++){
                    uint64_t out_degree = degrees[v-1];
                    if(out_degree == std::numeric_limits<uint64_t>::max()){
                        continue; // the vertex does not exist
                    } else if (out_degree == 0){ // this is a sink
                        dangling_sum += scores[v-1];
                    } else {
                        outgoing_contrib[v-1] = scores[v-1] / out_degree;
                    }
                }
                dangling_sum /= num_vertices;
#pragma omp parallel
                {
                    uint8_t thread_id = graph->get_openmp_worker_thread_id();
#pragma omp for schedule(dynamic, 64)
                    for (uint64_t v = 1; v <= max_vid; v++) {
                        if (degrees[v-1] == std::numeric_limits<uint64_t>::max())[[unlikely]] { continue; }
                        double incoming_total = 0;
                        uint32_t current_delta_offset = 0;
                        auto current_block = txn.get_block_header(v,1,thread_id,&current_delta_offset);
                        //determine which block to read
                        if(current_block->get_creation_time()<=read_ts)[[likely]]{
                            auto latest_version_start_offset = current_block->latest_version_delta_num()*LIGHT_DELTA_SIZE;
                            auto normal_end_offset = current_block->calculate_padding_size()+ latest_version_start_offset;
                            BaseEdgeDelta* current_delta;
                            LightEdgeDelta* current_light_delta;
                            if(current_delta_offset>normal_end_offset){
                                current_delta = current_block->get_edge_delta(current_delta_offset);
                            }
                            if(latest_version_start_offset)[[likely]]{
                                current_light_delta = current_block->get_light_edge_delta(latest_version_start_offset);
                            }
                            //start scanning
                            while(current_delta_offset>normal_end_offset){
                                //need lazy update
                                auto original_ts = current_delta->creation_ts.load(std::memory_order_acquire);
                                if(!original_ts)[[unlikely]]{
                                    current_delta_offset -= ENTRY_DELTA_SIZE;
                                    current_delta++;
                                    continue;
                                }
                                if(is_txn_id(original_ts)){
                                    uint64_t status = 0;
                                    if (txn_tables->get_status(original_ts, status))[[likely]] {
                                        if (status == IN_PROGRESS)[[likely]] {
                                            current_delta_offset -= ENTRY_DELTA_SIZE;
                                            current_delta++;
                                            continue;
                                        } else {
                                            if (status != ABORT)[[likely]] {
#if CHECKED_PUT_EDGE
                                                current_block->update_previous_delta_invalidate_ts(current_delta->toID,
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
                                                    txn_tables->reduce_op_count(original_ts,1);
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
                                }
                                if (current_delta->delta_type != EdgeDeltaType::DELETE_DELTA) {
                                    uint64_t current_invalidation_ts = current_delta->invalidate_ts.load(
                                            std::memory_order_acquire);
                                    //found the visible edge
                                    if(original_ts<=read_ts&&(current_invalidation_ts==0||current_invalidation_ts>read_ts)){
                                        incoming_total += outgoing_contrib[current_delta->toID-1];
                                    }
                                }
                                current_delta_offset -= ENTRY_DELTA_SIZE;
                                current_delta++;
                            }
#if EDGE_DELTA_TEST
                            if(normal_end_offset!=current_delta_offset){
                                throw std::runtime_error("error, scan corruption");
                            }
#endif
                            if(normal_end_offset!=latest_version_start_offset)[[likely]]{
                                current_delta_offset = latest_version_start_offset;
                            }
                            while(current_delta_offset>0){
                                auto current_invalidation_ts = current_light_delta->invalidate_ts.load(std::memory_order_acquire);
                                if(current_light_delta->creation_ts<=read_ts&&(current_invalidation_ts>read_ts||current_invalidation_ts==0)){
                                    incoming_total += outgoing_contrib[current_light_delta->toID-1];
                                }
                                current_light_delta++;
                                current_delta_offset-=LIGHT_DELTA_SIZE;
                            }
                        }else{//read previous block, not happening in static setting
                            throw std::runtime_error("error, should not reach here");
                            txn.unregister_thread_block_access(thread_id);
                            bool found = false;
                            while (current_block->get_previous_ptr()) {
                                current_block = graph->get_block_manager().convert<EdgeDeltaBlockHeader>(
                                        current_block->get_previous_ptr());
                                if (read_ts >= current_block->get_creation_time()) {
                                    found = true;
                                    break;
                                }
                            }
                            if(found)[[likely]]{

                            }
                        }
                        scores[v-1] = base_score + damping_factor * (incoming_total + dangling_sum);
                        txn.unregister_thread_block_access(thread_id);
                    }
                    txn.thread_on_openmp_section_finish(thread_id);
                }
                graph->on_openmp_parallel_session_finish();
            }
            result.resize(max_vid);
#pragma omp parallel
            {
                uint8_t thread_id = graph->get_openmp_worker_thread_id();
#pragma omp for
                for(uint64_t logical_id = 1; logical_id <= max_vid; logical_id++){
                    std::string_view payload = txn.get_vertex(logical_id,thread_id);//they store external vid in the vertex data for experiments
                    if(payload.empty()){ // the vertex does not exist
                        result[logical_id-1] = std::make_pair(std::numeric_limits<uint64_t>::max(), std::numeric_limits<double>::max());
                    } else {
                        /*if(*(reinterpret_cast<const uint64_t*>(payload.data()))==8196461){
                            std::cout<<" max vid is "<<max_vid<<" iteration is "<< logical_id<<std::endl;
                            std::cout<< *(reinterpret_cast<const uint64_t*>(payload.data())) <<" "<< scores[logical_id-1]<< std::endl;
                        }*/
                        result[logical_id-1] = std::make_pair(*(reinterpret_cast<const uint64_t*>(payload.data())), scores[logical_id-1]);
                    }
                }
                txn.thread_on_openmp_section_finish(thread_id);
            }
            graph->on_openmp_parallel_session_finish();
            txn.commit();
        }
        inline std::vector<double>* get_raw_result(){
            return &scores;
        }
        inline std::vector<std::pair<uint64_t, double>>* get_result(){
            return &result;
        }
    private:
        BwGraph* graph;
        std::vector<double> scores;
        uint64_t num_vertices;
        std::vector<std::pair<uint64_t, double>>result;
    };
}
