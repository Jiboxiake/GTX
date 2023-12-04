//
// Created by zhou822 on 6/17/23.
//

#include "bwgraph.hpp"
#include "core/bwgraph_include.hpp"
#include <omp.h>
#include <charconv>

using namespace bg;
namespace impl = bwgraph;

Graph:: ~Graph(){
    commit_server_shutdown();
    commit_manager_worker.join();
}
Graph::Graph(std::string block_path, size_t _max_block_size, std::string wal_path) :graph(std::make_unique<impl::BwGraph>(block_path, _max_block_size, wal_path)){
    commit_manager_worker =std::thread(&Graph::commit_server_start, this);
}

vertex_t Graph::get_max_allocated_vid() {return graph->get_max_allocated_vid();}

RWTransaction Graph::begin_read_write_transaction() {
    return std::make_unique<impl::RWTransaction>(graph->begin_read_write_transaction());
}

ROTransaction Graph::begin_read_only_transaction() {
   // std::cout<<"ro"<<std::endl;
    return std::make_unique<impl::ROTransaction>(graph->begin_read_only_transaction());
}

bg::SharedROTransaction Graph::begin_shared_read_only_transaction() {
    //graph->get_thread_manager().print_debug_stats();
    return {std::make_unique<impl::SharedROTransaction>(graph->begin_shared_ro_transaction()),this};
}

bwgraph::EdgeDeltaBlockHeader *Graph::get_edge_block(bg::vertex_t vid, bg::label_t l) {
    auto& index_entry = graph->get_vertex_index_entry(vid);
    auto target_label_block = graph->get_block_manager().convert<bwgraph::EdgeLabelBlock>(index_entry.edge_label_block_ptr);
    bwgraph::BwLabelEntry* target_label_entry;
    auto result = target_label_block->reader_lookup_label(l,target_label_entry);
    return graph->get_block_manager().convert<bwgraph::EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
}
void Graph::commit_server_start() {
    graph->get_commit_manager().server_loop();
}
void Graph::commit_server_shutdown() {
    graph->get_commit_manager().shutdown_signal();
}

uint8_t Graph::get_worker_thread_id() {
    return graph->get_worker_thread_id();
}

void Graph::print_thread_id_allocation() {
    graph->get_thread_manager().print_debug_stats();
}
void Graph::execute_manual_checking(bg::vertex_t vid) {
    graph->execute_manual_delta_block_checking(vid);
}
bool Graph::is_txn_table_empty() {
    auto& txn_tables = graph->get_txn_tables();
    for(auto i=0; i<worker_thread_num-1; i++){
        if(!txn_tables.get_table(i).is_empty()){
            return false;
        }
    }
    return true;
}

void Graph::print_garbage_queue_status() {
    graph->print_garbage_status();
}
void Graph::thread_exit() {
    graph->thread_exit();
}

void Graph::garbage_clean() {
    graph->garbage_clean();
}
void Graph::force_consolidation_clean() {
    graph->force_consolidation_clean();
}

void Graph::set_worker_thread_num(uint64_t new_size) {
    graph->set_worker_thread_num(new_size);
}
/*
 * sets the writer number in the mixed workload
 */
void Graph::set_writer_thread_num(uint64_t writer_num){
    graph->set_writer_thread_num(writer_num);
}

void Graph::on_finish_loading(){
    graph->on_finish_loading();
}
/*
 * to be cached by the reader
 */
uint8_t Graph::get_openmp_worker_thread_id() {
    return graph->get_openmp_worker_thread_id();
}

void Graph::print_and_clear_txn_stats() {
#if TRACK_COMMIT_ABORT
    graph->print_and_clear_txn_stats();
#endif
#if TRACK_GARBAGE_RECORD_TIME
    graph->get_block_manager().print_avg_alloc_time();
    graph->get_block_manager().print_avg_free_time();
    graph->get_block_manager().print_avg_garbage_record_time();
#endif
}

void Graph::on_openmp_txn_start(uint64_t read_ts) {
    graph->on_openmp_transaction_start(read_ts);
}

void Graph::on_openmp_section_finishing() {
    graph->on_openmp_parallel_session_finish();
}

void Graph::manual_commit_server_shutdown() {
    graph->get_commit_manager().shutdown_signal();
    commit_manager_worker.join();
}

void Graph::manual_commit_server_restart() {
    graph->get_commit_manager().restart();
    commit_manager_worker =std::thread(&Graph::commit_server_start, this);
}

void Graph::eager_consolidation_on_edge_delta_block(vertex_t vid, label_t label) {
    graph->eager_consolidation_on_edge_delta_block(vid,label);
}

void Graph::whole_label_graph_eager_consolidation(bg::label_t label) {
    auto max_vid = graph->get_max_allocated_vid();
#pragma omp parallel for
    for(vertex_t i=1; i<=max_vid;i++){
        graph->eager_consolidation_on_edge_delta_block(i,label);
    }
}

void Graph::configure_distinct_readers_and_writers(uint64_t reader_count, uint64_t writer_count) {
    manual_commit_server_shutdown();
    graph->configure_distinct_readers_and_writers(reader_count,writer_count);
    manual_commit_server_restart();
}

void Graph::on_openmp_workloads_finish() {
    graph->on_openmp_workloads_finish();
}
//read only transactions
ROTransaction::ROTransaction(std::unique_ptr<bwgraph::ROTransaction> _txn) :txn(std::move(_txn)){}

ROTransaction:: ~ROTransaction() = default;

std::string_view ROTransaction::get_vertex(bg::vertex_t src) {return txn->get_vertex(src);}

std::string_view ROTransaction::get_edge(bg::vertex_t src, bg::vertex_t dst, bg::label_t label) {
   // std::cout<<"ro"<<std::endl;
    while(true){
        auto result = txn->get_edge(src,dst,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return result.second;
        }
    }
}

double ROTransaction::get_edge_weight(bg::vertex_t src, bg::vertex_t dst, bg::label_t label) {
    double* weight = nullptr;
    while(true){
        auto result = txn->get_edge_weight(src,label,dst,weight);
        if(result == bwgraph::Txn_Operation_Response::SUCCESS)[[likely]]{
            if(weight)[[likely]]{
                return *weight;
            }else{
                return std::numeric_limits<double>::signaling_NaN();
            }
        }
    }
}

EdgeDeltaIterator ROTransaction::get_edges(bg::vertex_t src, bg::label_t label) {
   // std::cout<<"ro"<<std::endl;
    while(true){
        auto result = txn->get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }
    }
}

SimpleEdgeDeltaIterator ROTransaction::simple_get_edges(bg::vertex_t src, bg::label_t label) {
    while(true){
        auto result = txn->simple_get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::SimpleEdgeDeltaIterator>(result.second);
        }
    }
}

void ROTransaction::commit() {txn->commit();}

SharedROTransaction::SharedROTransaction(std::unique_ptr<bwgraph::SharedROTransaction> _txn, Graph* source): txn(std::move(_txn)), graph(source) {}

SharedROTransaction::~SharedROTransaction() = default;

void SharedROTransaction::commit() {txn->commit();}

uint64_t SharedROTransaction::get_read_timestamp() {
    return txn->get_read_ts();
}

std::string_view SharedROTransaction::static_get_vertex(bg::vertex_t src) {
    return txn->static_get_vertex(src);
}

std::string_view SharedROTransaction::static_get_edge(bg::vertex_t src, bg::vertex_t dst, bg::label_t label) {
    return txn->static_get_edge(src,dst,label);
}

StaticEdgeDeltaIterator SharedROTransaction::static_get_edges(bg::vertex_t src, bg::label_t label) {
    return std::make_unique<impl::StaticEdgeDeltaIterator>(txn->static_get_edges(src,label));
}
void  SharedROTransaction::static_get_edges(vertex_t src, label_t label, StaticEdgeDeltaIterator& edge_iterator){
    edge_iterator.clear();
    txn->static_get_edges(src,label,edge_iterator.iterator);
}
std::string_view SharedROTransaction::get_vertex(bg::vertex_t src) {
    return txn->get_vertex(src);
}
std::string_view SharedROTransaction::get_vertex(bg::vertex_t src, uint8_t thread_id) {
    return txn->get_vertex(src,thread_id);
}
std::string_view SharedROTransaction::get_edge(bg::vertex_t src, bg::vertex_t dst, bg::label_t label) {
    while(true){
        auto result = txn->get_edge(src,dst,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return result.second;
        }
    }
}

void SharedROTransaction::thread_on_openmp_section_finish(uint8_t thread_id) {
    txn->thread_on_openmp_section_finish(thread_id);
}
std::string_view
SharedROTransaction::get_edge(bg::vertex_t src, bg::vertex_t dst, bg::label_t label, uint8_t thread_id) {
    while (true) {
        auto result = txn->get_edge(src, dst, label, thread_id);
        if (result.first == bwgraph::Txn_Operation_Response::SUCCESS) {
            return result.second;
        }
    }
}

SimpleEdgeDeltaIterator SharedROTransaction::generate_edge_delta_iterator(uint8_t thread_id) {
    return std::make_unique<impl::SimpleEdgeDeltaIterator>(txn->generate_edge_iterator(thread_id));
}

HybridEdgeDeltaIterator SharedROTransaction::generate_hybrid_edge_delta_iterator(uint8_t thread_id) {
    return std::make_unique<impl::SimpleEdgeDeltaIterator>(txn->generate_edge_iterator(thread_id));
}

StaticEdgeDeltaIterator SharedROTransaction::generate_static_edge_delta_iterator(){
    return std::make_unique<impl::StaticEdgeDeltaIterator>(txn->generate_static_edge_iterator());
}

EdgeDeltaIterator SharedROTransaction::get_edges(bg::vertex_t src, bg::label_t label, uint8_t thread_id) {
    while(true){
        auto result = txn->get_edges(src,label, thread_id);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }
    }
}
EdgeDeltaIterator SharedROTransaction::get_edges(bg::vertex_t src, bg::label_t label) {
    while(true){
        auto result = txn->get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }
    }
}
SimpleEdgeDeltaIterator SharedROTransaction::simple_get_edges(bg::vertex_t src, bg::label_t label) {
    while(true){
        auto result = txn->simple_get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::SimpleEdgeDeltaIterator>(result.second);
        }
    }
}
SimpleEdgeDeltaIterator SharedROTransaction::simple_get_edges(bg::vertex_t src, bg::label_t label, uint8_t thread_id) {
    while(true){
        auto result = txn->simple_get_edges(src,label,thread_id);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::SimpleEdgeDeltaIterator>(result.second);
        }
    }
}

void SharedROTransaction::simple_get_edges(bg::vertex_t src, bg::label_t label, uint8_t thread_id,
                                           SimpleEdgeDeltaIterator &edge_iterator) {
    while(true){
        auto result = txn->simple_get_edges(src,label,thread_id,edge_iterator.iterator);
        if(result == bwgraph::Txn_Operation_Response::SUCCESS){
            return;
        }
    }
}

void SharedROTransaction::hybrid_get_edges(bg::vertex_t src, bg::label_t label, uint8_t thread_id,
                                           bg::HybridEdgeDeltaIterator &edge_iterator) {
    while(true){
        auto result = txn->simple_get_edges(src,label,thread_id,edge_iterator.iterator);
        if(result == bwgraph::Txn_Operation_Response::SUCCESS){
            return;
        }
    }
}

void SharedROTransaction::print_debug_info() {
    std::cout<<"Shared RO Transaction printing debug info"<<std::endl;
    graph->print_thread_id_allocation();
}
Graph *SharedROTransaction::get_graph() {
    return graph;
}
//read-write transactions
RWTransaction::~RWTransaction() = default;

RWTransaction::RWTransaction(std::unique_ptr<bwgraph::RWTransaction> _txn):txn(std::move(_txn)) {}

vertex_t RWTransaction::new_vertex() {return txn->create_vertex();}

void RWTransaction::put_vertex(bg::vertex_t vertex_id, std::string_view data) {
#if TRACK_EXECUTION_TIME
    auto start = std::chrono::high_resolution_clock::now();
#endif
    auto result = txn->update_vertex(vertex_id,data);
#if TRACK_EXECUTION_TIME
    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
    txn->get_graph().local_thread_vertex_write_time.local()+= duration.count();
#endif
    if(result==bwgraph::Txn_Operation_Response::SUCCESS){
        return;
    }else{
        throw RollbackExcept("write write conflict vertex");
    }
}

void RWTransaction::put_edge(bg::vertex_t src, bg::label_t label, bg::vertex_t dst, std::string_view edge_data) {
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->put_edge(src,dst,label,edge_data);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == bwgraph::Txn_Operation_Response::SUCCESS){
            return;
        }else if(result ==bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict edge");
        }
    }
}

bool
RWTransaction::checked_put_edge(bg::vertex_t src, bg::label_t label, bg::vertex_t dst, std::string_view edge_data) {
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->checked_put_edge(src,dst,label,edge_data);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        //std::cout<<"executed"<<std::endl;
        if(result == bwgraph::Txn_Operation_Response::SUCCESS_NEW_DELTA){
            return true;
        }else if(result == bwgraph::Txn_Operation_Response::SUCCESS_EXISTING_DELTA){
            return false;
        }
        else if(result ==bwgraph::Txn_Operation_Response::FAIL){
#if TRACK_COMMIT_ABORT
            txn->get_graph().register_abort();
#endif
            //std::cout<<"abort"<<std::endl;
            throw RollbackExcept("write write conflict edge");
        }
#if TRACK_COMMIT_ABORT
        txn->get_graph().register_loop();
#endif
    }
}

bool RWTransaction::checked_single_put_edge(bg::vertex_t src, bg::label_t label, bg::vertex_t dst,
                                            std::string_view edge_data) {
    while (true) {
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->checked_single_edge_insertion(src, dst, label, edge_data);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        //std::cout<<"executed"<<std::endl;
        if (result == bwgraph::Txn_Operation_Response::SUCCESS_NEW_DELTA) {
            return true;
        } else if (result == bwgraph::Txn_Operation_Response::SUCCESS_EXISTING_DELTA) {
            return false;
        } else if (result == bwgraph::Txn_Operation_Response::FAIL)[[unlikely]] {
#if TRACK_COMMIT_ABORT
            txn->get_graph().register_abort();
#endif
            //std::cout<<"abort"<<std::endl;
            throw RollbackExcept("write write conflict edge");
        }
#if TRACK_COMMIT_ABORT
        txn->get_graph().register_loop();
#endif
    }
}
void RWTransaction::delete_edge(bg::vertex_t src, bg::label_t label, bg::vertex_t dst) {
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->delete_edge(src,dst,label);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == bwgraph::Txn_Operation_Response::SUCCESS){
            return;
        }else if(result ==bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("write write conflict edge");
        }
    }
}

bool RWTransaction::checked_delete_edge(bg::vertex_t src, bg::label_t label, bg::vertex_t dst) {
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->checked_delete_edge(src,dst,label);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == bwgraph::Txn_Operation_Response::SUCCESS_EXISTING_DELTA){
            return true;
        }else if(result == bwgraph::Txn_Operation_Response::SUCCESS_NEW_DELTA){
            return false;
        }
        else if(result ==bwgraph::Txn_Operation_Response::FAIL){
#if TRACK_COMMIT_ABORT
            txn->get_graph().register_abort();
#endif
            throw RollbackExcept("write write conflict edge");
        }
    }
}

bool RWTransaction::checked_single_delete_edge(bg::vertex_t src, bg::label_t label, bg::vertex_t dst) {
    while(true){
#if TRACK_EXECUTION_TIME
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto result = txn->checked_single_edge_deletion(src,dst,label);
#if TRACK_EXECUTION_TIME
        auto stop = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(stop - start);
        txn->get_graph().local_thread_edge_write_time.local()+= duration.count();
#endif
        if(result == bwgraph::Txn_Operation_Response::SUCCESS_EXISTING_DELTA){
            return true;
        }else if(result == bwgraph::Txn_Operation_Response::SUCCESS_NEW_DELTA){
            return false;
        }
        else if(result ==bwgraph::Txn_Operation_Response::FAIL){
#if TRACK_COMMIT_ABORT
            txn->get_graph().register_abort();
#endif
            throw RollbackExcept("write write conflict edge");
        }
    }
}
std::string_view RWTransaction::get_vertex(bg::vertex_t src) {
    return txn->get_vertex(src);
}

std::string_view RWTransaction::get_edge(bg::vertex_t src, bg::vertex_t dst, bg::label_t label) {
    while(true){
        auto result = txn->get_edge(src,dst,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return result.second;
        }else if(result.first == bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("found write write conflict on previous write when reading edge");
        }
    }
}

EdgeDeltaIterator RWTransaction::get_edges(bg::vertex_t src, bg::label_t label) {
    while(true){
        auto result = txn->get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::EdgeDeltaIterator>(result.second);
        }else if(result.first == bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("found write write conflict on previous write when scanning edges");
        }
    }
}

SimpleEdgeDeltaIterator RWTransaction::simple_get_edges(bg::vertex_t src, bg::label_t label) {
    while(true){
        auto result = txn->simple_get_edges(src,label);
        if(result.first==bwgraph::Txn_Operation_Response::SUCCESS){
            return std::make_unique<impl::SimpleEdgeDeltaIterator>(result.second);
        }else if(result.first == bwgraph::Txn_Operation_Response::FAIL){
            throw RollbackExcept("found write write conflict on previous write when scanning edges");
        }
    }
}

bool RWTransaction::commit() {
#if USING_EAGER_COMMIT
    return txn->eager_commit();
#else
    return txn->commit();
#endif
}

void RWTransaction::abort() {
    txn->abort();
}

EdgeDeltaIterator::EdgeDeltaIterator(std::unique_ptr<bwgraph::EdgeDeltaIterator> _iter):iterator(std::move(_iter)) {}

EdgeDeltaIterator::~EdgeDeltaIterator() = default;

void EdgeDeltaIterator::close() {iterator->close();}

void EdgeDeltaIterator::next() {
    current_delta = iterator->next_delta();
}
bool EdgeDeltaIterator::valid() {
    next();
    return current_delta!= nullptr;
}
vertex_t EdgeDeltaIterator::dst_id() const {
    return current_delta->toID;
}

std::string_view EdgeDeltaIterator::edge_delta_data() const {
    if(current_delta->data_length<=16){
        return std::string_view (current_delta->data, current_delta->data_length);
    }else{
        return std::string_view (iterator->get_data(current_delta->data_offset),current_delta->data_length);
    }
}

//simple edge iterator
SimpleEdgeDeltaIterator::SimpleEdgeDeltaIterator(std::unique_ptr<bwgraph::SimpleEdgeDeltaIterator> _iter):iterator(std::move(_iter)) {

}

SimpleEdgeDeltaIterator::~SimpleEdgeDeltaIterator() = default;

void SimpleEdgeDeltaIterator::close() {iterator->close();}

void SimpleEdgeDeltaIterator::next() {
    current_delta = iterator->next_delta();
}
void SimpleEdgeDeltaIterator::next_second_round() {
    current_delta = iterator->next_delta_second_round();
}
bool SimpleEdgeDeltaIterator::valid() {
    next();
    return current_delta!= nullptr;
}
bool SimpleEdgeDeltaIterator::valid_second_round() {
    next_second_round();
    return current_delta!= nullptr;
}
vertex_t SimpleEdgeDeltaIterator::dst_id() const {
    return current_delta->toID;
}

uint64_t SimpleEdgeDeltaIterator::get_vertex_degree() {
    return iterator->vertex_degree();
}

std::string_view SimpleEdgeDeltaIterator::edge_delta_data() const {
    if(current_delta->data_length<=16){
        return std::string_view (current_delta->data, current_delta->data_length);
    }else{
        return std::string_view (iterator->get_data(current_delta->data_offset),current_delta->data_length);
    }
}

double SimpleEdgeDeltaIterator::edge_delta_weight() const {
    return  *reinterpret_cast<double*>(current_delta->data);
}

StaticEdgeDeltaIterator::StaticEdgeDeltaIterator(std::unique_ptr<bwgraph::StaticEdgeDeltaIterator> _iter):iterator(std::move(_iter)) {}

StaticEdgeDeltaIterator::~StaticEdgeDeltaIterator() = default;

void StaticEdgeDeltaIterator::next() {
    current_delta = iterator->next_delta();
}

bool StaticEdgeDeltaIterator::valid() {
    next();
    return current_delta!= nullptr;
}

vertex_t StaticEdgeDeltaIterator::dst_id() const {
    return current_delta->toID;
}

std::string_view StaticEdgeDeltaIterator::edge_delta_data() const {
    if(current_delta->data_length<=16){
        return std::string_view (current_delta->data, current_delta->data_length);
    }else{
        return std::string_view (iterator->get_data(current_delta->data_offset),current_delta->data_length);
    }
}

uint32_t StaticEdgeDeltaIterator::vertex_degree(){
    return iterator->get_degree();
}
double StaticEdgeDeltaIterator::get_weight() {
    return *reinterpret_cast<double*>(current_delta->data);
}

HybridEdgeDeltaIterator::HybridEdgeDeltaIterator(std::unique_ptr<bwgraph::SimpleEdgeDeltaIterator> _iter):iterator(std::move(_iter)) {

}

HybridEdgeDeltaIterator::~HybridEdgeDeltaIterator() = default;

void HybridEdgeDeltaIterator::next() {
    iterator->next();
}

bool HybridEdgeDeltaIterator::valid() {
    iterator->next();
    return iterator->valid();
}

void HybridEdgeDeltaIterator::close() {
    iterator->close();
}

vertex_t HybridEdgeDeltaIterator::dst_id() const {
    return iterator->dst_id();
}

std::string_view HybridEdgeDeltaIterator::edge_delta_data() const {
    return iterator->get_edge_data();
}

double HybridEdgeDeltaIterator::get_weight() {
    return iterator->get_weight();
}

uint32_t HybridEdgeDeltaIterator::neighborhood_size() {
    return iterator->vertex_degree();
}