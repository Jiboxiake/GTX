//
// Created by zhou822 on 6/17/23.
//

//#ifndef BWGRAPH_LIB_BWGRAPH_HPP
//#define BWGRAPH_LIB_BWGRAPH_HPP
#pragma once
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <stdexcept>
#include <thread>
namespace  bwgraph{
    class BwGraph;
    class RWTransaction;
    class ROTransaction;
    class SharedROTransaction;
    class EdgeDeltaIterator;
    class BaseEdgeDelta;
    class LightEdgeDelta;
    class SimpleEdgeDeltaIterator;
    class StaticEdgeDeltaIterator;
    class EdgeDeltaBlockHeader;
}
namespace bg {
    using label_t = uint16_t;
    using vertex_t = uint64_t;
    using order_t = uint8_t;
    using timestamp_t = uint64_t;
    //using lazy_update_map = std::unordered_map<uint64_t,int32_t>;//txn local cache for lazy update
    using delta_chain_id_t = int32_t;

    class RWTransaction;
    class ROTransaction;
    class SharedROTransaction;
    class EdgeDeltaIterator;
    class SimpleEdgeDeltaIterator;
    class HybridEdgeDeltaIterator;
    //class SimpleObjectEdgeDeltaIterator;
    class StaticEdgeDeltaIterator;
    class Graph {
    public:
        Graph(std::string block_path = "",size_t _max_block_size = 1ul << 40,
                std::string wal_path = "");
        ~Graph();

        ROTransaction begin_read_only_transaction();
        SharedROTransaction begin_shared_read_only_transaction();
        RWTransaction begin_read_write_transaction();
        vertex_t get_max_allocated_vid();
        void commit_server_start();
        void commit_server_shutdown();
        uint8_t get_worker_thread_id();
        void execute_manual_checking(vertex_t vid);
        bool is_txn_table_empty();
        void force_consolidation_clean();
        void thread_exit();
        void print_garbage_queue_status();
        void set_worker_thread_num(uint64_t new_size);
        void set_writer_thread_num(uint64_t writer_thread_num);
        void on_finish_loading();
        uint8_t get_openmp_worker_thread_id();
        void on_openmp_txn_start(uint64_t read_ts);
        void on_openmp_section_finishing();
        void garbage_clean();
        void manual_commit_server_shutdown();
        void manual_commit_server_restart();
        void eager_consolidation_on_edge_delta_block(vertex_t vid, label_t label);
        void whole_label_graph_eager_consolidation(label_t label);
        void configure_distinct_readers_and_writers(uint64_t reader_count, uint64_t writer_count);
        void on_openmp_workloads_finish();
        void print_and_clear_txn_stats();
        //for debug
        bwgraph::EdgeDeltaBlockHeader* get_edge_block(vertex_t vid, label_t l);
        void print_thread_id_allocation();
    private:
        const std::unique_ptr<bwgraph::BwGraph> graph;
        std::thread commit_manager_worker;

    };

    class ROTransaction{
    public:
        ROTransaction(std::unique_ptr<bwgraph::ROTransaction> _txn);
        ~ROTransaction();
        void commit();
        //read operations:
        std::string_view get_vertex(vertex_t src);
        std::string_view get_edge(vertex_t src, vertex_t dst, label_t label);
        double get_edge_weight(vertex_t src, vertex_t dst, label_t label);
        EdgeDeltaIterator get_edges(vertex_t src, label_t label);
        SimpleEdgeDeltaIterator simple_get_edges(vertex_t src, label_t label);
    private:
        const std::unique_ptr<bwgraph::ROTransaction> txn;
    };

    class RollbackExcept : public std::runtime_error
    {
    public:
        RollbackExcept(const std::string &what_arg) : std::runtime_error(what_arg) {}
        RollbackExcept(const char *what_arg) : std::runtime_error(what_arg) {}
    };

    class SharedROTransaction{
    public:
        SharedROTransaction(std::unique_ptr<bwgraph::SharedROTransaction> _txn, Graph* source_graph);
        ~SharedROTransaction();
        void commit();
        //read operations:
        std::string_view get_vertex(vertex_t src);
        std::string_view get_vertex(vertex_t src,uint8_t thread_id);
        std::string_view get_edge(vertex_t src, vertex_t dst, label_t label);
        EdgeDeltaIterator get_edges(vertex_t src, label_t label);
        std::string_view get_edge(vertex_t src, vertex_t dst, label_t label,uint8_t thread_id);
        EdgeDeltaIterator get_edges(vertex_t src, label_t label,uint8_t thread_id);
        SimpleEdgeDeltaIterator simple_get_edges(vertex_t src, label_t label);
        SimpleEdgeDeltaIterator simple_get_edges(vertex_t src, label_t label,uint8_t thread_id);
        void simple_get_edges(vertex_t src, label_t label, uint8_t thread_id, SimpleEdgeDeltaIterator& edge_iterator);
        void hybrid_get_edges(vertex_t src, label_t label, uint8_t thread_id, HybridEdgeDeltaIterator& edge_iterator);
        SimpleEdgeDeltaIterator generate_edge_delta_iterator(uint8_t thread_id);
        HybridEdgeDeltaIterator generate_hybrid_edge_delta_iterator(uint8_t thread_id);
        StaticEdgeDeltaIterator generate_static_edge_delta_iterator();
        std::string_view static_get_vertex(vertex_t src);
        std::string_view static_get_edge(vertex_t src, vertex_t dst, label_t label);
        StaticEdgeDeltaIterator static_get_edges(vertex_t src, label_t label);
        void static_get_edges(vertex_t src, label_t label, StaticEdgeDeltaIterator& edge_iterator);
        uint64_t get_read_timestamp();
        void print_debug_info();
        void thread_on_openmp_section_finish(uint8_t thread_id);
        Graph* get_graph();
    private:
        const std::unique_ptr<bwgraph::SharedROTransaction> txn;
        Graph* graph;
    };
    class RWTransaction{
    public:
        RWTransaction(std::unique_ptr<bwgraph::RWTransaction> _txn);
        ~RWTransaction();
        bool commit();
        void abort();
        //read operations:
        std::string_view get_vertex(vertex_t src);
        std::string_view get_edge(vertex_t src, vertex_t dst, label_t label);
        //bool get_edge_property(vertex_t src, vertex_t dst, label_t label,std::string_view result);
        EdgeDeltaIterator get_edges(vertex_t src, label_t label);
        SimpleEdgeDeltaIterator simple_get_edges(vertex_t src, label_t label);
        //write operations:
        vertex_t new_vertex();
        void put_vertex(vertex_t vertex_id, std::string_view data);
        //bool del_vertex(vertex_t vertex_id, bool recycle = false);
        void put_edge(vertex_t src, label_t label, vertex_t dst, std::string_view edge_data);
        void delete_edge(vertex_t src, label_t label, vertex_t dst);//right now just ensure final result does not contain this edge
        bool checked_put_edge(vertex_t src, label_t label, vertex_t dst, std::string_view edge_data);
        bool checked_delete_edge(vertex_t src, label_t label, vertex_t dst);
    private:
        const std::unique_ptr<bwgraph::RWTransaction> txn;
    };
    class EdgeDeltaIterator{
    public:
        EdgeDeltaIterator(std::unique_ptr<bwgraph::EdgeDeltaIterator> _iter);
        ~EdgeDeltaIterator();

        //bool valid() const;
        bool valid();
        void close();
        void next();
        vertex_t dst_id() const;
        std::string_view  edge_delta_data() const;
    private:
        const std::unique_ptr<bwgraph::EdgeDeltaIterator> iterator;
        bwgraph::BaseEdgeDelta* current_delta;
    };
    class SimpleEdgeDeltaIterator{
    public:
        SimpleEdgeDeltaIterator(std::unique_ptr<bwgraph::SimpleEdgeDeltaIterator> _iter);
        ~SimpleEdgeDeltaIterator();

        bool valid();
        bool valid_second_round();
        void close();
        void next();
        void next_second_round();
        vertex_t dst_id() const;
        std::string_view  edge_delta_data() const;
        double edge_delta_weight() const;
        uint64_t get_vertex_degree();
        std::unique_ptr<bwgraph::SimpleEdgeDeltaIterator> iterator;
    private:
        bwgraph::BaseEdgeDelta* current_delta;
        //bwgraph::LightEdgeDelta* current_light_delta;
        //uint32_t remaining_normal_delta_count;
        //uint32_t remaining_light_delta_count;

    };
    class StaticEdgeDeltaIterator{
    public:
        StaticEdgeDeltaIterator(std::unique_ptr<bwgraph::StaticEdgeDeltaIterator> _iter);
        ~StaticEdgeDeltaIterator();
        void clear(){
            current_delta = nullptr;
        }
        bool valid();
        //void close();
        void next();
        uint32_t vertex_degree();
        vertex_t dst_id() const;
        std::string_view  edge_delta_data() const;
        double get_weight();
        std::unique_ptr<bwgraph::StaticEdgeDeltaIterator> iterator;
    private:
       
        bwgraph::BaseEdgeDelta* current_delta;
    };

    class HybridEdgeDeltaIterator{
    public:
        HybridEdgeDeltaIterator(std::unique_ptr<bwgraph::SimpleEdgeDeltaIterator> _iter);
        ~HybridEdgeDeltaIterator();
        void close();
        bool valid();
        void next();
        vertex_t dst_id()const;
        std::string_view  edge_delta_data() const;
        double get_weight();
        uint32_t neighborhood_size();
        std::unique_ptr<bwgraph::SimpleEdgeDeltaIterator> iterator;
    };
   /* class SimpleObjectEdgeDeltaIterator{
        SimpleObjectEdgeDeltaIterator();

    };*/
} // bg

//#endif //BWGRAPH_LIB_BWGRAPH_HPP
