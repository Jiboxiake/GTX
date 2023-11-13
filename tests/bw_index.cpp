//
// Created by zhou822 on 6/7/23.
//
#include <doctest/doctest/doctest.h>
#include <core/bw_index.hpp>
#include "core/bwgraph.hpp"
using namespace bwgraph;

TEST_CASE("Test DocTest"){
    int i=0;
    CHECK_EQ(i,0);
    std::cout<<"passed"<<std::endl;
}
//passed, with vertex delta creation
TEST_CASE("Vertex Index Test 1"){
    BlockManager block_manager("", 1e+11);
    VertexIndex vertex_index(block_manager);
    for(uint64_t i=0; i<10; i++){
        vertex_t vid = vertex_index.get_next_vid();
        auto& entry = vertex_index.get_vertex_index_entry(vid);
        entry.valid.store(true);
        auto vertex_delta_ptr = block_manager.alloc(6);
        entry.vertex_delta_chain_head_ptr = vertex_delta_ptr;
        auto vertex_delta = block_manager.convert<VertexDeltaHeader>(vertex_delta_ptr);
        vertex_delta->fill_metadata(vid,0,6,0);
        CHECK_EQ(vid,i+1);
    }
    for(uint64_t i=1; i<=10; i++){
        auto& entry = vertex_index.get_vertex_index_entry(i);
        CHECK(entry.valid);//txn will manually set it true
        CHECK_EQ(entry.edge_label_block_ptr,0);
        auto vertex_delta = block_manager.convert<VertexDeltaHeader>(entry.vertex_delta_chain_head_ptr.load());
        CHECK_EQ(vertex_delta->get_order(),6);
        CHECK_EQ(vertex_delta->get_creation_ts(),i);
        //std::cout<<entry.vertex_delta_chain_head_ptr.load()<<std::endl;
        CHECK_EQ(vertex_delta->get_previous_ptr(),0);
        CHECK_EQ(vertex_delta->get_data_size(),0);
        CHECK_EQ(vertex_delta->get_max_data_storage(),32);
        //CHECK_EQ(entry.vertex_delta_chain_head_ptr,0);
    }
}
TEST_CASE("Vertex Index Test 2"){
    BwGraph* g = new BwGraph();
    auto& vertex_index = g->get_vertex_index();
    for(int i=0; i<100000000; i++){
        auto vid = vertex_index.get_next_vid();
        CHECK_EQ(vid,i+1);
    }
    delete g;
}
void thread_create_vertex(std::atomic_bool& blocking, VertexIndex& vertex_index){
    while(blocking.load());
    for(int i=0; i<10000000; i++){
        vertex_index.get_next_vid();
    }
}
TEST_CASE("Vertex Index Test 3"){
    BwGraph* g = new BwGraph();
    auto& vertex_index = g->get_vertex_index();
    std::atomic_bool blocking = true;
    std::vector<std::thread>workers;
    for(int i=0; i<10; i++){
        workers.emplace_back(std::thread(thread_create_vertex, std::ref(blocking), std::ref(vertex_index)));
    }
    blocking.store(false);
    for(int i=0; i<10; i++){
        workers.at(i).join();
    }
    CHECK_EQ(vertex_index.get_next_vid(),100000001);
    delete g;
}
//test edge label entry
TEST_CASE("Edge Label Entry Test 1"){
    BwGraph* g = new BwGraph();
    TxnTables txn_table(g);
    VertexIndex vertex_index(g->get_block_manager());
    for(uint64_t i=0; i<10; i++){
        vertex_t vid = vertex_index.get_next_vid();
        auto& entry = vertex_index.get_vertex_index_entry(vid);
        entry.valid.store(true);
        auto vertex_delta_ptr = g->get_block_manager().alloc(6);
        entry.vertex_delta_chain_head_ptr = vertex_delta_ptr;
        auto vertex_delta = g->get_block_manager().convert<VertexDeltaHeader>(vertex_delta_ptr);
        vertex_delta->fill_metadata(vid,0,6,0);
        CHECK_EQ(vid,i+1);
        auto label_block_ptr= g->get_block_manager().alloc(size_to_order(sizeof(EdgeLabelBlock)));
        entry.edge_label_block_ptr = label_block_ptr;
        auto edge_label_block = g->get_block_manager().convert<EdgeLabelBlock>(label_block_ptr);
        edge_label_block->fill_information(vid,&g->get_block_manager());
        BwLabelEntry* test_entry;
        CHECK_FALSE(edge_label_block->reader_lookup_label(1,test_entry));
        auto label_entry = edge_label_block->writer_lookup_label(1,&txn_table,vid);
        CHECK(label_entry);
        CHECK_EQ(label_entry->label,1);
    }
    for(uint64_t i=1; i<=10; i++){
        auto& entry = vertex_index.get_vertex_index_entry(i);
        auto edge_label_block = g->get_block_manager().convert<EdgeLabelBlock>(entry.edge_label_block_ptr);
        BwLabelEntry* test_entry;
        CHECK(edge_label_block->reader_lookup_label(1,test_entry));
        CHECK_EQ(test_entry->block_version_number,0);
        CHECK(test_entry->valid);
        CHECK_EQ(test_entry->label,1);
        auto block = g->get_block_manager().convert<EdgeDeltaBlockHeader>(test_entry->block_ptr);
        CHECK_EQ(block->get_order(),DEFAULT_EDGE_DELTA_BLOCK_ORDER);
        //CHECK_EQ(block->get_owner_id(),i);
        CHECK_EQ(block->get_creation_time(),i);
        CHECK_FALSE(block->get_previous_ptr());
        delete test_entry->delta_chain_index;
    }
    delete g;
}

TEST_CASE("Edge Label Entry Test 2"){
    BwGraph* g = new BwGraph();
    TxnTables txn_table(g);
    VertexIndex vertex_index(g->get_block_manager());
    for(uint64_t i=0; i<10; i++){
        vertex_t vid = vertex_index.get_next_vid();
        auto& entry = vertex_index.get_vertex_index_entry(vid);
        entry.valid.store(true);
        auto vertex_delta_ptr = g->get_block_manager().alloc(6);
        entry.vertex_delta_chain_head_ptr = vertex_delta_ptr;
        auto vertex_delta = g->get_block_manager().convert<VertexDeltaHeader>(vertex_delta_ptr);
        vertex_delta->fill_metadata(vid,0,6,0);
        CHECK_EQ(vid,i+1);
        auto label_block_ptr= g->get_block_manager().alloc(size_to_order(sizeof(EdgeLabelBlock)));
        entry.edge_label_block_ptr = label_block_ptr;
        auto edge_label_block = g->get_block_manager().convert<EdgeLabelBlock>(label_block_ptr);
        edge_label_block->fill_information(vid,&g->get_block_manager());
        BwLabelEntry* test_entry;
        CHECK_FALSE(edge_label_block->reader_lookup_label(1,test_entry));
        auto label_entry = edge_label_block->writer_lookup_label(1,&txn_table,1);
        CHECK(label_entry);
        CHECK_EQ(label_entry->label,1);
        CHECK_EQ(label_entry->block_version_number,0);
        label_entry = edge_label_block->writer_lookup_label(2,&txn_table,2);
        CHECK(label_entry);
        CHECK_EQ(label_entry->label,2);
        CHECK_EQ(label_entry->block_version_number,0);
        label_entry = edge_label_block->writer_lookup_label(3,&txn_table,3);
        CHECK(label_entry);
        CHECK_EQ(label_entry->label,3);
        CHECK_EQ(label_entry->block_version_number,0);
    }
    for(uint64_t i=1; i<=10; i++){
        auto& entry = vertex_index.get_vertex_index_entry(i);
        auto edge_label_block = g->get_block_manager().convert<EdgeLabelBlock>(entry.edge_label_block_ptr);
        BwLabelEntry* test_entry;
        for(int j=1; j<=3; j++){
            CHECK(edge_label_block->reader_lookup_label(j,test_entry));
            CHECK(test_entry->valid);
            CHECK_EQ(test_entry->label,j);
            CHECK_EQ(test_entry->block_version_number,0);
            auto block = g->get_block_manager().convert<EdgeDeltaBlockHeader>(test_entry->block_ptr);
            CHECK_EQ(block->get_order(),DEFAULT_EDGE_DELTA_BLOCK_ORDER);
            //CHECK_EQ(block->get_owner_id(),i);
            CHECK_EQ(block->get_creation_time(),j);
            CHECK_FALSE(block->get_previous_ptr());
            delete test_entry->delta_chain_index;
        }
    }
    delete g;
}

TEST_CASE("Edge Label Entry Test 3"){
    BwGraph* g = new BwGraph();
    TxnTables txn_table(g);
    VertexIndex vertex_index(g->get_block_manager());
    for(uint64_t i=0; i<10; i++){
        vertex_t vid = vertex_index.get_next_vid();
        auto& entry = vertex_index.get_vertex_index_entry(vid);
        entry.valid.store(true);
        auto vertex_delta_ptr = g->get_block_manager().alloc(6);
        entry.vertex_delta_chain_head_ptr = vertex_delta_ptr;
        auto vertex_delta = g->get_block_manager().convert<VertexDeltaHeader>(vertex_delta_ptr);
        vertex_delta->fill_metadata(vid,0,6,0);
        CHECK_EQ(vid,i+1);
        auto label_block_ptr= g->get_block_manager().alloc(size_to_order(sizeof(EdgeLabelBlock)));
        entry.edge_label_block_ptr = label_block_ptr;
        auto edge_label_block = g->get_block_manager().convert<EdgeLabelBlock>(label_block_ptr);
        edge_label_block->fill_information(vid,&g->get_block_manager());
        BwLabelEntry* test_entry;
        for(int j=1; j<=5;j++){
            CHECK_FALSE(edge_label_block->reader_lookup_label(j,test_entry));
            auto label_entry = edge_label_block->writer_lookup_label(j,&txn_table,j);
            CHECK(label_entry);
            CHECK_EQ(label_entry->label,j);
            CHECK_EQ(label_entry->block_version_number,0);
        }
        CHECK(edge_label_block->next_ptr.load());
    }
    for(uint64_t i=1; i<=10; i++){
        auto& entry = vertex_index.get_vertex_index_entry(i);
        auto edge_label_block = g->get_block_manager().convert<EdgeLabelBlock>(entry.edge_label_block_ptr);
        BwLabelEntry* test_entry;
        for(int j=1; j<=5; j++){
            CHECK(edge_label_block->reader_lookup_label(j,test_entry));
            auto compare_entry = edge_label_block->writer_lookup_label(j,&txn_table,j*10);
            CHECK_EQ(test_entry,compare_entry);
            CHECK(test_entry->valid);
            CHECK_EQ(test_entry->label,j);
            CHECK_EQ(test_entry->block_version_number,0);
            auto block = g->get_block_manager().convert<EdgeDeltaBlockHeader>(test_entry->block_ptr);
            CHECK_EQ(block->get_order(),DEFAULT_EDGE_DELTA_BLOCK_ORDER);
            //CHECK_EQ(block->get_owner_id(),i);
            CHECK_EQ(block->get_creation_time(),j);
            CHECK_FALSE(block->get_previous_ptr());
            delete test_entry->delta_chain_index;
        }
    }
    delete g;
}
void thread_access_label_entry(std::atomic_bool& blocking,EdgeLabelBlock* edge_label_block,int32_t thread_id, label_t label, std::vector<BwLabelEntry*>& label_access_result,TxnTables* txn_tables){
    while(blocking.load());
    BwLabelEntry* test_entry;
    auto compare_entry = edge_label_block->writer_lookup_label(label,txn_tables,3);
    CHECK(edge_label_block->reader_lookup_label(label,test_entry));
    CHECK_EQ(test_entry,compare_entry);
    label_access_result[thread_id]= test_entry;
}
TEST_CASE("Edge Label Entry Test 4"){
    BwGraph* g = new BwGraph();
    TxnTables txn_table(g);
    VertexIndex& vertex_index =g->get_vertex_index();
    vertex_t vid = vertex_index.get_next_vid();
    auto& entry = vertex_index.get_vertex_index_entry(vid);
    auto label_block_ptr= g->get_block_manager().alloc(size_to_order(sizeof(EdgeLabelBlock)));
    entry.edge_label_block_ptr = label_block_ptr;
    auto edge_label_block = g->get_block_manager().convert<EdgeLabelBlock>(label_block_ptr);
    edge_label_block->fill_information(vid,&g->get_block_manager());
    std::vector<BwLabelEntry*> label_access_result;
    label_access_result.resize(10);
    std::atomic_bool blocking = true;
    std::vector<std::thread>workers;
    for(int i=0; i<10; i++){
        workers.emplace_back(std::thread(thread_access_label_entry, std::ref(blocking), edge_label_block, i, 1, std::ref(label_access_result),&txn_table));
    }
    blocking.store(false);
    for(int i=0; i<10; i++){
        workers.at(i).join();
    }
    for(int j=1; j<10; j++){
        CHECK_EQ(label_access_result[0],label_access_result[j]);
    }
    CHECK_EQ(edge_label_block->label_entries[0].label,1);
    CHECK(edge_label_block->label_entries[0].delta_chain_index);
    CHECK(edge_label_block->label_entries[0].block_ptr);
    CHECK(edge_label_block->label_entries[0].valid);
    for(int i=1; i<=2;i++){
        CHECK_FALSE(edge_label_block->label_entries[i].label);
        CHECK_FALSE(edge_label_block->label_entries[i].delta_chain_index);
        CHECK_FALSE(edge_label_block->label_entries[i].block_ptr);
        CHECK_FALSE(edge_label_block->label_entries[i].valid);
    }
    delete label_access_result[0]->delta_chain_index;
    delete g;
}
TEST_CASE("Edge Label Entry Test 5"){
    BwGraph* g = new BwGraph();
    TxnTables txn_table(g);
    VertexIndex& vertex_index =g->get_vertex_index();
    vertex_t vid = vertex_index.get_next_vid();
    auto& entry = vertex_index.get_vertex_index_entry(vid);
    auto label_block_ptr= g->get_block_manager().alloc(size_to_order(sizeof(EdgeLabelBlock)));
    entry.edge_label_block_ptr = label_block_ptr;
    auto edge_label_block = g->get_block_manager().convert<EdgeLabelBlock>(label_block_ptr);
    edge_label_block->fill_information(vid,&g->get_block_manager());
    std::vector<BwLabelEntry*> label_access_result;
    label_access_result.resize(40);
    std::atomic_bool blocking = true;
    std::vector<std::thread>workers;
    for(int i=0; i<10; i++){
        workers.emplace_back(std::thread(thread_access_label_entry, std::ref(blocking), edge_label_block, 3*i+0, 1, std::ref(label_access_result),&txn_table));
        workers.emplace_back(std::thread(thread_access_label_entry, std::ref(blocking), edge_label_block, 3*i+1, 2, std::ref(label_access_result),&txn_table));
        workers.emplace_back(std::thread(thread_access_label_entry, std::ref(blocking), edge_label_block, 3*i+2, 3, std::ref(label_access_result),&txn_table));
    }
    blocking.store(false);
    for(int i=0; i<30; i++){
        workers.at(i).join();
    }
    for(int j=0; j<10; j++){
        CHECK_NE(label_access_result[3*j+0],label_access_result[3*j+1]);
        CHECK_NE(label_access_result[3*j+0],label_access_result[3*j+2]);
        CHECK_NE(label_access_result[3*j+1],label_access_result[3*j+2]);
        for(int i=0; i<10; i++){
            CHECK_EQ(label_access_result[3*j+0],label_access_result[3*i+0]);
            CHECK_EQ(label_access_result[3*j+1],label_access_result[3*i+1]);
            CHECK_EQ(label_access_result[3*j+2],label_access_result[3*i+2]);
        }
    }
    CHECK_FALSE(edge_label_block->next_ptr);
    CHECK_EQ(edge_label_block->offset,3);
    delete label_access_result[0]->delta_chain_index;
    delete label_access_result[1]->delta_chain_index;
    delete label_access_result[2]->delta_chain_index;
    delete g;
}
