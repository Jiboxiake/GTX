//
// Created by zhou822 on 6/10/23.
//
#include <doctest/doctest/doctest.h>
#include <core/bw_index.hpp>
#include "core/bwgraph.hpp"
#include "core/mini_bwgraph.hpp"
using namespace bwgraph;

TEST_CASE("BwGraph allocation test"){
    BwGraph bwGraph;
    auto& commit_manager = bwGraph.get_commit_manager();
    //manually setup some blocks
    auto& vertex_index = bwGraph.get_vertex_index();
    auto& block_manager = bwGraph.get_block_manager();
    //initial state setup
    for(int32_t i=0; i<1000000; i++){
        vertex_t vid = vertex_index.get_next_vid();
        auto& vertex_entry = bwGraph.get_vertex_index_entry(vid);
        //allocate initial blocks
        vertex_entry.vertex_delta_chain_head_ptr = block_manager.alloc(size_to_order(sizeof(VertexDeltaHeader))+1);
        VertexDeltaHeader* vertex_delta = block_manager.convert<VertexDeltaHeader>( vertex_entry.vertex_delta_chain_head_ptr.load());
        vertex_delta->fill_metadata(0,32,6);
        char data[32];
        for(int j=0;j<32; j++){
            data[j]=static_cast<char>(vid%32);
        }
        vertex_delta->write_data(data);
        vertex_entry.edge_label_block_ptr= block_manager.alloc(size_to_order(sizeof(EdgeLabelBlock)));
        EdgeLabelBlock* edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_entry.edge_label_block_ptr);
        edge_label_block->fill_information(vid,&block_manager);
        edge_label_block->writer_lookup_label(1,&bwGraph.get_txn_tables(),0);
        vertex_entry.valid.store(true);
    }
    for(vertex_t vid=1; vid<=1000000; vid++){
        auto& vertex_entry = bwGraph.get_vertex_index_entry(vid);
        CHECK(vertex_entry.valid);
        CHECK(vertex_entry.vertex_delta_chain_head_ptr);
        CHECK(vertex_entry.edge_label_block_ptr);
        VertexDeltaHeader* vertex_delta = block_manager.convert<VertexDeltaHeader>( vertex_entry.vertex_delta_chain_head_ptr.load());
        CHECK_EQ(vertex_delta->get_order(),6);
        CHECK_EQ(vertex_delta->get_creation_ts(),0);
        CHECK_EQ(vertex_delta->get_data_size(),32);
        CHECK_EQ(vertex_delta->get_max_data_storage(),32);
        char* data = vertex_delta->get_data();
        for(size_t j=0; j<vertex_delta->get_data_size();j++){
            CHECK_EQ(data[j],static_cast<char>(vid%32));
        }
        CHECK_FALSE(vertex_delta->get_previous_ptr());
        EdgeLabelBlock* edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_entry.edge_label_block_ptr);
        BwLabelEntry* target_entry;
        CHECK(edge_label_block->reader_lookup_label(1,target_entry));
        BwLabelEntry* compare_entry = edge_label_block->writer_lookup_label(1, &bwGraph.get_txn_tables(),0);
        CHECK_EQ(target_entry,compare_entry);
        CHECK(target_entry->valid);
        CHECK(target_entry->block_ptr);
        CHECK_EQ(target_entry->label,1);
        CHECK_EQ(target_entry->state,EdgeDeltaBlockState::NORMAL);
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_entry->block_ptr);
        CHECK_EQ(current_block->get_creation_time(),0);
        CHECK_EQ(target_entry->delta_chain_index->size(),current_block->get_delta_chain_num());
        //CHECK_EQ(current_block->get_owner_id(),vid);
        CHECK_EQ(current_block->get_order(),DEFAULT_EDGE_DELTA_BLOCK_ORDER);
        CHECK_FALSE(current_block->get_current_offset());
        CHECK_FALSE(current_block->get_previous_ptr());
    }
    for(vertex_t vid=1; vid<=1000000; vid++){
        auto& vertex_entry = bwGraph.get_vertex_index_entry(vid);
        EdgeLabelBlock* edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_entry.edge_label_block_ptr);
        BwLabelEntry* target_entry;
        CHECK(edge_label_block->reader_lookup_label(1,target_entry));
        delete target_entry->delta_chain_index;
    }
    //commit_manager_worker = std::thread(&CommitManager::server_loop, &commit_manager);//start executing the commit manager
}

TEST_CASE("Mini Bw Edge Test under doctest"){
    for(int i=0; i<5; i++){
        MiniBwGraph* test = new MiniBwGraph(20);
        //test->execute_edge_vertex_test();
        test->execute_checked_edge_vertex_test();
        delete test;
    }
}