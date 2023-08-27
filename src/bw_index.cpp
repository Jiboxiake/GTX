//
// Created by zhou822 on 5/27/23.
//
#include "core/bw_index.hpp"
#include "core/block_manager.hpp"
#include "core/utils.hpp"
#include "core/block.hpp"
namespace bwgraph{
    void EdgeLabelBlock::deallocate_all_delta_chains_indices() {
        uint8_t current_offset = offset.load(std::memory_order_acquire);
        for(uint8_t i=0; i<current_offset;i++){
            if(label_entries[i].delta_chain_index)
                delete label_entries[i].delta_chain_index;
        }
        if(next_ptr){
            EdgeLabelBlock* next_block = block_manager->convert<EdgeLabelBlock>(next_ptr);
            next_block->deallocate_all_delta_chains_indices();
        }
    }
    bool EdgeLabelBlock::reader_lookup_label(bwgraph::label_t target_label, bwgraph::BwLabelEntry *&target_entry) {
        uint8_t current_offset=0;
        do{
            current_offset = offset.load(std::memory_order_acquire);
            for(int i=0; i<current_offset;){
                if(label_entries[i].valid.load(std::memory_order_acquire)){
                    if(label_entries[i].label==target_label){
                        target_entry = &label_entries[i];
                        return true;
                    }
                }else{
                    continue;
                }
                i++;
            }
        }while(current_offset!=offset.load(std::memory_order_acquire));
        if(next_ptr){
            if(current_offset!=BW_LABEL_BLOCK_SIZE){
                throw LabelBlockPointerException();
            }
            return block_manager->convert<EdgeLabelBlock>(next_ptr)->reader_lookup_label(target_label,target_entry);
        }else{
            return false;
        }
    }
    //todo: double check this function
    //todo:: modify to add consolidation time?
    //will always succeed
    BwLabelEntry *EdgeLabelBlock::writer_lookup_label(bwgraph::label_t target_label, TxnTables* txn_tables,timestamp_t txn_read_ts) {
        //loop until we observe concurrent updates
        EdgeLabelBlock* current_label_block = this;
        while(true){
            uint8_t current_offset=0;
            //loop check the current block
            do{
                current_offset = current_label_block->offset.load(std::memory_order_acquire);
                for(int i=0; i<current_offset;){
                    if(current_label_block->label_entries[i].valid.load(std::memory_order_acquire)){
                        if(current_label_block->label_entries[i].label==target_label){
                            return &(current_label_block->label_entries[i]);
                        }
                    }else{
                        continue;
                    }
                    i++;
                }
            }while(current_offset!=current_label_block->offset.load(std::memory_order_acquire));
            //did not find target entry, and reach the end of the delta chain, but the current has space, allocate there
            if(current_offset<BW_LABEL_BLOCK_SIZE){
                uint8_t new_offset = current_offset+1;
                if(current_label_block->offset.compare_exchange_strong(current_offset,new_offset, std::memory_order_acq_rel)){
                    current_label_block->label_entries[current_offset].label = target_label;
                    //todo:: now we are testing using large blocks Libin:reversed
                    current_label_block->label_entries[current_offset].block_ptr = block_manager->alloc(DEFAULT_EDGE_DELTA_BLOCK_ORDER-1);
                    auto new_edge_delta_block = block_manager->convert<SimpleEdgeDeltaBlockHeader>(current_label_block->label_entries[current_offset].block_ptr);

                    //todo::debug
                  /*  if(new_edge_delta_block->get_current_offset()!=0){
                        std::cout<<"owner id is "<<new_edge_delta_block->get_owner_id()<<" creation ts is "<<new_edge_delta_block->get_creation_time()<<" order is "<<static_cast<int32_t>(new_edge_delta_block->get_order())<<" previous ptr is "<<new_edge_delta_block->get_previous_ptr()<<" delta chain num is "<<new_edge_delta_block->get_delta_chain_num()<<std::endl;
                        auto debug_vertex_delta = block_manager->convert<VertexDeltaHeader>(current_label_block->label_entries[current_offset].block_ptr);
                        std::cout<<" creation ts is "<<debug_vertex_delta->get_creation_ts()<<" order is "<<static_cast<int32_t>(debug_vertex_delta->get_order())<<" previous ptr is "<<debug_vertex_delta->get_previous_ptr()<<" data size is "<<debug_vertex_delta->get_data_size()<<" data storage is "<<debug_vertex_delta->get_max_data_storage()<<std::endl;
                        throw std::runtime_error("bad block allocation before");
                    }*/
                    //todo:: now we are testing using large blocks Libin:reversed
                    //new_edge_delta_block->fill_metadata(owner_id,txn_read_ts,0,24,txn_tables,current_label_block->label_entries[current_offset].delta_chain_index);
                    new_edge_delta_block->fill_metadata(txn_read_ts,txn_read_ts,0,DEFAULT_EDGE_DELTA_BLOCK_ORDER-1,txn_tables);
                    //todo::debug
                  /*  if(new_edge_delta_block->get_current_offset()!=0){
                        throw std::runtime_error("bad block allocation after");
                    }*/
                    current_label_block->label_entries[current_offset].state.store(EdgeDeltaBlockState::NORMAL,std::memory_order_release);
                    current_label_block->label_entries[current_offset].block_version_number.store(0,std::memory_order_release);
                    current_label_block->label_entries[current_offset].valid.store(true,std::memory_order_release);
                    return &current_label_block->label_entries[current_offset];
                }else{//if allocation failed, someone else must have allocated a new entry, re-loop and re-observe what's going on by not changing current block and continue
                    continue;
                }
            }else{//if the current block has no space, go to the next block
                auto current_next_ptr = current_label_block->next_ptr.load(std::memory_order_acquire);
                //allocate new block and install using CAS
                if(!current_next_ptr){
                    order_t new_order = size_to_order(sizeof(EdgeLabelBlock));
                    uintptr_t new_next_ptr = block_manager->alloc(new_order);
                    auto new_block =  block_manager->convert<EdgeLabelBlock>(new_next_ptr);
                    new_block->offset=1;
                    new_block->block_manager = block_manager;
                    new_block->label_entries[0].label = target_label;
                    //todo:: allocate a block as well maybe?
                    new_block->label_entries[0].block_ptr =block_manager->alloc(DEFAULT_EDGE_DELTA_BLOCK_ORDER-1);
                    auto new_edge_delta_block = block_manager->convert<SimpleEdgeDeltaBlockHeader>(new_block->label_entries[0].block_ptr);
                   // new_block->label_entries[0].delta_chain_index = new std::vector<AtomicDeltaOffset>();
                    new_edge_delta_block->fill_metadata(txn_read_ts,txn_read_ts,0,DEFAULT_EDGE_DELTA_BLOCK_ORDER-1,txn_tables);
                   // new_block->label_entries[0].delta_chain_index->resize(new_edge_delta_block->get_delta_chain_num());
                    new_block->label_entries[0].state.store(EdgeDeltaBlockState::NORMAL,std::memory_order_release);
                    new_block->label_entries[0].block_version_number.store(0,std::memory_order_release);
                    new_block->label_entries[0].valid.store(true,std::memory_order_release);
                    if(current_label_block->next_ptr.compare_exchange_strong(current_next_ptr,new_next_ptr,std::memory_order_acq_rel)){
                        return &new_block->label_entries[0];
                    }else{
                        //deallocate failed installation
                        delete new_block->label_entries[0].delta_chain_index;
                        auto zero_out_ptr = block_manager->convert<uint8_t>(new_block->label_entries[0].block_ptr);
                        memset(zero_out_ptr,'\0',1ul<<(DEFAULT_EDGE_DELTA_BLOCK_ORDER-1));
                        block_manager->free(new_block->label_entries[0].block_ptr,(DEFAULT_EDGE_DELTA_BLOCK_ORDER-1));
                        zero_out_ptr = block_manager->convert<uint8_t>(new_next_ptr);
                        memset(zero_out_ptr,'\0',1ul<<new_order);
                        block_manager->free(new_next_ptr,new_order);
                        current_label_block =  block_manager->convert<EdgeLabelBlock>(current_label_block->next_ptr.load());
                        continue;
                    }
                }else{
                    current_label_block = block_manager->convert<EdgeLabelBlock>(current_label_block->next_ptr.load());
                    continue;
                }
            }
        }
    }

}


