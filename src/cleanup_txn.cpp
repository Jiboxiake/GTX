//
// Created by zhou822 on 7/7/23.
//
#include "core/cleanup_txn.hpp"
#include "core/utils.hpp"
using namespace bwgraph;
/*
 * scans the corresponding edge delta block. It checks whether the block is quite full already
 */
bool Cleanup_Transaction::work_on_edge_block(uint64_t block_id, uint64_t block_version) {
#if USING_EAGER_CONSOLIDATION
#else
    std::cout<<"cleanup txn executed"<<std::endl;
#endif
    auto [src, label] = decompose_block_id(block_id);
    auto& vertex_index_entry = graph.get_vertex_index_entry(src);
    if(!vertex_index_entry.valid.load()){
        return true;//impossible
    }
    BwLabelEntry* target_label_entry;
    auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
    auto found = edge_label_block->reader_lookup_label(label,target_label_entry);
    if(!found){
        return true;//impossible
    }
    if(BlockStateVersionProtectionScheme::writer_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        if(target_label_entry->block_version_number.load()!=block_version){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return true; //already cleaned by someone else
        }
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        uint64_t current_combined_offset = current_block->get_current_offset();
        if(current_block->is_overflow_offset(current_combined_offset)){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return true;//already overflow, so someone will clean it up very soon.
        }
        uint32_t current_head_delta_offset = EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset);
        BaseEdgeDelta* current_head_delta = current_block->get_edge_delta(current_head_delta_offset);
        uint64_t current_head_ts = current_head_delta->creation_ts.load();
        if(!current_head_ts){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return true;//someone updating the block right now
        }
        //if head ts is txn ID, larger timestamp: then concurrent txns wrote to the block, we are safe. If head ts is abort, concurrent txn will retry, also safe
        if(current_head_ts>=read_timestamp){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return true;//the block is not a cold spot, it may get consolidated eventually
        }
        if((read_timestamp-current_head_ts)<cold_spot_threshold){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return false;//someone has updated it a while ago, may need to go back and check it again
        }
        //now cleanup the block, start by eagerly setting the block to overflow:
        //calculate a large number to insert
        uint32_t data_size = (uint32_t) (current_combined_offset >> 32);
        uint32_t delta_size = (uint32_t) (current_combined_offset & SIZE2MASK);
        uint32_t block_size = current_block->get_size();
        uint32_t padding_size = block_size-data_size-delta_size+64;
        auto append_result = allocate_delta(current_block,padding_size);
        if(append_result==EdgeDeltaInstallResult::CAUSE_OVERFLOW){
            //I do consolidation
            consolidation(target_label_entry,current_block,block_id);
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return true;
        }else if(append_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return true;//someone doing consolidation
        }else{
            throw std::runtime_error("error, should overflow");
        }
    }
    return true;//under consolidation right now, so we can peacefully exist
}

/*
 * consolidate the block if needed
 */
void Cleanup_Transaction::consolidation(bwgraph::BwLabelEntry *current_label_entry,
                                        bwgraph::EdgeDeltaBlockHeader *current_block, uint64_t block_id) {
    BlockStateVersionProtectionScheme::install_exclusive_state(EdgeDeltaBlockState::OVERFLOW,thread_id,block_id,current_label_entry,block_access_ts_table);
    uint32_t original_delta_offset = current_delta_offset-ENTRY_DELTA_SIZE;
    uint32_t original_data_offset = current_data_offset;
    uint64_t to_restore_offset = combine_offset(original_delta_offset, original_data_offset);
    current_block->set_offset(to_restore_offset);//restore the offset to allow concurrent transactions to read the block
#if EDGE_DELTA_TEST
    if(current_block->already_overflow()){
         throw std::runtime_error("error, should restored to an unoverflow offset");
    }
#endif
    BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::CONSOLIDATION,current_label_entry);
    BaseEdgeDelta* current_delta = current_block->get_edge_delta(original_delta_offset);
    auto current_head_ts = current_delta->creation_ts.load();
    if(!current_head_ts){
        throw std::runtime_error("error, under mutex state all installed deltas should be valid");
    }
    if(current_head_ts>=read_timestamp){
        //good block, just return
        BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::NORMAL,current_label_entry);
        return;
    }
    std::unordered_set<vertex_t> edge_latest_versions_records;
    std::vector<uint32_t> edge_latest_version_offsets;
    timestamp_t largest_invalidation_ts = std::numeric_limits<uint64_t>::min();//most recent previous version
    timestamp_t largest_creation_ts = std::numeric_limits<uint64_t>::min();//most recent (committed) version
    size_t data_size = 0;
    std::unordered_map<uint64_t, std::vector<uint32_t>>in_progress_delta_per_txn;
    int32_t current_delta_chain_num = current_block->get_delta_chain_num();
    std::set<delta_chain_id_t> to_check_delta_chains;
    size_t previous_version_count =0;
    size_t aborted_delta_count =0;
    while(original_delta_offset>0){
        //should there be no invalid deltas
        timestamp_t original_ts = current_delta->creation_ts.load();
        if(!original_ts){
            throw std::runtime_error("all writer transactions should already installed their deltas");
        }
        //do lazy update if possible
        if(is_txn_id(original_ts)){
            uint64_t status = 0;
            if(txn_tables.get_status(original_ts,status)){
                if(status == IN_PROGRESS){
                    //do nothing
                }else{
                    if(status!=ABORT){
                        current_block->update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_version_offset, status);
                        if(current_delta->lazy_update(original_ts,status)){
                            //record lazy update
                            record_lazy_update_record(&lazy_update_records, original_ts);
                        }

                    }
                    //if status == abort, must already be eager aborted
#if EDGE_DELTA_TEST
                    if(current_delta->creation_ts.load()!=status){
                        throw LazyUpdateException();
                    }
#endif
                }
            }
        }
        original_ts = current_delta->creation_ts.load();
        //now check the status
        if(is_txn_id(original_ts)){
            auto in_progress_txn_deltas_emplace_result = in_progress_delta_per_txn.try_emplace(original_ts, std::vector<uint32_t>());
            in_progress_txn_deltas_emplace_result.first->second.emplace_back(original_delta_offset);
            data_size+=current_delta->data_length+ENTRY_DELTA_SIZE;
            to_check_delta_chains.emplace(calculate_owner_delta_chain_id(current_delta->toID, current_delta_chain_num));
        }else if(original_ts!=ABORT){
            //skip committed delete deltas
            if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                vertex_t toID = current_delta->toID;
                auto latest_version_emplace_result = edge_latest_versions_records.emplace(toID);
                //if indeed latest version
                if(latest_version_emplace_result.second){
                    edge_latest_version_offsets.emplace_back(original_delta_offset);
                    largest_creation_ts = (largest_creation_ts>=original_ts)? largest_creation_ts:original_ts;
                    data_size+=current_delta->data_length+ENTRY_DELTA_SIZE;
                }else{
                    if(!current_delta->invalidate_ts){
                        throw LazyUpdateException();
                    }
                    largest_invalidation_ts = (largest_invalidation_ts>=current_delta->invalidate_ts.load())? largest_invalidation_ts:current_delta->invalidate_ts.load();
                    previous_version_count++;
                }
            }else{
                //still need to count delete delta as latest delta
                vertex_t toID = current_delta->toID;
                auto latest_version_emplace_result =edge_latest_versions_records.emplace(toID);
                if(!latest_version_emplace_result.second){
                    previous_version_count++;
                }
            }
        }else{
            aborted_delta_count++;
        }
        original_delta_offset-=ENTRY_DELTA_SIZE;
        current_delta++;
    }
    //todo:: maybe change the ratio a bit
    //still a good block, not many previous versions or aborted versions, can directly return now
    if(edge_latest_version_offsets.size()>(aborted_delta_count+previous_version_count)){
        BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::NORMAL,current_label_entry);
        return;
    }
    data_size = (data_size==0)? ENTRY_DELTA_SIZE:data_size;
    //analyze scan finished, now apply heuristics
    //use the block creation time vs. latest committed write to estimate lifespan
    //uint64_t lifespan = largest_creation_ts - current_block->get_consolidation_time(); /*current_label_entry->consolidation_time;*/ //approximate lifespan of the block
    //todo:; apply different heuristics
    /*size_t new_block_size = calculate_nw_block_size_from_lifespan(data_size,lifespan,20);
    auto new_order = size_to_order(new_block_size);*/
    auto new_order = calculate_new_fit_order(data_size+sizeof(EdgeDeltaBlockHeader));
    auto new_block_ptr = block_manager.alloc(new_order);
    auto new_block = block_manager.convert<EdgeDeltaBlockHeader>(new_block_ptr);
    //for debug
    /*  if(largest_invalidation_ts){
          std::cout<<"found"<<std::endl;
      }*/
    new_block->fill_metadata(largest_invalidation_ts, read_timestamp, current_label_entry->block_ptr,new_order, &txn_tables,current_label_entry->delta_chain_index);
    int32_t new_block_delta_chain_num = new_block->get_delta_chain_num();
    std::vector<AtomicDeltaOffset> new_delta_chains_index(new_block_delta_chain_num);
    //start installing latest version
    int64_t signed_edge_last_version_size = static_cast<int64_t>(edge_latest_version_offsets.size());
    for(int64_t i = (signed_edge_last_version_size-1); i>=0; i--){
        current_delta = current_block->get_edge_delta(edge_latest_version_offsets.at(i));
        if(is_txn_id(current_delta->creation_ts.load())){
            throw ConsolidationException();//latest versions should all be committed deltas
        }
        //find which delta chain the latest version delta belongs to
        delta_chain_id_t target_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
        const char* data = current_block->get_edge_data(current_delta->data_offset);
        auto& new_delta_chains_index_entry = new_delta_chains_index.at(target_delta_chain_id);
        uint32_t new_block_delta_offset = new_delta_chains_index_entry.get_offset();//if cannot be locked
        //latest versions do not need to worry about previous versions in the new block
        auto consolidation_append_result = new_block->append_edge_delta(current_delta->toID,current_delta->creation_ts.load(),current_delta->delta_type,data,current_delta->data_length,new_block_delta_offset);
        if(consolidation_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!consolidation_append_result.second){
            throw ConsolidationException();
        }
        new_delta_chains_index_entry.update_offset(consolidation_append_result.second);
    }
    //until this step: it is the same as normal Consolidation
    //we installed all committed latest versions and form delta chains
    //now we start the exclusive installation phase
    BlockStateVersionProtectionScheme::install_exclusive_state(EdgeDeltaBlockState::INSTALLATION,thread_id,block_id,current_label_entry,block_access_ts_table);
    //wait for all validating transactions to finish
    while(true){
        for(auto it = to_check_delta_chains.begin(); it!= to_check_delta_chains.end();){
            //get the delta chain head of this delta chain in the original block, check if a validation is happening
            current_delta = current_block->get_edge_delta(current_label_entry->delta_chain_index->at(*it).get_raw_offset());
            uint64_t original_ts = current_delta->creation_ts.load();
            if(is_txn_id(original_ts)){
                uint64_t status =0;
                if(txn_tables.get_status(original_ts,status)){
                    //this transaction still validating, so we will come back later
                    if(status == IN_PROGRESS){
                        it++;
                        continue;
                    }else if(status ==ABORT){ //validating txn aborted, so consolidating thread will help with lazy update, its deltas in the current block is all or nothing (eager aborted)
                        uint32_t to_abort_offset = current_delta->previous_offset;
                        if(!current_delta->lazy_update(original_ts,status)){
                            throw ConsolidationException();//we should never fail lazy update during an exclusive state
                        }
                        auto abort_lazy_update_emplace_result = lazy_update_records.try_emplace(original_ts,1);
                        if(!abort_lazy_update_emplace_result.second){
                            abort_lazy_update_emplace_result.first->second++;
                        }
                        while(to_abort_offset!=0){
                            current_delta = current_block->get_edge_delta(to_abort_offset);
                            if(current_delta->creation_ts!=original_ts){
                                break;
                            }else if(current_delta->creation_ts==original_ts){
                                if(current_delta->lazy_update(original_ts,status)){
                                    abort_lazy_update_emplace_result.first->second++;
                                }else{
                                    throw ConsolidationException();
                                }
                            }
#if CONSOLIDATION_TEST
                            else if(current_delta->creation_ts==ABORT){
                                throw EagerAbortException();//aborted delta should not exit in the delta chain (except my own deltas that I'm aborting )
                            }
#endif
                            to_abort_offset = current_delta->previous_offset;
                        }
                        it = to_check_delta_chains.erase(it);//this delta chain is cleaned
                    }else{//original_ts txn committed after passing validation phase
                        uint32_t to_commit_offset = current_delta->previous_offset;
                        if(!current_delta->lazy_update(original_ts, status)){
                            throw ConsolidationException();
                        }
                        auto commit_lazy_update_emplace_result = lazy_update_records.try_emplace(original_ts,1);
                        if(!commit_lazy_update_emplace_result.second){
                            commit_lazy_update_emplace_result.first->second++;
                        }
                        while(to_commit_offset!=0){
                            current_delta= current_block->get_edge_delta(to_commit_offset);
                            if(current_delta->creation_ts!=original_ts&&current_delta->creation_ts!=status){//someone can already lazy update parts of txn's deltas due to concurrent readers
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts==ABORT){
                                    throw LazyUpdateException();
                                }
#endif
                                break;
                            }else if(current_delta->creation_ts==original_ts){
                                if(!current_delta->lazy_update(original_ts,status)){
                                    throw ConsolidationException();
                                }
                                commit_lazy_update_emplace_result.first->second++;
                            }
                            to_commit_offset = current_delta->previous_offset;
                        }
                        it = to_check_delta_chains.erase(it);
                    }
                }else{
                    throw ConsolidationException();//Installation phase is supposed to be exclusive.
                }
            }else{
                //this delta chain is not under validating or someone already lazy updated for it
                it = to_check_delta_chains.erase(it);
            }
        }
        if(to_check_delta_chains.empty()){
            break;
        }
    }
    //now check all in progress deltas recorded earlier
    //need 2 for loops because committed deltas should be before in-progress deltas
    for(auto it = in_progress_delta_per_txn.begin();it!=in_progress_delta_per_txn.end();){
        //uint64_t status;
        auto& all_delta_offsets_of_txn = it->second;
        current_delta = current_block->get_edge_delta(all_delta_offsets_of_txn.at(0));
        //skip them, will be installed at the end.
        if(is_txn_id(current_delta->creation_ts.load())){
            it++;
        }else if(current_delta->creation_ts.load()==ABORT){
            it = in_progress_delta_per_txn.erase(it);
        }else{//committed deltas
            timestamp_t commit_ts = current_delta->creation_ts.load();
            int64_t txn_own_deltas_size = static_cast<int64_t>(all_delta_offsets_of_txn.size());
            for(int64_t i = txn_own_deltas_size-1; i>=0; i--){
                uint64_t txn_id = it->first;
                current_delta= current_block->get_edge_delta(all_delta_offsets_of_txn.at(i));
                if(current_delta->lazy_update(txn_id,commit_ts)){
                    //todo:: delete this
                    record_lazy_update_record(&lazy_update_records,txn_id);//todo: will this really happen?
                }
                //only install non-delete deltas
                if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                    const char* data = current_block->get_edge_data(current_delta->data_offset);
                    delta_chain_id_t new_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
                    auto& new_delta_chains_index_entry = new_delta_chains_index.at(new_delta_chain_id);
                    uint32_t previous_version_offset=0;
                    if(current_delta->delta_type==EdgeDeltaType::UPDATE_DELTA){
                        //get potential previous version and install invalidation ts
                        previous_version_offset = new_block->fetch_previous_version_offset(current_delta->toID,new_delta_chains_index_entry.get_offset(),current_delta->creation_ts.load(),lazy_update_records);
#if EDGE_DELTA_TEST
                        if(!previous_version_offset){
                            throw std::runtime_error("error, under checked version, an update delta must have a previous version");
                        }
#endif
                    }
                    //can get raw offset because no lock yet
                    // auto commit_delta_append_result = new_block->append_edge_delta(current_delta->toID,commit_ts,current_delta->delta_type,data,current_delta->data_length,new_delta_chains_index_entry.get_offset());
                    auto commit_delta_append_result = new_block->checked_append_edge_delta(current_delta->toID,commit_ts,current_delta->delta_type,data,current_delta->data_length,new_delta_chains_index_entry.get_offset(),previous_version_offset);
                    //can be a newer version:
                    //new_block->update_previous_delta_invalidate_ts(current_delta->toID,new_delta_chains_index_entry.get_offset(),commit_ts);
                    if(commit_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!commit_delta_append_result.second){
                        throw ConsolidationException();
                    }
                    new_delta_chains_index_entry.update_offset(commit_delta_append_result.second);
                }else{
                    delta_chain_id_t new_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
                    auto& new_delta_chains_index_entry = new_delta_chains_index.at(new_delta_chain_id);
                    //get potential previous version and install invalidation ts
                    //a committed delete delta must have a previous version of the edge, so update the invalidation ts of previous version and get its offset
                    uint32_t previous_version_offset = new_block->fetch_previous_version_offset(current_delta->toID,new_delta_chains_index_entry.get_offset(),current_delta->creation_ts.load(),lazy_update_records);
#if EDGE_DELTA_TEST
                    if(!previous_version_offset){
                        throw std::runtime_error("error, under checked version, a delete delta must have a previous version");
                    }
#endif
                    auto commit_delta_append_result = new_block->checked_append_edge_delta(current_delta->toID,commit_ts,current_delta->delta_type,nullptr,current_delta->data_length,new_delta_chains_index_entry.get_offset(),previous_version_offset);
                    //can be a newer version:
                    //new_block->update_previous_delta_invalidate_ts(current_delta->toID,new_delta_chains_index_entry.get_offset(),commit_ts);
                    if(commit_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!commit_delta_append_result.second){
                        throw ConsolidationException();
                    }
                    new_delta_chains_index_entry.update_offset(commit_delta_append_result.second);
                }
            }
            it = in_progress_delta_per_txn.erase(it);
        }
    }
    //finally install those still in-progress deltas per txn
    //they should contain previous version offset as well, but should not
    //update previous version invalidate ts is safe, no real conflict would happen with our locking protocol
    for(auto it = in_progress_delta_per_txn.begin(); it!=in_progress_delta_per_txn.end();it++){
        uint64_t txn_id = it->first;
#if CONSOLIDATION_TEST
        uint64_t status;
        if(txn_tables.get_status(txn_id, status)){
            if(status!=IN_PROGRESS){
                throw ConsolidationException();
            }
        }else{
            throw ConsolidationException();
        }
#endif
        auto& txn_in_progress_deltas = it->second;
        std::vector<uint32_t>local_delta_chains_index_cache(new_block_delta_chain_num);
        for(int32_t j=0; j<new_block_delta_chain_num;j++){
            local_delta_chains_index_cache[j]=new_delta_chains_index[j].get_offset();
        }
        int64_t txn_delta_num = static_cast<int64_t>(txn_in_progress_deltas.size());
        for(int64_t i =txn_delta_num-1; i>=0; i--){
            current_delta = current_block->get_edge_delta(txn_in_progress_deltas.at(i));
#if CONSOLIDATION_TEST
            if(current_delta->creation_ts.load()!=txn_id){
                throw ConsolidationException();
            }
#endif
            const char* data = current_block->get_edge_data(current_delta->data_offset);
            delta_chain_id_t delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
            uint32_t previous_version_offset = 0;
            if(current_delta->delta_type!=EdgeDeltaType::INSERT_DELTA){
                previous_version_offset = new_block->fetch_previous_version_offset(current_delta->toID,local_delta_chains_index_cache[delta_chain_id],current_delta->creation_ts.load(),lazy_update_records);
#if EDGE_DELTA_TEST
                if(!previous_version_offset){
                    throw std::runtime_error("error, under checked version, a delete or update delta must have a previous version");
                }
#endif
            }
            //auto in_progress_delta_append_result = new_block->append_edge_delta(current_delta->toID, txn_id, current_delta->delta_type, data, current_delta->data_length, local_delta_chains_index_cache[delta_chain_id]);
            auto in_progress_delta_append_result = new_block->checked_append_edge_delta(current_delta->toID, txn_id, current_delta->delta_type, data, current_delta->data_length, local_delta_chains_index_cache[delta_chain_id],previous_version_offset);
#if CONSOLIDATION_TEST
            if(in_progress_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!in_progress_delta_append_result.second){
                throw ConsolidationException();
            }
#endif
            local_delta_chains_index_cache[delta_chain_id]= in_progress_delta_append_result.second;
        }
    }
    //now consolidation is over
    per_thread_garbage_queue.register_entry(current_label_entry->block_ptr,current_block->get_order(),largest_invalidation_ts);
    if(per_thread_garbage_queue.need_collection()){
        auto safe_ts = block_access_ts_table.calculate_safe_ts();
        per_thread_garbage_queue.free_block(safe_ts);
    }
    *current_label_entry->delta_chain_index = std::move(new_delta_chains_index);//todo::check its correctness
    current_label_entry->block_ptr = new_block_ptr;
/*    if(new_block->already_overflow()){
        throw ConsolidationException();
    }*/
    current_label_entry->block_version_number.fetch_add(1);//increase version by 1 /*consolidation_time.store(commit_manager.get_current_read_ts());*/
    BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::NORMAL,current_label_entry);
}

void Cleanup_Transaction::force_to_work_on_edge_block(uint64_t block_id) {
#if USING_EAGER_CONSOLIDATION
#else
    std::cout<<"forced cleanup txn executed"<<std::endl;
#endif
    auto [src, label] = decompose_block_id(block_id);
    auto& vertex_index_entry = graph.get_vertex_index_entry(src);
    if(!vertex_index_entry.valid.load()){
        return;//impossible
    }
    BwLabelEntry* target_label_entry;
    auto edge_label_block = block_manager.convert<EdgeLabelBlock>(vertex_index_entry.edge_label_block_ptr);
    auto found = edge_label_block->reader_lookup_label(label,target_label_entry);
    if(!found){
        return;//impossible
    }
    if(BlockStateVersionProtectionScheme::writer_access_block(thread_id,block_id,target_label_entry,block_access_ts_table)){
        auto current_block = block_manager.convert<EdgeDeltaBlockHeader>(target_label_entry->block_ptr);
        uint64_t current_combined_offset = current_block->get_current_offset();
        //will be consolidated right now by someone else
        if(current_block->is_overflow_offset(current_combined_offset)){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return;//already overflow, so someone will clean it up very soon.
        }
        //uint32_t current_head_delta_offset = EdgeDeltaBlockHeader::get_delta_offset_from_combined_offset(current_combined_offset);
        //BaseEdgeDelta* current_head_delta = current_block->get_edge_delta(current_head_delta_offset);
        //now cleanup the block, start by eagerly setting the block to overflow:
        //calculate a large number to insert
        uint32_t data_size = (uint32_t) (current_combined_offset >> 32);
        uint32_t delta_size = (uint32_t) (current_combined_offset & SIZE2MASK);
        uint32_t block_size = current_block->get_size();
        uint32_t padding_size = block_size-data_size-delta_size;
        auto append_result = allocate_delta(current_block,padding_size);
        if(append_result==EdgeDeltaInstallResult::CAUSE_OVERFLOW){
            //I do consolidation
            force_to_consolidation(target_label_entry,current_block,block_id);
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return;
        }else if(append_result==EdgeDeltaInstallResult::ALREADY_OVERFLOW){
            BlockStateVersionProtectionScheme::release_protection(thread_id,block_access_ts_table);
            return;//someone doing consolidation
        }else{
            throw std::runtime_error("error, should overflow");
        }
    }
    return;
}

void Cleanup_Transaction::force_to_consolidation(bwgraph::BwLabelEntry *current_label_entry,
                                                 bwgraph::EdgeDeltaBlockHeader *current_block, uint64_t block_id) {
    BlockStateVersionProtectionScheme::install_exclusive_state(EdgeDeltaBlockState::OVERFLOW,thread_id,block_id,current_label_entry,block_access_ts_table);
    uint32_t original_delta_offset = current_delta_offset-ENTRY_DELTA_SIZE;
    uint32_t original_data_offset = current_data_offset;
    uint64_t to_restore_offset = combine_offset(original_delta_offset, original_data_offset);
    current_block->set_offset(to_restore_offset);//restore the offset to allow concurrent transactions to read the block
#if EDGE_DELTA_TEST
    if(current_block->already_overflow()){
        throw std::runtime_error("error, should restored to an unoverflow offset");
    }
#endif
    BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::CONSOLIDATION,current_label_entry);
    BaseEdgeDelta* current_delta = current_block->get_edge_delta(original_delta_offset);
    if(!current_delta->creation_ts.load()){
        throw std::runtime_error("error, under mutex state all installed deltas should be valid");
    }
   // auto current_head_ts = current_delta->creation_ts.load();
    std::unordered_set<vertex_t> edge_latest_versions_records;
    std::vector<uint32_t> edge_latest_version_offsets;
    timestamp_t largest_invalidation_ts = std::numeric_limits<uint64_t>::min();//most recent previous version
    timestamp_t largest_creation_ts = std::numeric_limits<uint64_t>::min();//most recent (committed) version
    size_t data_size = 0;
    std::unordered_map<uint64_t, std::vector<uint32_t>>in_progress_delta_per_txn;
    int32_t current_delta_chain_num = current_block->get_delta_chain_num();
    std::set<delta_chain_id_t> to_check_delta_chains;
    size_t previous_version_count =0;
    size_t aborted_delta_count =0;
    while(original_delta_offset>0){
        timestamp_t original_ts = current_delta->creation_ts.load();
        //should there be no invalid deltas
        if(!original_ts){
            throw std::runtime_error("all writer transactions should already installed their deltas");
        }
        //do lazy update if possible
        if(is_txn_id(original_ts)){
            uint64_t status = 0;
            if(txn_tables.get_status(original_ts,status)){
                if(status == IN_PROGRESS){
                    //do nothing
                }else{
                    if(status!=ABORT){
                        current_block->update_previous_delta_invalidate_ts(current_delta->toID, current_delta->previous_version_offset, status);
                        if(current_delta->lazy_update(original_ts,status)){
                            //record lazy update
                            record_lazy_update_record(&lazy_update_records, original_ts);
                        }

                    }
                    //if status == abort, must already be eager aborted
#if EDGE_DELTA_TEST
                    if(current_delta->creation_ts.load()!=status){
                        throw LazyUpdateException();
                    }
#endif
                }
            }
        }
        original_ts = current_delta->creation_ts.load();
        //now check the status
        if(is_txn_id(original_ts)){
            auto in_progress_txn_deltas_emplace_result = in_progress_delta_per_txn.try_emplace(original_ts, std::vector<uint32_t>());
            in_progress_txn_deltas_emplace_result.first->second.emplace_back(original_delta_offset);
            data_size+=current_delta->data_length+ENTRY_DELTA_SIZE;
            to_check_delta_chains.emplace(calculate_owner_delta_chain_id(current_delta->toID, current_delta_chain_num));
        }else if(original_ts!=ABORT){
            //skip committed delete deltas
            if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                vertex_t toID = current_delta->toID;
                auto latest_version_emplace_result = edge_latest_versions_records.emplace(toID);
                //if indeed latest version
                if(latest_version_emplace_result.second){
                    edge_latest_version_offsets.emplace_back(original_delta_offset);
                    largest_creation_ts = (largest_creation_ts>=original_ts)? largest_creation_ts:original_ts;
                    data_size+=current_delta->data_length+ENTRY_DELTA_SIZE;
                }else{
                    if(!current_delta->invalidate_ts){
                        throw LazyUpdateException();
                    }
                    largest_invalidation_ts = (largest_invalidation_ts>=current_delta->invalidate_ts.load())? largest_invalidation_ts:current_delta->invalidate_ts.load();
                    previous_version_count++;
                }
            }else{
                //still need to count delete delta as latest delta
                vertex_t toID = current_delta->toID;
                auto latest_version_emplace_result =edge_latest_versions_records.emplace(toID);
                if(!latest_version_emplace_result.second){
                    previous_version_count++;
                }
            }
        }else{
            aborted_delta_count++;
        }
        original_delta_offset-=ENTRY_DELTA_SIZE;
        current_delta++;
    }
    //todo:: maybe change the ratio a bit
    //still a good block, not many previous versions or aborted versions, can directly return now
    if(edge_latest_version_offsets.size()>(aborted_delta_count+previous_version_count)){
        BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::NORMAL,current_label_entry);
        return; //todo:: check why returning here is troublesome
    }
    data_size = (data_size==0)? ENTRY_DELTA_SIZE:data_size;
    //analyze scan finished, now apply heuristics
    //use the block creation time vs. latest committed write to estimate lifespan
    //uint64_t lifespan = largest_creation_ts - current_block->get_consolidation_time(); /*current_label_entry->consolidation_time;*/ //approximate lifespan of the block
    //todo:; apply different heuristics
    /*size_t new_block_size = calculate_nw_block_size_from_lifespan(data_size,lifespan,20);
    auto new_order = size_to_order(new_block_size);*/
    auto new_order = calculate_new_fit_order(data_size+sizeof(EdgeDeltaBlockHeader));
    auto new_block_ptr = block_manager.alloc(new_order);
    auto new_block = block_manager.convert<EdgeDeltaBlockHeader>(new_block_ptr);
    //for debug
    /*  if(largest_invalidation_ts){
          std::cout<<"found"<<std::endl;
      }*/
    new_block->fill_metadata(largest_invalidation_ts, read_timestamp, current_label_entry->block_ptr,new_order, &txn_tables,current_label_entry->delta_chain_index);
    int32_t new_block_delta_chain_num = new_block->get_delta_chain_num();
    std::vector<AtomicDeltaOffset> new_delta_chains_index(new_block_delta_chain_num);
    //start installing latest version
    int64_t signed_edge_last_version_size = static_cast<int64_t>(edge_latest_version_offsets.size());
    for(int64_t i = (signed_edge_last_version_size-1); i>=0; i--){
        current_delta = current_block->get_edge_delta(edge_latest_version_offsets.at(i));
        if(is_txn_id(current_delta->creation_ts.load())){
            throw ConsolidationException();//latest versions should all be committed deltas
        }
        //find which delta chain the latest version delta belongs to
        delta_chain_id_t target_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
        const char* data = current_block->get_edge_data(current_delta->data_offset);
        auto& new_delta_chains_index_entry = new_delta_chains_index.at(target_delta_chain_id);
        uint32_t new_block_delta_offset = new_delta_chains_index_entry.get_offset();//if cannot be locked
        auto consolidation_append_result = new_block->append_edge_delta(current_delta->toID,current_delta->creation_ts.load(),current_delta->delta_type,data,current_delta->data_length,new_block_delta_offset);
        if(consolidation_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!consolidation_append_result.second){
            throw ConsolidationException();
        }
        new_delta_chains_index_entry.update_offset(consolidation_append_result.second);
    }
    //until this step: it is the same as normal Consolidation
    //we installed all committed latest versions and form delta chains
    //now we start the exclusive installation phase
    BlockStateVersionProtectionScheme::install_exclusive_state(EdgeDeltaBlockState::INSTALLATION,thread_id,block_id,current_label_entry,block_access_ts_table);
    //wait for all validating transactions to finish
    while(true){
        for(auto it = to_check_delta_chains.begin(); it!= to_check_delta_chains.end();){
            //get the delta chain head of this delta chain in the original block, check if a validation is happening
            current_delta = current_block->get_edge_delta(current_label_entry->delta_chain_index->at(*it).get_raw_offset());
            uint64_t original_ts = current_delta->creation_ts.load();
            if(is_txn_id(original_ts)){
                uint64_t status =0;
                if(txn_tables.get_status(original_ts,status)){
                    //this transaction still validating, so we will come back later
                    if(status == IN_PROGRESS){
                        it++;
                        continue;
                    }else if(status ==ABORT){ //validating txn aborted, so consolidating thread will help with lazy update, its deltas in the current block is all or nothing (eager aborted)
                        uint32_t to_abort_offset = current_delta->previous_offset;
                        if(!current_delta->lazy_update(original_ts,status)){
                            throw ConsolidationException();//we should never fail lazy update during an exclusive state
                        }
                        auto abort_lazy_update_emplace_result = lazy_update_records.try_emplace(original_ts,1);
                        if(!abort_lazy_update_emplace_result.second){
                            abort_lazy_update_emplace_result.first->second++;
                        }
                        while(to_abort_offset!=0){
                            current_delta = current_block->get_edge_delta(to_abort_offset);
                            if(current_delta->creation_ts!=original_ts){
                                break;
                            }else if(current_delta->creation_ts==original_ts){
                                if(current_delta->lazy_update(original_ts,status)){
                                    abort_lazy_update_emplace_result.first->second++;
                                }else{
                                    throw ConsolidationException();
                                }
                            }
#if CONSOLIDATION_TEST
                            else if(current_delta->creation_ts==ABORT){
                                throw EagerAbortException();//aborted delta should not exit in the delta chain (except my own deltas that I'm aborting )
                            }
#endif
                            to_abort_offset = current_delta->previous_offset;
                        }
                        it = to_check_delta_chains.erase(it);//this delta chain is cleaned
                    }else{//original_ts txn committed after passing validation phase
                        uint32_t to_commit_offset = current_delta->previous_offset;
                        if(!current_delta->lazy_update(original_ts, status)){
                            throw ConsolidationException();
                        }
                        auto commit_lazy_update_emplace_result = lazy_update_records.try_emplace(original_ts,1);
                        if(!commit_lazy_update_emplace_result.second){
                            commit_lazy_update_emplace_result.first->second++;
                        }
                        while(to_commit_offset!=0){
                            current_delta= current_block->get_edge_delta(to_commit_offset);
                            if(current_delta->creation_ts!=original_ts&&current_delta->creation_ts!=status){//someone can already lazy update parts of txn's deltas due to concurrent readers
#if EDGE_DELTA_TEST
                                if(current_delta->creation_ts==ABORT){
                                    throw LazyUpdateException();
                                }
#endif
                                break;
                            }else if(current_delta->creation_ts==original_ts){
                                if(!current_delta->lazy_update(original_ts,status)){
                                    throw ConsolidationException();
                                }
                                commit_lazy_update_emplace_result.first->second++;
                            }
                            to_commit_offset = current_delta->previous_offset;
                        }
                        it = to_check_delta_chains.erase(it);
                    }
                }else{
                    throw ConsolidationException();//Installation phase is supposed to be exclusive.
                }
            }else{
                //this delta chain is not under validating or someone already lazy updated for it
                it = to_check_delta_chains.erase(it);
            }
        }
        if(to_check_delta_chains.empty()){
            break;
        }
    }
    //now check all in progress deltas recorded earlier
    //need 2 for loops because committed deltas should be before in-progress deltas
    for(auto it = in_progress_delta_per_txn.begin();it!=in_progress_delta_per_txn.end();){
        //uint64_t status;
        auto& all_delta_offsets_of_txn = it->second;
        current_delta = current_block->get_edge_delta(all_delta_offsets_of_txn.at(0));
        //skip them, will be installed at the end.
        if(is_txn_id(current_delta->creation_ts.load())){
            it++;
        }else if(current_delta->creation_ts.load()==ABORT){
            it = in_progress_delta_per_txn.erase(it);
        }else{//committed deltas
            timestamp_t commit_ts = current_delta->creation_ts.load();
            int64_t txn_own_deltas_size = static_cast<int64_t>(all_delta_offsets_of_txn.size());
            for(int64_t i = txn_own_deltas_size-1; i>=0; i--){
                uint64_t txn_id = it->first;
                current_delta= current_block->get_edge_delta(all_delta_offsets_of_txn.at(i));
                if(current_delta->lazy_update(txn_id,commit_ts)){
                    //todo:: delete this
                    record_lazy_update_record(&lazy_update_records,txn_id);//todo: will this really happen?
                }
                //only install non-delete deltas
                if(current_delta->delta_type!=EdgeDeltaType::DELETE_DELTA){
                    const char* data = current_block->get_edge_data(current_delta->data_offset);
                    delta_chain_id_t new_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
                    auto& new_delta_chains_index_entry = new_delta_chains_index.at(new_delta_chain_id);
                    uint32_t previous_version_offset=0;
                    if(current_delta->delta_type==EdgeDeltaType::UPDATE_DELTA){
                        //get potential previous version and install invalidation ts
                        previous_version_offset = new_block->fetch_previous_version_offset(current_delta->toID,new_delta_chains_index_entry.get_offset(),current_delta->creation_ts.load(),lazy_update_records);
#if EDGE_DELTA_TEST
                        if(!previous_version_offset){
                            throw std::runtime_error("error, under checked version, an update delta must have a previous version");
                        }
#endif
                    }
                    //can get raw offset because no lock yet
                    // auto commit_delta_append_result = new_block->append_edge_delta(current_delta->toID,commit_ts,current_delta->delta_type,data,current_delta->data_length,new_delta_chains_index_entry.get_offset());
                    auto commit_delta_append_result = new_block->checked_append_edge_delta(current_delta->toID,commit_ts,current_delta->delta_type,data,current_delta->data_length,new_delta_chains_index_entry.get_offset(),previous_version_offset);
                    //can be a newer version:
                    //new_block->update_previous_delta_invalidate_ts(current_delta->toID,new_delta_chains_index_entry.get_offset(),commit_ts);
                    if(commit_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!commit_delta_append_result.second){
                        throw ConsolidationException();
                    }
                    new_delta_chains_index_entry.update_offset(commit_delta_append_result.second);
                }else{
                    delta_chain_id_t new_delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
                    auto& new_delta_chains_index_entry = new_delta_chains_index.at(new_delta_chain_id);
                    //get potential previous version and install invalidation ts
                    //a committed delete delta must have a previous version of the edge, so update the invalidation ts of previous version and get its offset
                    uint32_t previous_version_offset = new_block->fetch_previous_version_offset(current_delta->toID,new_delta_chains_index_entry.get_offset(),current_delta->creation_ts.load(),lazy_update_records);
#if EDGE_DELTA_TEST
                    if(!previous_version_offset){
                        throw std::runtime_error("error, under checked version, a delete delta must have a previous version");
                    }
#endif
                    auto commit_delta_append_result = new_block->checked_append_edge_delta(current_delta->toID,commit_ts,current_delta->delta_type,nullptr,current_delta->data_length,new_delta_chains_index_entry.get_offset(),previous_version_offset);
                    //can be a newer version:
                    //new_block->update_previous_delta_invalidate_ts(current_delta->toID,new_delta_chains_index_entry.get_offset(),commit_ts);
                    if(commit_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!commit_delta_append_result.second){
                        throw ConsolidationException();
                    }
                    new_delta_chains_index_entry.update_offset(commit_delta_append_result.second);
                }
            }
            it = in_progress_delta_per_txn.erase(it);
        }
    }
    //finally install those still in-progress deltas per txn
    //they should contain previous version offset as well, but should not
    //update previous version invalidate ts is safe, no real conflict would happen with our locking protocol
    for(auto it = in_progress_delta_per_txn.begin(); it!=in_progress_delta_per_txn.end();it++){
        uint64_t txn_id = it->first;
#if CONSOLIDATION_TEST
        uint64_t status;
        if(txn_tables.get_status(txn_id, status)){
            if(status!=IN_PROGRESS){
                throw ConsolidationException();
            }
        }else{
            throw ConsolidationException();
        }
#endif
        auto& txn_in_progress_deltas = it->second;
        std::vector<uint32_t>local_delta_chains_index_cache(new_block_delta_chain_num);
        for(int32_t j=0; j<new_block_delta_chain_num;j++){
            local_delta_chains_index_cache[j]=new_delta_chains_index[j].get_offset();
        }
        int64_t txn_delta_num = static_cast<int64_t>(txn_in_progress_deltas.size());
        for(int64_t i =txn_delta_num-1; i>=0; i--){
            current_delta = current_block->get_edge_delta(txn_in_progress_deltas.at(i));
#if CONSOLIDATION_TEST
            if(current_delta->creation_ts.load()!=txn_id){
                throw ConsolidationException();
            }
#endif
            const char* data = current_block->get_edge_data(current_delta->data_offset);
            delta_chain_id_t delta_chain_id = new_block->get_delta_chain_id(current_delta->toID);
            uint32_t previous_version_offset = 0;
            if(current_delta->delta_type!=EdgeDeltaType::INSERT_DELTA){
                previous_version_offset = new_block->fetch_previous_version_offset(current_delta->toID,local_delta_chains_index_cache[delta_chain_id],current_delta->creation_ts.load(),lazy_update_records);
#if EDGE_DELTA_TEST
                if(!previous_version_offset){
                    throw std::runtime_error("error, under checked version, a delete or update delta must have a previous version");
                }
#endif
            }
            auto in_progress_delta_append_result = new_block->append_edge_delta(current_delta->toID, txn_id, current_delta->delta_type, data, current_delta->data_length, local_delta_chains_index_cache[delta_chain_id]);
#if CONSOLIDATION_TEST
            if(in_progress_delta_append_result.first!=EdgeDeltaInstallResult::SUCCESS||!in_progress_delta_append_result.second){
                throw ConsolidationException();
            }
#endif
            local_delta_chains_index_cache[delta_chain_id]= in_progress_delta_append_result.second;
        }
    }
    //now consolidation is over
    per_thread_garbage_queue.register_entry(current_label_entry->block_ptr,current_block->get_order(),largest_invalidation_ts);
    if(per_thread_garbage_queue.need_collection()){
        auto safe_ts = block_access_ts_table.calculate_safe_ts();
        per_thread_garbage_queue.free_block(safe_ts);
    }
    *current_label_entry->delta_chain_index = std::move(new_delta_chains_index);//todo::check its correctness
    current_label_entry->block_ptr = new_block_ptr;
/*    if(new_block->already_overflow()){
        throw ConsolidationException();
    }*/
    //todo::for debug
   /* for(int i=4000; i<4050; i++){
        std::cout<<i<<"th entry is "<<current_label_entry->delta_chain_index->at(i).get_offset()<<" ";
    }*/
    current_label_entry->block_version_number.fetch_add(1);//increase version by 1 /*consolidation_time.store(commit_manager.get_current_read_ts());*/
    BlockStateVersionProtectionScheme::install_shared_state(EdgeDeltaBlockState::NORMAL,current_label_entry);
}

