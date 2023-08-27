//
// Created by zhou822 on 5/23/23.
//
#pragma once
//#ifndef BWGRAPH_V2_GRAPH_GLOBAL_HPP
#define BWGRAPH_V2_GRAPH_GLOBAL_HPP
//system-wise worker thread count
/*#ifndef WORKER_THREAD_NUM
#define WORKER_THREAD_NUM 32
#endif*/

constexpr uint32_t worker_thread_num = 64;
//transaction states
#define ABORT 0x7FFFFFFFFFFFFFFF
#define IN_PROGRESS 0
#define MAX_TS 0x6FFFFFFFFFFFFFFF
//transaction table states
#define USING_ARRAY_TABLE true
//for array txn table only
#define NO_TXN_ENTRY 0
//#define Per_Thread_Table_Size 32
constexpr uint32_t per_thread_table_size = 32;
constexpr uint32_t clean_threshold = per_thread_table_size/4;
constexpr uint64_t garbage_collection_transaction_threshold = 4096;
constexpr uint64_t garbage_collection_entry_num_threshold = 10;
constexpr uint64_t garbage_collection_size_threshold = 1ul<<20;
//block allocation
#define DEFAULT_EDGE_DELTA_BLOCK_ORDER 9
#define BUCKET_SIZE 33554432//536870912/16 //for test use smaller bucket size
#define BUCKET_NUM 8
#define BAD_BLOCK_ID 0
#define USING_WAL false
#define USING_PESSIMISTIC_MODE true
constexpr uint64_t placeholder_txn_id = 0x80FFFFFFFFFFFFFF;//all commit ts is greater than 0, and initial
constexpr uint64_t tombstone_vid = 0xFFFFFFFFFFFFFFFF;
constexpr uint64_t shared_txn_op_threshold = 64;
constexpr uint64_t eager_blocks_clean_threshold = 4096; //after how many new version creation, we need to do eager clean
constexpr uint64_t cold_spot_threshold = 256;
constexpr uint8_t index_lookup_order_threshold = 12;
constexpr uint64_t edge_block_hotspot_threshold = 1ul<<20;
//constexpr uint64_t max_writer_num = 40;
#define COMMIT_TEST false
#define TRACK_EXECUTION_TIME false
#define CHECKED_PUT_EDGE true
#define USING_RANGE_CLEAN false
#define USING_EAGER_COMMIT true
#define PRINTING_FINAL_GARBAGE_STATUS false
#define USING_EAGER_CONSOLIDATION false
#define USING_PREFETCH true
#define USING_COMMIT_WAIT_WORK true
#define USING_SEPARATED_TABLES true
#define USING_QUEUE_PREVIOUS_VERSION true
#define TRACK_COMMIT_ABORT false
#define TRACK_GARBAGE_RECORD_TIME false

//#endif //BWGRAPH_V2_GRAPH_GLOBAL_HPP
