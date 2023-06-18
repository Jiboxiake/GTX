//
// Created by zhou822 on 5/28/23.
//
//reference:: https://rigtorp.se/spinlock/
//#ifndef BWGRAPH_V2_SPIN_LATCH_HPP
//#define BWGRAPH_V2_SPIN_LATCH_HPP
#pragma once
#include <atomic>

namespace bwgraph{
    struct spinlock {
        std::atomic<bool> lock_ = {0};

        void lock() noexcept {
            for (;;) {
                // Optimistically assume the lock is free on the first try
                if (!lock_.exchange(true, std::memory_order_acquire)) {
                    return;
                }
                // Wait for lock to be released without generating cache misses
                while (lock_.load(std::memory_order_relaxed)) {
                    // Issue X86 PAUSE or ARM YIELD instruction to reduce contention between
                    // hyper-threads
                    __builtin_ia32_pause();
                }
            }
        }

        bool try_lock() noexcept {
            // First do a relaxed load to check if lock is free in order to prevent
            // unnecessary cache misses if someone does while(!try_lock())
            return !lock_.load(std::memory_order_relaxed) &&
                   !lock_.exchange(true, std::memory_order_acquire);
        }

        void unlock() noexcept {
            lock_.store(false, std::memory_order_release);
        }
    };
}
//#endif //BWGRAPH_V2_SPIN_LATCH_HPP
