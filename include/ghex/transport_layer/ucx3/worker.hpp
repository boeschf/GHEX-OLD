/* 
 * GridTools
 * 
 * Copyright (c) 2014-2019, ETH Zurich
 * All rights reserved.
 * 
 * Please, refer to the LICENSE file in the root directory.
 * SPDX-License-Identifier: BSD-3-Clause
 * 
 */
#ifndef INCLUDED_GHEX_TL_UCX_WORKER_HPP
#define INCLUDED_GHEX_TL_UCX_WORKER_HPP

#include <map>
#include <deque>
#include <mutex>
#include "./error.hpp"
#include "./endpoint.hpp"
#include <atomic>
#include <sched.h>

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct atomic_mutex
                {
                    std::atomic<bool> m_flag;

                    atomic_mutex() noexcept : m_flag(false) {}

                    inline bool try_lock() noexcept
                    {
                        bool expected = false;
                        return m_flag.compare_exchange_strong(expected,true, std::memory_order_relaxed);
                    }

                    inline bool try_unlock() noexcept
                    {
                        bool expected = true;
                        return m_flag.compare_exchange_strong(expected,false, std::memory_order_relaxed);
                    }

                    inline void lock() noexcept
                    {
                        while (!try_lock()) 
                        {
                        //sched_yield();
                        }
                    }

                    inline void unlock() noexcept
                    {
                        while (!try_unlock()) 
                        {
                        //sched_yield();
                        }
                        //sched_yield();
                    }
                };

                struct context_t;

                struct worker_t
                {
                    using rank_type = typename endpoint_t::rank_type;
                    using tag_type  = int;
                    //using mutex_type = std::mutex;
                    using mutex_type = atomic_mutex;
                    //using lock_type  = std::lock_guard<mutex_type>;

                    struct ucp_worker_handle
                    {
                        ucp_worker_h m_worker;
                        bool         m_moved = false;

                        ucp_worker_handle() noexcept : m_moved{true} {}
                        ucp_worker_handle(const ucp_worker_handle&) = delete;
                        ucp_worker_handle& operator=(const ucp_worker_handle&) = delete;
                        
                        ucp_worker_handle(ucp_worker_handle&& other) noexcept
                        : m_worker(other.m_worker)
                        , m_moved(other.m_moved)
                        {
                            other.m_moved = true;
                        }

                        ucp_worker_handle& operator=(ucp_worker_handle&& other) noexcept
                        {
                            destroy();
                            m_worker.~ucp_worker_h();
                            ::new((void*)(&m_worker)) ucp_worker_h{other.m_worker};
                            m_moved = other.m_moved;
                            other.m_moved = true;
                            return *this;
                        }

                        ~ucp_worker_handle() { destroy(); }

                        static void empty_send_cb(void*, ucs_status_t) {}

                        void destroy() noexcept
                        {
                            if (!m_moved)
                            {
                                //ucs_status_ptr_t request = ucp_worker_flush_nb(m_worker, 0, &ucp_worker_handle::empty_send_cb);
                                //if (reinterpret_cast<std::uintptr_t>(request) == UCS_OK)
                                //{
                                //    /* done */
                                //}
                                //else if (UCS_PTR_IS_ERR(request))
                                //{
                                //    std::terminate();
                                //}
                                //else
                                //{
                                //    ucs_status_t status;
                                //    do
                                //    {
                                //        ucp_worker_progress(m_worker);
                                //        status = ucp_request_check_status(request);
                                //    }
                                //    while( status == UCS_INPROGRESS);
                                //    ucp_request_release(request);
                                //}
                                ucp_worker_destroy(m_worker);
                            }
                        }

                        operator bool() const noexcept { return m_moved; }
                        operator ucp_worker_h() const noexcept { return m_worker; }
                              ucp_worker_h& get()       noexcept { return m_worker; }
                        const ucp_worker_h& get() const noexcept { return m_worker; }
                    };

                    using cache_type      = std::map<rank_type, endpoint_t>;

                    context_t*         m_context;
                    std::size_t        m_index;
                    bool               m_shared;
                    rank_type          m_rank;
                    rank_type          m_size;
                    ucp_worker_handle  m_worker;
                    address_t          m_address;
                    cache_type         m_endpoint_cache;
                    std::unique_ptr<mutex_type> m_mutex;

                    worker_t() = default;
                    worker_t(context_t* context, std::size_t index, bool shared = true);
                    worker_t(const worker_t&) = delete;
                    worker_t(worker_t&& other) noexcept = default;
                    worker_t& operator=(const worker_t&) = delete;
                    worker_t& operator=(worker_t&&) noexcept = default;

                    rank_type rank() const noexcept { return m_rank; }
                    rank_type size() const noexcept { return m_size; }
                    std::size_t index() const noexcept { return m_index; }
                    ucp_worker_h get() const noexcept { return m_worker.get(); }
                    address_t address() const noexcept { return m_address; }
                    const endpoint_t& connect(rank_type rank);


                    inline void lock() noexcept { m_mutex->lock(); }
                    inline void unlock() noexcept { m_mutex->unlock(); }
                    inline bool try_lock() noexcept { return m_mutex->try_lock(); }
                    //inline void lock() noexcept {  }
                    //inline void unlock() noexcept {  }
                    //inline bool try_lock() noexcept { return true; }
                };
            
            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_WORKER_HPP */

