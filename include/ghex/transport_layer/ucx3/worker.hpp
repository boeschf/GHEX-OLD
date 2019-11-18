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
#include "./error.hpp"
#include "./endpoint.hpp"
#include "./request_state.hpp"

namespace gridtools {
    namespace ghex {
        namespace tl {
            namespace ucx {

                struct context_t;

                struct worker_t
                {
                    using rank_type = typename endpoint_t::rank_type;
                    using tag_type  = int;

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

                        void destroy() noexcept
                        {
                            if (!m_moved)
                                ucp_worker_destroy(m_worker);
                        }

                        operator bool() const noexcept { return m_moved; }
                        operator ucp_worker_h() const noexcept { return m_worker; }
                              ucp_worker_h& get()       noexcept { return m_worker; }
                        const ucp_worker_h& get() const noexcept { return m_worker; }
                    };

                    using cache_type      = std::map<rank_type, endpoint_t>;


                    struct ref_message
                    {
                        unsigned char* m_data;
                        std::size_t    m_size;
                        using value_type = unsigned char;
                        unsigned char* data() noexcept {return m_data;}
                        std::size_t size() const noexcept { return m_size; }
                    };
                   
                    struct queue_item
                    {
                        ref_message        m_message;
                        ucp_ep_h           m_ep;
                        std::uint_fast64_t m_tag;
                        //shared_request     m_shared_request;
                        std::shared_ptr<shared_request_state>  m_shared_request;
                    };

                    using send_queue_type    = std::deque<queue_item>;
                    using send_inflight_type = std::vector<void*>;

                    context_t*         m_context;
                    std::size_t        m_index;
                    rank_type          m_rank;
                    rank_type          m_size;
                    ucp_worker_handle  m_worker;
                    address_t          m_address;
                    cache_type         m_endpoint_cache;
                    /* Trial with queued sends
                    send_queue_type    m_send_queue;
                    send_inflight_type m_send_inflights;*/

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

                    void progress(worker_t* other_worker);

                    /* Trial with queued sends
                    // not thread-safe
                    void progress_send_queue()
                    {
                        // check send_inflights
                        for (std::size_t i=0; i<m_send_inflights.size(); ++i)
                        {
                            if (ucp_request_check_status(m_send_inflights[i]) != UCS_INPROGRESS)
                            {
                                //if (i+1u < m_send_inflights().size())
                                //{
                                    m_send_inflights[i--] = m_send_inflights.back();
                                //}
                                m_send_inflights.pop_back();
                            }
                        }

                        const std::size_t m_max_num_inflights = 5;
                        if (m_send_inflights.size() < m_max_num_inflights)
                        {
                            // send queued messages
                            const std::size_t to_be_sent = m_max_num_inflights - m_send_inflights.size();
                            for (std::size_t i=0; i<to_be_sent; ++i)
                            {
                                if (m_send_queue.size() > 0u)
                                {
                                    auto& item = m_send_queue.front();
                                    auto ret = send_low_level(item.m_message, item.m_ep, item.m_tag);
                                    if (reinterpret_cast<std::uintptr_t>(ret) == UCS_OK)
                                    {
                                        // send operation is completed immediately and the call-back function is not invoked
                                        item.m_shared_request.get()->m_ptr = nullptr;
                                        item.m_shared_request.get()->m_enqueued = false;
                                    }
                                    else if(!UCS_PTR_IS_ERR(ret))
                                    {
                                        item.m_shared_request.get()->m_ptr = (void*)ret;
                                        item.m_shared_request.get()->m_enqueued = false;
                                    }
                                    else
                                    {
                                        // an error occurred
                                        throw std::runtime_error("ghex: ucx error - send operation failed");
                                    }
                                    m_send_queue.pop_front();
                                }
                                else
                                    break;
                            }
                        }
                    }

                    // may be thread-safe
                    void progress()
                    {
                        ucp_worker_progress(get());
                    }

                    // thread-safe
                    bool test(shared_request_state* state) const
                    {
                        if (state->m_enqueued)
                            return false;
                        else if (!state->m_ptr)
                            return true;
                        else
                            return (ucp_request_check_status(state->m_ptr) != UCS_INPROGRESS);
                    }

                    // not thread-safe
                    void wait(shared_request_state* state)
                    {
                        while (state->m_enqueued)
                        {
                            progress();
                            progress_send_queue();
                        }
                        if (!state->m_ptr) return;
                        while(!test(state))
                        {
                            progress();
                        }
                    }

                    // not thread-safe
                    template<typename Message>
                    std::shared_ptr<shared_request_state> send(Message& msg, rank_type dst, tag_type tag)
                    {
                        const auto& ep = connect(dst);
                        const auto stag = ((std::uint_fast64_t)tag << 32) | 
                                           (std::uint_fast64_t)(m_rank);

                        ref_message r_msg{reinterpret_cast<unsigned char*>(msg.data()), msg.size()*sizeof(typename Message::value_type)};

                        auto s_req = std::make_shared<shared_request_state>(shared_request_state{this, nullptr, true});
                        m_send_queue.push_back(queue_item{ r_msg, ep, stag, s_req});
                        progress_send_queue();
                        return s_req;
                    }
                
                    static void empty_send_callback(void *, ucs_status_t) {}
                    static void empty_recv_callback(void *, ucs_status_t, ucp_tag_recv_info_t*) {}

                    // may be thread-safe
                    template<typename Message>
                    std::shared_ptr<shared_request_state> recv(Message& msg, rank_type src, tag_type tag)
                    {
                        const auto rtag = ((std::uint_fast64_t)tag << 32) | 
                                           (std::uint_fast64_t)(src);
                        ucs_status_ptr_t ret = ucp_tag_recv_nb(
                            get(),                                           // worker
                            msg.data(),                                      // buffer
                            msg.size()*sizeof(typename Message::value_type), // buffer size
                            ucp_dt_make_contig(1),                           // data type
                            rtag,                                            // tag
                            ~std::uint_fast64_t(0ul),                        // tag mask
                            &worker_t::empty_recv_callback);                 // callback function pointer: empty here
                        if(!UCS_PTR_IS_ERR(ret))
                        {
                            return std::make_shared<shared_request_state>(shared_request_state{this, (void*)ret, false});
                        }
                        else
                        {
                            // an error occurred
                            throw std::runtime_error("ghex: ucx error - recv operation failed");
                        }
                    }
                
                    template<typename Message>
                    ucs_status_ptr_t send_low_level(Message& msg, ucp_ep_h ep, std::uint_fast64_t tag)
                    {
                        return ucp_tag_send_nb(
                            ep,                                              // destination
                            msg.data(),                                      // buffer
                            msg.size()*sizeof(typename Message::value_type), // buffer size
                            ucp_dt_make_contig(1),                           // data type
                            tag,                                             // tag
                            &worker_t::empty_send_callback);                 // callback function pointer: empty here
                    }*/
                };


                /* Trial with queued sends
                void request2::wait()
                {
                    if (m_ptr)
                        m_ptr->m_worker->wait(m_ptr.get());
                }
                
                bool request2::test()
                {
                    if (m_ptr)
                        return m_ptr->m_worker->test(m_ptr.get());
                    else
                        return true;
                }

                bool request2::ready()
                {
                    if (m_ptr)
                    {
                        if (m_ptr->m_enqueued)
                        {
                            m_ptr->m_worker->progress();
                            m_ptr->m_worker->progress_send_queue();
                        }
                        else
                        {
                            m_ptr->m_worker->progress();
                        }
                        return m_ptr->m_worker->test(m_ptr.get());
                    }
                    else
                        return true;
                }*/

            
            } // namespace ucx
        } // namespace tl
    } // namespace ghex
} // namespace gridtools

#endif /* INCLUDED_GHEX_TL_UCX_WORKER_HPP */

