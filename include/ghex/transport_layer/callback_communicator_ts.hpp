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
#ifndef INCLUDED_GHEX_TL_CALLBACK_COMMUNICATOR_TS_HPP
#define INCLUDED_GHEX_TL_CALLBACK_COMMUNICATOR_TS_HPP

#include <boost/lockfree/queue.hpp>
#include "./callback_communicator.hpp"

namespace gridtools
{
    namespace ghex
    {
        namespace tl {

            template<class Allocator = std::allocator<unsigned char>>
            class callback_communicator_ts
            {
            public: // member types
                
                using tag_type          = int;
                using rank_type         = int;
                using allocator_type    = Allocator;
                using message_type      = shared_message_buffer<allocator_type>;


                struct request_state
                {
                    volatile bool m_ready = false;
                    bool is_ready() const noexcept
                    {
                        return m_ready;
                    }
                };

                struct request
                {
                    std::shared_ptr<request_state> m_request_state;
                    bool is_ready() const noexcept 
                    {
                        return m_request_state->is_ready(); 
                    }
                };

            private: // member types

                struct any_future
                {
                    struct iface
                    {
                        virtual bool ready() = 0;
                        virtual ~iface() {}
                    };
                    template<class Future>
                    struct holder : public iface
                    {
                        Future m_future;
                        holder() = default;
                        holder(Future&& fut): m_future{std::move(fut)} {}
                        bool ready() override { return m_future.ready(); }
                    };

                    std::unique_ptr<iface> m_ptr;

                    template<class Future>
                    any_future(Future&& fut)
                    : m_ptr{std::make_unique<holder<Future>>(std::move(fut))}
                    {}

                    bool ready() { return m_ptr->ready(); }
                };

                // necessary meta information for each send/receive operation
                struct element_type
                {
                    using message_arg_type = message_type;
                    std::function<void(message_arg_type, rank_type, tag_type)> m_cb;
                    rank_type    m_rank;
                    tag_type     m_tag;
                    any_future   m_future;
                    message_type m_msg;
                    std::shared_ptr<request_state> m_request_state;
                };
                using send_element_type   = element_type;
                using recv_element_type   = element_type;
                using lock_free_alloc_t   = boost::lockfree::allocator<std::allocator<unsigned char>>;
                using send_container_type = boost::lockfree::queue<send_element_type*, lock_free_alloc_t, boost::lockfree::fixed_sized<false>>;
                using recv_container_type = boost::lockfree::queue<recv_element_type*, lock_free_alloc_t, boost::lockfree::fixed_sized<false>>;


            private: // members

                allocator_type      m_alloc;
                send_container_type m_sends;
                recv_container_type m_recvs;

            public: // ctors

                /** @brief construct from a basic transport communicator
                  * @param comm  the underlying transport communicator
                  * @param alloc the allocator instance to be used for constructing messages */
                callback_communicator_ts(allocator_type alloc = allocator_type{}) 
                : m_alloc(alloc), m_sends(128), m_recvs(128) {}

                callback_communicator_ts(const callback_communicator_ts&) = delete;
                callback_communicator_ts(callback_communicator_ts&&) = default;

                /** terminates the program if the queues are not empty */
                ~callback_communicator_ts() 
                { 
                    // consume all
                }

            public: // send

                template<typename Comm, typename CallBack>
                request send(Comm& comm, message_type msg, rank_type dst, tag_type tag, CallBack&& cb)
                {
                    GHEX_CHECK_CALLBACK
                    request req{std::make_shared<request_state>()};
                    auto fut = comm.send(msg,dst,tag);
                    auto element_ptr = new send_element_type{std::forward<CallBack>(cb), dst, tag, std::move(fut), std::move(msg), req.m_request_state};
                    while (!m_sends.push(element_ptr)) {}
                    return req;
                }

            public: // receive

                template<typename Comm, typename CallBack>
                request recv(Comm& comm, message_type msg, rank_type src, tag_type tag, CallBack&& cb)
                {
                    GHEX_CHECK_CALLBACK
                    request req{std::make_shared<request_state>()};
                    auto fut = comm.recv(msg,src,tag);
                    auto element_ptr = new recv_element_type{std::forward<CallBack>(cb), src, tag, std::move(fut), std::move(msg), req.m_request_state};
                    while (!m_recvs.push(element_ptr)) {}
                    return req;
                }

            public: // progress

                std::size_t progress()
                {
                    std::size_t num_completed = 0u;
                    num_completed += run(m_sends);
                    num_completed += run(m_recvs);
                    return num_completed;
                }

            private: // implementation

                template<typename Queue>
                std::size_t run(Queue& d)
                {
                    send_element_type* ptr = nullptr;
                    if (d.pop(ptr))
                    {
                        if (ptr->m_future.ready())
                        {
                            // call the callback
                            ptr->m_cb(std::move(ptr->m_msg), ptr->m_rank, ptr->m_tag);
                            ptr->m_request_state->m_ready = true;
                            delete ptr;
                            return 1u;
                        }
                        else
                        {
                            while( !d.push(ptr) ) {}
                            return 0u;
                        }
                    }
                    else return 0u;
                }
            };

        } // namespace tl
    } // namespace ghex
}// namespace gridtools

#endif/*INCLUDED_GHEX_TL_CALLBACK_COMMUNICATOR_TS_HPP */

