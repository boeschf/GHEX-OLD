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
#ifndef INCLUDED_GHEX_TL_CONTINUATION_COMMUNICATOR_HPP
#define INCLUDED_GHEX_TL_CONTINUATION_COMMUNICATOR_HPP

#include <boost/lockfree/queue.hpp>
#include "./callback_communicator.hpp"

namespace gridtools{
    namespace ghex {
        namespace tl {

            // implementation details here:
            namespace cont_detail {

                // shared request state
                struct request_state
                {
                    // volatile is needed to prevent the compiler
                    // from optimizing away the check of this member
                    volatile bool m_ready = false;
                    bool is_ready() const noexcept { return m_ready; }
                };

                // simple request class which is returned from send and recv calls
                struct request
                {
                    std::shared_ptr<request_state> m_request_state;
                    bool is_ready() const noexcept { return m_request_state ? m_request_state->is_ready() : true; }
                };

                // type-erased message
                struct any_message
                {
                    using value_type = unsigned char;

                    struct iface
                    {
                        virtual unsigned char* data() noexcept = 0;
                        virtual const unsigned char* data() const noexcept = 0;
                        virtual std::size_t size() const noexcept = 0;
                        virtual ~iface() {}
                    };

                    template<class Message>
                    struct holder final : public iface
                    {
                        using value_type = typename Message::value_type;
                        Message m_message;
                        holder(Message&& m): m_message{std::move(m)} {}

                        unsigned char* data() noexcept override { return reinterpret_cast<unsigned char*>(m_message.data()); }
                        const unsigned char* data() const noexcept override { return reinterpret_cast<const unsigned char*>(m_message.data()); }
                        std::size_t size() const noexcept override { return sizeof(value_type)*m_message.size(); }
                    };

                    std::unique_ptr<iface> m_ptr;

                    template<class Message>
                    any_message(Message&& m) : m_ptr{std::make_unique<holder<Message>>(std::move(m))} {}
                    any_message(any_message&&) = default;

                    unsigned char* data() noexcept { return m_ptr->data(); }
                    const unsigned char* data() const noexcept { return m_ptr->data(); }
                    std::size_t size() const noexcept { return m_ptr->size(); }
                };

                // simple wrapper around an l-value reference message (stores pointer and size)
                template<typename T>
                struct ref_message
                {
                    using value_type = T;//unsigned char;
                    T* m_data;
                    std::size_t m_size;
                    T* data() noexcept { return m_data; }
                    const T* data() const noexcept { return m_data; }
                    std::size_t size() const noexcept { return m_size; }
                };

                // simple shared message which is internally used for send_multi
                template<typename Message>
                struct shared_message
                {
                    using value_type = typename Message::value_type;
                    std::shared_ptr<Message> m_message;

                    shared_message(Message&& m) : m_message{std::make_shared<Message>(std::move(m))} {}
                    shared_message(const shared_message&) = default;
                    shared_message(shared_message&&) = default;

                    value_type* data() noexcept { return m_message->data(); }
                    const value_type* data() const noexcept { return m_message->data(); }
                    std::size_t size() const noexcept { return m_message->size(); }
                };
                
                // type-erased future
                struct any_future
                {
                    struct iface
                    {
                        virtual bool ready() = 0;
                        virtual ~iface() {}
                    };

                    template<class Future>
                    struct holder final : public iface
                    {
                        Future m_future;
                        holder() = default;
                        holder(Future&& fut): m_future{std::move(fut)} {}
                        bool ready() override { return m_future.ready(); }
                    };

                    std::unique_ptr<iface> m_ptr;

                    template<class Future>
                    any_future(Future&& fut) : m_ptr{std::make_unique<holder<Future>>(std::move(fut))} {}

                    bool ready() { return m_ptr->ready(); }
                };

            } // namespace cont_detail


            
            // thread-safe shared communicator which handles callbacks
            // note: no templates, everything is type-erased
            // relies on future-based basic communicator which is passed for every send/recv
            class continuation_communicator
            {
            public: // member types
                
                using tag_type          = int;
                using rank_type         = int;
                // this is the message type returned in the callback:
                using message_type      = cont_detail::any_message; 
                // returned from send/recv to check for completion
                using request           = cont_detail::request;

            private: // member types

                // wrapper for messages passed by l-value reference
                template<typename T>
                using ref_message       = cont_detail::ref_message<T>;

                // necessary meta information for each send/receive operation
                struct element_type
                {
                    using message_arg_type = message_type;
                    std::function<void(message_arg_type, rank_type, tag_type)> m_cb;
                    rank_type                                                  m_rank;
                    tag_type                                                   m_tag;
                    cont_detail::any_future                                    m_future;
                    message_type                                               m_msg;
                    std::shared_ptr<cont_detail::request_state>                m_request_state;
                };
                // we need thread-safe queues
                using lock_free_alloc_t   = boost::lockfree::allocator<std::allocator<unsigned char>>;
                using send_container_type = boost::lockfree::queue<element_type*, lock_free_alloc_t, boost::lockfree::fixed_sized<false>>;
                using recv_container_type = boost::lockfree::queue<element_type*, lock_free_alloc_t, boost::lockfree::fixed_sized<false>>;

            private: // members

                send_container_type m_sends;
                recv_container_type m_recvs;

            public: // ctors

                continuation_communicator() : m_sends(128), m_recvs(128) {}
                continuation_communicator(const continuation_communicator&) = delete;
                continuation_communicator(continuation_communicator&&) = default;
                ~continuation_communicator() { /* TODO: consume all*/ }
                
            public: // send

                // use basic comm to post the send and place the callback in a queue
                // returns a request to check for completion
                // takes ownership of message if it is an r-value reference!
                template<typename Comm, typename Message, typename CallBack>
                request send(Comm& comm, Message&& msg, rank_type dst, tag_type tag, CallBack&& cb)
                {
                    GHEX_CHECK_CALLBACK
                    using is_rvalue = std::is_rvalue_reference<decltype(std::forward<Message>(msg))>;
                    return send(comm, std::forward<Message>(msg), dst, tag, std::forward<CallBack>(cb), is_rvalue());
                }
                
                // no-callback version
                template<typename Comm, typename Message>
                request send(Comm& comm, Message&& msg, rank_type dst, tag_type tag)
                {
                    return send(comm, std::forward<Message>(msg), dst, tag, [](message_type,rank_type,tag_type){});
                }

            public: // send multi

                // use basic comm to post the sends and place the callback in a queue
                // returns a vector of request to check for completion
                // takes ownership of message if it is an r-value reference!
                // internally transforms the callback (and the message if moved in) into shared objects
                template <typename Comm, typename Message, typename Neighs, typename CallBack>
                std::vector<request> send_multi(Comm& comm, Message&& msg, const Neighs& neighs, tag_type tag, CallBack&& cb)
                {
                    GHEX_CHECK_CALLBACK
                    using is_rvalue = std::is_rvalue_reference<decltype(std::forward<Message>(msg))>;
                    return send_multi(comm, std::forward<Message>(msg), neighs, tag, std::forward<CallBack>(cb), is_rvalue());
                }

                // no-callback version
                template <typename Comm, typename Message, typename Neighs>
                std::vector<request> send_multi(Comm& comm, Message&& msg, const Neighs& neighs, tag_type tag)
                {
                    return send_multi(comm, std::forward<Message>(msg), neighs, tag, [](message_type,rank_type,tag_type){});
                }

            public: // receive

                // use basic comm to post the recv and place the callback in a queue
                // returns a request to check for completion
                // takes ownership of message if it is an r-value reference!
                template<typename Comm, typename Message, typename CallBack>
                request recv(Comm& comm, Message&& msg, rank_type src, tag_type tag, CallBack&& cb)
                {
                    GHEX_CHECK_CALLBACK
                    using is_rvalue = std::is_rvalue_reference<decltype(std::forward<Message>(msg))>;
                    return recv(comm, std::forward<Message>(msg), src, tag, std::forward<CallBack>(cb), is_rvalue());
                }

                // no-callback version
                template<typename Comm, typename Message>
                request recv(Comm& comm, Message&& msg, rank_type src, tag_type tag)
                {
                    return recv(comm, std::forward<Message>(msg), src, tag, [](message_type,rank_type,tag_type){});
                }

            public: // progress

                // progress the ques and return the number of progressed callbacks
                std::size_t progress()
                {
                    std::size_t num_completed = 0u;
                    num_completed += run(m_sends);
                    num_completed += run(m_recvs);
                    return num_completed;
                }


            private: // implementation

                template<typename Comm, typename Message, typename CallBack>
                request send(Comm& comm, Message& msg, rank_type dst, tag_type tag, CallBack&& cb, std::false_type)
                {
                    using V = typename Message::value_type;
                    request req{std::make_shared<cont_detail::request_state>()};
                    auto fut = comm.send(msg,dst,tag);
                    auto element_ptr = new element_type{std::forward<CallBack>(cb), dst, tag, std::move(fut), 
                                                        ref_message<V>{msg.data(),msg.size()}, req.m_request_state};
                    while (!m_sends.push(element_ptr)) {}
                    return req;
                }
                
                template<typename Comm, typename Message, typename CallBack>
                request send(Comm& comm, Message&& msg, rank_type dst, tag_type tag, CallBack&& cb, std::true_type)
                {
                    request req{std::make_shared<cont_detail::request_state>()};
                    auto fut = comm.send(msg,dst,tag);
                    auto element_ptr = new element_type{std::forward<CallBack>(cb), dst, tag, std::move(fut), 
                                                        std::move(msg), req.m_request_state};
                    while (!m_sends.push(element_ptr)) {}
                    return req;
                }

                template<typename Comm, typename Message, typename CallBack>
                request recv(Comm& comm, Message& msg, rank_type src, tag_type tag, CallBack&& cb, std::false_type)
                {
                    using V = typename Message::value_type;
                    request req{std::make_shared<cont_detail::request_state>()};
                    auto fut = comm.recv(msg,src,tag);
                    auto element_ptr = new element_type{std::forward<CallBack>(cb), src, tag, std::move(fut), 
                                                        ref_message<V>{msg.data(),msg.size()}, req.m_request_state};
                    while (!m_recvs.push(element_ptr)) {}
                    return req;
                }

                template<typename Comm, typename Message, typename CallBack>
                request recv(Comm& comm, Message&& msg, rank_type src, tag_type tag, CallBack&& cb, std::true_type)
                {
                    request req{std::make_shared<cont_detail::request_state>()};
                    auto fut = comm.recv(msg,src,tag);
                    auto element_ptr = new element_type{std::forward<CallBack>(cb), src, tag, std::move(fut), 
                                                        std::move(msg), req.m_request_state};
                    while (!m_recvs.push(element_ptr)) {}
                    return req;
                }

                template <typename Comm, typename Message, typename Neighs, typename CallBack>
                std::vector<request> send_multi(Comm& comm, Message& msg, const Neighs& neighs, tag_type tag, CallBack&& cb, std::false_type)
                {
                    using cb_type = typename std::remove_cv<typename std::remove_reference<CallBack>::type>::type;
                    auto cb_ptr = std::make_shared<cb_type>( std::forward<CallBack>(cb) );
                    std::vector<request> reqs;
                    for (auto id : neighs)
                        reqs.push_back( send(comm, msg, id, tag,
                                [cb_ptr](message_type m, rank_type r, tag_type t)
                                {
                                    // if (cb_ptr->use_count == 1)
                                    (*cb_ptr)(std::move(m),r,t); 
                                }) );
                    return reqs;
                }

                template <typename Comm, typename Message, typename Neighs, typename CallBack>
                std::vector<request> send_multi(Comm& comm, Message&& msg, const Neighs& neighs, tag_type tag, CallBack&& cb, std::true_type)
                {
                    using cb_type = typename std::remove_cv<typename std::remove_reference<CallBack>::type>::type;
                    auto cb_ptr = std::make_shared<cb_type>( std::forward<CallBack>(cb) );
                    cont_detail::shared_message<Message> s_msg{std::move(msg)};
                    std::vector<request> reqs;
                    for (auto id : neighs)
                    {
                        auto s_msg_cpy = s_msg;
                        reqs.push_back( send(comm, std::move(s_msg_cpy), id, tag,
                                [cb_ptr](message_type m, rank_type r, tag_type t)
                                {
                                    // if (cb_ptr->use_count == 1)
                                    (*cb_ptr)(std::move(m),r,t); 
                                }) );
                    }
                    return reqs;
                }

                template<typename Queue>
                std::size_t run(Queue& d)
                {
                    element_type* ptr = nullptr;
                    if (d.pop(ptr))
                    {
                        if (ptr->m_future.ready())
                        {
                            // call the callback
                            ptr->m_cb(std::move(ptr->m_msg), ptr->m_rank, ptr->m_tag);
                            // make request ready
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

#endif/*INCLUDED_GHEX_TL_CONTINUATION_COMMUNICATOR_HPP */

